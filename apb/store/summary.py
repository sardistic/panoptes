"""Read-only, time-windowed brief over data the map already collects."""
from __future__ import annotations

import json
import time
from collections import Counter

from apb.common.models import IncidentType
from apb.store import events, metrics, radio, sigbuf, snapshots, state

PERIOD_SECONDS = {"minute": 60, "day": 86400, "week": 7 * 86400}
_WEATHER = {"hz_nws", "nws", "spc", "nhc", "sigmet", "ndbc"}
_ECOLOGY = {"airnow", "openaq", "firms", "nifc_fire", "hms_smoke", "usgs_flood", "safecast"}
_OTHER = {"adsb", "faa_tfr", "fema", "odin", "volcano", "faa_delay",
          "acled", "emsc", "gdacs", "radar", "amtrak", "squawk", "hz_usgs"}
_TYPES = {item.value for item in IncidentType}


def _counts(counts: Counter) -> list[dict]:
    return [{"label": label, "count": n} for label, n in
            sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))]


def _section(title: str, **totals: int) -> dict:
    return {"title": title, "totals": totals, "groups": [], "highlights": []}


def _group(section: dict, label: str, counts: Counter) -> None:
    if counts:
        section["groups"].append({"label": label, "items": _counts(counts)})


def _shared_rows(connection, prefix: str, marker: str, window: tuple) -> tuple:
    incidents = connection.execute(f"""
        SELECT metro, type, COUNT(*) AS n, SUM(COALESCE(emerging, 0)) AS emerging,
               MAX(threat_score) AS severity, MAX(SUBSTR(summary, 1, 180)) AS example
        FROM {prefix}incidents WHERE first_seen > {marker} AND first_seen <= {marker}
        GROUP BY metro, type
        ORDER BY severity DESC, n DESC, metro, type
        """, window).fetchall()
    fused = connection.execute(f"""
        SELECT uid, types, lat, lon, source_count, COUNT(*) OVER () AS n
        FROM {prefix}fused_events WHERE first_seen > {marker} AND first_seen <= {marker}
        ORDER BY peak_score DESC, first_seen DESC, uid LIMIT 5
        """, window).fetchall()
    signals = connection.execute(f"""
        SELECT buffer, COUNT(*) AS n
        FROM {prefix}signal_buffer
        WHERE COALESCE(first_seen, ts) > {marker} AND COALESCE(first_seen, ts) <= {marker}
        GROUP BY buffer
        """, window).fetchall()
    return incidents, fused, {r["buffer"]: r["n"] for r in signals}


def _local_rows(period: str, window: tuple) -> tuple:
    with snapshots.db_lock:
        c = radio._conn()
        if period == "minute":
            volume = c.execute("""
                SELECT key, system, COUNT(*) AS n
                FROM radio_recent WHERE seen > ? AND seen <= ? GROUP BY key, system
                """, window).fetchall()
        else:
            volume = c.execute("""
                SELECT key, system, SUM(calls) AS n
                FROM radio_calls WHERE ts > ? AND ts <= ? GROUP BY key, system
                """, window).fetchall()
        surges = c.execute("""
            SELECT key, system, COUNT(*) AS n FROM radio_calls
            WHERE ts > ? AND ts <= ? AND z >= 3 AND calls >= 8 GROUP BY key, system
            """, window).fetchall()
        classes = c.execute("""
            SELECT kind, type, COUNT(*) AS n FROM radio_classifications
            WHERE ts > ? AND ts <= ? GROUP BY kind, type
            """, window).fetchall()
        dispatches = c.execute("""
            SELECT DISTINCT incident_type, lat, lon FROM radio_transcripts
            WHERE done_at > ? AND done_at <= ? AND status = 'done' AND kind = 'dispatch'
              AND lat IS NOT NULL AND lon IS NOT NULL
            ORDER BY priority DESC, done_at DESC LIMIT 12
            """, window).fetchall()
        c = metrics._conn()
        facts = c.execute("""
            SELECT category, topic, detail AS example, cell, n FROM (
                SELECT category, topic, detail, cell, COUNT(*) OVER (PARTITION BY category, topic) AS n,
                       ROW_NUMBER() OVER (PARTITION BY category, topic ORDER BY ts DESC, id) AS latest
                FROM fact_updates WHERE ts > ? AND ts <= ?
            ) WHERE latest = 1 ORDER BY n DESC, topic
            """, window).fetchall()
        wildlife = c.execute("""
            SELECT detail AS species, COUNT(*) AS n FROM fact_updates
            WHERE ts > ? AND ts <= ? AND category = 'ecology' GROUP BY detail
            """, window).fetchall()
        flags = c.execute("""
            SELECT current.flags, (
                SELECT previous.flags FROM cell_notable_log previous
                WHERE previous.cell = current.cell AND previous.ts < current.ts
                ORDER BY previous.ts DESC LIMIT 1
            ) AS previous
            FROM cell_notable_log current
            WHERE current.ts > ? AND current.ts <= ? AND current.flags <> '[]'
            """, window).fetchall()
    return volume, surges, classes, dispatches, facts, wildlife, flags


def _feed_section(metro: str, incident_type: str, feeds: dict) -> str:
    feed = feeds.get(metro)
    if metro.startswith("radio_"):
        return "radio"
    if metro in _WEATHER or (metro in {"hz_eonet", "eonet"} and incident_type == "weather"):
        return "weather"
    if metro in _ECOLOGY or (metro in {"hz_eonet", "eonet"} and incident_type == "fire"):
        return "ecology"
    if metro in _OTHER or metro.startswith(("hz_", "t511_")) or (feed and feed.hidden):
        return "other"
    return "events"


def _flag_topic(flag: str) -> str:
    if " at the " in flag and "percentile" in flag:
        return flag.split(" at the ", 1)[0]
    for prefix in ("AQI ", "gusts ", "visibility ", "drought ", "geomagnetic storm:", "unstable atmosphere:"):
        if flag.startswith(prefix):
            return prefix.rstrip(" :")
    return flag


def build(period: str, feeds: dict, place_name) -> dict:
    from apb.ingest import rdio

    now = time.time()
    window = (now - PERIOD_SECONDS[period], now)
    if state.is_postgres():
        snapshots._init_postgres()
        events._init_postgres()
        sigbuf._init_postgres()
        with state.pg_connection() as c:
            incidents, fused, signals = _shared_rows(c, "panoptes_", "%s", window)
    else:
        with snapshots.db_lock:
            events._conn()
            incidents, fused, signals = _shared_rows(sigbuf._conn(), "", "?", window)
    volume, surges, classes, dispatches, facts, wildlife, flags = _local_rows(period, window)

    def system_name(row) -> str:
        key, system = row["key"], row["system"]
        inst = rdio.instance(key)
        if inst and system is not None:
            return rdio._label(inst, system)
        return f"Scanner {key}:{system}" if system is not None else "Unassigned traffic"

    sections = {
        "events": _section("Incidents & emerging events", incident_reports=0,
                           new_fused_events=fused[0]["n"] if fused else 0, marked_emerging=0),
        "radio": _section("Radio", **{
            "call records received" if period == "minute" else "calls tallied": sum(r["n"] or 0 for r in volume),
            "systems reporting": sum(r["system"] is not None for r in volume),
            "calls classified": sum(r["n"] for r in classes),
            "surge windows": sum(r["n"] for r in surges),
            "located reports": 0,
        }),
        "weather": _section("Weather & storms", feed_reports=0, sampled_condition_updates=0),
        "ecology": _section("Ecology & environment", feed_reports=0, sightings=0),
        "notable": _section("Notable facts", new_flags=0),
        "other": _section("Other feeds", feed_reports=0, news_items=signals.get("news", 0),
                          social_items=signals.get("social", 0)),
    }
    source_counts = {key: Counter() for key in sections}
    fact_counts = {key: Counter() for key in ("weather", "ecology")}
    incident_types: Counter = Counter()
    shown_sources = set()
    for row in incidents:
        metro, kind = row["metro"] or "unknown", row["type"] or "other"
        key = _feed_section(metro, kind, feeds)
        section = sections[key]
        total = {"radio": "located reports", "events": "incident_reports"}.get(key, "feed_reports")
        section["totals"][total] += row["n"]
        if key == "events":
            section["totals"]["marked_emerging"] += row["emerging"]
            incident_types[kind.replace("_", " ")] += row["n"]
        if key == "radio":
            continue
        feed = feeds.get(metro)
        name = feed.name if feed else metro.replace("_", " ")
        source_counts[key][name] += row["n"]
        if key in {"weather", "ecology"} and row["example"] and (key, metro) not in shown_sources:
            if len(section["highlights"]) < 4:
                section["highlights"].append({"label": name, "detail": row["example"]})
                shown_sources.add((key, metro))
    for key in ("events", "weather", "ecology", "other"):
        _group(sections[key], "Reporting feeds", source_counts[key])
    _group(sections["events"], "Incident types", incident_types)

    for row in fused:
        types = row["types"]
        if isinstance(types, str):
            types = json.loads(types)
        types = types or {"activity": 1}
        kind = max(types, key=types.get)
        place = place_name(row["lat"], row["lon"])
        sections["events"]["highlights"].append({
            "label": kind.replace("_", " "), "detail": f"{place} · {row['source_count']} source kinds"
            if place else f"{row['source_count']} source kinds",
            "uid": row["uid"], "lat": row["lat"], "lon": row["lon"],
        })

    radio_types, systems = Counter(), Counter()
    for row in classes:
        kind, itype = row["kind"], row["type"]
        label = itype.replace("_", " ") if kind == "dispatch" and itype in _TYPES else {
            "dispatch": "other dispatch", "unit": "unit traffic", "noise": "noise", "tones": "tones",
        }.get(kind, "other traffic")
        radio_types[label] += row["n"]
    for row in volume:
        systems[system_name(row)] += row["n"] or 0
    surging_systems = Counter()
    for row in surges:
        surging_systems[system_name(row)] += row["n"]
    _group(sections["radio"], "Classified traffic", radio_types)
    _group(sections["radio"], "Calls by system", systems)
    _group(sections["radio"], "Surges by system", surging_systems)
    highlights = sections["radio"]["highlights"]
    for row in dispatches:
        place = place_name(row["lat"], row["lon"])
        kind = row["incident_type"]
        if place and kind in _TYPES and kind != "noise":
            item = {"label": f"{kind.replace('_', ' ')} dispatch", "detail": place}
            if item not in highlights:
                highlights.append(item)
            if len(highlights) == 3:
                break

    for row in facts:
        category = row["category"]
        section = sections[category]
        total = "sightings" if category == "ecology" else "sampled_condition_updates"
        section["totals"][total] += row["n"]
        fact_counts[category][row["topic"]] += row["n"]
        item = {"label": row["topic"], "detail": row["example"]}
        if row["cell"]:
            lat, lon = (float(coord) + metrics.CELL_DEG / 2 for coord in row["cell"].split(","))
            place = place_name(lat, lon) or f"{lat:.2f}, {lon:.2f}"
            item.update(detail=f"{row['example']} near {place}", lat=lat, lon=lon)
        section["highlights"].append(item)
    for key in ("weather", "ecology"):
        _group(sections[key], "Sampled facts", fact_counts[key])
    _group(sections["ecology"], "Sightings by species", Counter({row["species"]: row["n"] for row in wildlife}))

    flag_counts = Counter()
    for row in flags:
        old = {_flag_topic(f) for f in json.loads(row["previous"] or "[]")}
        new = {_flag_topic(f) for f in json.loads(row["flags"])}
        flag_counts.update(new - old)
    sections["notable"]["totals"]["new_flags"] = sum(flag_counts.values())
    _group(sections["notable"], "New flags by type", flag_counts)

    read = _quick_read(sections, period)
    for section in sections.values():
        section["totals"] = [{"label": label.replace("_", " "), "count": n}
                             for label, n in section["totals"].items()]
    return {"period": period, "since": window[0], "until": now,
            "read": read, "sections": list(sections.values())}


def _quick_read(sections: dict, period: str) -> str:
    """Put a readable lead above the counts on every refresh, even without a model."""
    incident = sections["events"]
    radio_totals = sections["radio"]["totals"]
    event_count = incident["totals"]["new_fused_events"]
    reports = incident["totals"]["incident_reports"]
    sentences = []
    if event_count:
        lead = incident["highlights"][0]
        noun = "event" if event_count == 1 else "events"
        sentences.append(f"Panoptes detected {event_count:,} new {noun}. "
                         f"Among the highest-ranked reports was {lead['label']} ({lead['detail']}).")
    elif reports:
        noun = "report" if reports == 1 else "reports"
        types = next(g["items"] for g in incident["groups"] if g["label"] == "Incident types")
        lead = types[0]
        sentences.append(f"Panoptes received {reports:,} new incident {noun}, led by "
                         f"{lead['count']:,} classified as {lead['label']}.")
    calls = radio_totals["call records received" if period == "minute" else "calls tallied"]
    if calls:
        noun = "call" if calls == 1 else "calls"
        verb = "received records for" if period == "minute" else "tallied"
        sentences.append(f"Scanner feeds {verb} {calls:,} {noun}.")
    elif radio_totals["calls classified"]:
        sentences.append(f"Panoptes classified {radio_totals['calls classified']:,} scanner recordings.")
    if sections["radio"]["highlights"] and not event_count:
        lead = sections["radio"]["highlights"][0]
        sentences.append(f"Radio traffic included a {lead['label']} in the {lead['detail']} area.")
    for key, noun in (("weather", "weather"), ("ecology", "environment")):
        section = sections[key]
        if section["highlights"]:
            lead = section["highlights"][0]
            sentences.append(f"The {noun} feeds highlighted {lead['label']} ({lead['detail']}).")
    if not sentences and any(any(s["totals"].values()) for s in sections.values()):
        return "New reports arrived from the monitored feeds. The details below show where activity was recorded."
    return " ".join(sentences) or "Panoptes received no new reports in this window."


def event_sources(uid: str, until: float) -> dict:
    """Source counts for a selected fused event as eligible for the brief's cutoff."""
    if state.is_postgres():
        events._init_postgres()
        with state.pg_connection() as c:
            row = c.execute("SELECT sources FROM panoptes_fused_events "
                            "WHERE uid = %s AND last_seen <= %s", (uid, until)).fetchone()
        return (row["sources"] or {}) if row else {}
    with snapshots.db_lock:
        row = events._conn().execute("SELECT sources FROM fused_events "
                                     "WHERE uid = ? AND last_seen <= ?", (uid, until)).fetchone()
    return json.loads(row["sources"] or "{}") if row else {}
