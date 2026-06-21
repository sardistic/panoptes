"""Query + alerting API over stored incidents (dissemination layer).

Only redacted incident records are exposed here. Raw audio and unredacted
transcripts are intentionally NOT served by this API.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from apb.context.feeds import feeds_near
from apb.ingest.cad import FEEDS as CAD_FEEDS
from apb.ingest.cad import (CadIngest, load_adsb, load_arcgis_catalog, load_catalog,
                            load_faa_tfr, load_fema, load_firms, load_hazard,
                            load_faa_delays, load_hms_smoke, load_ndbc, load_nhc,
                            load_odin, load_openaq, load_p2c, load_pulsepoint,
                            load_southern, load_spc, load_traffic511, load_usgs_flood,
                            load_volcano)

import os as _os
import threading
import time as _time

from apb.store import snapshots

_cad = CadIngest()
# Auto-register every live-ready feed found by the discovery sweeps (countrywide).
_a = load_catalog()
_b = load_arcgis_catalog()
_c = load_pulsepoint()
_d = load_p2c()
_e = load_southern()
_f = load_hazard()
_g = load_traffic511()
_h = load_adsb() if _os.environ.get("APB_ADSB") else 0  # heavier (14 polls/cycle); opt-in
_i = load_faa_tfr()
_j = load_fema()
_k = load_firms()  # only registers when FIRMS_MAP_KEY is set
_l = load_odin()
_m = load_usgs_flood()
_n = load_openaq()  # only registers when OPENAQ_KEY is set
_o = load_volcano()
_p = load_hms_smoke()
_q = load_ndbc()
_r = load_spc()
_s = load_nhc()
_t = load_faa_delays()
print(f"[api] live CAD feeds: {len(CAD_FEEDS)} "
      f"(socrata +{_a}, arcgis +{_b}, pulsepoint +{_c}, p2c +{_d}, southern +{_e}, "
      f"hazard +{_f}, traffic511 +{_g}, adsb +{_h}, tfr +{_i}, fema +{_j}, firms +{_k}, "
      f"odin +{_l}, flood +{_m}, openaq +{_n}, volcano +{_o}, smoke +{_p}, ndbc +{_q}, "
      f"spc +{_r}, nhc +{_s}, faa_delay +{_t})")


def _poller(interval: float = 120.0):
    """Continuously snapshot the national stream into the DB so coverage/history grow."""
    snapshots.conn()  # init
    n = 0
    while True:
        try:
            wrote = snapshots.record(_cad.overview(limit_per=80, max_age_hours=168))
            n += 1
            if n % 30 == 0:
                snapshots.prune()
            print(f"[poller] snapshot {n}: +{wrote} rows, db={snapshots.stats()}")
        except Exception as e:
            print(f"[poller] error: {e}")
        _time.sleep(interval)


threading.Thread(target=_poller, daemon=True).start()

# Optional live Bluesky firehose -> rolling social-signal buffer for /live/fused.
# Off unless APB_BLUESKY is set AND `websockets` is installed (kept out of lean prod).
if _os.environ.get("APB_BLUESKY"):
    from apb.fusion import social_store as _social
    _social.start(keep_unplaced=bool(_os.environ.get("APB_BLUESKY_KEEP_UNPLACED")))

# Optional keyless news-RSS poller -> rolling news-signal buffer for /live/fused.
if _os.environ.get("APB_NEWS"):
    from apb.fusion import news_store as _news
    _news.start(keep_unplaced=bool(_os.environ.get("APB_NEWS_KEEP_UNPLACED")))

# Optional keyless Reddit/Mastodon RSS poller -> shares the social-signal buffer.
if _os.environ.get("APB_SOCIAL_RSS"):
    from apb.fusion import social_store as _social_rss
    _social_rss.start_rss(keep_unplaced=bool(_os.environ.get("APB_SOCIAL_RSS_KEEP_UNPLACED")))

# Optional aisstream maritime firehose -> rolling vessel buffer for /live/maritime.
# Needs AISSTREAM_KEY + `pip install websockets`; kept out of the lean prod image.
if _os.environ.get("AISSTREAM_KEY"):
    from apb.fusion import maritime_store as _maritime
    _maritime.start()

# DB stack (sqlalchemy/geoalchemy/psycopg) is imported lazily so the live/map UI and
# /feeds work with only FastAPI installed — no Postgres/PostGIS required to test.
def _db():
    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from apb.store.db import ActivityRow, IncidentRow, engine
    return select, Session, ActivityRow, IncidentRow, engine

app = FastAPI(title="APB", version="0.1.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

_WEB_DIR = Path(__file__).resolve().parents[2] / "web"


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/incidents")
def incidents(
    metro: str | None = None,
    incident_type: str | None = None,
    min_threat: float = 0.0,
    minutes: int = 60,
    limit: int = 200,
):
    """Recent incidents, filterable. Powers the map/list UI."""
    select, Session, _, IncidentRow, engine = _db()
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    stmt = select(IncidentRow).where(
        IncidentRow.extracted_at >= since,
        IncidentRow.threat_score >= min_threat,
    )
    if metro:
        stmt = stmt.where(IncidentRow.metro == metro)
    if incident_type:
        stmt = stmt.where(IncidentRow.incident_type == incident_type)
    stmt = stmt.order_by(IncidentRow.extracted_at.desc()).limit(limit)

    with Session(engine) as s:
        rows = s.scalars(stmt).all()
    return [
        {
            "call_id": r.call_id,
            "metro": r.metro,
            "type": r.incident_type,
            "summary": r.summary,
            "location": r.location_text,
            "sentiment": r.sentiment,
            "threat_score": r.threat_score,
            "emerging": r.is_emerging,
            "at": r.extracted_at.isoformat(),
        }
        for r in rows
    ]


@app.get("/activity")
def activity(metro: str | None = None, minutes: int = 30, anomalous_only: bool = False):
    """Activity-first signal: recent per-talkgroup windows. Works on encrypted systems
    (metadata-only). Powers the map's heat/volume layer."""
    select, Session, ActivityRow, _, engine = _db()
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    stmt = select(ActivityRow).where(ActivityRow.window_start >= since)
    if metro:
        stmt = stmt.where(ActivityRow.metro == metro)
    if anomalous_only:
        stmt = stmt.where(ActivityRow.is_anomalous.is_(True))
    stmt = stmt.order_by(ActivityRow.window_start.desc()).limit(1000)
    with Session(engine) as s:
        rows = s.scalars(stmt).all()
    return [
        {
            "metro": r.metro, "system_id": r.system_id, "talkgroup": r.talkgroup,
            "label": r.talkgroup_label, "window_start": r.window_start.isoformat(),
            "call_count": r.call_count, "airtime_sec": r.total_airtime_sec,
            "encrypted": r.encrypted, "baseline": r.baseline_call_count,
            "zscore": r.zscore, "anomalous": r.is_anomalous,
        }
        for r in rows
    ]


@app.get("/emerging")
def emerging(metro: str | None = None, minutes: int = 30):
    """Incidents flagged by the anomaly/clustering layer as emerging threats."""
    select, Session, _, IncidentRow, engine = _db()
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    stmt = select(IncidentRow).where(
        IncidentRow.extracted_at >= since,
        IncidentRow.is_emerging.is_(True),
    )
    if metro:
        stmt = stmt.where(IncidentRow.metro == metro)
    stmt = stmt.order_by(IncidentRow.threat_score.desc())
    with Session(engine) as s:
        rows = s.scalars(stmt).all()
    return [{"call_id": r.call_id, "metro": r.metro, "type": r.incident_type,
             "summary": r.summary, "threat_score": r.threat_score} for r in rows]


@app.get("/live/incidents")
def live_incidents(metro: str = "seattle", limit: int = 400,
                   max_age_hours: float = 0.0):
    """Real live CAD/911 dispatch (geocoded). max_age_hours>0 keeps only incidents
    within that window (0 = no time filter)."""
    rows = _cad.fetch(metro, limit)
    if max_age_hours > 0:
        import time as _t
        cutoff = _t.time() - max_age_hours * 3600
        rows = [d for d in rows if d.get("ts") and d["ts"] >= cutoff]
    return rows


@app.get("/live/overview")
def live_overview(limit_per: int = 60, max_age_hours: float = 72.0):
    """National aggregate: live fetch UNIONed with accumulated DB history (dedup), so
    coverage grows over time instead of being limited to each feed's latest page."""
    live = _cad.overview(limit_per, max_age_hours)
    merged = {(d["metro"], str(d["call_id"])): d for d in snapshots.query(max_age_hours)}
    for d in live:                       # live wins on conflict (freshest)
        merged[(d["metro"], str(d["call_id"]))] = d
    return list(merged.values())


@app.get("/live/hazards")
def live_hazards(source: str | None = None, max_age_hours: float = 24.0):
    """No-key national hazard/event feeds (USGS quakes, NWS alerts, NASA EONET).
    `source` filters to one of: usgs, nws, eonet. These also seed the correlation layer."""
    import time as _t
    from apb.ingest.hazard import SOURCES
    keys = [source] if source in SOURCES else list(SOURCES)
    cutoff = _t.time() - max_age_hours * 3600 if max_age_hours > 0 else 0
    out: list[dict] = []
    for k in keys:
        for d in _cad.fetch("hz_" + k):
            if not cutoff or (d.get("ts") and d["ts"] >= cutoff):
                out.append(d)
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


@app.get("/live/traffic")
def live_traffic(system: str | None = None, include_planned: bool = False,
                 max_age_hours: float = 0.0):
    """State DOT 511 traffic incidents (crashes/hazards/closures). `system` filters to a
    state key (e.g. 'ny'). Planned roadwork excluded unless include_planned=true."""
    import time as _t
    from apb.ingest.traffic511 import SYSTEMS
    keys = [system] if system in SYSTEMS else list(SYSTEMS)
    cutoff = _t.time() - max_age_hours * 3600 if max_age_hours > 0 else 0
    out: list[dict] = []
    for k in keys:
        rows = _cad._fetch_traffic511(CAD_FEEDS["t511_" + k]) if ("t511_" + k) in CAD_FEEDS else []
        for d in rows:
            if include_planned or d.get("threat_score", 0) > 0.2:
                if not cutoff or (d.get("ts") and d["ts"] >= cutoff):
                    out.append(d)
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


@app.get("/live/aircraft")
def live_aircraft():
    """ADS-B loitering rotorcraft (police/medevac orbiting a scene) — an early proxy for
    a major ground incident. Stateful: accuracy improves as the watcher accumulates track
    history. Enable the background watcher with APB_ADSB=1."""
    return _cad.fetch("adsb") if "adsb" in CAD_FEEDS else []


@app.get("/live/declarations")
def live_declarations(source: str | None = None, max_age_hours: float = 0.0):
    """Authority event signals: FAA Temporary Flight Restrictions + FEMA disaster
    declarations. `source` filters to 'faa_tfr' or 'fema'. These anchor the
    correlation layer (a TFR/declaration explains a local surge)."""
    import time as _t
    keys = [source] if source in ("faa_tfr", "fema") else ["faa_tfr", "fema"]
    cutoff = _t.time() - max_age_hours * 3600 if max_age_hours > 0 else 0
    out: list[dict] = []
    for k in keys:
        if k in CAD_FEEDS:
            for d in _cad.fetch(k):
                if not cutoff or (d.get("ts") and d["ts"] >= cutoff):
                    out.append(d)
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


@app.get("/live/fire")
def live_fire(max_age_hours: float = 0.0):
    """NASA FIRMS active-fire pixels (VIIRS). Empty unless FIRMS_MAP_KEY is configured."""
    import time as _t
    if "firms" not in CAD_FEEDS:
        return []
    cutoff = _t.time() - max_age_hours * 3600 if max_age_hours > 0 else 0
    out = [d for d in _cad.fetch("firms")
           if not cutoff or (d.get("ts") and d["ts"] >= cutoff)]
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


def _live_feed(slug: str, max_age_hours: float) -> list[dict]:
    """Shared helper: fetch one hidden feed, optionally time-filtered, newest first."""
    import time as _t
    if slug not in CAD_FEEDS:
        return []
    cutoff = _t.time() - max_age_hours * 3600 if max_age_hours > 0 else 0
    out = [d for d in _cad.fetch(slug)
           if not cutoff or (d.get("ts") and d["ts"] >= cutoff)]
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


@app.get("/live/outages")
def live_outages(max_age_hours: float = 0.0):
    """ODIN current power outages (county-level), largest first."""
    return _live_feed("odin", max_age_hours)


@app.get("/live/flood")
def live_flood(max_age_hours: float = 0.0):
    """NOAA NWPS river gauges currently at/above flood 'action' stage."""
    return _live_feed("usgs_flood", max_age_hours)


@app.get("/live/airquality")
def live_airquality(max_age_hours: float = 0.0):
    """OpenAQ PM2.5 spikes (Unhealthy+). Empty unless OPENAQ_KEY is configured."""
    return _live_feed("openaq", max_age_hours)


@app.get("/live/maritime")
def live_maritime(max_age_hours: float = 2.0):
    """Live AIS vessel positions (US coastal). Empty unless AISSTREAM_KEY is configured."""
    from apb.fusion import maritime_store
    out = maritime_store.recent(max_age_hours)
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


@app.get("/live/volcano")
def live_volcano(max_age_hours: float = 0.0):
    """USGS elevated-status volcanoes (aviation color code / alert level)."""
    return _live_feed("volcano", max_age_hours)


@app.get("/live/smoke")
def live_smoke(max_age_hours: float = 0.0):
    """NOAA HMS satellite smoke plumes (Light/Medium/Heavy)."""
    return _live_feed("hms_smoke", max_age_hours)


@app.get("/live/marine")
def live_marine(max_age_hours: float = 0.0):
    """NDBC buoys currently reporting high seas / gale-force winds."""
    return _live_feed("ndbc", max_age_hours)


@app.get("/live/storm_reports")
def live_storm_reports(max_age_hours: float = 0.0):
    """SPC preliminary storm reports (observed tornado/hail/wind), today."""
    return _live_feed("spc", max_age_hours)


@app.get("/live/cyclones")
def live_cyclones(max_age_hours: float = 0.0):
    """NHC active tropical cyclones (position/intensity). Empty out of season."""
    return _live_feed("nhc", max_age_hours)


@app.get("/live/airport_delays")
def live_airport_delays(max_age_hours: float = 0.0):
    """FAA airport ground delays, ground stops, and closures with reason."""
    return _live_feed("faa_delay", max_age_hours)


@app.get("/db/stats")
def db_stats():
    """How much history the snapshot poller has accumulated."""
    return snapshots.stats()


@app.get("/baseline/anomalies")
def baseline_anomalies(window_hours: float = 1.0, lookback_hours: float = 72.0,
                       z: float = 2.0):
    """Rate-anomaly detection from accumulated history: metros whose incident rate in
    the most recent `window_hours` is >= z std-devs above their own trailing baseline."""
    from apb.infer.baseline import detect_rate_anomalies
    anomalies = detect_rate_anomalies(snapshots.query(lookback_hours, limit=20000),
                                      window_hours=window_hours, z_threshold=z)
    for a in anomalies:                       # attach a center for map rendering
        feed = CAD_FEEDS.get(a["metro"])
        if feed and feed.center:
            a["lat"], a["lon"] = feed.center[0], feed.center[1]
        a["name"] = feed.name if feed else a["metro"]
    return anomalies


@app.get("/live/emerging")
def live_emerging(min_count: int = 3, threat_floor: float = 0.5,
                  max_age_hours: float = 24.0):
    """Emerging events: spatial clusters of converging, elevated-severity incidents
    from the RECENT live stream (default last 24h)."""
    from apb.infer.cluster import detect
    clusters = detect(_cad.overview(max_age_hours=max_age_hours),
                      min_count=min_count, threat_floor=threat_floor)
    return [c.__dict__ for c in clusters]


def _parse_kinds(kinds: str | None) -> set[str] | None:
    """Comma-separated source_kind filter -> set, or None for all. Powers per-family
    filtering across the fusion endpoints (cad, traffic, weather, aircraft, social,
    news, context, radio_metadata, radio_transcript)."""
    if not kinds:
        return None
    return {k.strip() for k in kinds.split(",") if k.strip()}


@app.get("/live/signals")
def live_signals(limit_per: int = 60, max_age_hours: float = 24.0,
                 include_seed: bool = True, include_live: bool = True,
                 kinds: str | None = None):
    """Normalized source signals across CAD/history plus optional local seed rows.

    This is the source-fusion substrate: every sensor becomes the same shape before
    clustering. `kinds` filters by sensor family (e.g. kinds=cad,social,weather).
    `data/social_seed.jsonl` can be used for offline social/news tests.
    """
    from apb.fusion.signals import dict_signal
    from apb.fusion.sources import cad_signals, load_seed_signals
    from apb.fusion import news_store, social_store
    signals = cad_signals(_cad, limit_per=limit_per, max_age_hours=max_age_hours,
                          include_live=include_live)
    _cut = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).timestamp()
    if include_seed:   # seed is offline test data — drop stale rows so live windows stay live
        signals.extend(s for s in load_seed_signals() if s.observed_at.timestamp() >= _cut)
    signals.extend(social_store.recent(max_age_hours))   # live Bluesky firehose
    signals.extend(news_store.recent(max_age_hours))     # live news RSS
    want = _parse_kinds(kinds)
    if want is not None:
        signals = [s for s in signals if s.source_kind.value in want]
    signals.sort(key=lambda s: s.observed_at, reverse=True)
    return [dict_signal(s) for s in signals[:5000]]


@app.get("/live/fused")
def live_fused(min_count: int = 2, min_sources: int = 1, min_score: float = 1.2,
               max_age_hours: float = 24.0, limit_per: int = 20,
               include_seed: bool = True, include_live: bool = True,
               kinds: str | None = None):
    """Cross-source event clusters ranked by surge score.

    Unlike /live/emerging, this is built for APB's broader goal: radio/CAD/social/
    traffic/news signals can reinforce each other when they converge in place/time.
    `kinds` restricts the clustered substrate to given sensor families.
    """
    from apb.fusion.cluster import detect
    from apb.fusion.sources import cad_signals, load_seed_signals
    from apb.fusion import news_store, social_store
    signals = cad_signals(_cad, limit_per=limit_per, max_age_hours=max_age_hours,
                          include_live=include_live)
    if include_seed:
        signals.extend(load_seed_signals())
    signals.extend(social_store.recent(max_age_hours))   # live Bluesky firehose
    signals.extend(news_store.recent(max_age_hours))     # live news RSS
    want = _parse_kinds(kinds)
    if want is not None:
        signals = [s for s in signals if s.source_kind.value in want]
    return [e.__dict__ for e in detect(
        signals, min_count=min_count, min_sources=min_sources, min_score=min_score,
        max_age_hours=max_age_hours)]


@app.get("/live/social")
def live_social(max_age_hours: float = 24.0, limit: int = 800):
    """Placed social/news signals (seed + live Bluesky buffer) for the map social layer.
    Rendered as their own markers so the firehose is visible even without CAD corroboration."""
    from apb.fusion.signals import dict_signal
    from apb.fusion.sources import load_seed_signals
    from apb.fusion import news_store, social_store
    out = [s for s in (load_seed_signals() + social_store.recent(max_age_hours)
                       + news_store.recent(max_age_hours))
           if s.lat is not None and s.lon is not None]
    out.sort(key=lambda s: s.observed_at, reverse=True)
    return [dict_signal(s) for s in out[:limit]]


@app.get("/live/metros")
def live_metros():
    """Metros with a live CAD feed for the dropdown (hidden bulk feeds excluded)."""
    feeds = sorted((f for f in CAD_FEEDS.values() if not f.hidden),
                   key=lambda f: f.name.lower())
    return [{"metro": f.metro, "name": f.name, "state": f.state,
             "center": list(f.center) if f.center else None} for f in feeds]


@app.get("/feeds")
def feeds(lat: float, lon: float, radius_m: float = 800.0):
    """On-demand public feeds near an incident (cameras/traffic). Not stored."""
    return [f.__dict__ for f in feeds_near(lat, lon, radius_m)]


@app.get("/correlate")
def correlate(lat: float, lon: float, types: str | None = None,
              timespan: str = "3d"):
    """Spike -> likely cause: recent GDELT news near a spike's place/time. Free, no key.
    `types` = comma-separated incident types from the cluster (shapes the news query)."""
    from apb.context.gdelt import correlate as _corr
    tl = [t for t in (types or "").split(",") if t]
    return _corr(lat, lon, tl or None, timespan)


# Serve the test UI at / (mounted last so it doesn't shadow API routes).
if _WEB_DIR.exists():
    app.mount("/", StaticFiles(directory=str(_WEB_DIR), html=True), name="web")
