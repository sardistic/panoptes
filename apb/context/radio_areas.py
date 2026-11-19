"""Area context from transcribed radio: what is going on where, per radio system.

Every EVERY seconds, each system with at least MIN_CALLS transcribed calls in the last
WINDOW seconds (and something new since its last summary) gets one text-only Gemini
call that groups those calls by the town, county or road they concern and writes one
line of activity per area ("deer strike on NC 280, Highway Patrol responding").

What is published: the area name, that line, a priority and a call count, placed on
the ~2 km cell of the calls the model attributes to it (when any were geocoded) or on
the town's gazetteer point. Never published: addresses beyond a road or block, names,
unit identifiers, plates or medical details — the prompt forbids them and blockify()
is the backstop. Daily cap APB_RADIO_AREAS_DAILY model calls (default 300).
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time

from apb.context.radio_stt import TYPES

log = logging.getLogger(__name__)

EVERY = float(os.environ.get("APB_RADIO_AREAS_EVERY", "900"))
WINDOW = float(os.environ.get("APB_RADIO_AREAS_WINDOW", "3600"))
MIN_CALLS = 3
MAX_PER_RUN = int(os.environ.get("APB_RADIO_AREAS_PER_RUN", "8"))
MAX_LINES = 60

_stop = threading.Event()
_thread: threading.Thread | None = None
_stats = {"runs": 0, "summaries": 0, "errors": 0, "budget_stops": 0, "last": 0.0, "last_error": None}

PROMPT = """You turn public-safety radio traffic into a public situational-awareness \
map. Below are machine transcripts of recent calls on one radio system, numbered, and the \
incidents already open on it (labelled T1, T2, ...). Answer with ONLY one JSON object:
{"incidents": [{"thread": the open incident's label (e.g. "T2") when these calls continue it, else null,
   "title": at most 60 characters, e.g. "Vehicle vs deer on NC 280" or "Structure fire on Oak Ave",
   "type": one of [TYPE_LIST],
   "town": the town or city it is in or next to, else null,
   "state": two-letter state code or null,
   "where": the most precise place said — a block ("1200 block of Oak Ave"), an intersection \
("NC 280 at Deep Woods Dr"), a road or a landmark; used only to place it on the map, never shown,
   "activity": one sentence, at most 180 characters, on what happened and how it developed,
   "priority": 0.0 (routine) to 1.0 (active threat to life),
   "status": "active" or "cleared",
   "calls": [the numbers of the calls that belong to it]}],
 "areas": [{"area": a town, city, county or highway corridor, e.g. "Hendersonville" or "I-26 near \
Fletcher" — never a street, cove, intersection or address (use the town, or "unknown"),
   "town": the town or city if the area is in or next to one, else null,
   "state": two-letter state code or null,
   "activity": one sentence, at most 160 characters: call types, tempo, which kinds of \
agencies (fire, EMS, sheriff, highway patrol, air medical),
   "priority": 0.0 to 1.0,
   "calls": [the numbers of the calls about this area]}]}
An incident is one event: its dispatch and the traffic about it (units responding, on \
scene, updates, transport, clearing). Put a call in an incident only when it clearly \
belongs — same place, same units, or an explicit reference — and continue an open \
incident rather than starting a duplicate. Traffic tied to no event (status checks, \
acknowledgements, routine stops) goes in no incident. Areas cover all the calls.
In title, activity and area: leave out people's names, house and block numbers (name the \
road, never a number on it), unit or aircraft identifiers and licence plates. Medical \
calls are just "a medical call": never the condition, injury, symptom, age or sex (no \
"seizure", "overdose", "hemorrhage", "panic attack"). Skip radio checks and noise. Do not \
invent anything the calls do not say."""

OPEN_SEC = 5400.0                                  # an incident with no call for 90 min is closed


# conditions that must not be published even if the model names them
_MEDICAL = re.compile(
    r"\b(?:seizures?|overdos\w*|hemorrhag\w*|bleeding|panic attacks?|cardiac arrest|"
    r"heart attacks?|strokes?|unconscious\w*|unresponsive|diabetic\w*|suicid\w*|"
    r"psychiatric|psych\b|mental health|pregnan\w*|childbirth|allergic\w*|"
    r"breathing (?:problems?|difficult\w*)|difficulty breathing|chest pains?|"
    r"intoxicat\w*|drunk|syncope|fainted|sick person|lift assist|dead body|doa|deceased|"
    r"death|cpr|gunshot wounds?|stab wounds?|lacerations?|fractures?)\b", re.I)


def off() -> bool:
    return os.environ.get("APB_RADIO_AREAS_OFF", "").strip().lower() in ("1", "true", "yes", "on")


def daily_cap() -> int:
    try:
        return max(0, int(os.environ.get("APB_RADIO_AREAS_DAILY", "300")))
    except ValueError:
        return 300


def due(now: float | None = None) -> list[tuple[tuple, list[dict]]]:
    """[((key, system), transcripts newest first)] that deserve a fresh summary, focus first."""
    from apb.context.radio_stt import focus
    from apb.store import radio
    now = now or time.time()
    groups: dict[tuple, list[dict]] = {}
    for t in radio.transcripts(WINDOW, limit=4000):
        if t.get("kind") in ("noise", "tones") or not (t.get("text") or t.get("summary")):
            continue
        groups.setdefault((t["key"], t["system"]), []).append(t)
    last = {(a["key"], a["system"]): a for a in radio.latest_areas(3 * 86400)}
    foc = focus()
    out = []
    for k, rows in groups.items():
        if len(rows) < MIN_CALLS:
            continue
        prev = last.get(k)
        if prev and (prev.get("newest") or 0) >= max(r["ts"] for r in rows):
            continue                                       # nothing new since the last one
        out.append((k, rows))
    out.sort(key=lambda kv: (kv[0] not in foc, -len(kv[1])))
    return out


def _line(n: int, t: dict) -> str:
    when = time.strftime("%H:%M", time.gmtime(t["ts"]))
    where = t.get("location_text") or "?"
    body = (t.get("text") or t.get("summary") or "").replace("\n", " ")[:300]
    return (f"{n}. [{when}Z, talkgroup {t.get('tg_label') or t.get('talkgroup')}] "
            f"{t.get('kind')}/{t.get('incident_type')} @ {where}: {body}")


def parse(text: str) -> dict:
    """{"incidents": [...], "areas": [...]} from the reply (a bare array = areas only)."""
    m = re.search(r"[\[{].*[\]}]", text or "", re.S)
    if not m:
        return {"incidents": [], "areas": []}
    try:
        obj = json.loads(m.group(0))
    except ValueError:
        return {"incidents": [], "areas": []}
    if isinstance(obj, list):
        obj = {"areas": obj}
    if not isinstance(obj, dict):
        return {"incidents": [], "areas": []}
    return {k: [o for o in (obj.get(k) or []) if isinstance(o, dict)] for k in ("incidents", "areas")}


_UNIT = re.compile(r"\b(?:Engine|Medic|Ladder|Truck|Rescue|Squad|Unit|Car|Trooper|Deputy|Mama|Air|"
                   r"Medic|Ambulance|Battalion|Chief|Tower|Brush|Tanker)\s*-?\s*\d+[A-Z]?\b", re.I)


_BLOCK = re.compile(r"\b(?:the\s+)?\d+\s*(?:-\s*)?(?:block|blk)\s+(?:of\s+)?", re.I)
_STREETY = re.compile(r"\b(?:Road|Rd|Street|St|Avenue|Ave|Drive|Dr|Lane|Ln|Court|Ct|Cove|Cv|Boulevard|"
                      r"Blvd|Way|Place|Pl|Circle|Cir|Terrace|Ter|Trail|Trl|Loop|Run|Row|Alley|Path)\.?\s*$|"
                      r"\b(?:and|&|at)\b", re.I)


def clean(s) -> str:
    """Backstop on what the model wrote: no house or block numbers, no unit identifiers,
    no medical conditions (they become "medical")."""
    from apb.context.radio_stt import blockify
    s = blockify(str(s or "").strip()) or ""
    s = _BLOCK.sub("", s)
    s = re.sub(r"\s*\[REDACTED\]", "", s)
    s = _UNIT.sub(lambda m: re.sub(r"\s*-?\s*\d+[A-Z]?$", "", m.group(0)), s)
    s = _MEDICAL.sub("medical", s)
    s = re.sub(r"\b(?:a|an)\s+medical\b", "a medical", s, flags=re.I)
    s = re.sub(r"\bmedical(?:\s*(?:,|and|or|/)\s*medical)+\b", "medical", s, flags=re.I)
    s = re.sub(r"\bmedical\s+(?:call|emergency|incident)s?\s+call\b", "medical call", s, flags=re.I)
    s = re.sub(r"\ba medical(?=\s*(?:[.,;)]|$))", "a medical call", s, flags=re.I)
    return re.sub(r"\s{2,}", " ", s).strip()[:200]


def area_label(a: dict) -> str:
    """A town, county or corridor — a street or intersection becomes its town (or unknown)."""
    name = clean(a.get("area"))[:60]
    if not name or _STREETY.search(name) and not re.search(r"\b(?:County|I-\d+|US \d+|NC \d+|Highway|Hwy|Route|Interstate)\b", name):
        town = clean(a.get("town"))[:60]
        return town if town and not _STREETY.search(town) else "unknown"
    return name


def place(area: dict, rows: list[dict], near: tuple | None) -> tuple | None:
    """A ~2 km cell for an area: the calls it was built from that were geocoded, else the
    town on the gazetteer if it is within 250 km of the system. Never the state centre."""
    from apb.context.radio_stt import _km, coarse
    idx = [int(i) for i in (area.get("calls") or []) if str(i).isdigit()]
    pts = [(rows[i - 1]["lat"], rows[i - 1]["lon"]) for i in idx
           if 1 <= i <= len(rows) and rows[i - 1].get("lat") is not None]
    if pts:
        return coarse((sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts)))
    town = str(area.get("town") or "").strip().lower().replace(".", "")
    if not town or town == "unknown":
        return None
    try:
        from apb.fusion.places import _gaz
        hit = _gaz().get(town)
    except Exception:
        hit = None
    if not hit or (near and _km((hit[0], hit[1]), near) > 250.0):
        return None
    return coarse((hit[0], hit[1]))


def summarise(key: str, system: int, rows: list[dict], gen=None, now: float | None = None) -> list[dict]:
    from apb.context import explain
    from apb.ingest import rdio
    from apb.store import radio
    inst = rdio.instance(key) or {}
    label = rdio._label(inst, system) if inst else str(system)
    pl = rdio.place_info(inst, label, system) if inst else {}
    rows = sorted(rows, key=lambda r: r["ts"])[-MAX_LINES:]
    now = now or time.time()
    head = (f"System: {label}. Listener near {inst.get('place') or 'unknown'}"
            + (f"; the system serves {pl['area']}" if pl.get("area") else "") + ".")
    open_ = [t for t in radio.threads(OPEN_SEC, key, system) if t.get("status") != "cleared"][:12]
    labels = {f"T{i}": t for i, t in enumerate(open_, 1)}
    opened = "\n".join(
        f"{lab} (since {time.strftime('%H:%M', time.gmtime(t['first_ts']))}Z, last "
        f"{time.strftime('%H:%M', time.gmtime(t['last_ts']))}Z): {t.get('title')} — at "
        f"{t.get('loc_text') or t.get('area') or '?'} — {t.get('activity')}" for lab, t in labels.items())
    text = (PROMPT.replace("TYPE_LIST", ", ".join(TYPES)) + "\n\n" + head
            + "\n\nOpen incidents:\n" + (opened or "(none)")
            + "\n\nCalls:\n" + "\n".join(_line(n, t) for n, t in enumerate(rows, 1)))
    raw, _ = (gen or (lambda parts, max_tokens: explain.generate_usage(parts, max_tokens, feature="radio_areas")))(
        [{"text": text}], max_tokens=1600)
    reply = parse(raw)
    _stats["threads"] = _stats.get("threads", 0) + link(key, system, reply["incidents"], rows, labels, inst, pl, now)
    areas = []
    for a in reply["areas"]:
        act = clean(a.get("activity"))
        if not act:
            continue
        try:
            pr = max(0.0, min(1.0, float(a.get("priority") or 0.2)))
        except (TypeError, ValueError):
            pr = 0.2
        n = len([i for i in (a.get("calls") or []) if str(i).isdigit()])
        pt = place(a, rows, pl.get("loc"))
        areas.append({"area": area_label(a),
                      "state": (str(a.get("state") or "").upper()[:2] or None),
                      "activity": act, "priority": round(pr, 2), "calls": n,
                      "lat": pt[0] if pt else None, "lon": pt[1] if pt else None})
    areas.sort(key=lambda a: (a["area"] == "unknown", -a["priority"], -a["calls"]))
    radio.add_areas(key, system, len(rows), max(r["ts"] for r in rows), areas, now=now)
    return areas


def _nums(obj: dict, n: int) -> list[int]:
    out = []
    for i in obj.get("calls") or []:
        try:
            k = int(i)
        except (TypeError, ValueError):
            continue
        if 1 <= k <= n:
            out.append(k)
    return out


def link(key: str, system: int, incidents: list[dict], rows: list[dict], labels: dict,
         inst: dict, pl: dict, now: float, placer=None) -> int:
    """Fold the model's incidents into stored threads: continue an open one by its label
    or start a new one, place it (internally precise, published as a ~2 km cell) and hand
    a newly placed one to fusion. Returns threads written."""
    import hashlib
    from apb.context import radio_place
    from apb.context.radio_stt import blockify, coarse
    from apb.store import radio
    placer = placer or radio_place.place
    claimed: set[int] = set()
    written = 0
    for inc in incidents:
        nums = [k for k in _nums(inc, len(rows)) if k not in claimed]
        prev = labels.get(str(inc.get("thread") or "").strip().upper())
        if not nums and not prev:
            continue
        claimed.update(nums)
        mine = [rows[k - 1] for k in nums]
        ids = {int(r["id"]) for r in mine}
        if prev:
            t = dict(prev)
        else:
            if not mine:
                continue
            first = min(mine, key=lambda r: r["ts"])
            tid = hashlib.sha1(f"{key}:{system}:{first['id']}".encode()).hexdigest()[:12]
            t = {"tid": tid, "key": key, "system": system, "first_ts": first["ts"], "calls": [],
                 "published": 0, "precision": None, "loc_text": None}
        t["calls"] = sorted(set(t.get("calls") or []) | ids)
        if mine:
            t["first_ts"] = min([t.get("first_ts") or mine[0]["ts"]] + [r["ts"] for r in mine])
            t["last_ts"] = max([t.get("last_ts") or 0] + [r["ts"] for r in mine])
        itype = str(inc.get("type") or t.get("type") or "other")
        t["type"] = itype if itype in TYPES else "other"
        t["title"] = clean(inc.get("title"))[:80] or t.get("title") or t["type"].replace("_", " ")
        t["activity"] = clean(inc.get("activity")) or t.get("activity")
        town = clean(inc.get("town"))[:60] or None
        t["area"] = area_label({"area": town or t.get("area") or "unknown", "town": town})
        t["state"] = (str(inc.get("state") or t.get("state") or "").upper()[:2] or None)
        try:
            t["priority"] = round(max(0.0, min(1.0, float(inc.get("priority")))), 2)
        except (TypeError, ValueError):
            t["priority"] = t.get("priority") or 0.3
        t["status"] = "cleared" if str(inc.get("status") or "").lower() == "cleared" else "active"
        where = blockify(str(inc.get("where") or "").strip()[:160]) or None
        if where != t.get("loc_text") or t.get("precision") not in ("address", "intersection"):
            located = [r for r in radio.transcripts_for(key, t["calls"]).values() if r.get("lat") is not None]
            pt, prec = placer(where, town, t["state"], located, inst.get("place"), pl.get("loc"),
                              bool(pl.get("approx")))
            rank = {"address": 3, "intersection": 2, "town": 1, None: 0}
            if pt and rank[prec] >= rank.get(t.get("precision"), 0):
                cell = coarse(pt)
                t["lat"], t["lon"], t["precision"] = cell[0], cell[1], prec
            t["loc_text"] = where or t.get("loc_text")
            if pt and prec in ("address", "intersection") and not t.get("published"):
                if not located:                    # a located call already made its own incident
                    _to_fusion(t, now)
                t["published"] = 1
        t["updated"] = now
        radio.save_thread(t)
        written += 1
    return written


def _to_fusion(t: dict, now: float) -> None:
    from datetime import datetime, timezone
    from apb.store import snapshots
    pr = float(t.get("priority") or 0.3)
    snapshots.record([{
        "metro": f"radio_{t['key']}", "call_id": f"rthr{t['tid']}", "type": t["type"],
        "summary": f"{t['title']} (heard on radio)", "location": t.get("area") or "general area",
        "sentiment": ["calm", "routine", "elevated", "urgent", "distress"][min(4, int(pr * 5))],
        "threat_score": round(pr, 2), "emerging": pr >= 0.8, "lat": t["lat"], "lon": t["lon"],
        "at": datetime.fromtimestamp(t["first_ts"], tz=timezone.utc).isoformat(), "ts": t["first_ts"]}])


def public_threads(max_age_sec: float = 10800.0) -> list[dict]:
    """What /live/radio/threads serves: each incident, its ~2 km cell and precision word,
    and its calls in order with a cleaned line each. Never the place text."""
    from apb.ingest import rdio
    from apb.store import radio
    out = []
    for t in radio.threads(max_age_sec):
        inst = rdio.instance(t["key"])
        calls = radio.transcripts_for(t["key"], t["calls"])
        out.append({
            "id": t["tid"], "sys": f"{t['key']}:{t['system']}",
            "system": rdio._label(inst, t["system"]) if inst else None,
            "type": t.get("type"), "title": clean(t.get("title")), "activity": clean(t.get("activity")),
            "area": area_label({"area": t.get("area")}), "state": t.get("state"),
            "priority": t.get("priority"), "status": t.get("status"), "precision": t.get("precision"),
            "lat": t.get("lat"), "lon": t.get("lon"), "first_ts": t.get("first_ts"), "last_ts": t.get("last_ts"),
            "calls": [{"id": c["id"], "ts": c["ts"], "talkgroup": c.get("tg_label"), "kind": c.get("kind"),
                       "line": clean(c.get("text") or c.get("summary"))[:240],
                       "audio": f"/live/radio/audio/{t['key']}/{c['id']}"}
                      for c in sorted(calls.values(), key=lambda c: c["ts"])]})
    return out


def run_once(now: float | None = None, gen=None) -> int:
    from apb.store import spend
    done = 0
    for (key, system), rows in due(now)[:MAX_PER_RUN]:
        cap = daily_cap()
        if not spend.allow("radio_area_calls", 1, monthly_cap=cap * 31, daily_cap=cap):
            _stats["budget_stops"] += 1
            break
        try:
            summarise(key, system, rows, gen=gen, now=now)
            done += 1
        except Exception as e:
            _stats["errors"] += 1
            _stats["last_error"] = str(e)[:200]
            log.info("radio areas: %s/%s: %s", key, system, str(e)[:160])
    _stats["summaries"] += done
    _stats["runs"] += 1
    _stats["last"] = now or time.time()
    return done


def public(max_age_sec: float = 5400.0) -> list[dict]:
    """What /live/radio/areas serves: system, when, and the cleaned area lines."""
    from apb.ingest import rdio
    from apb.store import radio
    out = []
    for a in radio.latest_areas(max_age_sec):
        inst = rdio.instance(a["key"])
        out.append({"id": f"{a['key']}:{a['system']}",
                    "system": rdio._label(inst, a["system"]) if inst else None,
                    "ts": a["ts"], "calls": a["calls"],
                    "areas": [{**{k: x.get(k) for k in ("state", "priority", "calls", "lat", "lon")},
                               "area": area_label({"area": x.get("area")}),     # stored rows predate a rule
                               "activity": clean(x.get("activity"))} for x in a["areas"]]})
    return out


def _loop() -> None:
    _stop.wait(300)                                    # let some transcripts land first
    while not _stop.is_set():
        try:
            run_once()
        except Exception as e:
            _stats["errors"] += 1
            _stats["last_error"] = str(e)[:200]
            log.warning("radio areas run failed: %s", e)
        _stop.wait(EVERY)


def start() -> bool:
    global _thread
    from apb.context import explain
    if _thread or off() or not explain.api_key() or daily_cap() <= 0:
        return False
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="radio-areas")
    _thread.start()
    log.info("radio areas: on, every %d s, %d summaries a day max", EVERY, daily_cap())
    return True


def stop() -> None:
    _stop.set()


def status() -> dict:
    from apb.context.radio_stt import focus
    from apb.store import spend
    return {**_stats, "running": bool(_thread and _thread.is_alive()), "daily_cap": daily_cap(),
            "used": spend.used("radio_area_calls"), "focus": sorted(f"{k}:{s}" for k, s in focus())}
