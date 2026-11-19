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

log = logging.getLogger(__name__)

EVERY = float(os.environ.get("APB_RADIO_AREAS_EVERY", "900"))
WINDOW = float(os.environ.get("APB_RADIO_AREAS_WINDOW", "3600"))
MIN_CALLS = 3
MAX_PER_RUN = int(os.environ.get("APB_RADIO_AREAS_PER_RUN", "8"))
MAX_LINES = 60

_stop = threading.Event()
_thread: threading.Thread | None = None
_stats = {"runs": 0, "summaries": 0, "errors": 0, "budget_stops": 0, "last": 0.0, "last_error": None}

PROMPT = """You summarise public-safety radio traffic for a public situational-awareness \
map. Below are machine transcripts of recent calls on one radio system, numbered. Group \
them by the AREA they concern (a town, county, highway corridor or district; "unknown" \
when the calls do not say) and answer with ONLY a JSON array:
[{"area": the area's name as people would say it, e.g. "Hendersonville" or "I-26 near Fletcher",
  "town": the town or city if the area is in or next to one, else null,
  "state": two-letter state code or null,
  "activity": one sentence, at most 160 characters, on what is going on there: call types, \
tempo, which kinds of agencies (fire, EMS, sheriff, highway patrol, air medical),
  "priority": 0.0 (routine) to 1.0 (active threat to life),
  "calls": [the numbers of the calls that belong to this area]}]
Leave out people's names, house numbers (a road or "the 1200 block" is fine), unit or \
aircraft identifiers, licence plates and any medical or victim details. Skip radio checks \
and noise. Do not invent anything the calls do not say."""


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


def parse(text: str) -> list[dict]:
    m = re.search(r"\[.*\]", text or "", re.S)
    if not m:
        return []
    try:
        arr = json.loads(m.group(0))
    except ValueError:
        return []
    return [o for o in arr if isinstance(o, dict)] if isinstance(arr, list) else []


_UNIT = re.compile(r"\b(?:Engine|Medic|Ladder|Truck|Rescue|Squad|Unit|Car|Trooper|Deputy|Mama|Air|"
                   r"Medic|Ambulance|Battalion|Chief|Tower|Brush|Tanker)\s*-?\s*\d+[A-Z]?\b", re.I)


def clean(s) -> str:
    """Backstop on what the model wrote: blocks, not house numbers; no unit identifiers."""
    from apb.context.radio_stt import blockify
    s = blockify(str(s or "").strip()) or ""
    s = _UNIT.sub(lambda m: re.sub(r"\s*-?\s*\d+[A-Z]?$", "", m.group(0)), s)
    return re.sub(r"\s{2,}", " ", s)[:200]


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
    head = (f"System: {label}. Listener near {inst.get('place') or 'unknown'}"
            + (f"; the system serves {pl['area']}" if pl.get("area") else "") + ".")
    text = PROMPT + "\n\n" + head + "\n\n" + "\n".join(_line(n, t) for n, t in enumerate(rows, 1))
    raw, _ = (gen or (lambda parts, max_tokens: explain.generate_usage(parts, max_tokens, feature="radio_areas")))(
        [{"text": text}], max_tokens=900)
    areas = []
    for a in parse(raw):
        act = clean(a.get("activity"))
        if not act:
            continue
        try:
            pr = max(0.0, min(1.0, float(a.get("priority") or 0.2)))
        except (TypeError, ValueError):
            pr = 0.2
        n = len([i for i in (a.get("calls") or []) if str(i).isdigit()])
        pt = place(a, rows, pl.get("loc"))
        areas.append({"area": clean(a.get("area"))[:60] or "unknown",
                      "state": (str(a.get("state") or "").upper()[:2] or None),
                      "activity": act, "priority": round(pr, 2), "calls": n,
                      "lat": pt[0] if pt else None, "lon": pt[1] if pt else None})
    areas.sort(key=lambda a: (a["area"] == "unknown", -a["priority"], -a["calls"]))
    radio.add_areas(key, system, len(rows), max(r["ts"] for r in rows), areas, now=now)
    return areas


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
                    "areas": [{k: x.get(k) for k in ("area", "state", "activity", "priority", "calls", "lat", "lon")}
                              for x in a["areas"]]})
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
