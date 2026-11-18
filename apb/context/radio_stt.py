"""Smart radio — transcribe the calls that matter, inside a daily audio budget.

The rdio lane already records every call's metadata (time, system, talkgroup). This
worker decides which of those calls are worth hearing, fetches just those through the
egress proxy, and has Gemini transcribe them and pull out what a map needs.

What "matters" is decided per talkgroup from the instance's own tags and labels: fire,
EMS and law DISPATCH talkgroups first (they announce the incident type and address,
which is what PulsePoint used to give us), tactical channels only when their system is
surging or an active event sits nearby, and public works / schools / transit never.

Budget: `APB_RADIO_STT_DAILY_SEC` seconds of audio a day (default 7200 = two hours),
charged through the spend store before each batch, so a busy night stops at the cap
instead of running up a bill. Gemini bills audio by tokens (32 per second per Google's
docs); the real token count of each batch is kept in the status.

Privacy: the model is told to redact names, full street addresses (block level and
intersections are kept), phone numbers, plates, dates of birth and medical or victim
details, and only that redacted text is stored. The radio unit ids an instance sends
with each call are never stored. A dispatch with a block-level address is geocoded
(US Census geocoder, checked against the system's own area) and written to the
incident store, where it shows on the map like any dispatch feed. APB_RADIO_STT_OFF
disables the worker.
"""
from __future__ import annotations

import base64
import json
import logging
import math
import os
import re
import threading
import time
from datetime import datetime, timezone

import httpx

log = logging.getLogger(__name__)

EVERY = float(os.environ.get("APB_RADIO_STT_EVERY", "60"))
MIN_WEIGHT = float(os.environ.get("APB_RADIO_STT_MIN", "0.8"))
PER_RUN = int(os.environ.get("APB_RADIO_STT_PER_RUN", "16"))
PER_INSTANCE = 5
BATCH = 8
MAX_AGE = 480.0                  # a call older than eight minutes is no longer news
MIN_SEC, MAX_SEC = 2.5, 90.0     # "copy" / "10-4" and long patches are not worth the budget
TOK_PER_SEC = 32.0

_CENSUS = "https://geocoding.geo.census.gov/geocoder"
_http = httpx.Client(timeout=12.0, headers={"User-Agent": "apb/0.1 (panoptes.run; radio geocoding)"})
_stop = threading.Event()
_thread: threading.Thread | None = None
_state_cache: dict[tuple, str | None] = {}
_mime_label: dict[str, str] = {}             # learned: which mime label Gemini accepts
_stats = {"runs": 0, "clips": 0, "batches": 0, "errors": 0, "budget_stops": 0,
          "audio_tokens": 0, "located": 0, "incidents": 0, "last": 0.0, "last_error": None}

TYPES = ("traffic", "medical", "fire", "assault", "robbery", "shots_fired", "domestic",
         "suspicious", "pursuit", "welfare", "other", "noise")


def off() -> bool:
    return os.environ.get("APB_RADIO_STT_OFF", "").strip().lower() in ("1", "true", "yes", "on")


def daily_sec() -> int:
    try:
        return max(0, int(os.environ.get("APB_RADIO_STT_DAILY_SEC", "7200")))
    except ValueError:
        return 7200


# ── what matters ────────────────────────────────────────────────────────────

_FIRE = re.compile(r"\bfire\b|\bfd\b|fireground|\bems\b|medic|ambulance|rescue|\bals\b|\bbls\b")
_EMS = re.compile(r"\bems\b|medic|ambulance|\bals\b|\bbls\b")
_LAW = re.compile(r"\blaw\b|police|\bpd\b|sheriff|\bso\b|trooper|state patrol|\bhp\b|patrol|deput")
_DISP = re.compile(r"dispatch|\bdisp\b|\bdsp\b")
_TAC = re.compile(r"\btac|tactical|fireground|\bfg\b|talk ?around|command")
_LOW = re.compile(r"public works|\bdpw\b|utilit|school|transit|\bbus\b|highway|\broads?\b|\bparks?\b|"
                  r"water|sewer|maint|animal|\btest|telemetry|\bdata\b|admin|security|hospital|"
                  r"\bevents?\b|interop|mutual aid|amateur|\bham\b|gmrs|\bfrs\b|business")


def importance(tg: dict) -> tuple[float, str]:
    """(weight 0..1, family) for one talkgroup from its tag, label, name and group."""
    text = " ".join(str(tg.get(k) or "") for k in ("tag", "label", "name", "group")).lower()
    fire, ems, law = bool(_FIRE.search(text)), bool(_EMS.search(text)), bool(_LAW.search(text))
    disp, tac = bool(_DISP.search(text)), bool(_TAC.search(text))
    if disp and (fire or ems):
        return 1.0, ("ems" if ems and "fire" not in text else "fire")
    if disp and law:
        return 0.85, "law"
    if disp:
        return 0.8, "dispatch"
    if _LOW.search(text):
        return 0.05, "other"
    if fire and tac:
        return 0.55, "fire"
    if law and tac:
        return 0.45, "law"
    if fire:
        return 0.5, "fire"
    if law:
        return 0.4, "law"
    if "emergency" in text:
        return 0.4, "other"
    return 0.2, "other"


def _km(a: tuple, b: tuple) -> float:
    coslat = max(0.2, math.cos(math.radians(a[0])))
    return math.hypot(a[0] - b[0], (a[1] - b[1]) * coslat) * 111.0


def _context() -> tuple[dict, list[tuple]]:
    """Systems surging right now, and the places of active events."""
    from apb.store import radio
    surging = {(r["key"], r["system"]): r["z"] for r in radio.latest(1800.0)
               if r.get("z") is not None and r["z"] >= 3.0}
    events: list[tuple] = []
    try:
        from apb.store import events as event_store
        now = time.time()
        events = [(e["lat"], e["lon"]) for e in event_store.query(max_age_hours=1.0, limit=300)
                  if (e.get("latest_score") or 0) >= 3.0 and now - (e.get("last_seen") or 0) <= 2400]
    except Exception as e:
        log.info("radio stt: events unavailable: %s", e)
    return surging, events


def pick(now: float | None = None) -> list[dict]:
    """The calls worth transcribing now, best first, bounded per instance and per run."""
    from apb.ingest import rdio
    from apb.store import radio
    now = now or time.time()
    surging, events = _context()
    closed = {k for k, st in radio.states().items() if st.get("state") == "closed"}
    places: dict[tuple, tuple] = {}
    out: list[dict] = []
    for r in radio.candidates(now - MAX_AGE):
        inst = rdio.instance(r["key"])
        if not inst or r["key"] in closed:
            continue
        tg = rdio.talkgroup(r["key"], r["system"], r["talkgroup"])
        w, family = importance(tg)
        if w < 0.3:
            continue                                   # never worth a boost
        boost = 1.0
        if (r["key"], r["system"]) in surging:
            boost *= 1.8
        if events:
            pk = (r["key"], r["system"])
            if pk not in places:
                places[pk] = rdio.system_place(inst, rdio._label(inst, r["system"]))
            loc, approx = places[pk]
            if loc and not approx and any(_km(loc, ev) <= 20.0 for ev in events):
                boost *= 1.4
        weight = round(min(2.0, w * boost), 3)
        if weight >= MIN_WEIGHT:
            out.append({**r, "weight": weight, "family": family, "tg": tg})
    out.sort(key=lambda c: (-c["weight"], -c["ts"]))
    per: dict[str, int] = {}
    chosen = []
    for c in out:
        if per.get(c["key"], 0) >= PER_INSTANCE:
            continue
        per[c["key"]] = per.get(c["key"], 0) + 1
        chosen.append(c)
        if len(chosen) >= PER_RUN:
            break
    return chosen


# ── audio ───────────────────────────────────────────────────────────────────

_MP3_KBPS = {1: (0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320),     # MPEG-1 L3
             2: (0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160)}         # MPEG-2/2.5 L3


def est_seconds(data: bytes, mime: str) -> float:
    """Clip length from the first MP3 frame's bitrate, else assume 40 kbit/s (measured
    against Gemini's own audio token counts on the first production batches)."""
    kbps = 40
    if mime == "audio/mpeg":
        i = 0
        if data[:3] == b"ID3" and len(data) > 10:
            i = 10 + ((data[6] & 0x7F) << 21 | (data[7] & 0x7F) << 14 | (data[8] & 0x7F) << 7 | (data[9] & 0x7F))
        while i < min(len(data) - 3, 8192):
            if data[i] == 0xFF and data[i + 1] & 0xE0 == 0xE0:
                version = (data[i + 1] >> 3) & 0x03         # 3 = MPEG-1
                idx = data[i + 2] >> 4
                table = _MP3_KBPS[1 if version == 3 else 2]
                if 0 < idx < 15 and table[idx]:
                    kbps = table[idx]
                    break
            i += 1
    return round(len(data) * 8 / (kbps * 1000.0), 1)


def gemini_mime(mime: str) -> str:
    return _mime_label.get(mime) or {"audio/mpeg": "audio/mp3", "audio/mp4": "audio/aac"}.get(mime, mime)


_ALT_MIME = {"audio/mp4": ["audio/aac", "audio/mp4", "audio/m4a"], "audio/mpeg": ["audio/mp3", "audio/mpeg"]}


# ── the model ───────────────────────────────────────────────────────────────

PROMPT = """You transcribe public-safety radio for a situational-awareness map. The \
attached clips are separate radio calls, each preceded by a line naming its radio system \
and talkgroup. Return ONLY a JSON array with one object per clip, in order:
{"i": clip number (1-based),
 "transcript": the words spoken (radio jargon kept, PII redacted),
 "kind": "dispatch" (a dispatcher assigning units to an incident) | "unit" (units talking) | "tones" (paging tones only) | "noise" (unintelligible, radio check, test),
 "incident_type": one of [""" + ", ".join(TYPES) + """],
 "location_text": the incident location at block or intersection level (e.g. "100 block of Main St", "5th Ave and Elm St"), or null,
 "municipality": the city or town named, or null,
 "state": two-letter US state (or Canadian province) code if the system or talkgroup names or the call make it clear, else null,
 "units": [unit names such as "Engine 5", "Medic 12"],
 "priority": 0.0 (routine) to 1.0 (active threat to life),
 "summary": one line, at most 140 characters}
Redaction (apply to transcript, location_text and summary): replace people's names, house \
numbers beyond block level (write "1200 block of", not "1234"), phone numbers, licence \
plates, dates of birth, and medical or victim details beyond the call type with \
[REDACTED]. Do not invent anything not said; use null when unclear."""


_STREET = (r"(?:[NSEW]\.?\s+)?(?:[A-Z0-9][\w'.-]*\s+){0,4}?"
           r"(?:Road|Rd|Street|St|Avenue|Ave|Drive|Dr|Lane|Ln|Court|Ct|Boulevard|Blvd|Way|Place|"
           r"Pl|Crescent|Cres|Circle|Cir|Highway|Hwy|Pike|Parkway|Pkwy|Terrace|Ter|Trail|Trl|"
           r"Loop|Run|Row|Square|Sq|Alley|Path|Turnpike|Tpke)\b\.?")
_HOUSE = re.compile(r"\b(\d{1,6})(\s*-?\s*)(" + _STREET + ")")


def blockify(text: str | None) -> str | None:
    """'505 Mill Road' -> '500 block of Mill Road', everywhere in a string. The model is
    asked to do this; this makes it true even when it does not (seen in production)."""
    if not text:
        return text

    def sub(m):
        before = text[max(0, m.start() - 9):m.start()].lower()
        if "block of" in before or "blk" in before:
            return m.group(0)                          # already block level
        n = int(m.group(1))
        return f"{n // 100 * 100} block of {m.group(3)}"
    return _HOUSE.sub(sub, text)


def build_parts(clips: list[dict]) -> list[dict]:
    parts: list[dict] = [{"text": PROMPT}]
    for n, c in enumerate(clips, 1):
        tg = c["tg"]
        parts.append({"text": f"Clip {n}: system \"{c['system_label']}\", talkgroup \"{tg['label']}\""
                              f" ({tg['tag']}{', ' + tg['group'] if tg.get('group') else ''})"
                              + (f", area {c['area']}" if c.get("area") else "")})
        parts.append({"inline_data": {"mime_type": gemini_mime(c["mime"]),
                                      "data": base64.b64encode(c["data"]).decode()}})
    return parts


def parse_reply(text: str, n: int) -> dict[int, dict]:
    """{clip index 0-based: result} from the model's JSON array (fences / 0-based tolerated)."""
    m = re.search(r"\[.*\]", text or "", re.S)
    if not m:
        return {}
    try:
        arr = json.loads(m.group(0))
    except ValueError:
        return {}
    rows = [o for o in arr if isinstance(o, dict)] if isinstance(arr, list) else []

    def idx(o):
        try:
            return int(o.get("i"))
        except (TypeError, ValueError):
            return -99
    zero = any(idx(o) == 0 for o in rows)
    out = {}
    for o in rows:
        k = idx(o) - (0 if zero else 1)
        if 0 <= k < n:
            out[k] = o
    return out


def audio_tokens(usage: dict) -> int:
    for d in usage.get("promptTokensDetails") or []:
        if str(d.get("modality", "")).upper() == "AUDIO":
            return int(d.get("tokenCount") or 0)
    return 0


# ── geocoding ───────────────────────────────────────────────────────────────

_BLOCK = re.compile(r"^\s*(\d{1,6})\s*(?:hundred\s+)?(?:block|blk)\s*(?:of\s+)?(.+)$", re.I)
_ADDR = re.compile(r"^\s*\d{1,6}\s+\S.+$")
_INTERSECTION = re.compile(r"\s(?:and|&|at|/|x)\s", re.I)


def block_address(loc: str | None) -> str | None:
    """'1200 block of N Elm St' -> '1200 N Elm St'. Intersections and vague places
    return None (the Census geocoder only resolves addresses)."""
    if not loc or "[REDACTED]" in loc:
        return None
    loc = loc.strip().rstrip(".")
    m = _BLOCK.match(loc)
    if m:
        return f"{m.group(1)} {m.group(2).strip()}"
    if _ADDR.match(loc) and not _INTERSECTION.search(loc):
        return loc
    return None


def state_at(lat: float, lon: float) -> str | None:
    key = (round(lat, 1), round(lon, 1))
    if key in _state_cache:
        return _state_cache[key]
    st = None
    try:
        r = _http.get(f"{_CENSUS}/geographies/coordinates", params={
            "x": lon, "y": lat, "benchmark": "Public_AR_Current", "vintage": "Current_Current",
            "layers": "States", "format": "json"})
        r.raise_for_status()
        rows = (r.json().get("result", {}).get("geographies", {}) or {}).get("States") or []
        st = rows[0].get("STUSAB") if rows else None
    except (httpx.HTTPError, ValueError) as e:
        log.info("radio stt: state lookup failed: %s", e)
        return None                                   # not cached: retry next time
    _state_cache[key] = st
    return st


_US = {"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
       "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
       "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
       "VA", "WA", "WV", "WI", "WY", "DC", "PR"}


def geocode(loc: str | None, municipality: str | None, area: str | None,
            near: tuple | None, approx: bool, state_hint: str | None = None) -> tuple[float, float] | None:
    """A block-level address in the system's own area, or None.

    The state comes from the listener's town when known, else from the model (it reads
    "Burlington County Public Safety" as New Jersey; the gazetteer picks the biggest
    Burlington, in North Carolina), else from the system's place when that is not a
    guess. Census only answers inside the state asked, so a model-given state needs no
    distance check against an approximate place."""
    addr = block_address(loc)
    if not addr:
        return None
    city, state = None, None
    if area:
        bits = [b.strip() for b in area.split(",")]
        if len(bits) >= 2 and len(bits[1]) == 2 and bits[1].isupper():
            city, state = bits[0], bits[1]
    hint = (state_hint or "").strip().upper()
    hint = hint if hint in _US else None
    from_hint = False
    if not state and hint:
        state, from_hint = hint, True
    if not state and near and not approx:
        state = state_at(near[0], near[1])
    if not state or state not in _US:
        return None
    city = (municipality or city or "").strip()
    q = ", ".join(x for x in (addr, city, state) if x)
    try:
        r = _http.get(f"{_CENSUS}/locations/onelineaddress", params={
            "address": q, "benchmark": "Public_AR_Current", "format": "json"})
        r.raise_for_status()
        matches = r.json().get("result", {}).get("addressMatches") or []
    except (httpx.HTTPError, ValueError) as e:
        log.info("radio stt: geocode failed: %s", e)
        return None
    if not matches:
        return None
    c = matches[0].get("coordinates") or {}
    if c.get("x") is None or c.get("y") is None:
        return None
    pt = (float(c["y"]), float(c["x"]))
    if from_hint and approx:
        return pt
    if not near:
        return None
    return pt if _km(pt, near) <= (150.0 if approx else 80.0) else None


# ── one run ─────────────────────────────────────────────────────────────────

def _incident(c: dict, res: dict, pt: tuple) -> dict:
    itype = res.get("incident_type") if res.get("incident_type") in TYPES else "other"
    pr = max(0.0, min(1.0, float(res.get("priority") or 0.3)))
    when = datetime.fromtimestamp(c["ts"], tz=timezone.utc)
    sentiment = ["calm", "routine", "elevated", "urgent", "distress"][min(4, int(pr * 5))]
    return {"metro": f"radio_{c['key']}", "call_id": f"rdio{c['id']}", "type": itype,
            "summary": (res.get("summary") or res.get("transcript") or itype)[:240],
            "location": (res.get("location_text") or "")[:160], "sentiment": sentiment,
            "threat_score": round(pr, 2), "emerging": pr >= 0.8, "lat": pt[0], "lon": pt[1],
            "at": when.isoformat(), "ts": c["ts"], "source": "radio_stt"}


def run_once(now: float | None = None, fetch=None, gen=None, locate=None) -> int:
    """Pick, fetch, transcribe, store. Returns clips transcribed. Injectable for tests."""
    from apb.context import explain
    from apb.ingest import rdio
    from apb.store import radio, snapshots, spend
    now = now or time.time()
    picked = pick(now)
    if not picked:
        return 0
    fetch = fetch or rdio.fetch_calls
    gen = gen or explain.generate_usage
    locate = locate or geocode
    clips: list[dict] = []
    by_key: dict[str, list[dict]] = {}
    for c in picked:
        by_key.setdefault(c["key"], []).append(c)
    for key, cs in by_key.items():
        inst = rdio.instance(key)
        try:
            got = fetch(inst, [c["id"] for c in cs])
        except Exception as e:
            _stats["errors"] += 1
            log.info("radio stt: fetch %s: %s", key, str(e)[:100])
            continue
        if not got:
            continue                                   # over the instance's allowance: next run
        for c in cs:
            if c["id"] not in got:
                radio.add_transcript(key, c["id"], c["ts"], c["system"], c["talkgroup"], "failed",
                                     weight=c["weight"])
                continue
            data, mime = got[c["id"]]
            sec = est_seconds(data, mime)
            if not MIN_SEC <= sec <= MAX_SEC:
                radio.add_transcript(key, c["id"], c["ts"], c["system"], c["talkgroup"], "skipped",
                                     weight=c["weight"], seconds=sec)
                continue
            clips.append({**c, "data": data, "mime": mime, "seconds": sec,
                          "system_label": rdio._label(inst, c["system"]), "area": inst.get("place"),
                          "inst": inst})
    done = 0
    for i in range(0, len(clips), BATCH):
        batch = clips[i:i + BATCH]
        need = math.ceil(sum(c["seconds"] for c in batch))
        cap = daily_sec()
        if not spend.allow("radio_stt_sec", need, monthly_cap=cap * 31, daily_cap=cap):
            _stats["budget_stops"] += 1
            break                                      # untried calls simply age out
        try:
            text, usage = gen(build_parts(batch), max_tokens=220 * len(batch))
        except Exception as e:
            _stats["errors"] += 1
            _stats["last_error"] = str(e)[:200]
            detail = getattr(e, "detail", "") or str(e)
            if "mime" in detail.lower() or "unsupported" in detail.lower():
                for c in batch:                        # learn another label for this container
                    alts = _ALT_MIME.get(c["mime"], [])
                    cur = gemini_mime(c["mime"])
                    nxt = alts[(alts.index(cur) + 1) % len(alts)] if cur in alts else (alts[0] if alts else cur)
                    _mime_label[c["mime"]] = nxt
            log.info("radio stt: model: %s", str(e)[:160])
            continue                                   # these calls are retried while still fresh
        _stats["batches"] += 1
        tok = audio_tokens(usage)
        _stats["audio_tokens"] += tok
        if tok:                                        # charge what Gemini actually heard
            spend.refund("radio_stt_sec", need - math.ceil(tok / TOK_PER_SEC))
        results = parse_reply(text, len(batch))
        for res in results.values():
            for f in ("transcript", "location_text", "summary"):
                if isinstance(res.get(f), str):
                    res[f] = blockify(res[f])
        incidents = []
        for k, c in enumerate(batch):
            res = results.get(k)
            if not res:
                radio.add_transcript(c["key"], c["id"], c["ts"], c["system"], c["talkgroup"], "failed",
                                     weight=c["weight"], seconds=c["seconds"])
                continue
            kind = str(res.get("kind") or "unit")
            pt = None
            if kind == "dispatch" and res.get("incident_type") not in (None, "noise"):
                loc, approx = rdio.system_place(c["inst"], c["system_label"])
                pt = locate(res.get("location_text"), res.get("municipality"), c.get("area"), loc, approx,
                            res.get("state"))
            radio.add_transcript(
                c["key"], c["id"], c["ts"], c["system"], c["talkgroup"], "done",
                weight=c["weight"], tg_label=c["tg"]["label"], tag=c["tg"]["tag"], kind=kind,
                incident_type=res.get("incident_type"), text=str(res.get("transcript") or "")[:1200],
                summary=str(res.get("summary") or "")[:200],
                location_text=(str(res["location_text"])[:160] if res.get("location_text") else None),
                units=[str(u)[:30] for u in (res.get("units") or [])][:12],
                priority=res.get("priority"), seconds=c["seconds"],
                lat=pt[0] if pt else None, lon=pt[1] if pt else None)
            done += 1
            if pt:
                _stats["located"] += 1
                incidents.append(_incident(c, res, pt))
        if incidents:
            snapshots.record(incidents)
            _stats["incidents"] += len(incidents)
    _stats["clips"] += done
    _stats["runs"] += 1
    _stats["last"] = now
    return done


def _loop() -> None:
    time.sleep(120)                                    # let the lane learn configs first
    while not _stop.is_set():
        try:
            run_once()
        except Exception as e:
            _stats["errors"] += 1
            _stats["last_error"] = str(e)[:200]
            log.warning("radio stt run failed: %s", e)
        _stop.wait(EVERY)


def scrub_existing() -> int:
    """Apply the block-level backstop to transcripts stored before it existed."""
    from apb.store import radio
    changed = 0
    with radio._lock:
        c = radio._conn()
        rows = c.execute("SELECT key, id, text, summary, location_text FROM radio_transcripts "
                         "WHERE status = 'done'").fetchall()
        for r in rows:
            new = [blockify(r[2]), blockify(r[3]), blockify(r[4])]
            if new != [r[2], r[3], r[4]]:
                c.execute("UPDATE radio_transcripts SET text = ?, summary = ?, location_text = ? "
                          "WHERE key = ? AND id = ?", (*new, r[0], r[1]))
                changed += 1
        c.commit()
    return changed


def start() -> bool:
    global _thread
    from apb.context import explain
    try:
        n = scrub_existing()
        if n:
            log.info("radio stt: scrubbed house numbers from %d stored transcripts", n)
    except Exception as e:
        log.warning("radio stt: scrub failed: %s", e)
    if _thread or off() or not explain.api_key() or daily_sec() <= 0:
        return False
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="radio-stt")
    _thread.start()
    log.info("radio stt: on, %d s of audio a day, weight >= %.2f", daily_sec(), MIN_WEIGHT)
    return True


def stop() -> None:
    _stop.set()


def status() -> dict:
    from apb.store import radio, spend
    return {**_stats, "running": bool(_thread and _thread.is_alive()), "daily_sec": daily_sec(),
            "used_sec": spend.used("radio_stt_sec"), "last_24h": radio.stt_stats(24.0),
            "mime_labels": dict(_mime_label)}
