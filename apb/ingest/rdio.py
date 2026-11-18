"""Rdio Scanner activity lane — how busy are the radios, without listening to them.

Hobbyists run Rdio Scanner (github.com/chuot/rdio-scanner) to publish their own SDR
captures of public-safety radio systems. An instance that needs no access code will
list its recent calls as metadata only — id, time, system, talkgroup — with no audio.
This lane asks each open instance for its newest calls every `APB_RDIO_EVERY` seconds
(600) and turns the answer into a call count per system, which the radio store scores
against that system's own normal. A county's fire talkgroups going from 20 calls in ten
minutes to 90 is a signal hours before a press release.

Rules this lane keeps:
  * metadata only: one `LCL` request (200 newest calls) per visit; audio is never asked for
  * never an access code: an instance that answers `PIN` is marked closed and left alone
  * one short connection per instance per visit, a handful of instances per minute,
    exponential backoff to six hours on failure
  * not from the home address: every connection goes through `APB_RDIO_PROXY`
    (a SOCKS proxy; in production the WARP egress helper). Without a proxy the lane
    does not start unless `APB_RDIO_DIRECT=1` is set deliberately.

The instance list (`APB_RDIO_INSTANCES`, default `rdio_instances.json` beside the
state DB) comes from `apb.discover.rdio_probe`. It holds operators' addresses and is
kept out of git; the API only ever shows a short hash and the system labels.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import math
import os
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

UA = "apb/0.1 (+https://panoptes.run; public-safety activity map; call metadata only)"
EVERY = float(os.environ.get("APB_RDIO_EVERY", "600"))
PER_TICK = int(os.environ.get("APB_RDIO_PER_TICK", "6"))
_SAMPLE = 200
_CFG_TTL = 24 * 3600.0

_stop = threading.Event()
_thread: threading.Thread | None = None
_instances: list[dict] = []
_cfg: dict[str, tuple[float, dict]] = {}       # key -> (fetched at, {system id: {...}})
_stats = {"polls": 0, "calls": 0, "errors": 0, "closed": 0, "last": 0.0, "audio_fetched": 0,
          "audio_refused": 0, "reloads": 0}
_loaded_mtime = 0.0
# One budget per instance for every audio request (playback + transcription): these
# are volunteers' home connections, so a popular page must not turn into a flood.
CAL_PER_MIN = int(os.environ.get("APB_RDIO_CAL_PER_MIN", "20"))
_cal_hits: dict[str, deque] = {}
_cal_lock = threading.Lock()


def off() -> bool:
    return os.environ.get("APB_RDIO_OFF", "").strip().lower() in ("1", "true", "yes", "on")


def proxy() -> str | None:
    return os.environ.get("APB_RDIO_PROXY", "").strip() or None


def instances_path() -> Path:
    explicit = os.environ.get("APB_RDIO_INSTANCES", "").strip()
    if explicit:
        return Path(explicit)
    return Path(os.environ.get("APB_DB_PATH", "data/apb.sqlite")).parent / "rdio_instances.json"


def key_of(inst: dict) -> str:
    return hashlib.sha1(f"{inst['host']}:{inst.get('port') or 443}".encode()).hexdigest()[:10]


def locate(place: str | None) -> tuple[float, float] | None:
    """(lat, lon) for a "City, ST" / "City, ST, CC" string from the probe file."""
    if not place:
        return None
    from apb.common.states import state_centroid
    from apb.fusion.places import _gaz
    parts = [p.strip() for p in place.split(",")]
    city = parts[0].lower().replace(".", "")
    hit = _gaz().get(city)
    us = len(parts) == 2 and len(parts[1]) == 2 and parts[1].isupper()
    if us:
        clat, clon = state_centroid(parts[1])
        if clat is None:
            return (hit[0], hit[1]) if hit else None
        if hit:
            km = math.hypot(hit[0] - clat, (hit[1] - clon) * math.cos(math.radians(clat))) * 111.0
            if km <= 500.0:
                return hit[0], hit[1]
        return clat, clon                         # right state, wrong (or unknown) city
    return (hit[0], hit[1]) if hit else None


def load_instances(path: Path | None = None) -> list[dict]:
    """Open instances from the probe file, one per distinct system set."""
    path = path or instances_path()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    seen: dict[tuple, dict] = {}
    out: list[dict] = []
    rows = [r for r in raw.get("instances", []) if r.get("state") == "open" and r.get("systems")]
    # connect by name where there is one (home addresses change); a duplicate reached
    # by address on a home line still tells us where the radio is
    rows.sort(key=lambda r: (r["host"].replace(".", "").isdigit(), bool(r.get("dc"))))
    for r in rows:
        sig = (tuple(sorted(str(s.get("label")) for s in r["systems"])), r.get("talkgroups"))
        place = None if r.get("dc") else r.get("place")
        if sig in seen:
            if place and not seen[sig]["place"]:
                seen[sig]["place"], seen[sig]["loc"] = place, locate(place)
            continue
        inst = {"host": r["host"], "port": r.get("port"), "scheme": r.get("scheme") or "ws",
                "place": place, "loc": locate(place),
                "labels": {s.get("id"): str(s.get("label") or "") for s in r["systems"]}}
        seen[sig] = inst
        out.append(inst)
    for inst in out:
        inst["key"] = key_of(inst)
    return out


_places = {"at": 0.0, "learned": {}, "profiles": {}}


def _place_tables() -> tuple[dict, dict]:
    if time.time() - _places["at"] > 60.0:
        from apb.store import radio
        try:
            _places.update(at=time.time(), learned=radio.learned_places(), profiles=radio.profiles())
        except Exception as e:
            log.info("rdio: place tables unavailable: %s", e)
            _places["at"] = time.time()
    return _places["learned"], _places["profiles"]


def place_info(inst: dict, label: str, system=None) -> dict:
    """Where a system is and how we know. Best evidence first:
      dispatches  the median of its own geocoded dispatches (3 or more)
      profile     the area the model read off the system and talkgroup names,
                  checked against the Census state at that point
      label       the system label resolved in the gazetteer near the listener
      listener    the volunteer's own town — where the antenna is, not the radio
    Listener and weak profiles are approximate."""
    from apb.fusion.places import resolve_place
    key = inst.get("key")
    learned, profiles = _place_tables()
    if system is not None:
        lp = learned.get((key, system))
        if lp:
            return {"loc": (lp["lat"], lp["lon"]), "approx": False, "by": "dispatches", "area": None,
                    "state": (profiles.get((key, system)) or {}).get("state")}
        pf = profiles.get((key, system))
        if pf and pf.get("lat") is not None and (pf.get("confidence") or 0) >= 0.4:
            return {"loc": (pf["lat"], pf["lon"]), "approx": bool(pf.get("statewide"))
                    or (pf.get("confidence") or 0) < 0.6, "by": "profile", "area": pf.get("area"),
                    "state": pf.get("state")}
    hit = resolve_place(label) if label else None
    base = inst.get("loc")
    if hit and base:
        km = math.hypot(hit.lat - base[0], (hit.lon - base[1]) * math.cos(math.radians(base[0]))) * 111.0
        if km <= 400.0:
            return {"loc": (hit.lat, hit.lon), "approx": False, "by": "label", "area": None, "state": None}
        return {"loc": base, "approx": True, "by": "listener", "area": inst.get("place"), "state": None}
    if hit:
        return {"loc": (hit.lat, hit.lon), "approx": True, "by": "label", "area": None, "state": None}
    return {"loc": base, "approx": True, "by": "listener", "area": inst.get("place"), "state": None}


def system_place(inst: dict, label: str, system=None) -> tuple[tuple[float, float] | None, bool]:
    p = place_info(inst, label, system)
    return p["loc"], p["approx"]


def count_calls(results: list[dict], last_id: int | None) -> tuple[int, dict[int, dict]]:
    """(calls since last_id, {system: {"n": calls, "tg": {talkgroup: n}}}) from the
    newest-first sample. Ids are sequential per instance, so the id gap is the exact
    call count even when it exceeds the sample; the sample then gives the shares."""
    ids = [r.get("id") for r in results if isinstance(r.get("id"), int)]
    if not ids or last_id is None:
        return 0, {}
    newest = max(ids)
    total = newest - last_id
    if total <= 0:
        return 0, {}
    fresh = [r for r in results if isinstance(r.get("id"), int) and r["id"] > last_id]
    scale = total / len(fresh) if fresh and total > len(fresh) else 1.0
    per: dict[int, dict] = {}
    for r in fresh:
        s = per.setdefault(r.get("system"), {"n": 0.0, "tg": {}})
        s["n"] += scale
        s["tg"][r.get("talkgroup")] = s["tg"].get(r.get("talkgroup"), 0) + 1
    for s in per.values():
        s["n"] = round(s["n"])
    return total, per


async def _ask(inst: dict, want_cfg: bool) -> dict:
    import websockets
    port = inst.get("port")
    url = f"{inst['scheme']}://{inst['host']}" + (f":{port}" if port else "") + "/"
    kw = {"open_timeout": 15, "max_size": 16_000_000, "user_agent_header": UA}
    if proxy():
        kw["proxy"] = proxy()
    if inst["scheme"] == "wss" and inst["host"].replace(".", "").isdigit():
        import ssl
        ctx = ssl.create_default_context()        # bare addresses have no matching certificate
        ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE
        kw["ssl"] = ctx
    out: dict = {}
    async with websockets.connect(url, **kw) as ws:
        if want_cfg:
            await ws.send(json.dumps(["CFG"]))
        await ws.send(json.dumps(["LCL", {"limit": _SAMPLE, "offset": 0, "sort": -1}]))
        for _ in range(6):
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            cmd = msg[0] if isinstance(msg, list) and msg else None
            body = msg[1] if isinstance(msg, list) and len(msg) > 1 else None
            if cmd == "PIN":
                out["pin"] = True
                return out
            if cmd == "CFG" and isinstance(body, dict):
                out["cfg"] = body
            if cmd == "LCL" and isinstance(body, dict):
                out["results"] = body.get("results") or []
                return out
    return out


def _systems_from_cfg(cfg: dict) -> dict:
    out = {}
    for s in cfg.get("systems") or []:
        tgs = {}
        for t in s.get("talkgroups") or []:
            tgs[t.get("id")] = {"label": str(t.get("label") or t.get("id"))[:60],
                                "name": str(t.get("name") or "")[:80],
                                "tag": str(t.get("tag") or "Untagged")[:40],
                                "group": str(t.get("group") or "")[:40]}
        out[s.get("id")] = {"label": str(s.get("label") or "")[:80],
                            "tags": {k: v["tag"] for k, v in tgs.items()}, "tg": tgs}
    return out


def talkgroup(key: str, system, tg) -> dict:
    """Label / name / tag / group for one talkgroup (from the instance's own config)."""
    s = _cfg.get(key, (0, {}))[1].get(system, {})
    return (s.get("tg") or {}).get(tg) or {"label": str(tg), "name": "", "tag": "Untagged", "group": ""}


def instance(key: str) -> dict | None:
    return next((i for i in _instances if i["key"] == key), None)


def parse_ts(v) -> float | None:
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def cal_allowed(key: str, n: int = 1, now: float | None = None) -> bool:
    """Reserve n audio requests against the instance's per-minute allowance."""
    now = now or time.time()
    with _cal_lock:
        q = _cal_hits.setdefault(key, deque())
        while q and q[0] < now - 60.0:
            q.popleft()
        if len(q) + n > CAL_PER_MIN:
            return False
        q.extend([now] * n)
        return True


def audio_mime(name: str | None, data: bytes) -> str:
    if data[:3] == b"ID3" or (len(data) > 1 and data[0] == 0xFF and data[1] & 0xE0 == 0xE0):
        return "audio/mpeg"
    if data[4:8] == b"ftyp":
        return "audio/mp4"
    if data[:4] == b"RIFF":
        return "audio/wav"
    if data[:4] == b"OggS":
        return "audio/ogg"
    ext = (name or "").rsplit(".", 1)[-1].lower()
    return {"mp3": "audio/mpeg", "m4a": "audio/mp4", "aac": "audio/aac", "wav": "audio/wav",
            "ogg": "audio/ogg"}.get(ext, "application/octet-stream")


def _audio_bytes(a) -> bytes:
    if isinstance(a, dict) and isinstance(a.get("data"), list):
        return bytes(a["data"])
    if isinstance(a, list):
        return bytes(a)
    if isinstance(a, str):
        try:
            return base64.b64decode(a)
        except ValueError:
            return b""
    return b""


def _connect(inst: dict) -> tuple[str, dict]:
    port = inst.get("port")
    url = f"{inst['scheme']}://{inst['host']}" + (f":{port}" if port else "") + "/"
    kw = {"open_timeout": 15, "max_size": 16_000_000, "user_agent_header": UA}
    if proxy():
        kw["proxy"] = proxy()
    if inst["scheme"] == "wss" and inst["host"].replace(".", "").isdigit():
        import ssl
        ctx = ssl.create_default_context()        # bare addresses have no matching certificate
        ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE
        kw["ssl"] = ctx
    return url, kw


async def _fetch_calls(inst: dict, ids: list[int]) -> dict:
    """{call id: (audio bytes, mime)} for the requested calls, over ONE connection.
    Only the audio and its type are kept; the transmitting unit's id is discarded."""
    import websockets
    url, kw = _connect(inst)
    out: dict = {}
    async with websockets.connect(url, **kw) as ws:
        for cid in ids:
            await ws.send(json.dumps(["CAL", cid]))
            for _ in range(8):
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
                if isinstance(raw, bytes):
                    continue
                msg = json.loads(raw)
                cmd = msg[0] if isinstance(msg, list) and msg else None
                body = msg[1] if isinstance(msg, list) and len(msg) > 1 else None
                if cmd == "PIN":
                    out["__pin__"] = True
                    return out
                if cmd == "CAL" and isinstance(body, dict):
                    data = _audio_bytes(body.get("audio"))
                    if data:
                        out[body.get("id", cid)] = (data, audio_mime(body.get("audioName"), data))
                    break
    return out


def fetch_calls(inst: dict, ids: list[int]) -> dict:
    """Synchronous wrapper with the per-instance allowance; {} when over it."""
    ids = list(dict.fromkeys(int(i) for i in ids))[:10]
    if not ids:
        return {}
    if not proxy() and os.environ.get("APB_RDIO_DIRECT", "") != "1":
        return {}
    if not cal_allowed(inst["key"], len(ids)):
        _stats["audio_refused"] += len(ids)
        return {}
    got = asyncio.run(_fetch_calls(inst, ids))
    if got.pop("__pin__", None):
        from apb.store import radio
        radio.set_state(inst["key"], state="closed", next_at=time.time() + 30 * 86400.0)
        _stats["closed"] += 1
    _stats["audio_fetched"] += len(got)
    return got


def poll_instance(inst: dict, now: float | None = None, ask=None) -> int:
    """Visit one instance and store its counts. Returns calls seen (0 on first visit)."""
    from apb.store import radio
    now = now or time.time()
    key = inst["key"]
    st = radio.state(key)
    have = _cfg.get(key)
    want_cfg = not have or now - have[0] > _CFG_TTL
    try:
        got = (ask or (lambda i, w: asyncio.run(_ask(i, w))))(inst, want_cfg)
    except Exception as e:
        fails = (st["fails"] or 0) + 1
        radio.set_state(key, fails=fails, state="failing" if fails >= 3 else st["state"],
                        next_at=now + min(6 * 3600.0, EVERY * 2 ** min(fails, 6)))
        _stats["errors"] += 1
        log.info("rdio %s: %s: %s", key, type(e).__name__, str(e)[:100])
        return 0
    if got.get("pin"):
        radio.set_state(key, state="closed", next_at=now + 30 * 86400.0)
        _stats["closed"] += 1
        log.info("rdio %s now asks for an access code — leaving it alone", key)
        return 0
    if got.get("cfg"):
        _cfg[key] = (now, _systems_from_cfg(got["cfg"]))
    systems = _cfg.get(key, (0, {}))[1]
    results = got.get("results")
    if results is None:
        radio.set_state(key, fails=(st["fails"] or 0) + 1, next_at=now + EVERY)
        return 0
    ids = [r["id"] for r in results if isinstance(r.get("id"), int)]
    newest = max(ids) if ids else st["last_id"]
    radio.add_recent(key, [(r["id"], parse_ts(r.get("dateTime")) or now, r.get("system"), r.get("talkgroup"))
                           for r in results if isinstance(r.get("id"), int)
                           and (st["last_id"] is None or r["id"] > st["last_id"])])
    total, per = count_calls(results, st["last_id"])
    if st["last_id"] is not None and st["last_poll"] and (newest or 0) >= st["last_id"]:
        counts: dict[int, tuple[int, dict]] = {}
        for sid in set(systems) | set(per):
            row = per.get(sid, {"n": 0, "tg": {}})
            tagmap = systems.get(sid, {}).get("tags", {})
            tags: dict[str, int] = {}
            for tg, n in row["tg"].items():
                tag = tagmap.get(tg, "Untagged")
                tags[tag] = tags.get(tag, 0) + n
            counts[sid] = (row["n"], tags)
        lon = inst["loc"][1] if inst.get("loc") else None
        radio.record(key, counts, now - st["last_poll"], lon=lon, now=now)
    radio.set_state(key, last_id=newest, last_poll=now, state="open", fails=0, next_at=now + EVERY)
    _stats["polls"] += 1
    _stats["calls"] += total
    _stats["last"] = now
    return total


def _due(now: float) -> list[dict]:
    from apb.store import radio
    known = radio.states()
    due = []
    for inst in _instances:
        st = known.get(inst["key"])
        if st and st.get("state") == "closed":
            continue
        if not st or (st.get("next_at") or 0) <= now:
            due.append((st.get("last_poll") or 0 if st else 0, inst))
    due.sort(key=lambda x: x[0])
    return [i for _, i in due[:PER_TICK]]


def maybe_reload() -> bool:
    """Pick up a re-probed instance list without a restart."""
    global _instances, _loaded_mtime
    try:
        mtime = instances_path().stat().st_mtime
    except OSError:
        return False
    if mtime <= _loaded_mtime:
        return False
    fresh = load_instances()
    _loaded_mtime = mtime
    if not fresh:
        return False
    before = {i["key"] for i in _instances}
    _instances = fresh
    _stats["reloads"] += 1
    log.info("rdio: instance list reloaded — %d instances (%d new)", len(fresh),
             len({i["key"] for i in fresh} - before))
    return True


def _loop() -> None:
    time.sleep(45)
    while not _stop.is_set():
        try:
            maybe_reload()
            for inst in _due(time.time()):
                if _stop.is_set():
                    break
                poll_instance(inst)
                _stop.wait(2.0)
        except Exception as e:
            _stats["errors"] += 1
            log.warning("rdio loop: %s", e)
        _stop.wait(60.0)


def start() -> bool:
    """Start the lane if there is an instance list and an egress proxy."""
    global _thread, _instances, _loaded_mtime
    if _thread or off():
        return False
    _instances = load_instances()
    if not _instances:
        return False
    try:
        _loaded_mtime = instances_path().stat().st_mtime
    except OSError:
        pass
    if not proxy() and os.environ.get("APB_RDIO_DIRECT", "") != "1":
        log.warning("rdio: %d instances listed but APB_RDIO_PROXY is not set — lane not started "
                    "(it must not connect from this host's own address)", len(_instances))
        return False
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="rdio")
    _thread.start()
    log.info("rdio: watching %d open instances (%d systems) every %ds via %s",
             len(_instances), sum(len(i["labels"]) for i in _instances), int(EVERY),
             "proxy" if proxy() else "DIRECT")
    return True


def stop() -> None:
    _stop.set()


def _label(inst: dict, system) -> str:
    live = _cfg.get(inst["key"], (0, {}))[1].get(system, {}).get("label")
    return live or inst["labels"].get(system) or f"system {system}"


def activity(max_age_sec: float = 1800.0) -> list[dict]:
    """Latest count per system with its label, place and surprise — for the API."""
    from apb.store import radio
    by_key = {i["key"]: i for i in _instances}
    out = []
    for r in radio.latest(max_age_sec):
        inst = by_key.get(r["key"])
        if not inst:
            continue
        label = _label(inst, r["system"])
        pl = place_info(inst, label, r["system"])
        loc = pl["loc"]
        top = sorted(r["tags"].items(), key=lambda kv: -kv[1])[:3]
        out.append({"id": f"{r['key']}:{r['system']}", "system": label, "ts": r["ts"],
                    "calls": r["calls"], "per10": r["per10"], "normal": r["base"], "z": r["z"],
                    "tags": dict(top), "lat": loc[0] if loc else None,
                    "lon": loc[1] if loc else None, "approx": pl["approx"],
                    "placed_by": pl["by"], "area": pl["area"] or inst.get("place")})
    out.sort(key=lambda d: (-(d["z"] if d["z"] is not None else -99), -d["per10"]))
    return out


def status() -> dict:
    from apb.store import radio
    return {**_stats, "running": bool(_thread and _thread.is_alive()), "listed": len(_instances),
            "via_proxy": bool(proxy()), **radio.stats()}
