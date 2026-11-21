"""Trunking Recorder web players: public scanner call lists, read through the egress proxy.

Trunking Recorder (Windows, by Jason Veenstra) serves a public "listen" page on many
hobbyists' machines (listen.sac-radio.com, listen.placer-radio.com, ...). Its page lists
calls by POSTing a DataTables request to `/calljson`; each row has a sequential
`DT_RowId`, `StartTime` (UTC), `SystemID`, `TargetID`/`TargetLabel`/`TargetTag` (the
talkgroup) and `AudioFilename` (an MP3 path on the same server). `SourceID` (the radio
unit) is never stored.

This lane polls each open instance every APB_TRUNKREC_EVERY seconds (120) for its newest
calls and records them like Rdio calls (radio_recent + counts into radio_calls), so
transcription, threads, the map and the live feed treat them like any scanner. Audio is
fetched only when a call is transcribed or played, within the per-instance allowance.
Every request goes through APB_RDIO_PROXY (WARP) — never from the home address.
Instance list: state/trunkrec_instances.json (out of git; written by
apb.discover.trunkrec_probe).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
from collections import OrderedDict
from datetime import datetime
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

EVERY = float(os.environ.get("APB_TRUNKREC_EVERY", "120"))
SAMPLE = 100
UA = "apb/0.1 (+https://panoptes.run; public-safety map)"
_stop = threading.Event()
_thread: threading.Thread | None = None
_insts: list[dict] = []
_audio: "OrderedDict[tuple, str]" = OrderedDict()
_AUDIO_MAX = 60000
_stats = {"listed": 0, "polls": 0, "calls": 0, "errors": 0, "last": 0.0, "last_error": None}


def off() -> bool:
    return os.environ.get("APB_TRUNKREC_OFF", "").strip().lower() in ("1", "true", "yes", "on")


def list_path() -> Path:
    from apb.store import snapshots
    return Path(os.environ.get("APB_TRUNKREC_LIST", "").strip() or snapshots.DB_PATH.parent / "trunkrec_instances.json")


def key_of(base: str) -> str:
    return hashlib.sha1(("trunkrec:" + base.rstrip("/")).encode()).hexdigest()[:10]


def system_id(name: str) -> int:
    """A stable small integer for a Trunking Recorder SystemID string."""
    return int(hashlib.sha1(str(name).encode()).hexdigest()[:5], 16) % 100000


def tg_id(target: str) -> int | None:
    m = re.match(r"\s*(\d{1,9})", str(target or ""))
    return int(m.group(1)) if m else None


def tag_guess(label: str, tag: str = "") -> str:
    """A talkgroup tag in the Rdio vocabulary from Trunking Recorder's label / tag."""
    if tag and tag.strip("|"):
        return tag.split("|")[0][:40]
    t = (label or "").lower()
    disp = bool(re.search(r"disp|dispatch|\bdsp\b|main\b|primary", t))
    fire = bool(re.search(r"\bfire\b|\bfd\b|rescue|\bfire\w*", t))
    law = bool(re.search(r"police|\bpd\b|sheriff|\bso\b|patrol|law|\bchp\b|trooper", t))
    ems = bool(re.search(r"\bems\b|medic|ambulance|\bals\b", t))
    if fire:
        return "Fire Dispatch" if disp else "Fire-Tac"
    if ems:
        return "EMS Dispatch" if disp else "EMS-Tac"
    if law:
        return "Law Dispatch" if disp else "Law Tac"
    return "Untagged"


def client() -> httpx.Client:
    from apb.ingest import rdio
    proxy = rdio.proxy()
    # many players sit on a bare address with a self-signed certificate; the data is public
    return httpx.Client(timeout=20.0, headers={"User-Agent": UA}, proxy=proxy, follow_redirects=True,
                        verify=False)


def calls_request(length: int = SAMPLE) -> dict:
    return {"draw": 1, "start": 0, "length": length, "order": [{"column": 0, "dir": "desc"}],
            "columns": [{"data": "CallTime"}], "search": {"value": ""}, "SmartSort": False}


def load() -> list[dict]:
    try:
        raw = json.loads(list_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    out = []
    for r in raw.get("instances", []):
        if r.get("state") != "open" or not r.get("base"):
            continue
        place = None if r.get("dc") else r.get("place")
        labels = {system_id(s): s for s in r.get("systems") or []}
        out.append({"key": key_of(r["base"]), "kind": "trunkrec", "base": r["base"].rstrip("/"),
                    "host": r.get("host"), "port": r.get("port"), "place": place,
                    "loc": _locate(place), "labels": labels})
    return out


def _locate(place: str | None):
    if not place:
        return None
    try:
        from apb.ingest.rdio import locate
        return locate(place)
    except Exception:
        return None


def instances() -> list[dict]:
    return _insts


def _register(inst: dict, rows: list[dict]) -> None:
    """Keep the Rdio lane's config view of this instance current: systems + talkgroups."""
    from apb.ingest import rdio
    have = rdio._cfg.get(inst["key"], (0, {}))[1]
    for r in rows:
        sid, tg = system_id(r.get("SystemID")), tg_id(r.get("TargetID"))
        if tg is None:
            continue
        s = have.setdefault(sid, {"label": str(r.get("SystemLabel") or r.get("SystemID") or "")[:80],
                                  "tags": {}, "tg": {}})
        inst["labels"].setdefault(sid, s["label"])
        if tg not in s["tg"]:
            label = str(r.get("TargetLabel") or tg).split("|")[0][:40]
            tag = tag_guess(label, str(r.get("TargetTag") or ""))
            s["tg"][tg] = {"label": label, "name": label, "tag": tag, "group": ""}
            s["tags"][tg] = tag
    rdio._cfg[inst["key"]] = (time.time() + 10 * 365 * 86400.0, have)


def parse_rows(data: dict) -> list[dict]:
    """Rdio-shaped call rows from a /calljson reply: id, ts, system, talkgroup."""
    out = []
    for r in data.get("data") or []:
        cid, tg = r.get("DT_RowId"), tg_id(r.get("TargetID"))
        if not isinstance(cid, int) or tg is None:
            continue
        try:
            ts = datetime.fromisoformat(str(r.get("StartTime")).replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
        out.append({"id": cid, "ts": ts, "system": system_id(r.get("SystemID")), "talkgroup": tg,
                    "audio": r.get("AudioFilename")})
    return out


def poll(inst: dict, now: float | None = None, post=None) -> int:
    """Read one instance's newest calls and record them. Returns calls since last poll."""
    from apb.ingest import rdio
    from apb.store import radio
    now = now or time.time()
    key = inst["key"]
    st = radio.state(key)
    try:
        if post:
            data = post(inst, calls_request())
        else:
            with client() as c:
                r = c.post(f"{inst['base']}/calljson", content=json.dumps(calls_request()),
                           headers={"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"})
                if r.status_code == 401:
                    radio.set_state(key, state="closed", next_at=now + 30 * 86400.0)
                    return 0
                r.raise_for_status()
                data = r.json()
    except (httpx.HTTPError, ValueError) as e:
        fails = (st["fails"] or 0) + 1
        radio.set_state(key, fails=fails, state="failing" if fails >= 3 else st["state"],
                        next_at=now + min(6 * 3600.0, EVERY * 2 ** min(fails, 6)))
        _stats["errors"] += 1
        _stats["last_error"] = f"{inst.get('host')}: {type(e).__name__}"
        return 0
    raw = data.get("data") or []
    _register(inst, raw)
    rows = parse_rows(data)
    for r in rows:
        if r.get("audio"):
            _audio[(key, r["id"])] = r["audio"]
    while len(_audio) > _AUDIO_MAX:
        _audio.popitem(last=False)
    last = st["last_id"]
    fresh = [r for r in rows if last is None or r["id"] > last]
    radio.add_recent(key, [(r["id"], r["ts"], r["system"], r["talkgroup"]) for r in fresh])
    newest = max([r["id"] for r in rows] + [last or 0]) or None
    total, per = rdio.count_calls(rows, last)
    if last is not None and st["last_poll"]:
        cfg = rdio._cfg.get(key, (0, {}))[1]
        counts = {}
        for sid in set(cfg) | set(per):
            row = per.get(sid, {"n": 0, "tg": {}})
            tagmap = cfg.get(sid, {}).get("tags", {})
            tags: dict[str, int] = {}
            for tg, n in row["tg"].items():
                tag = tagmap.get(tg, "Untagged")
                tags[tag] = tags.get(tag, 0) + n
            counts[sid] = (row["n"], tags)
        radio.record(key, counts, now - st["last_poll"], lon=inst["loc"][1] if inst.get("loc") else None, now=now)
    radio.set_state(key, last_id=newest, last_poll=now, state="open", fails=0, next_at=now + EVERY)
    _stats["polls"] += 1
    _stats["calls"] += total
    _stats["last"] = now
    return total


def fetch(inst: dict, ids: list[int], get=None) -> dict:
    """{id: (mp3 bytes, mime)} for calls whose audio path this lane has seen."""
    from apb.ingest import rdio
    out = {}
    want = [i for i in ids if (inst["key"], int(i)) in _audio]
    if not want or not rdio.cal_allowed(inst["key"], len(want)):
        return out
    try:
        c = None if get else client()
        for i in want:
            path = _audio[(inst["key"], int(i))]
            url = inst["base"] + (path if path.startswith("/") else "/" + path)
            data = get(url) if get else c.get(url).content
            if data:
                out[int(i)] = (data, rdio.audio_mime(path, data))
    except httpx.HTTPError as e:
        log.info("trunkrec audio %s: %s", inst.get("host"), type(e).__name__)
    finally:
        if not get and c is not None:
            c.close()
    return out


def _loop() -> None:
    from apb.store import radio
    _stop.wait(30)
    while not _stop.is_set():
        now = time.time()
        known = radio.states()
        for inst in list(_insts):
            if _stop.is_set():
                break
            st = known.get(inst["key"]) or {}
            if st.get("state") == "closed" or (st.get("next_at") or 0) > now:
                continue
            try:
                poll(inst)
            except Exception as e:
                _stats["errors"] += 1
                _stats["last_error"] = str(e)[:160]
            _stop.wait(2.0)
        _stop.wait(30.0)


def start() -> bool:
    global _thread, _insts
    from apb.ingest import rdio
    if _thread or off():
        return False
    if not rdio.proxy():
        log.info("trunkrec: no egress proxy — not reading from this address")
        return False
    _insts = load()
    _stats["listed"] = len(_insts)
    if not _insts:
        return False
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="trunkrec")
    _thread.start()
    log.info("trunkrec: %d instances, every %d s, through the proxy", len(_insts), EVERY)
    return True


def stop() -> None:
    _stop.set()


def status() -> dict:
    return {**_stats, "running": bool(_thread and _thread.is_alive())}
