"""Event-driven camera tasking — when something fires, go and look.

The background sampler reads a rotating ~50 stills an hour out of tens of thousands
of cameras, so the chance it happens to be looking at an incident is negligible.
Tasking inverts that: when a fused event crosses `APB_TASK_SCORE`, the nearest public
cameras are read by the vision model with the event described, and the model says
per frame whether the picture supports it.

A check is stored against the event (`event_camera_checks`), the per-frame reads go
into the normal camera-observation store, and a supporting check becomes a `camera`
signal in the fusion layer — an independent source kind that raises the event's
corroboration and score.

Bounded by construction: `APB_TASK_PER_RUN` events per run (3), one check per event
per `APB_TASK_RECHECK_MIN` (45), `APB_TASK_DAILY` model calls a day (150) through the
spend store, six stills per call, cameras within `APB_TASK_KM` (4). Runs on the
sampler thread; APB_TASKING_OFF disables.
"""
from __future__ import annotations

import base64
import json
import logging
import math
import os
import re
import sqlite3
import threading
import time

from apb.store import snapshots

log = logging.getLogger(__name__)

_lock = snapshots.db_lock
_ready = False
_qlock = threading.Lock()
_queue: dict[str, dict] = {}
_state = {"runs": 0, "checks": 0, "supported": 0, "no_cameras": 0, "errors": 0, "last": 0.0}

_MAX_STILLS = 6


def _num(env: str, default: float) -> float:
    try:
        return float(os.environ.get(env, default))
    except ValueError:
        return default


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS event_camera_checks (
            uid TEXT NOT NULL, ts REAL NOT NULL, lat REAL, lon REAL, top_type TEXT,
            cameras INTEGER, supports INTEGER, contradicts INTEGER, verdict TEXT,
            frames TEXT, overall TEXT);
        CREATE INDEX IF NOT EXISTS idx_ecc ON event_camera_checks(uid, ts);
        CREATE INDEX IF NOT EXISTS idx_ecc_ts ON event_camera_checks(ts);
        """)
        c.commit()
        _ready = True
    return c


def off() -> bool:
    return os.environ.get("APB_TASKING_OFF", "").strip().lower() in ("1", "true", "yes", "on")


def _top_type(e: dict) -> str:
    types = e.get("types") or {}
    if isinstance(types, str):
        try:
            types = json.loads(types)
        except ValueError:
            types = {}
    ranked = sorted(types.items(), key=lambda kv: -kv[1]) if isinstance(types, dict) else []
    return next((k for k, _ in ranked if k != "other"), ranked[0][0] if ranked else "other")


def _priority(e: dict) -> float:
    """New and rising events first — they are the ones a look can still inform."""
    boost = 1.5 if (e.get("trend") == "rising" or (e.get("age_min") or 0) < 60) else 1.0
    return (e.get("latest_score") or 0) * boost * (1.0 + 0.25 * ((e.get("source_count") or 1) - 1))


def last_check_ts(uid: str) -> float:
    with _lock:
        r = _conn().execute("SELECT MAX(ts) FROM event_camera_checks WHERE uid = ?", (uid,)).fetchone()
    return (r[0] or 0.0) if r else 0.0


def enqueue(events: list[dict]) -> int:
    """Offer this cycle's registry rows; the strongest unchecked ones are queued."""
    if off():
        return 0
    floor = _num("APB_TASK_SCORE", 5.0)
    recheck = _num("APB_TASK_RECHECK_MIN", 45.0) * 60.0
    now = time.time()
    added = 0
    for e in sorted(events, key=_priority, reverse=True):
        if (e.get("latest_score") or 0) < floor or e.get("lat") is None:
            continue
        if now - (e.get("last_seen") or 0) > 2400:
            continue                                   # no longer active
        if (e.get("age_min") or 0) > 360 and e.get("trend") != "rising":
            continue                                   # a standing cluster: nothing new to see
        if now - last_check_ts(e["uid"]) < recheck:
            continue
        with _qlock:
            if e["uid"] not in _queue and len(_queue) < 40:
                _queue[e["uid"]] = e
                added += 1
    return added


def nearest_cameras(registry, lat: float, lon: float, km: float, limit: int = _MAX_STILLS) -> list[dict]:
    """Registry cameras with a still, nearest first, within `km`."""
    dlat = km / 111.0
    coslat = max(0.2, math.cos(math.radians(lat)))
    dlon = dlat / coslat
    rows = registry.query(bbox=(lon - dlon, lat - dlat, lon + dlon, lat + dlat),
                          limit=100000, online_only=True, extras=False)
    out = []
    for r in rows:
        if not r.get("image_url"):
            continue
        d = math.hypot(r["lat"] - lat, (r["lon"] - lon) * coslat) * 111.0
        if d <= km:
            out.append((d, r))
    out.sort(key=lambda x: x[0])
    return [{**r, "_km": round(d, 2)} for d, r in out[:limit]]


def prompt_for(event: dict, cams: list[dict]) -> str:
    heads = event.get("summaries") or []
    if isinstance(heads, str):
        try:
            heads = json.loads(heads)
        except ValueError:
            heads = []
    reports = "; ".join(str(h)[:140] for h in heads[:3]) or "no text"
    names = "\n".join(f"{i + 1}. {c.get('name') or c['id']} ({c['_km']} km away)"
                      for i, c in enumerate(cams))
    return (
        "Dispatch, traffic and social signals suggest an ongoing incident near these public "
        f"cameras. Reported kind: {_top_type(event)}. Recent reports: {reports}\n"
        f"The attached images, in order, are live stills from:\n{names}\n"
        "For EACH image decide whether it shows clear visible evidence of that incident: "
        "emergency vehicles or flashing lights, smoke or flames, a crash scene, lanes blocked "
        "or traffic diverted, a gathered crowd, flooding. Ordinary traffic, a single stopped "
        "or parked vehicle, road works and normal night-time scenes are NOT evidence. "
        "Reply with ONLY two lines.\n"
        "Line 1 starts with OBS: followed by a JSON array of objects {\"i\": n (1-based image "
        "number), \"vehicles\": int, \"pedestrians\": int, \"road_wet\": bool, \"visibility\": "
        "\"good|reduced|poor\", \"evidence\": \"supports|contradicts|none\", \"notable\": "
        "\"short phrase or empty\"}. Use \"contradicts\" only when the camera clearly covers "
        "the spot and the scene is plainly normal; use \"none\" when the frame is unavailable, "
        "dark, or simply does not show the area.\n"
        "Line 2 starts with VERDICT: followed by yes, no or unclear, then a dash and one "
        "sentence on what the cameras show. Say yes only if at least one frame clearly shows "
        "the incident or the response to it; no if the frames cover the area and it looks "
        "normal; unclear otherwise.")


def overall_of(verdict: str) -> str:
    """yes / no / unclear from the head of the verdict line."""
    m = re.match(r"\W*(yes|no|unclear)\b", verdict.strip().lower())
    return m.group(1) if m else "unclear"


def parse_reply(text: str, n: int) -> tuple[list[tuple[int, dict]], str]:
    """([(frame index 0-based, observation)], verdict) from the model's two lines."""
    verdict = ""
    v = re.search(r"VERDICT:\s*(.+)", text)
    if v:
        verdict = v.group(1).strip().strip("`")[:300]
    m = re.search(r"OBS:\s*`?\s*(\[.*?\])\s*`?\s*(?:\n|$)", text, re.S)
    if not m:
        m = re.search(r"OBS:\s*`?\s*(\[.*\])", text, re.S)
    if not m:
        return [], verdict
    try:
        obs = json.loads(m.group(1))
    except ValueError:
        return [], verdict
    if not isinstance(obs, list):
        return [], verdict
    obs = [o for o in obs if isinstance(o, dict)]

    def idx(o) -> int:
        try:
            return int(o.get("i", 0))
        except (TypeError, ValueError):
            return -99
    zero_based = any(idx(o) == 0 for o in obs)         # asked for 1-based; not always honoured
    out = []
    for o in obs:
        k = idx(o) - (0 if zero_based else 1)
        if 0 <= k < n:
            out.append((k, o))
    return out, verdict


def record_check(uid: str, lat: float, lon: float, top_type: str, cameras: int,
                 supports: int, contradicts: int, verdict: str, frames: list[dict]) -> None:
    now = time.time()
    # the model's own bottom line decides; per-frame "supports" alone proved too lenient
    # (a single car on the shoulder "supported" a cluster the same reply called normal)
    overall = overall_of(verdict) if cameras else "none"
    if overall == "yes" and not supports:
        overall = "unclear"
    text = re.sub(r"^\W*(yes|no|unclear)\W+", "", verdict.strip(), flags=re.I) if cameras else verdict
    with _lock:
        c = _conn()
        c.execute("INSERT INTO event_camera_checks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                  (uid, now, lat, lon, top_type, cameras, supports, contradicts, text[:300],
                   json.dumps(frames)[:4000], overall))
        c.execute("DELETE FROM event_camera_checks WHERE ts < ?", (now - 14 * 86400,))
        c.commit()


def _row(r) -> dict:
    d = dict(r)
    try:
        d["frames"] = json.loads(d.get("frames") or "[]")
    except ValueError:
        d["frames"] = []
    return d


def checks_for(uid: str, limit: int = 20) -> list[dict]:
    with _lock:
        rows = _conn().execute("SELECT * FROM event_camera_checks WHERE uid = ? ORDER BY ts DESC LIMIT ?",
                               (uid, limit)).fetchall()
    return [_row(r) for r in rows]


def recent_checks(max_age_hours: float = 6.0) -> list[dict]:
    with _lock:
        rows = _conn().execute("SELECT * FROM event_camera_checks WHERE ts >= ? ORDER BY ts DESC",
                               (time.time() - max_age_hours * 3600,)).fetchall()
    return [_row(r) for r in rows]


def latest_by_uid(max_age_hours: float = 6.0) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for r in recent_checks(max_age_hours):               # newest first
        out.setdefault(r["uid"], r)
    return out


def check_event(registry, event: dict) -> dict | None:
    """Read the cameras nearest one event. Returns the stored check (None if skipped)."""
    from apb.context import explain
    from apb.store import metrics as mstore, spend
    lat, lon = event["lat"], event["lon"]
    top = _top_type(event)
    cams = nearest_cameras(registry, lat, lon, _num("APB_TASK_KM", 4.0))
    if not cams:
        _state["no_cameras"] += 1
        record_check(event["uid"], lat, lon, top, 0, 0, 0, "no public camera nearby", [])
        return {"cameras": 0}
    parts: list[dict] = []
    got: list[dict] = []
    for cam in cams:
        try:
            snap = registry.snapshot(cam["id"])
        except Exception:
            snap = None
        if snap and len(snap[0]) <= 700_000:
            parts.append({"inline_data": {"mime_type": snap[1], "data": base64.b64encode(snap[0]).decode()}})
            got.append(cam)
    if not got:
        record_check(event["uid"], lat, lon, top, 0, 0, 0, "nearby cameras returned no image", [])
        return {"cameras": 0}
    daily = int(_num("APB_TASK_DAILY", 150))
    if not spend.allow("gemini_tasking", 1, monthly_cap=daily * 31, daily_cap=daily):
        log.info("tasking: daily model budget reached")
        return None
    text = explain.generate([{"text": prompt_for(event, got)}] + parts, max_tokens=700,
                            feature="camera_tasking")
    obs, verdict = parse_reply(text, len(got))
    if not obs and not verdict:
        log.info("tasking: unparseable reply: %r", text[:160])
    frames, by_cell = [], {}
    for k, o in obs:
        cam = got[k]
        ev = str(o.get("evidence") or "none").lower()
        ev = ev if ev in ("supports", "contradicts") else "none"
        frames.append({"camera_id": cam["id"], "name": cam.get("name"), "km": cam["_km"],
                       "evidence": ev, "notable": str(o.get("notable") or "")[:120]})
        by_cell.setdefault(mstore.cell_for(cam["lat"], cam["lon"]), []).append({**o, "camera_id": cam["id"]})
    for cell, rows in by_cell.items():
        mstore.record_camera_obs(cell, rows)
    supports = sum(1 for f in frames if f["evidence"] == "supports")
    contradicts = sum(1 for f in frames if f["evidence"] == "contradicts")
    record_check(event["uid"], lat, lon, top, len(got), supports, contradicts, verdict, frames)
    _state["checks"] += 1
    _state["supported"] += 1 if (supports and overall_of(verdict) == "yes") else 0
    return {"cameras": len(got), "supports": supports, "contradicts": contradicts, "verdict": verdict}


def run(registry) -> int:
    """Drain up to APB_TASK_PER_RUN queued events. Called from the sampler loop."""
    from apb.context import explain
    if off() or not explain.api_key():
        return 0
    if getattr(registry, "_loading", None):               # inventories still warming
        return 0
    with _qlock:
        batch = sorted(_queue.values(), key=_priority, reverse=True)
        batch = batch[:int(_num("APB_TASK_PER_RUN", 3))]
        for e in batch:
            _queue.pop(e["uid"], None)
    done = 0
    for e in batch:
        try:
            if check_event(registry, e) is not None:
                done += 1
        except Exception as err:
            _state["errors"] += 1
            log.info("tasking %s: %s", e.get("uid"), err)
    if batch:
        _state["runs"] += 1
        _state["last"] = time.time()
    return done


def status() -> dict:
    with _qlock:
        queued = len(_queue)
    return {**_state, "queued": queued, "off": off()}
