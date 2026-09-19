"""Background samplers — the map watches places so baselines exist before anyone asks.

Two loops, one thread, leader-only (same election as the poller):

  facts sampler   every FACTS_EVERY seconds, run the facts collector in `lite` mode
                  (keyless sources + PurpleAir; no billed/quota lanes) over a rotating
                  batch of cells: cells of looks from the last 7 days first, then the
                  live-CAD metro centers. Each pass records the tracked scalars, so the
                  30-day baselines and "notable" flags fill in within days.
  camera sampler  every CAMS_EVERY seconds, send a rotating batch of camera stills to
                  Gemini with the structured-observation prompt only, and store the
                  vehicle / pedestrian / wet-road / visibility reads per cell — a
                  traffic-and-crowd index derived from the cameras themselves.

Both are cheap by construction: cell batches are small, camera reads are ~50 stills
an hour, and everything keyed-with-quota is excluded. APB_SAMPLER_OFF disables.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time

log = logging.getLogger(__name__)

FACTS_EVERY = float(os.environ.get("APB_SAMPLER_FACTS_SEC", "2700"))     # 45 min
FACTS_BATCH = int(os.environ.get("APB_SAMPLER_FACTS_BATCH", "40"))
CAMS_EVERY = float(os.environ.get("APB_SAMPLER_CAMS_SEC", "3600"))
CAMS_BATCH = int(os.environ.get("APB_SAMPLER_CAMS_BATCH", "50"))
_stop = threading.Event()
_thread: threading.Thread | None = None
_state = {"facts_runs": 0, "facts_cells": 0, "cams_runs": 0, "cams_frames": 0, "last_facts": 0.0, "last_cams": 0.0,
          "cursor": 0, "cam_cursor": 0, "errors": 0}


# Top US metros by population — watched even without a CAD feed so the NOTABLE layer
# and baselines cover the whole map, not only cities that publish dispatch data.
METROS: list[tuple[float, float]] = [
    (40.7128, -74.0060), (34.0522, -118.2437), (41.8781, -87.6298), (32.7767, -96.7970), (29.7604, -95.3698),
    (38.9072, -77.0369), (25.7617, -80.1918), (39.9526, -75.1652), (33.7490, -84.3880), (33.4484, -112.0740),
    (42.3601, -71.0589), (37.7749, -122.4194), (33.9806, -117.3755), (32.7157, -117.1611), (44.9778, -93.2650),
    (27.9506, -82.4572), (39.7392, -104.9903), (47.6062, -122.3321), (38.6270, -90.1994), (39.2904, -76.6122),
    (28.5383, -81.3792), (35.2271, -80.8431), (29.4241, -98.4936), (30.3322, -81.6557), (45.5152, -122.6784),
    (36.1627, -86.7816), (36.1699, -115.1398), (39.7684, -86.1581), (37.3382, -121.8863), (36.8508, -76.2859),
    (30.2672, -97.7431), (39.9612, -82.9988), (35.4676, -97.5164), (42.8864, -78.8784), (39.1031, -84.5120),
    (35.7796, -78.6382), (40.4406, -79.9959), (37.5407, -77.4360), (36.7378, -119.7871), (42.2808, -83.7430),
    (40.7608, -111.8910), (38.5816, -121.4944), (43.0389, -87.9065), (41.4993, -81.6944), (35.1495, -90.0490),
    (43.6150, -116.2023), (35.0844, -106.6504), (32.2226, -110.9747), (35.3733, -119.0187), (21.3069, -157.8583),
]


def _cells_to_watch(metro_centers: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """Recent-look cells first (deduped), then metro centers; stable order for the cursor."""
    from apb.store import looks as look_store, metrics as mstore
    seen: dict[str, tuple[float, float]] = {}
    try:
        for lk in look_store.query(max_age_hours=7 * 24, limit=300):
            b = lk["bounds"]
            if b.get("south") is None:
                continue
            lat, lon = (b["south"] + b["north"]) / 2, (b["west"] + b["east"]) / 2
            seen.setdefault(mstore.cell_for(lat, lon), (lat, lon))
    except Exception as e:
        log.info("sampler: looks unavailable: %s", e)
    for lat, lon in list(metro_centers) + METROS:
        seen.setdefault(mstore.cell_for(lat, lon), (lat, lon))
    return list(seen.values())


def _cell_bounds(lat: float, lon: float) -> dict:
    from apb.store.metrics import CELL_DEG
    import math
    s, w = math.floor(lat / CELL_DEG) * CELL_DEG, math.floor(lon / CELL_DEG) * CELL_DEG
    return {"south": round(s, 4), "north": round(s + CELL_DEG, 4), "west": round(w, 4), "east": round(w + CELL_DEG, 4)}


def facts_pass(metro_centers: list[tuple[float, float]]) -> int:
    from apb.context import facts
    cells = _cells_to_watch(metro_centers)
    if not cells:
        return 0
    start = _state["cursor"] % len(cells)
    take = min(FACTS_BATCH, len(cells))
    batch = (cells + cells)[start:start + take]
    _state["cursor"] = start + take
    done = 0
    for lat, lon in batch:
        if _stop.is_set():
            break
        try:
            facts.facts(_cell_bounds(lat, lon), timeout=12.0, lite=True)   # records + notable as a side effect
            done += 1
        except Exception as e:
            _state["errors"] += 1
            log.info("sampler facts %.3f,%.3f: %s", lat, lon, e)
    return done


_OBS_PROMPT = ("You are reading live traffic/public camera stills. For EACH attached image, in order, "
               "estimate what it shows. Reply with ONLY one line starting with OBS: followed by a JSON array of "
               "objects {\"i\": n (1-based image number), \"vehicles\": int, \"pedestrians\": int, \"road_wet\": bool, "
               "\"visibility\": \"good|reduced|poor\", \"notable\": \"short phrase or empty\"}. "
               "If a frame is black/unavailable use vehicles 0, pedestrians 0, visibility \"poor\", notable \"no image\".")


def cams_pass(registry) -> int:
    """Read a rotating batch of stills through Gemini (structured only) and store them."""
    from apb.context import explain
    from apb.store import metrics as mstore
    import base64
    if not explain.api_key():
        return 0
    if getattr(registry, "_loading", None):                  # inventories still warming after boot
        log.info("sampler cams: inventory still loading, skipping this pass")
        _state["last_cams"] = 0.0                            # try again next minute
        return 0
    rows = registry.query(limit=100000, online_only=True)
    rows = [r for r in rows if r.get("image_url")]
    if not rows:
        log.info("sampler cams: inventory has no still cameras yet")
        return 0
    # hash order, not id order: each batch spans vendors and time zones instead of
    # walking one state's inventory alphabetically
    import hashlib
    rows.sort(key=lambda r: hashlib.md5(r["id"].encode()).hexdigest())
    start = _state["cam_cursor"] % len(rows)
    take = min(CAMS_BATCH, len(rows))
    batch = (rows + rows)[start:start + take]
    _state["cam_cursor"] = start + take
    stored = 0
    for i in range(0, len(batch), 10):                     # ten stills per call
        chunk = batch[i:i + 10]
        parts = [{"text": _OBS_PROMPT}]
        got = []
        for cam in chunk:
            try:
                snap = registry.snapshot(cam["id"])
            except Exception:
                snap = None
            if snap and len(snap[0]) <= 700_000:
                parts.append({"inline_data": {"mime_type": snap[1], "data": base64.b64encode(snap[0]).decode()}})
                got.append(cam)
        if not got:
            log.info("sampler cams: no stills from %d cameras in chunk", len(chunk))
            continue
        try:
            text = explain.generate(parts, max_tokens=800)
        except Exception as e:
            _state["errors"] += 1
            log.info("sampler cams gemini: %s", e)
            continue
        m = re.search(r"OBS:\s*`?\s*(\[.*\])", text, re.S)      # models echo the prompt's backticks
        if not m:
            log.info("sampler cams: no OBS in reply: %r", text[:160])
            continue
        try:
            obs = json.loads(m.group(1))
        except ValueError:
            continue
        by_cell: dict[str, list] = {}
        zero_based = bool(obs) and any(int(o.get("i", 1)) == 0 for o in obs)   # asked for 1-based; not always honoured
        for o in obs:
            k = int(o.get("i", 0)) - (0 if zero_based else 1)
            if 0 <= k < len(got):
                cam = got[k]
                by_cell.setdefault(mstore.cell_for(cam["lat"], cam["lon"]), []).append({**o, "camera_id": cam["id"]})
        for cell, rs in by_cell.items():
            stored += mstore.record_camera_obs(cell, rs)
    return stored


def _loop(metro_centers, registry):
    time.sleep(90)                                        # let the inventories warm first
    while not _stop.is_set():
        now = time.time()
        if now - _state["last_facts"] >= FACTS_EVERY:
            _state["last_facts"] = now
            try:
                n = facts_pass(metro_centers)
                _state["facts_runs"] += 1
                _state["facts_cells"] += n
                log.info("sampler: facts over %d cells", n)
            except Exception as e:
                _state["errors"] += 1
                log.warning("sampler facts pass failed: %s", e)
        if now - _state["last_cams"] >= CAMS_EVERY and not os.environ.get("APB_CAMSAMPLER_OFF"):
            _state["last_cams"] = now
            try:
                n = cams_pass(registry)
                _state["cams_runs"] += 1
                _state["cams_frames"] += n
                log.info("sampler: %d camera frames read", n)
            except Exception as e:
                _state["errors"] += 1
                log.warning("sampler cams pass failed: %s", e)
        _stop.wait(60)


def start(metro_centers, registry) -> None:
    global _thread
    if _thread or os.environ.get("APB_SAMPLER_OFF", "").lower() in ("1", "true", "yes", "on"):
        return
    _thread = threading.Thread(target=_loop, args=(metro_centers, registry), daemon=True, name="sampler")
    _thread.start()


def stop() -> None:
    _stop.set()


def status() -> dict:
    return {**_state, "running": bool(_thread and _thread.is_alive())}
