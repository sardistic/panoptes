"""Box-metric history — turns snapshots into signals.

Every facts pass records its scalar metrics against a ~5 km grid cell. Later passes
over the same cell get a baseline (median, sample count, percentile rank of the
current value over the last 30 days), which is what lets the explainer say
"unusually high" instead of "high". Camera-derived observations (counts the model
read off stills) live here too, aggregated per cell for the last 24 h.
SQLite in the snapshot store's file, under the shared lock.
"""
from __future__ import annotations

import json
import math
import sqlite3
import time

from apb.store import snapshots

_lock = snapshots.db_lock
_ready = False
CELL_DEG = 0.05


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS box_metrics (
            cell TEXT NOT NULL, name TEXT NOT NULL, ts REAL NOT NULL, value REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_bm ON box_metrics(cell, name, ts);
        CREATE TABLE IF NOT EXISTS camera_obs (
            cell TEXT NOT NULL, camera_id TEXT, ts REAL NOT NULL, vehicles INTEGER, pedestrians INTEGER,
            road_wet INTEGER, visibility TEXT, notable TEXT);
        CREATE INDEX IF NOT EXISTS idx_co ON camera_obs(cell, ts);
        CREATE TABLE IF NOT EXISTS cell_notable (
            cell TEXT PRIMARY KEY, lat REAL, lon REAL, ts REAL NOT NULL, flags TEXT NOT NULL, lite INTEGER);
        CREATE TABLE IF NOT EXISTS cell_notable_log (cell TEXT NOT NULL, ts REAL NOT NULL, flags TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_cnl ON cell_notable_log(cell, ts);
        """)
        c.commit()
        _ready = True
    return c


def cell_for(lat: float, lon: float) -> str:
    return f"{math.floor(lat / CELL_DEG) * CELL_DEG:.2f},{math.floor(lon / CELL_DEG) * CELL_DEG:.2f}"


def record(cell: str, values: dict[str, float]) -> int:
    now = time.time()
    rows = [(cell, k, now, float(v)) for k, v in values.items()
            if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(float(v))]
    if not rows:
        return 0
    with _lock:
        c = _conn()
        # one sample per cell/metric per 10 minutes: repeated looks at one box do not stack
        c.execute("DELETE FROM box_metrics WHERE cell = ? AND ts > ?", (cell, now - 600))
        c.executemany("INSERT INTO box_metrics VALUES (?,?,?,?)", rows)
        c.execute("DELETE FROM box_metrics WHERE ts < ?", (now - 45 * 86400,))
        c.commit()
    return len(rows)


def baseline(cell: str, values: dict[str, float], days: int = 30) -> dict[str, dict]:
    """For each metric: median and sample count over `days`, and where the current
    value ranks (0-100 percentile) among past samples — only with >= 5 samples."""
    since = time.time() - days * 86400
    out: dict[str, dict] = {}
    with _lock:
        c = _conn()
        for name, cur in values.items():
            if not isinstance(cur, (int, float)) or isinstance(cur, bool):
                continue
            past = [r[0] for r in c.execute("SELECT value FROM box_metrics WHERE cell = ? AND name = ? AND ts >= ? AND ts < ?",
                                             (cell, name, since, time.time() - 600)).fetchall()]
            if len(past) < 5:
                continue
            past.sort()
            med = past[len(past) // 2]
            rank = round(100 * sum(1 for p in past if p <= cur) / len(past))
            out[name] = {"median": round(med, 2), "samples": len(past), "percentile": rank,
                         "vs_median": round(cur - med, 2)}
    return out


def record_camera_obs(cell: str, rows: list[dict]) -> int:
    now = time.time()
    with _lock:
        c = _conn()
        c.executemany("INSERT INTO camera_obs VALUES (?,?,?,?,?,?,?,?)", [
            (cell, r.get("camera_id"), now, r.get("vehicles"), r.get("pedestrians"),
             1 if r.get("road_wet") else 0 if r.get("road_wet") is not None else None,
             r.get("visibility"), (r.get("notable") or "")[:200]) for r in rows])
        c.execute("DELETE FROM camera_obs WHERE ts < ?", (now - 7 * 86400,))
        c.commit()
    return len(rows)


def camera_obs(cell: str, hours: float = 24.0) -> dict:
    since = time.time() - hours * 3600
    with _lock:
        rows = _conn().execute("SELECT vehicles, pedestrians, road_wet, visibility, notable, ts FROM camera_obs "
                               "WHERE cell = ? AND ts >= ? ORDER BY ts DESC", (cell, since)).fetchall()
    if not rows:
        return {}
    veh = [r[0] for r in rows if r[0] is not None]
    ped = [r[1] for r in rows if r[1] is not None]
    wet = [r[2] for r in rows if r[2] is not None]
    vis = {}
    for r in rows:
        if r[3]:
            vis[r[3]] = vis.get(r[3], 0) + 1
    return {"frames_read_24h": len(rows), "mean_vehicles_per_frame": round(sum(veh) / len(veh), 1) if veh else None,
            "mean_pedestrians_per_frame": round(sum(ped) / len(ped), 1) if ped else None,
            "wet_road_share": round(sum(wet) / len(wet), 2) if wet else None, "visibility": vis,
            "notable": [r[4] for r in rows if r[4]][:5]}


def record_notable(cell: str, lat: float, lon: float, flags: list[str], lite: bool = False) -> None:
    now = time.time()
    with _lock:
        c = _conn()
        # every pass is logged (flagged or not) so "flagged N of the last M passes" is honest;
        # one entry per cell per 10 minutes, 30-day retention
        c.execute("DELETE FROM cell_notable_log WHERE cell = ? AND ts > ?", (cell, now - 600))
        c.execute("INSERT INTO cell_notable_log VALUES (?,?,?)", (cell, now, json.dumps(flags)))
        c.execute("DELETE FROM cell_notable_log WHERE ts < ?", (now - 30 * 86400,))
        if flags:
            c.execute("INSERT INTO cell_notable (cell, lat, lon, ts, flags, lite) VALUES (?,?,?,?,?,?) "
                      "ON CONFLICT(cell) DO UPDATE SET lat=excluded.lat, lon=excluded.lon, ts=excluded.ts, "
                      "flags=excluded.flags, lite=excluded.lite", (cell, lat, lon, time.time(), json.dumps(flags), 1 if lite else 0))
        else:
            c.execute("DELETE FROM cell_notable WHERE cell = ?", (cell,))
        c.commit()


def notable_cells(max_age_hours: float = 6.0) -> list[dict]:
    with _lock:
        rows = _conn().execute("SELECT cell, lat, lon, ts, flags FROM cell_notable WHERE ts >= ? ORDER BY ts DESC",
                               (time.time() - max_age_hours * 3600,)).fetchall()
    out = [{"cell": r[0], "lat": r[1], "lon": r[2], "ts": r[3], "flags": json.loads(r[4])} for r in rows]
    for o in out:
        h = notable_history(o["cell"], days=7)
        o["streak"] = h["flagged_recent"]
        o["passes"] = h["passes"]
    return out


def notable_history(cell: str, days: int = 7) -> dict:
    """How persistent a cell's flags are: passes logged, how many raised flags, the
    flagged count within the last five passes, and the flags themselves (newest first)."""
    with _lock:
        rows = _conn().execute("SELECT ts, flags FROM cell_notable_log WHERE cell = ? AND ts >= ? ORDER BY ts DESC",
                               (cell, time.time() - days * 86400)).fetchall()
    recent = [{"ts": r[0], "flags": json.loads(r[1])} for r in rows]
    flagged = [r for r in recent if r["flags"]]
    seen: dict[str, int] = {}
    for r in flagged:
        for f in r["flags"]:
            k = f.split(" (")[0]
            seen[k] = seen.get(k, 0) + 1
    return {"passes": len(recent), "flagged": len(flagged), "flagged_recent": sum(1 for r in recent[:5] if r["flags"]),
            "recurring": sorted(seen.items(), key=lambda kv: -kv[1])[:6],
            "recent": [{"ts": r["ts"], "flags": r["flags"]} for r in recent[:5]]}


def camera_obs_series(cell: str, hours: float = 24.0, bins: int = 12) -> list[dict]:
    """Camera-derived activity binned over time (mean vehicles / pedestrians per frame,
    frames per bin); null where nothing was read. Oldest bin first."""
    now = time.time()
    since = now - hours * 3600
    with _lock:
        rows = _conn().execute("SELECT ts, vehicles, pedestrians FROM camera_obs WHERE cell = ? AND ts >= ?",
                               (cell, since)).fetchall()
    width = hours * 3600 / bins
    buckets: list[list] = [[] for _ in range(bins)]
    for ts, v, p in rows:
        i = min(bins - 1, int((ts - since) / width))
        buckets[i].append((v, p))
    out = []
    for i, b in enumerate(buckets):
        veh = [x[0] for x in b if x[0] is not None]
        ped = [x[1] for x in b if x[1] is not None]
        out.append({"t": round(since + i * width), "frames": len(b),
                    "vehicles": round(sum(veh) / len(veh), 1) if veh else None,
                    "pedestrians": round(sum(ped) / len(ped), 1) if ped else None})
    return out
