"""Radiation history: per-sensor hourly means and the model's summaries (snapshot SQLite).

Raw readings are not kept. Each new reading folds into its sensor's hourly bucket
(count, sum, max), so a sensor reporting every few minutes costs one row an hour.
Rows and summaries older than the cap period (APB_RAD_KEEP_DAYS) are pruned.
"""
from __future__ import annotations

import json
import sqlite3
import time

from apb.store import snapshots

_lock = snapshots.db_lock
_ready = False


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS rad_sensors (
            device TEXT PRIMARY KEY, lat REAL, lon REAL, tube TEXT, first_ts REAL,
            last_ts REAL, last_usvh REAL, last_cpm REAL);
        CREATE TABLE IF NOT EXISTS rad_hourly (
            device TEXT NOT NULL, hour INTEGER NOT NULL, n INTEGER NOT NULL, sum REAL NOT NULL,
            max REAL NOT NULL, PRIMARY KEY (device, hour));
        CREATE INDEX IF NOT EXISTS idx_radh_hour ON rad_hourly(hour);
        CREATE TABLE IF NOT EXISTS rad_summaries (
            ts REAL PRIMARY KEY, headline TEXT, summary TEXT, changes TEXT, level TEXT,
            stats TEXT, digest TEXT, model TEXT);
        """)
        c.commit()
        _ready = True
    return c


def add_readings(rows: list[dict]) -> int:
    """Fold readings newer than each sensor's last into hourly buckets. Returns how many were new."""
    if not rows:
        return 0
    with _lock:
        c = _conn()
        last = dict(c.execute("SELECT device, last_ts FROM rad_sensors").fetchall())
        new = [r for r in rows if r["ts"] > (last.get(r["device"]) or 0)]
        c.executemany(
            "INSERT INTO rad_hourly VALUES (?,?,1,?,?) ON CONFLICT(device, hour) DO UPDATE SET "
            "n = n + 1, sum = sum + excluded.sum, max = MAX(max, excluded.max)",
            [(r["device"], int(r["ts"] // 3600), r["usvh"], r["usvh"]) for r in new])
        c.executemany(
            "INSERT INTO rad_sensors VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(device) DO UPDATE SET "
            "lat = excluded.lat, lon = excluded.lon, tube = excluded.tube, last_ts = excluded.last_ts, "
            "last_usvh = excluded.last_usvh, last_cpm = excluded.last_cpm",
            [(r["device"], r["lat"], r["lon"], r["tube"], r["ts"], r["ts"], r["usvh"], r["cpm"]) for r in new])
        c.commit()
    return len(new)


def sensors(since: float) -> list[dict]:
    with _lock:
        return [dict(r) for r in _conn().execute("SELECT * FROM rad_sensors WHERE last_ts >= ?", (since,))]


def hourly(since_hour: int, device: str | None = None) -> list[tuple[str, int, float, float]]:
    """(device, hour, mean, max) since an hour index, oldest first."""
    sql = "SELECT device, hour, sum / n, max FROM rad_hourly WHERE hour >= ?"
    args: tuple = (since_hour,)
    if device:
        sql += " AND device = ?"
        args += (device,)
    with _lock:
        return [tuple(r) for r in _conn().execute(sql + " ORDER BY hour", args)]


def prune(keep_days: float, now: float | None = None) -> None:
    now = now or time.time()
    cut = now - keep_days * 86400
    with _lock:
        c = _conn()
        c.execute("DELETE FROM rad_hourly WHERE hour < ?", (int(cut // 3600),))
        c.execute("DELETE FROM rad_sensors WHERE last_ts < ?", (cut,))
        c.execute("DELETE FROM rad_summaries WHERE ts < ?", (cut,))
        c.commit()


def add_summary(ts: float, s: dict, stats: dict, digest: str, model: str) -> None:
    with _lock:
        c = _conn()
        c.execute("INSERT OR REPLACE INTO rad_summaries VALUES (?,?,?,?,?,?,?,?)",
                  (ts, s.get("headline"), s.get("summary"), s.get("changes"), s.get("level"),
                   json.dumps(stats, separators=(",", ":")), digest, model))
        c.commit()


def summaries(since: float, limit: int = 200) -> list[dict]:
    with _lock:
        rows = _conn().execute("SELECT * FROM rad_summaries WHERE ts >= ? ORDER BY ts DESC LIMIT ?",
                               (since, limit)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["stats"] = json.loads(d["stats"]) if d.get("stats") else {}
        out.append(d)
    return out


def counts() -> dict:
    with _lock:
        c = _conn()
        return {"sensors": c.execute("SELECT COUNT(*) FROM rad_sensors").fetchone()[0],
                "hourly_rows": c.execute("SELECT COUNT(*) FROM rad_hourly").fetchone()[0],
                "summaries": c.execute("SELECT COUNT(*) FROM rad_summaries").fetchone()[0]}
