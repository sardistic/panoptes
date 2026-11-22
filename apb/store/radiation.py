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
        CREATE TABLE IF NOT EXISTS rad_survey_cells (
            la INTEGER NOT NULL, lo INTEGER NOT NULL, n INTEGER NOT NULL, sum REAL NOT NULL,
            max REAL NOT NULL, first_ts REAL, last_ts REAL, last_log INTEGER, logs INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (la, lo)) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS rad_survey_logs (
            id INTEGER PRIMARY KEY, created REAL, first_ts REAL, last_ts REAL, kind TEXT, device TEXT,
            region TEXT, lat REAL, lon REAL, n INTEGER, cells INTEGER, mean REAL, max REAL,
            flagged_cells INTEGER, cities TEXT, credits TEXT, state TEXT, ingested REAL);
        CREATE INDEX IF NOT EXISTS idx_rsl_ingested ON rad_survey_logs(ingested);
        CREATE TABLE IF NOT EXISTS rad_survey_meta (k TEXT PRIMARY KEY, v TEXT);
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


# ---- drive surveys (Safecast bGeigie logs) -------------------------------------------
# Readings fold into ~100 m cells (lat/lon x 1000, rounded): count, sum, max, first/last
# seen, last log. One row per log records where and what it was, or why it was skipped.

def _survey_conn() -> sqlite3.Connection:
    return _conn()                       # the survey tables are created with the others


def survey_meta(k: str, default=None):
    with _lock:
        r = _survey_conn().execute("SELECT v FROM rad_survey_meta WHERE k = ?", (k,)).fetchone()
    return json.loads(r[0]) if r else default


def set_survey_meta(k: str, v) -> None:
    with _lock:
        c = _survey_conn()
        c.execute("INSERT INTO rad_survey_meta VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                  (k, json.dumps(v)))
        c.commit()


def survey_known(ids: list[int]) -> set[int]:
    if not ids:
        return set()
    with _lock:
        q = ",".join("?" * len(ids))
        return {r[0] for r in _survey_conn().execute(f"SELECT id FROM rad_survey_logs WHERE id IN ({q})", ids)}


def survey_cells_prior(keys: list[tuple[int, int]]) -> dict[tuple[int, int], tuple[int, float, int]]:
    """(la, lo) -> (n, mean, logs) before a new log is added."""
    out = {}
    with _lock:
        c = _survey_conn()
        for i in range(0, len(keys), 400):
            part = keys[i:i + 400]
            q = " OR ".join("(la = ? AND lo = ?)" for _ in part)
            for la, lo, n, s, logs in c.execute(f"SELECT la, lo, n, sum, logs FROM rad_survey_cells WHERE {q}",
                                                [v for k in part for v in k]):
                out[(la, lo)] = (n, s / n, logs)
    return out


def add_survey_log(row: dict, cells: dict[tuple[int, int], list]) -> None:
    """Record one log and fold its cells ([n, sum, max, first_ts, last_ts]) in one transaction."""
    with _lock:
        c = _survey_conn()
        c.executemany(
            "INSERT INTO rad_survey_cells VALUES (?,?,?,?,?,?,?,?,1) ON CONFLICT(la, lo) DO UPDATE SET "
            "n = n + excluded.n, sum = sum + excluded.sum, max = MAX(max, excluded.max), "
            "first_ts = MIN(first_ts, excluded.first_ts), last_ts = MAX(last_ts, excluded.last_ts), "
            "last_log = CASE WHEN excluded.last_ts >= last_ts THEN excluded.last_log ELSE last_log END, "
            "logs = logs + 1",
            [(la, lo, v[0], v[1], v[2], v[3], v[4], row["id"]) for (la, lo), v in cells.items()])
        cols = ("id", "created", "first_ts", "last_ts", "kind", "device", "region", "lat", "lon", "n", "cells",
                "mean", "max", "flagged_cells", "cities", "credits", "state", "ingested")
        c.execute(f"INSERT OR REPLACE INTO rad_survey_logs ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                  [row.get(k) for k in cols])
        c.commit()


def survey_cells(w: float, s: float, e: float, n: float, step: int, limit: int = 20000) -> list[tuple]:
    """Cells in a box, aggregated to `step` (in 1/1000 degree): (lat, lon, mean, max, n, last_ts)."""
    with _lock:
        rows = _survey_conn().execute(
            # +OFF keeps the integer division flooring (SQLite truncates toward zero)
            "SELECT ((la + ?) / ?) * ? - ?, ((lo + ?) / ?) * ? - ?, SUM(sum) / SUM(n), MAX(max), SUM(n), "
            "MAX(last_ts) FROM rad_survey_cells WHERE la BETWEEN ? AND ? AND lo BETWEEN ? AND ? "
            "GROUP BY (la + ?) / ?, (lo + ?) / ? LIMIT ?",
            (_OFF, step, step, _OFF, _OFF, step, step, _OFF, int(s * 1000), int(n * 1000), int(w * 1000),
             int(e * 1000), _OFF, step, _OFF, step, limit)).fetchall()
    return [(la / 1000 + step / 2000, lo / 1000 + step / 2000, m, mx, cnt, ts) for la, lo, m, mx, cnt, ts in rows]


_OFF = 1_000_000                     # divisible by every step used


def survey_logs(since: float = 0.0, region: str | None = None, limit: int = 200) -> list[dict]:
    sql = "SELECT * FROM rad_survey_logs WHERE state = 'ingested' AND COALESCE(last_ts, created) >= ?"
    args: list = [since]
    if region:
        sql += " AND region = ?"
        args.append(region)
    with _lock:
        rows = _survey_conn().execute(sql + " ORDER BY COALESCE(last_ts, created) DESC LIMIT ?", args + [limit])
        return [dict(r) for r in rows]


def survey_counts() -> dict:
    with _lock:
        c = _survey_conn()
        states = dict(c.execute("SELECT state, COUNT(*) FROM rad_survey_logs GROUP BY state").fetchall())
        regions = dict(c.execute("SELECT region, COUNT(*) FROM rad_survey_logs WHERE state = 'ingested' "
                                 "GROUP BY region").fetchall())
        cells = c.execute("SELECT COUNT(*), COALESCE(SUM(n), 0) FROM rad_survey_cells").fetchone()
    return {"logs_by_state": states, "ingested_by_region": regions, "cells": cells[0], "readings": cells[1]}
