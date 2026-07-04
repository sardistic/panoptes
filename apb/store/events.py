"""Persistent fused-event registry — gives clusters identity over time.

/live/fused recomputes clusters per request; this table is what remembers them.
The poller records each cycle's fused events here, matching against open events
by place + recency so one physical event keeps one uid as it grows/decays. That
enables lifecycle questions ("when did this start? is it growing?") and exactly-
once alerting (the notifier marks rows it has fired for).

Shares the snapshot store's SQLite file (APB_DB_PATH / data/apb.sqlite).
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
import time

from apb.store import snapshots

_lock = threading.Lock()
_ready = False

_MATCH_KM = 4.0           # same-event match distance between cycles
_MATCH_WINDOW_SEC = 3 * 3600.0


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS fused_events (
            uid TEXT PRIMARY KEY,
            lat REAL, lon REAL,
            first_seen REAL, last_seen REAL,
            peak_score REAL, peak_count INTEGER,
            latest_score REAL, latest_count INTEGER, source_count INTEGER,
            types TEXT, sources TEXT, summaries TEXT,
            notified INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_fev_seen ON fused_events(last_seen);
        """)
        c.commit()
        _ready = True
    return c


def _km(lat1, lon1, lat2, lon2) -> float:
    coslat = max(0.2, math.cos(math.radians(lat1)))
    return math.hypot(lat1 - lat2, (lon1 - lon2) * coslat) * 111.0


def record(events: list) -> int:
    """Upsert this cycle's FusedEvents; returns how many were new."""
    now = time.time()
    new = 0
    with _lock:
        c = _conn()
        open_rows = c.execute(
            "SELECT uid, lat, lon FROM fused_events WHERE last_seen >= ?",
            (now - _MATCH_WINDOW_SEC,)).fetchall()
        for e in events:
            match = next((r["uid"] for r in open_rows
                          if _km(e.lat, e.lon, r["lat"], r["lon"]) <= _MATCH_KM), None)
            payload = (e.lat, e.lon, now, e.surge_score, e.count,
                       e.surge_score, e.count, e.source_count,
                       json.dumps(e.types), json.dumps(e.sources),
                       json.dumps(e.summaries))
            if match:
                c.execute("""
                    UPDATE fused_events SET lat=?, lon=?, last_seen=?,
                        peak_score=MAX(peak_score, ?), peak_count=MAX(peak_count, ?),
                        latest_score=?, latest_count=?, source_count=?,
                        types=?, sources=?, summaries=?
                    WHERE uid=?""", payload + (match,))
            else:
                uid = f"fev:{round(e.lat, 3)}:{round(e.lon, 3)}:{int(now)}"
                c.execute("""
                    INSERT OR IGNORE INTO fused_events
                        (uid, lat, lon, first_seen, last_seen, peak_score, peak_count,
                         latest_score, latest_count, source_count, types, sources,
                         summaries)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (uid, e.lat, e.lon, now, now, e.surge_score, e.count,
                     e.surge_score, e.count, e.source_count,
                     json.dumps(e.types), json.dumps(e.sources),
                     json.dumps(e.summaries)))
                new += 1
        c.commit()
    return new


def query(max_age_hours: float = 24.0, limit: int = 200) -> list[dict]:
    """Events active within the window, newest activity first, with lifecycle."""
    cutoff = time.time() - max_age_hours * 3600
    with _lock:
        rows = _conn().execute(
            "SELECT * FROM fused_events WHERE last_seen >= ? "
            "ORDER BY last_seen DESC LIMIT ?", (cutoff, limit)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        for k in ("types", "sources", "summaries"):
            try:
                d[k] = json.loads(d[k] or "null")
            except ValueError:
                pass
        d["age_min"] = round((time.time() - d["first_seen"]) / 60.0, 1)
        d["growing"] = bool(d["latest_score"] >= d["peak_score"] * 0.99
                            and d["age_min"] > 3)
        out.append(d)
    return out


def unnotified(min_score: float) -> list[dict]:
    with _lock:
        rows = _conn().execute(
            "SELECT * FROM fused_events WHERE notified = 0 AND peak_score >= ?",
            (min_score,)).fetchall()
    return [dict(r) for r in rows]


def mark_notified(uids: list[str]) -> None:
    if not uids:
        return
    with _lock:
        c = _conn()
        c.executemany("UPDATE fused_events SET notified = 1 WHERE uid = ?",
                      [(u,) for u in uids])
        c.commit()


def prune(max_age_days: float = 14.0) -> int:
    cutoff = time.time() - max_age_days * 86400
    with _lock:
        c = _conn()
        n = c.execute("DELETE FROM fused_events WHERE last_seen < ?", (cutoff,)).rowcount
        c.commit()
    return n
