"""Persistent fused-event registry — gives clusters identity over time.

/live/fused recomputes clusters per request; this table is what remembers them.
The poller records each cycle's fused events here, matching against open events
by place + recency so one physical event keeps one uid as it grows/decays. That
enables lifecycle questions ("when did this start? is it growing?") and exactly-
once alerting (the notifier marks rows it has fired for).

Uses the snapshot store's SQLite file locally or shared PostgreSQL state when enabled.
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
import time

from apb.store import snapshots, state

_lock = snapshots.db_lock
_ready = False
_pg_ready = False
_pg_init_lock = threading.Lock()
_pg_claimed: set[str] = set()

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


def _init_postgres() -> None:
    global _pg_ready
    if _pg_ready:
        return
    with _pg_init_lock:
        if _pg_ready:
            return
        with state.pg_connection() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS panoptes_fused_events (
                    uid TEXT PRIMARY KEY, lat DOUBLE PRECISION, lon DOUBLE PRECISION,
                    first_seen DOUBLE PRECISION, last_seen DOUBLE PRECISION,
                    peak_score DOUBLE PRECISION, peak_count INTEGER,
                    latest_score DOUBLE PRECISION, latest_count INTEGER,
                    source_count INTEGER, types JSONB, sources JSONB, summaries JSONB,
                    notified INTEGER DEFAULT 0, claim_at DOUBLE PRECISION
                )
            """)
            c.execute("ALTER TABLE panoptes_fused_events "
                      "ADD COLUMN IF NOT EXISTS claim_at DOUBLE PRECISION")
            c.execute("CREATE INDEX IF NOT EXISTS panoptes_fev_seen ON panoptes_fused_events(last_seen)")
        _pg_ready = True


def _record_postgres(events: list, now: float) -> int:
    from psycopg.types.json import Jsonb

    _init_postgres()
    new = 0
    with state.pg_connection() as c:
        c.execute("SELECT pg_advisory_xact_lock(hashtext('panoptes_fused_events_record'))")
        open_rows = c.execute(
            "SELECT uid, lat, lon FROM panoptes_fused_events WHERE last_seen >= %s",
            (now - _MATCH_WINDOW_SEC,)).fetchall()
        for event in events:
            match = next((row["uid"] for row in open_rows
                          if _km(event.lat, event.lon, row["lat"], row["lon"]) <= _MATCH_KM), None)
            payload = (event.lat, event.lon, now, event.surge_score, event.count,
                       event.surge_score, event.count, event.source_count,
                       Jsonb(event.types), Jsonb(event.sources), Jsonb(event.summaries))
            if match:
                c.execute("""
                    UPDATE panoptes_fused_events SET lat=%s, lon=%s, last_seen=%s,
                        peak_score=GREATEST(peak_score, %s),
                        peak_count=GREATEST(peak_count, %s), latest_score=%s,
                        latest_count=%s, source_count=%s, types=%s, sources=%s,
                        summaries=%s WHERE uid=%s
                """, payload + (match,))
            else:
                uid = f"fev:{round(event.lat, 3)}:{round(event.lon, 3)}:{int(now)}"
                result = c.execute("""
                    INSERT INTO panoptes_fused_events
                        (uid, lat, lon, first_seen, last_seen, peak_score, peak_count,
                         latest_score, latest_count, source_count, types, sources, summaries)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(uid) DO NOTHING
                """, (uid, event.lat, event.lon, now, now, event.surge_score,
                      event.count, event.surge_score, event.count, event.source_count,
                      Jsonb(event.types), Jsonb(event.sources), Jsonb(event.summaries)))
                new += max(0, result.rowcount)
                open_rows.append({"uid": uid, "lat": event.lat, "lon": event.lon})
    return new


def record(events: list) -> int:
    """Upsert this cycle's FusedEvents; returns how many were new."""
    now = time.time()
    if state.is_postgres():
        return _record_postgres(events, now)
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
    if state.is_postgres():
        _init_postgres()
        with state.pg_connection() as c:
            rows = c.execute(
                "SELECT * FROM panoptes_fused_events WHERE last_seen >= %s "
                "ORDER BY last_seen DESC LIMIT %s", (cutoff, limit)).fetchall()
        out = [dict(row) for row in rows]
        for item in out:
            item["age_min"] = round((time.time() - item["first_seen"]) / 60.0, 1)
            item["growing"] = bool(item["latest_score"] >= item["peak_score"] * 0.99
                                   and item["age_min"] > 3)
        return out
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
    if state.is_postgres():
        global _pg_claimed
        _init_postgres()
        with state.pg_connection() as c:
            rows = c.execute("""
                WITH claims AS (
                    SELECT uid FROM panoptes_fused_events
                    WHERE (notified = 0 OR (notified = 2 AND claim_at < %s))
                        AND peak_score >= %s
                    ORDER BY first_seen LIMIT 100 FOR UPDATE SKIP LOCKED
                )
                UPDATE panoptes_fused_events AS event SET notified = 2, claim_at = %s
                FROM claims WHERE event.uid = claims.uid RETURNING event.*
            """, (time.time() - 300.0, min_score, time.time())).fetchall()
        _pg_claimed = {row["uid"] for row in rows}
        return [dict(row) for row in rows]
    with _lock:
        rows = _conn().execute(
            "SELECT * FROM fused_events WHERE notified = 0 AND peak_score >= ?",
            (min_score,)).fetchall()
    return [dict(r) for r in rows]


def mark_notified(uids: list[str]) -> None:
    if state.is_postgres():
        global _pg_claimed
        claimed = list(_pg_claimed)
        _pg_claimed = set()
        if not claimed and not uids:
            return
        with state.pg_connection() as c:
            if claimed:
                c.execute("UPDATE panoptes_fused_events SET notified = 0, claim_at = NULL "
                          "WHERE uid = ANY(%s)",
                          (claimed,))
            if uids:
                c.execute("UPDATE panoptes_fused_events SET notified = 1, claim_at = NULL "
                          "WHERE uid = ANY(%s)",
                          (uids,))
        return
    if not uids:
        return
    with _lock:
        c = _conn()
        c.executemany("UPDATE fused_events SET notified = 1 WHERE uid = ?",
                      [(u,) for u in uids])
        c.commit()


def prune(max_age_days: float = 14.0) -> int:
    cutoff = time.time() - max_age_days * 86400
    if state.is_postgres():
        _init_postgres()
        with state.pg_connection() as c:
            result = c.execute("DELETE FROM panoptes_fused_events WHERE last_seen < %s",
                               (cutoff,))
            return result.rowcount
    with _lock:
        c = _conn()
        n = c.execute("DELETE FROM fused_events WHERE last_seen < ?", (cutoff,)).rowcount
        c.commit()
    return n
