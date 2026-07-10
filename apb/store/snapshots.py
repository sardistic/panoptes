"""Lightweight snapshot store (stdlib SQLite) — accumulates the live incident stream
over time so coverage grows, history is retained, and temporal baselines become
possible. No Postgres/PostGIS required; this is the zero-dependency persistence layer
that backs the background poller.

Why it matters: a single live fetch only sees each feed's most-recent page, and many
feeds update slowly — so the instantaneous national view is sparse. By polling and
upserting continuously, the DB builds a complete recent picture (and real history for
rate baselines).
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from pathlib import Path

from apb.store import state

# On Railway a volume is mounted at /app/state (APB_DB_PATH points inside it) so
# accumulated history survives deploys. It must NOT live under /app/data — a volume
# there would shadow the committed catalogs baked into the image.
DB_PATH = Path(os.environ.get("APB_DB_PATH", "data/apb.sqlite"))
# One re-entrant lock protects the shared connection across snapshots, fused events,
# and signal buffers.  Those modules all use this same SQLite transaction stream.
db_lock = threading.RLock()
_lock = db_lock  # compatibility for existing callers/tests
_conn: sqlite3.Connection | None = None
_pg_ready = False
_pg_lock = threading.Lock()


def _init_postgres() -> None:
    global _pg_ready
    if _pg_ready:
        return
    with _pg_lock:
        if _pg_ready:
            return
        with state.pg_connection() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS panoptes_incidents (
                    metro TEXT NOT NULL, call_id TEXT NOT NULL, type TEXT,
                    summary TEXT, location TEXT, sentiment TEXT,
                    threat_score DOUBLE PRECISION, emerging INTEGER,
                    lat DOUBLE PRECISION, lon DOUBLE PRECISION, at TEXT, ts DOUBLE PRECISION,
                    first_seen DOUBLE PRECISION, last_seen DOUBLE PRECISION,
                    PRIMARY KEY (metro, call_id)
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS panoptes_inc_ts ON panoptes_incidents(ts)")
            c.execute("CREATE INDEX IF NOT EXISTS panoptes_inc_metro ON panoptes_incidents(metro)")
            c.execute("CREATE INDEX IF NOT EXISTS panoptes_inc_threat ON panoptes_incidents(threat_score)")
        _pg_ready = True


def conn() -> sqlite3.Connection:
    global _conn
    with db_lock:
        if _conn is None:
            DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            _conn = sqlite3.connect(str(DB_PATH), check_same_thread=False,
                                    timeout=10.0)
            _conn.row_factory = sqlite3.Row
            _conn.execute("PRAGMA journal_mode=WAL")
            _conn.execute("PRAGMA busy_timeout=10000")
            _conn.execute("PRAGMA foreign_keys=ON")
            _init(_conn)
        return _conn


def _init(c: sqlite3.Connection) -> None:
    version = c.execute("PRAGMA user_version").fetchone()[0]
    if version > 1:
        raise RuntimeError(f"database schema {version} is newer than this service supports")
    c.executescript("""
    CREATE TABLE IF NOT EXISTS incidents (
        metro TEXT, call_id TEXT, type TEXT, summary TEXT, location TEXT,
        sentiment TEXT, threat_score REAL, emerging INTEGER,
        lat REAL, lon REAL, at TEXT, ts REAL,
        first_seen REAL, last_seen REAL,
        PRIMARY KEY (metro, call_id)
    );
    CREATE INDEX IF NOT EXISTS idx_inc_ts ON incidents(ts);
    CREATE INDEX IF NOT EXISTS idx_inc_metro ON incidents(metro);
    CREATE INDEX IF NOT EXISTS idx_inc_threat ON incidents(threat_score);
    PRAGMA user_version=1;
    """)
    c.commit()


def _cell(v):
    """SQLite can't bind dict/list/tuple params — stringify them; pass scalars through."""
    return str(v) if isinstance(v, (dict, list, tuple)) else v


def record(incidents: list[dict]) -> int:
    """Upsert a batch of incidents. Returns rows written."""
    now = time.time()
    rows = [(
        _cell(d.get("metro")), str(d.get("call_id")), _cell(d.get("type")),
        _cell(d.get("summary")), _cell(d.get("location")), _cell(d.get("sentiment")),
        d.get("threat_score", 0.0), 1 if d.get("emerging") else 0,
        d.get("lat"), d.get("lon"), _cell(d.get("at")), d.get("ts"), now, now,
    ) for d in incidents if d.get("lat") is not None and d.get("call_id") is not None]
    if not rows:
        return 0
    if state.is_postgres():
        _init_postgres()
        with state.pg_connection() as c:
            with c.cursor() as cur:
                cur.executemany("""
                    INSERT INTO panoptes_incidents
                        (metro, call_id, type, summary, location, sentiment,
                         threat_score, emerging, lat, lon, at, ts, first_seen, last_seen)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(metro, call_id) DO UPDATE
                    SET last_seen=EXCLUDED.last_seen
                """, rows)
        return len(rows)
    with _lock:
        c = conn()
        c.executemany("""
            INSERT INTO incidents (metro, call_id, type, summary, location, sentiment,
                threat_score, emerging, lat, lon, at, ts, first_seen, last_seen)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(metro, call_id) DO UPDATE SET last_seen=excluded.last_seen
        """, rows)
        c.commit()
    return len(rows)


def query(max_age_hours: float = 24.0, metro: str | None = None,
          limit: int = 8000) -> list[dict]:
    """Recent incidents from history. Uses event time `ts` when known, else last_seen."""
    cutoff = time.time() - max_age_hours * 3600
    sql = ("SELECT * FROM incidents WHERE COALESCE(ts, last_seen) >= ? "
           + ("AND metro = ? " if metro else "")
           + "ORDER BY COALESCE(ts, last_seen) DESC LIMIT ?")
    args = [cutoff] + ([metro] if metro else []) + [limit]
    if state.is_postgres():
        _init_postgres()
        sql = ("SELECT * FROM panoptes_incidents WHERE COALESCE(ts, last_seen) >= %s "
               + ("AND metro = %s " if metro else "")
               + "ORDER BY COALESCE(ts, last_seen) DESC LIMIT %s")
        with state.pg_connection() as c:
            rows = c.execute(sql, args).fetchall()
        return [dict(r) for r in rows]
    with _lock:
        rows = conn().execute(sql, args).fetchall()
    return [dict(r) for r in rows]


def stats() -> dict:
    if state.is_postgres():
        _init_postgres()
        with state.pg_connection() as c:
            row = c.execute("""
                SELECT COUNT(*) AS total, COUNT(DISTINCT metro) AS metros,
                    COUNT(*) FILTER (WHERE COALESCE(ts,last_seen) >= %s) AS last_24h
                FROM panoptes_incidents
            """, (time.time() - 86400,)).fetchone()
        return {"backend": "postgres", **dict(row)}
    with _lock:
        c = conn()
        total = c.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
        metros = c.execute("SELECT COUNT(DISTINCT metro) FROM incidents").fetchone()[0]
        day = c.execute("SELECT COUNT(*) FROM incidents WHERE COALESCE(ts,last_seen) >= ?",
                        (time.time() - 86400,)).fetchone()[0]
    return {"backend": "sqlite", "total": total, "metros": metros, "last_24h": day}


def prune(max_age_days: float = 30.0) -> int:
    """Drop very old rows to keep the SQLite file bounded."""
    cutoff = time.time() - max_age_days * 86400
    if state.is_postgres():
        _init_postgres()
        with state.pg_connection() as c:
            result = c.execute(
                "DELETE FROM panoptes_incidents WHERE COALESCE(ts, last_seen) < %s",
                (cutoff,))
            return result.rowcount
    with _lock:
        c = conn()
        n = c.execute("DELETE FROM incidents WHERE COALESCE(ts, last_seen) < ?",
                      (cutoff,)).rowcount
        c.commit()
    return n
