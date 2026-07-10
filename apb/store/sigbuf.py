"""Signal-buffer persistence — social/news buffers survive restarts.

The rolling in-memory buffers in fusion.social_store / fusion.news_store are the
only home of collected social + news signals; before this, every deploy emptied
the social layer until the pollers refilled it. Buffered signals are mirrored
here (same SQLite file as snapshots) and re-hydrated on the first store start.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time

from apb.common.models import EventSignal
from apb.store import snapshots, state

log = logging.getLogger(__name__)

_lock = snapshots.db_lock
_ready = False
_pg_ready = False
_pg_init_lock = threading.Lock()


def _init_postgres() -> None:
    global _pg_ready
    if _pg_ready:
        return
    with _pg_init_lock:
        if _pg_ready:
            return
        with state.pg_connection() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS panoptes_signal_buffer (
                    dedupe_key TEXT PRIMARY KEY, buffer TEXT NOT NULL,
                    ts DOUBLE PRECISION NOT NULL, payload TEXT NOT NULL
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS panoptes_sigbuf_ts "
                      "ON panoptes_signal_buffer(buffer, ts)")
        _pg_ready = True


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS signal_buffer (
            dedupe_key TEXT PRIMARY KEY,
            buffer TEXT, ts REAL, payload TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sigbuf_ts ON signal_buffer(buffer, ts);
        """)
        c.commit()
        _ready = True
    return c


def save(buffer: str, signals: list[EventSignal]) -> None:
    """Mirror new signals; must never break the collector on failure."""
    if not signals:
        return
    try:
        rows = [(s.dedupe_key, buffer, s.observed_at.timestamp(),
                 s.model_dump_json()) for s in signals]
        if state.is_postgres():
            _init_postgres()
            with state.pg_connection() as c:
                with c.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO panoptes_signal_buffer
                            (dedupe_key, buffer, ts, payload) VALUES (%s,%s,%s,%s)
                        ON CONFLICT(dedupe_key) DO NOTHING
                    """, rows)
            return
        with _lock:
            c = _conn()
            c.executemany("INSERT OR IGNORE INTO signal_buffer VALUES (?,?,?,?)", rows)
            c.commit()
    except Exception as e:
        log.warning("save(%s) failed: %s", buffer, e)


def load(buffer: str, max_age_hours: float = 24.0, limit: int = 5000) -> list[EventSignal]:
    cutoff = time.time() - max_age_hours * 3600
    try:
        if state.is_postgres():
            _init_postgres()
            with state.pg_connection() as c:
                rows = c.execute(
                    "SELECT payload FROM panoptes_signal_buffer "
                    "WHERE buffer = %s AND ts >= %s ORDER BY ts DESC LIMIT %s",
                    (buffer, cutoff, limit)).fetchall()
        else:
            with _lock:
                rows = _conn().execute(
                    "SELECT payload FROM signal_buffer WHERE buffer = ? AND ts >= ? "
                    "ORDER BY ts DESC LIMIT ?", (buffer, cutoff, limit)).fetchall()
        out = []
        for r in rows:
            try:
                out.append(EventSignal.model_validate_json(r["payload"]))
            except ValueError:
                continue
        out.reverse()                     # oldest first, matching append order
        return out
    except Exception as e:
        log.warning("load(%s) failed: %s", buffer, e)
        return []


def prune(max_age_days: float = 3.0) -> int:
    cutoff = time.time() - max_age_days * 86400
    if state.is_postgres():
        _init_postgres()
        with state.pg_connection() as c:
            result = c.execute("DELETE FROM panoptes_signal_buffer WHERE ts < %s",
                               (cutoff,))
            return result.rowcount
    with _lock:
        c = _conn()
        n = c.execute("DELETE FROM signal_buffer WHERE ts < ?", (cutoff,)).rowcount
        c.commit()
    return n
