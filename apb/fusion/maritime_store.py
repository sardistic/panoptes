"""Live maritime-vessel buffer + optional aisstream worker (mirrors social_store).

Keeps the latest AIS position per vessel (keyed by MMSI) in a rolling, time-windowed
buffer that the /live/maritime map layer reads. Off by default — enable with
AISSTREAM_KEY set and `pip install websockets`. Kept out of the lean prod image.
"""
from __future__ import annotations

import asyncio
import threading
import time

import logging

log = logging.getLogger(__name__)

_lock = threading.Lock()
_vessels: dict[str, dict] = {}     # MMSI -> latest position row
_MAX = 8000
_started = False
_stop = threading.Event()
_thread: threading.Thread | None = None
_pg_ready = False
_pg_init_lock = threading.Lock()


def _init_postgres() -> None:
    global _pg_ready
    if _pg_ready:
        return
    from apb.store import state
    with _pg_init_lock:
        if _pg_ready:
            return
        with state.pg_connection() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS panoptes_vessels (
                    call_id TEXT PRIMARY KEY, ts DOUBLE PRECISION NOT NULL,
                    payload JSONB NOT NULL
                )
            """)
            connection.execute("CREATE INDEX IF NOT EXISTS panoptes_vessel_ts "
                               "ON panoptes_vessels(ts)")
        _pg_ready = True


def add(row: dict) -> None:
    from apb.store import state
    if state.is_postgres():
        from psycopg.types.json import Jsonb
        _init_postgres()
        with state.pg_connection() as connection:
            connection.execute("""
                INSERT INTO panoptes_vessels(call_id, ts, payload) VALUES (%s,%s,%s)
                ON CONFLICT(call_id) DO UPDATE SET ts=EXCLUDED.ts, payload=EXCLUDED.payload
            """, (row["call_id"], row.get("ts") or time.time(), Jsonb(row)))
        return
    with _lock:
        _vessels[row["call_id"]] = row
        if len(_vessels) > _MAX:    # drop oldest by timestamp
            for k in sorted(_vessels, key=lambda k: _vessels[k].get("ts") or 0
                            )[:len(_vessels) - _MAX]:
                _vessels.pop(k, None)


def recent(max_age_hours: float = 2.0) -> list[dict]:
    cutoff = time.time() - max_age_hours * 3600
    from apb.store import state
    if state.is_postgres():
        _init_postgres()
        with state.pg_connection() as connection:
            rows = connection.execute(
                "SELECT payload FROM panoptes_vessels WHERE ts >= %s "
                "ORDER BY ts DESC LIMIT %s", (cutoff, _MAX)).fetchall()
        return [row["payload"] for row in rows]
    with _lock:
        return [v for v in _vessels.values() if (v.get("ts") or 0) >= cutoff]


def stats() -> dict:
    from apb.store import state
    if state.is_postgres():
        _init_postgres()
        with state.pg_connection() as connection:
            count = connection.execute(
                "SELECT COUNT(*) AS count FROM panoptes_vessels WHERE ts >= %s",
                (time.time() - 7200,)).fetchone()["count"]
        return {"vessels": count, "running": _started, "shared": True}
    with _lock:
        return {"vessels": len(_vessels), "running": _started}


def start() -> bool:
    """Launch the aisstream consumer in a daemon thread (auto-reconnect). No-op if
    already running, no key, or `websockets` missing. Returns True if started."""
    global _started
    if _started:
        return False
    from apb.ingest.aisstream import api_key
    if not api_key():
        return False
    try:
        import websockets  # noqa: F401
    except ImportError:
        log.warning("websockets not installed; maritime stream disabled")
        return False

    async def _consume():
        from apb.ingest.aisstream import positions
        async for row in positions():
            if _stop.is_set():
                break
            add(row)

    def _run():
        while not _stop.is_set():
            try:
                asyncio.run(_consume())
            except Exception as e:
                log.warning(f"stream dropped: {e}; reconnecting in 10s")
                _stop.wait(10)

    global _thread
    _stop.clear()
    _thread = threading.Thread(target=_run, daemon=True, name="apb-maritime")
    _thread.start()
    _started = True
    log.info("maritime firehose started")
    return True


def stop(timeout: float = 2.0) -> None:
    _stop.set()
    if _thread and _thread.is_alive():
        _thread.join(timeout=timeout)
