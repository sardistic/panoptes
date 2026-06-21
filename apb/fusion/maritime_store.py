"""Live maritime-vessel buffer + optional aisstream worker (mirrors social_store).

Keeps the latest AIS position per vessel (keyed by MMSI) in a rolling, time-windowed
buffer that the /live/maritime map layer reads. Off by default — enable with
AISSTREAM_KEY set and `pip install websockets`. Kept out of the lean prod image.
"""
from __future__ import annotations

import asyncio
import threading
import time

_lock = threading.Lock()
_vessels: dict[str, dict] = {}     # MMSI -> latest position row
_MAX = 8000
_started = False


def add(row: dict) -> None:
    with _lock:
        _vessels[row["call_id"]] = row
        if len(_vessels) > _MAX:    # drop oldest by timestamp
            for k in sorted(_vessels, key=lambda k: _vessels[k].get("ts") or 0
                            )[:len(_vessels) - _MAX]:
                _vessels.pop(k, None)


def recent(max_age_hours: float = 2.0) -> list[dict]:
    cutoff = time.time() - max_age_hours * 3600
    with _lock:
        return [v for v in _vessels.values() if (v.get("ts") or 0) >= cutoff]


def stats() -> dict:
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
        print("[aisstream] websockets not installed; maritime stream disabled")
        return False

    async def _consume():
        from apb.ingest.aisstream import positions
        async for row in positions():
            add(row)

    def _run():
        while True:
            try:
                asyncio.run(_consume())
            except Exception as e:
                print(f"[aisstream] stream dropped: {e}; reconnecting in 10s")
                time.sleep(10)

    threading.Thread(target=_run, daemon=True).start()
    _started = True
    print("[aisstream] maritime firehose started")
    return True
