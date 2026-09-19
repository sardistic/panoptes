"""Metered spend for billed upstreams — a hard ceiling that survives restarts.

Counts are per (provider, period) in the snapshot store's SQLite file. `allow()` is
the gate: it reserves `n` units against the daily and monthly caps atomically and
returns False (charging nothing) when either would be exceeded.
"""
from __future__ import annotations

import sqlite3
import threading
import time

from apb.store import snapshots

_lock = snapshots.db_lock
_ready = False


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.execute("CREATE TABLE IF NOT EXISTS spend (provider TEXT, period TEXT, n INTEGER, "
                  "PRIMARY KEY (provider, period))")
        c.commit()
        _ready = True
    return c


def _periods(now: float | None = None) -> tuple[str, str]:
    t = time.gmtime(now or time.time())
    return time.strftime("%Y-%m", t), time.strftime("%Y-%m-%d", t)


def used(provider: str) -> dict:
    month, day = _periods()
    with _lock:
        rows = dict(_conn().execute("SELECT period, n FROM spend WHERE provider = ? AND period IN (?, ?)",
                                    (provider, month, day)).fetchall())
    return {"month": rows.get(month, 0), "day": rows.get(day, 0)}


def allow(provider: str, n: int, monthly_cap: int, daily_cap: int) -> bool:
    """Reserve n units if both caps hold; otherwise reserve nothing."""
    month, day = _periods()
    with _lock:
        c = _conn()
        rows = dict(c.execute("SELECT period, n FROM spend WHERE provider = ? AND period IN (?, ?)",
                              (provider, month, day)).fetchall())
        if rows.get(month, 0) + n > monthly_cap or rows.get(day, 0) + n > daily_cap:
            return False
        for period in (month, day):
            c.execute("INSERT INTO spend (provider, period, n) VALUES (?, ?, ?) "
                      "ON CONFLICT(provider, period) DO UPDATE SET n = n + excluded.n", (provider, period, n))
        c.commit()
    return True


def reset(provider: str) -> None:
    """Drop a provider's counters (tests)."""
    with _lock:
        c = _conn()
        c.execute("DELETE FROM spend WHERE provider = ?", (provider,))
        c.commit()
