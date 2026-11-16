"""Radio activity store — call counts per public scanner system, and each one's normal.

The rdio lane (apb/ingest/rdio.py) asks each open Rdio Scanner instance for its newest
call ids every few minutes. This store keeps one row per (instance, system, poll):
how many calls in the window, which talkgroup tags they were on, and how surprising
that count is against the same system's own history (Poisson + robust z from
apb.infer.baseline). Counts only — no audio, no transcripts, no unit identities.

Instances are keyed by a short hash, never by address. SQLite in the snapshot store's
file, leader-local.
"""
from __future__ import annotations

import json
import sqlite3
import time

from apb.store import snapshots

_lock = snapshots.db_lock
_ready = False
NORM_SEC = 600.0            # counts are compared as calls per ten minutes
_MIN_BASELINE = 6


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS radio_calls (
            key TEXT NOT NULL, system INTEGER NOT NULL, ts REAL NOT NULL, window REAL,
            calls INTEGER, per10 REAL, tags TEXT, z REAL, base REAL);
        CREATE INDEX IF NOT EXISTS idx_rc ON radio_calls(key, system, ts);
        CREATE INDEX IF NOT EXISTS idx_rc_ts ON radio_calls(ts);
        CREATE TABLE IF NOT EXISTS radio_state (
            key TEXT PRIMARY KEY, last_id INTEGER, last_poll REAL, state TEXT,
            fails INTEGER DEFAULT 0, next_at REAL DEFAULT 0);
        """)
        c.commit()
        _ready = True
    return c


def state(key: str) -> dict:
    with _lock:
        r = _conn().execute("SELECT * FROM radio_state WHERE key = ?", (key,)).fetchone()
    return dict(r) if r else {"key": key, "last_id": None, "last_poll": None,
                              "state": "new", "fails": 0, "next_at": 0.0}


def set_state(key: str, **fields) -> None:
    cur = {**state(key), **fields}
    with _lock:
        c = _conn()
        c.execute("INSERT INTO radio_state (key, last_id, last_poll, state, fails, next_at) "
                  "VALUES (:key,:last_id,:last_poll,:state,:fails,:next_at) "
                  "ON CONFLICT(key) DO UPDATE SET last_id=excluded.last_id, "
                  "last_poll=excluded.last_poll, state=excluded.state, fails=excluded.fails, "
                  "next_at=excluded.next_at", cur)
        c.commit()


def states() -> dict[str, dict]:
    with _lock:
        return {r["key"]: dict(r) for r in _conn().execute("SELECT * FROM radio_state")}


def record(key: str, counts: dict[int, tuple[int, dict]], window: float,
           lon: float | None = None, now: float | None = None) -> list[dict]:
    """Store one poll: {system id: (calls, {tag: n})}. Returns the stored rows with
    their surprise against that system's history."""
    from apb.infer.baseline import _slot, surprise
    now = now or time.time()
    window = max(60.0, window)
    out: list[dict] = []
    with _lock:
        c = _conn()
        slot_now = _slot(now, lon)
        for system, (calls, tags) in counts.items():
            per10 = calls * NORM_SEC / window
            past = c.execute("SELECT ts, per10 FROM radio_calls WHERE key = ? AND system = ? "
                             "AND ts >= ?", (key, system, now - 7 * 86400)).fetchall()
            same = [r[1] for r in past if _slot(r[0], lon) == slot_now]
            base = same if len(same) >= _MIN_BASELINE else [r[1] for r in past]
            z = mean = None
            if len(base) >= _MIN_BASELINE:
                s = surprise(round(per10), [round(x) for x in base])
                z, mean = round(s["zscore"], 2), round(s["mean"], 2)
            c.execute("INSERT INTO radio_calls VALUES (?,?,?,?,?,?,?,?,?)",
                      (key, system, now, window, calls, round(per10, 2),
                       json.dumps(tags), z, mean))
            out.append({"system": system, "calls": calls, "per10": round(per10, 2),
                        "tags": tags, "z": z, "base": mean})
        c.execute("DELETE FROM radio_calls WHERE ts < ?", (now - 14 * 86400,))
        c.commit()
    return out


def latest(max_age_sec: float = 1800.0) -> list[dict]:
    """The newest row per (instance, system) within the window."""
    since = time.time() - max_age_sec
    with _lock:
        rows = _conn().execute(
            "SELECT r.* FROM radio_calls r JOIN (SELECT key, system, MAX(ts) AS ts FROM radio_calls "
            "WHERE ts >= ? GROUP BY key, system) m ON r.key = m.key AND r.system = m.system "
            "AND r.ts = m.ts", (since,)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["tags"] = json.loads(d.get("tags") or "{}")
        except ValueError:
            d["tags"] = {}
        out.append(d)
    return out


def series(key: str, system: int, hours: float = 24.0) -> list[dict]:
    with _lock:
        rows = _conn().execute("SELECT ts, per10, z FROM radio_calls WHERE key = ? AND system = ? "
                               "AND ts >= ? ORDER BY ts", (key, system, time.time() - hours * 3600)).fetchall()
    return [{"ts": r[0], "per10": r[1], "z": r[2]} for r in rows]


def stats() -> dict:
    now = time.time()
    with _lock:
        c = _conn()
        polls = c.execute("SELECT COUNT(*), COALESCE(SUM(calls),0) FROM radio_calls WHERE ts >= ?",
                          (now - 3600,)).fetchone()
        st = c.execute("SELECT state, COUNT(*) FROM radio_state GROUP BY state").fetchall()
    return {"rows_1h": polls[0], "calls_1h": int(polls[1]), "instances": {r[0]: r[1] for r in st}}
