"""Persisted "looks" — every scene explanation the clip produced.

A look is the rectangle a user drew, what facet they asked for, and the grounded
answer (model, weather, cameras used, text). Persisting them means the analysis is
not lost when the bubble closes: the map draws recent looks as outlines, clicking
one re-opens the answer, and later looks over the same place can build on earlier
ones. SQLite (the snapshot store's file) under the shared db lock.
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid

from apb.store import snapshots

_lock = snapshots.db_lock
_ready = False


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS looks (
            uid TEXT PRIMARY KEY,
            ts REAL NOT NULL,
            south REAL, north REAL, west REAL, east REAL,
            focus TEXT, model TEXT, text TEXT,
            weather TEXT, cameras TEXT, camera_ids TEXT, counts TEXT, sky TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_looks_ts ON looks(ts);
        """)
        cols = [r[1] for r in c.execute("PRAGMA table_info(looks)")]
        if "sky" not in cols:                       # pre-sky rows keep working
            c.execute("ALTER TABLE looks ADD COLUMN sky TEXT")
        c.commit()
        _ready = True
    return c


def record(bounds: dict, focus: str, out: dict, counts: dict | None = None) -> str:
    uid = uuid.uuid4().hex[:12]
    with _lock:
        c = _conn()
        c.execute("INSERT INTO looks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            uid, time.time(), bounds.get("south"), bounds.get("north"), bounds.get("west"),
            bounds.get("east"), focus, out.get("model"), out.get("text"),
            json.dumps(out.get("weather") or {}), json.dumps(out.get("cameras") or []),
            json.dumps(out.get("camera_ids") or []), json.dumps(counts or {}),
            json.dumps(out.get("sky") or {})))
        c.commit()
    return uid


def _row(r) -> dict:
    return {"uid": r[0], "ts": r[1], "bounds": {"south": r[2], "north": r[3], "west": r[4], "east": r[5]},
            "focus": r[6], "model": r[7], "text": r[8], "weather": json.loads(r[9] or "{}"),
            "cameras": json.loads(r[10] or "[]"), "camera_ids": json.loads(r[11] or "[]"),
            "counts": json.loads(r[12] or "{}"), "sky": json.loads((r[13] if len(r) > 13 else None) or "{}")}


def query(max_age_hours: float = 24.0, limit: int = 200,
          bbox: tuple[float, float, float, float] | None = None) -> list[dict]:
    """Recent looks, newest first; bbox=(w,s,e,n) keeps looks that intersect it."""
    cutoff = time.time() - max_age_hours * 3600
    with _lock:
        rows = _conn().execute("SELECT * FROM looks WHERE ts >= ? ORDER BY ts DESC LIMIT ?",
                               (cutoff, limit)).fetchall()
    out = [_row(r) for r in rows]
    if bbox:
        w, s, e, n = bbox
        out = [x for x in out if x["bounds"]["west"] <= e and x["bounds"]["east"] >= w
               and x["bounds"]["south"] <= n and x["bounds"]["north"] >= s]
    return out


def get(uid: str) -> dict | None:
    with _lock:
        r = _conn().execute("SELECT * FROM looks WHERE uid = ?", (uid,)).fetchone()
    return _row(r) if r else None


def delete(uid: str) -> bool:
    with _lock:
        c = _conn()
        n = c.execute("DELETE FROM looks WHERE uid = ?", (uid,)).rowcount
        c.commit()
    return n > 0


def prior(bounds: dict, max_age_hours: float = 6.0, limit: int = 3) -> list[dict]:
    """Earlier looks overlapping this box — handed to the model as memory."""
    return query(max_age_hours, 50, (bounds["west"], bounds["south"],
                                     bounds["east"], bounds["north"]))[:limit]
