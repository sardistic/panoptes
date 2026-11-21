"""Persistent fused-event registry — gives clusters identity over time.

/live/fused recomputes clusters per request; this table is what remembers them.
The poller records each cycle's fused events here, matching against open events
by place + recency so one physical event keeps one uid as it grows/decays. That
enables lifecycle questions ("when did this start? is it growing?") and exactly-
once alerting (the notifier marks rows it has fired for).

Every cycle also appends one point per active event to `fused_event_history`, so an
event has a growth curve (rising / steady / falling) instead of only a peak and a
latest value. History always lives in the leader's SQLite file — it is written and
read by the poller's own process, like the box-metric store.

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
_hist_ready = False
_pg_ready = False
_pg_init_lock = threading.Lock()
_pg_claimed: set[str] = set()

_MATCH_KM = 4.0           # same-event match distance between cycles
_MATCH_WINDOW_SEC = 3 * 3600.0
_SANE_SCORE = 1000.0      # nothing the scorer can legitimately produce comes close


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
        CREATE INDEX IF NOT EXISTS idx_fev_first ON fused_events(first_seen);
        """)
        # rows written while far-future timestamps could inflate recency carry peaks in
        # the hundreds of thousands; a real score tops out well under 100
        c.execute("UPDATE fused_events SET latest_score = 0 WHERE latest_score > ?",
                  (_SANE_SCORE,))
        c.execute("UPDATE fused_events SET peak_score = latest_score WHERE peak_score > ?",
                  (_SANE_SCORE,))
        c.commit()
        _ready = True
    return c


def _hist_conn() -> sqlite3.Connection:
    global _hist_ready
    c = snapshots.conn()
    if not _hist_ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS fused_event_history (
            uid TEXT NOT NULL, ts REAL NOT NULL, score REAL, count INTEGER,
            source_count INTEGER);
        CREATE INDEX IF NOT EXISTS idx_feh ON fused_event_history(uid, ts);
        CREATE INDEX IF NOT EXISTS idx_feh_ts ON fused_event_history(ts);
        """)
        c.commit()
        _hist_ready = True
    return c


def _assign(events: list, open_rows: list) -> list[tuple[object, str | None]]:
    """Pair this cycle's clusters with open registry rows: strongest cluster first,
    each takes the NEAREST unclaimed row inside the match radius (most recently seen
    on a tie). One row serves one cluster per cycle, so two rows near the same
    cluster cannot both stay alive — the loser ages out of the window.
    (Taking the first row the index returned refreshed the stalest duplicate each
    cycle and kept every duplicate alive indefinitely.)"""
    claimed: set[str] = set()
    out: list[tuple[object, str | None]] = []
    for e in sorted(events, key=lambda ev: -ev.surge_score):
        best, best_key = None, None
        for r in open_rows:
            if r["uid"] in claimed:
                continue
            d = _km(e.lat, e.lon, r["lat"], r["lon"])
            if d > _MATCH_KM:
                continue
            key = (round(d, 2), -(r["last_seen"] or 0.0))
            if best_key is None or key < best_key:
                best, best_key = r["uid"], key
        if best:
            claimed.add(best)
        out.append((e, best))
    return out


def _log_history(points: list[tuple], now: float) -> None:
    if not points:
        return
    with _lock:
        c = _hist_conn()
        c.executemany("INSERT INTO fused_event_history VALUES (?,?,?,?,?)",
                      [(uid, now, score, count, sc) for uid, score, count, sc in points])
        c.commit()


def history(uid: str, limit: int = 300) -> list[dict]:
    """The event's growth curve, oldest point first."""
    with _lock:
        rows = _hist_conn().execute(
            "SELECT ts, score, count, source_count FROM fused_event_history "
            "WHERE uid = ? ORDER BY ts DESC LIMIT ?", (uid, limit)).fetchall()
    return [{"ts": r[0], "score": r[1], "count": r[2], "source_count": r[3]}
            for r in reversed(rows)]


def trend_of(scores: list[float]) -> dict:
    """Direction from a score series (oldest first): the latest point against the mean
    of up to three before it, and against the event's own median (its 'normal')."""
    if len(scores) < 2:
        return {"trend": "new", "ratio": None, "vs_median": None, "points": len(scores)}
    prev = scores[-4:-1]
    base = sum(prev) / len(prev)
    ratio = scores[-1] / base if base > 0 else None
    ordered = sorted(scores[:-1])
    med = ordered[len(ordered) // 2]
    trend = "steady"
    if ratio is not None and ratio >= 1.15:
        trend = "rising"
    elif ratio is not None and ratio <= 0.85:
        trend = "falling"
    return {"trend": trend, "ratio": round(ratio, 2) if ratio is not None else None,
            "vs_median": round(scores[-1] / med, 2) if med > 0 else None,
            "points": len(scores)}


def _series(uids: list[str], since: float) -> dict[str, list[float]]:
    if not uids:
        return {}
    out: dict[str, list[float]] = {}
    with _lock:
        rows = _hist_conn().execute(
            "SELECT uid, score FROM fused_event_history WHERE ts >= ? ORDER BY ts",
            (since,)).fetchall()
    want = set(uids)
    for uid, score in rows:
        if uid in want:
            out.setdefault(uid, []).append(score or 0.0)
    return out


def _decorate(rows: list[dict]) -> list[dict]:
    """Lifecycle fields shared by both backends."""
    now = time.time()
    series = _series([r["uid"] for r in rows], now - 24 * 3600.0)
    for d in rows:
        d["age_min"] = round((now - d["first_seen"]) / 60.0, 1)
        scores = series.get(d["uid"], [])
        d.update(trend_of(scores))
        d["spark"] = [round(x, 2) for x in scores[-16:]]
        d["growing"] = d["trend"] == "rising" or (
            d["trend"] == "new" and d["latest_score"] >= d["peak_score"] * 0.99
            and d["age_min"] > 3)
    return rows


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
            c.execute("CREATE INDEX IF NOT EXISTS panoptes_fev_first ON panoptes_fused_events(first_seen)")
            c.execute("UPDATE panoptes_fused_events SET latest_score = 0 "
                      "WHERE latest_score > %s", (_SANE_SCORE,))
            c.execute("UPDATE panoptes_fused_events SET peak_score = latest_score "
                      "WHERE peak_score > %s", (_SANE_SCORE,))
        _pg_ready = True


def _record_postgres(events: list, now: float) -> int:
    from psycopg.types.json import Jsonb

    _init_postgres()
    new = 0
    points: list[tuple] = []
    with state.pg_connection() as c:
        c.execute("SELECT pg_advisory_xact_lock(hashtext('panoptes_fused_events_record'))")
        open_rows = c.execute(
            "SELECT uid, lat, lon, last_seen FROM panoptes_fused_events WHERE last_seen >= %s",
            (now - _MATCH_WINDOW_SEC,)).fetchall()
        for event, match in _assign(events, open_rows):
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
                match = uid
            points.append((match, event.surge_score, event.count, event.source_count))
    _log_history(points, now)
    return new


def record(events: list) -> int:
    """Upsert this cycle's FusedEvents; returns how many were new."""
    now = time.time()
    if state.is_postgres():
        return _record_postgres(events, now)
    new = 0
    points: list[tuple] = []
    with _lock:
        c = _conn()
        open_rows = c.execute(
            "SELECT uid, lat, lon, last_seen FROM fused_events WHERE last_seen >= ?",
            (now - _MATCH_WINDOW_SEC,)).fetchall()
        for e, match in _assign(events, open_rows):
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
                match = uid
            points.append((match, e.surge_score, e.count, e.source_count))
        c.commit()
    _log_history(points, now)
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
        return _decorate([dict(row) for row in rows])
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
        out.append(d)
    return _decorate(out)


def get(uid: str) -> dict | None:
    """One event by uid, with its full growth curve."""
    if state.is_postgres():
        _init_postgres()
        with state.pg_connection() as c:
            row = c.execute("SELECT * FROM panoptes_fused_events WHERE uid = %s",
                            (uid,)).fetchone()
        rows = [dict(row)] if row else []
    else:
        with _lock:
            row = _conn().execute("SELECT * FROM fused_events WHERE uid = ?",
                                  (uid,)).fetchone()
        rows = []
        if row:
            d = dict(row)
            for k in ("types", "sources", "summaries"):
                try:
                    d[k] = json.loads(d[k] or "null")
                except ValueError:
                    pass
            rows = [d]
    if not rows:
        return None
    out = _decorate(rows)[0]
    out["history"] = history(uid)
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
    with _lock:
        c = _hist_conn()
        c.execute("DELETE FROM fused_event_history WHERE ts < ?", (cutoff,))
        c.commit()
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
