"""Alert log + scorekeeping — what fired, and was it right?

`alert_log` records every alert a rule raised for an event (first alert and each
escalation), whether or not a webhook was configured: with no webhook the rows are
"would have alerted", which is what makes thresholds tunable before anyone is paged.

`alert_outcomes` holds one row per alerted event and is closed against the news
buffer: the first placed headline of a matching kind near the event decides the
outcome —

  confirmed    news appeared after the alert  (lead_min > 0: we were first)
  trailing     news already existed            (lead_min < 0: we were late)
  unconfirmed  nothing in the news within 24 h (false alarm, or simply unreported)

Lead time over news and the confirmed share are the two numbers that say whether a
tuning change helped. SQLite in the snapshot store's file, leader-local.
"""
from __future__ import annotations

import json
import math
import sqlite3
import time

from apb.store import snapshots

_lock = snapshots.db_lock
_ready = False

_NEWS_KM = 30.0
_NEWS_BEFORE_SEC = 3 * 3600.0       # a headline this long before first_seen still counts
_CLOSE_AFTER_SEC = 24 * 3600.0


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, uid TEXT NOT NULL, rule TEXT NOT NULL,
            ts REAL NOT NULL, score REAL, kind TEXT, sent INTEGER DEFAULT 0, line TEXT);
        CREATE INDEX IF NOT EXISTS idx_al ON alert_log(uid, rule, ts);
        CREATE INDEX IF NOT EXISTS idx_al_ts ON alert_log(ts);
        CREATE TABLE IF NOT EXISTS alert_outcomes (
            uid TEXT PRIMARY KEY, rule TEXT, alert_ts REAL NOT NULL, first_seen REAL,
            lat REAL, lon REAL, top_type TEXT, score REAL, status TEXT DEFAULT 'pending',
            news_ts REAL, news_title TEXT, news_url TEXT, lead_min REAL, resolved_ts REAL);
        CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);
        """)
        c.commit()
        _ready = True
    return c


def last(uid: str, rule: str) -> dict | None:
    with _lock:
        r = _conn().execute("SELECT ts, score, kind, sent FROM alert_log WHERE uid = ? AND rule = ? "
                            "ORDER BY ts DESC LIMIT 1", (uid, rule)).fetchone()
    return dict(r) if r else None


def add(uid: str, rule: str, score: float, kind: str, sent: bool, line: str = "") -> None:
    with _lock:
        c = _conn()
        c.execute("INSERT INTO alert_log (uid, rule, ts, score, kind, sent, line) VALUES (?,?,?,?,?,?,?)",
                  (uid, rule, time.time(), score, kind, 1 if sent else 0, line[:500]))
        c.execute("DELETE FROM alert_log WHERE ts < ?", (time.time() - 60 * 86400,))
        c.commit()


def recent(hours: float = 24.0, limit: int = 200) -> list[dict]:
    with _lock:
        rows = _conn().execute("SELECT uid, rule, ts, score, kind, sent, line FROM alert_log "
                               "WHERE ts >= ? ORDER BY ts DESC LIMIT ?",
                               (time.time() - hours * 3600, limit)).fetchall()
    return [dict(r) for r in rows]


def for_event(uid: str) -> list[dict]:
    with _lock:
        rows = _conn().execute("SELECT rule, ts, score, kind, sent FROM alert_log WHERE uid = ? "
                               "ORDER BY ts", (uid,)).fetchall()
    return [dict(r) for r in rows]


def open_outcome(event: dict, rule: str) -> None:
    """Start scoring an event at its first alert (later alerts leave it alone)."""
    types = event.get("types") or {}
    if isinstance(types, str):
        try:
            types = json.loads(types)
        except ValueError:
            types = {}
    ranked = sorted(types.items(), key=lambda kv: -kv[1]) if isinstance(types, dict) else []
    top = next((k for k, _ in ranked if k != "other"), ranked[0][0] if ranked else None)
    with _lock:
        c = _conn()
        c.execute("INSERT OR IGNORE INTO alert_outcomes (uid, rule, alert_ts, first_seen, lat, lon, "
                  "top_type, score) VALUES (?,?,?,?,?,?,?,?)",
                  (event["uid"], rule, time.time(), event.get("first_seen"), event.get("lat"),
                   event.get("lon"), top, event.get("latest_score")))
        c.commit()


def _km(lat1, lon1, lat2, lon2) -> float:
    coslat = max(0.2, math.cos(math.radians(lat1)))
    return math.hypot(lat1 - lat2, (lon1 - lon2) * coslat) * 111.0


def score_pending(news: list, now: float | None = None) -> int:
    """Close pending outcomes against placed news signals (EventSignal-like objects
    with lat/lon/observed_at/normalized_type/summary/url). Returns how many closed."""
    now = now or time.time()
    with _lock:
        pending = [dict(r) for r in _conn().execute(
            "SELECT * FROM alert_outcomes WHERE status = 'pending'")]
    if not pending:
        return 0
    placed = [(s, s.observed_at.timestamp()) for s in news
              if s.lat is not None and s.lon is not None
              and s.normalized_type.value != "other"]
    closed = 0
    for o in pending:
        floor = (o["first_seen"] or o["alert_ts"]) - _NEWS_BEFORE_SEC
        best = None
        for s, ts in placed:
            if ts < floor or ts > now + 900:
                continue
            if o["top_type"] and o["top_type"] != "other" and s.normalized_type.value != o["top_type"]:
                continue
            if _km(o["lat"], o["lon"], s.lat, s.lon) > _NEWS_KM:
                continue
            if best is None or ts < best[1]:
                best = (s, ts)
        if best:
            lead = (best[1] - o["alert_ts"]) / 60.0
            status = "confirmed" if lead > 0 else "trailing"
            args = (status, best[1], best[0].summary[:300], best[0].url, round(lead, 1), now, o["uid"])
        elif now - o["alert_ts"] >= _CLOSE_AFTER_SEC:
            args = ("unconfirmed", None, None, None, None, now, o["uid"])
        else:
            continue
        with _lock:
            c = _conn()
            c.execute("UPDATE alert_outcomes SET status=?, news_ts=?, news_title=?, news_url=?, "
                      "lead_min=?, resolved_ts=? WHERE uid=?", args)
            c.execute("DELETE FROM alert_outcomes WHERE alert_ts < ?", (now - 90 * 86400,))
            c.commit()
        closed += 1
    return closed


def outcome(uid: str) -> dict | None:
    with _lock:
        r = _conn().execute("SELECT * FROM alert_outcomes WHERE uid = ?", (uid,)).fetchone()
    return dict(r) if r else None


def scorecard(days: float = 30.0) -> dict:
    """Precision and lead time over news for alerts raised in the window."""
    since = time.time() - days * 86400
    with _lock:
        rows = [dict(r) for r in _conn().execute(
            "SELECT * FROM alert_outcomes WHERE alert_ts >= ? ORDER BY alert_ts DESC", (since,))]
        sent = _conn().execute("SELECT COUNT(*), SUM(sent) FROM alert_log WHERE ts >= ?",
                               (since,)).fetchone()

    def _roll(items: list[dict]) -> dict:
        counts = {k: sum(1 for r in items if r["status"] == k)
                  for k in ("confirmed", "trailing", "unconfirmed", "pending")}
        resolved = counts["confirmed"] + counts["trailing"] + counts["unconfirmed"]
        leads = sorted(r["lead_min"] for r in items if r["status"] == "confirmed"
                       and r["lead_min"] is not None)
        return {
            "alerts": len(items), **counts,
            "news_matched_share": (round((counts["confirmed"] + counts["trailing"]) / resolved, 3)
                                   if resolved else None),
            "first_share": round(counts["confirmed"] / resolved, 3) if resolved else None,
            "median_lead_min": leads[len(leads) // 2] if leads else None,
        }

    by_rule: dict[str, list[dict]] = {}
    for r in rows:
        by_rule.setdefault(r["rule"] or "default", []).append(r)
    return {
        "days": days, **_roll(rows),
        "messages_logged": sent[0] or 0, "messages_sent": int(sent[1] or 0),
        "by_rule": {k: _roll(v) for k, v in by_rule.items()},
        "recent": [{k: r[k] for k in ("uid", "rule", "alert_ts", "status", "lead_min", "top_type",
                                      "score", "news_title", "news_url", "lat", "lon")}
                   for r in rows[:25]],
        "note": ("news_matched_share counts an alert as right when a placed headline of the "
                 "same kind appeared within 30 km; unreported real events score as unconfirmed."),
    }


def kv_get(key: str) -> str | None:
    with _lock:
        r = _conn().execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
    return r[0] if r else None


def kv_set(key: str, value: str) -> None:
    with _lock:
        c = _conn()
        c.execute("INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                  (key, value))
        c.commit()
