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
        CREATE TABLE IF NOT EXISTS radio_recent (
            key TEXT NOT NULL, id INTEGER NOT NULL, ts REAL NOT NULL, system INTEGER,
            talkgroup INTEGER, PRIMARY KEY (key, id));
        CREATE INDEX IF NOT EXISTS idx_rr_ts ON radio_recent(ts);
        CREATE INDEX IF NOT EXISTS idx_rr_ks ON radio_recent(key, system, ts);
        CREATE TABLE IF NOT EXISTS radio_transcripts (
            key TEXT NOT NULL, id INTEGER NOT NULL, ts REAL NOT NULL, system INTEGER,
            talkgroup INTEGER, status TEXT NOT NULL, weight REAL, tg_label TEXT, tag TEXT,
            kind TEXT, incident_type TEXT, text TEXT, summary TEXT, location_text TEXT,
            units TEXT, priority REAL, seconds REAL, lat REAL, lon REAL, done_at REAL,
            PRIMARY KEY (key, id));
        CREATE INDEX IF NOT EXISTS idx_rt_ts ON radio_transcripts(ts);
        CREATE TABLE IF NOT EXISTS radio_profiles (
            key TEXT NOT NULL, system INTEGER NOT NULL, area TEXT, state TEXT, lat REAL,
            lon REAL, statewide INTEGER, confidence REAL, dispatch_tgs TEXT, profiled_at REAL,
            PRIMARY KEY (key, system));
        CREATE TABLE IF NOT EXISTS radio_areas (
            key TEXT NOT NULL, system INTEGER NOT NULL, ts REAL NOT NULL, calls INTEGER,
            newest REAL, areas TEXT, PRIMARY KEY (key, system, ts));
        CREATE TABLE IF NOT EXISTS radio_threads (
            tid TEXT PRIMARY KEY, key TEXT NOT NULL, system INTEGER NOT NULL, first_ts REAL,
            last_ts REAL, type TEXT, title TEXT, activity TEXT, area TEXT, state TEXT,
            priority REAL, status TEXT, loc_text TEXT, lat REAL, lon REAL, precision TEXT,
            calls TEXT, updated REAL, published INTEGER DEFAULT 0);
        CREATE INDEX IF NOT EXISTS idx_rth ON radio_threads(key, system, last_ts);
        CREATE TABLE IF NOT EXISTS radio_stt_jobs (
            name TEXT PRIMARY KEY, created REAL NOT NULL, model TEXT, meta TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS radio_state (
            key TEXT PRIMARY KEY, last_id INTEGER, last_poll REAL, state TEXT,
            fails INTEGER DEFAULT 0, next_at REAL DEFAULT 0);
        """)
        cols = {r[1] for r in c.execute("PRAGMA table_info(radio_recent)").fetchall()}
        if "seen" not in cols:                    # when the lane first saw the call (live feed)
            c.execute("ALTER TABLE radio_recent ADD COLUMN seen REAL")
        c.execute("CREATE INDEX IF NOT EXISTS idx_rr_seen ON radio_recent(seen)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_rt_done ON radio_transcripts(done_at)")
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


RECENT_SEC = 6 * 3600.0
TRANSCRIPT_SEC = 3 * 86400.0


def add_recent(key: str, rows: list[tuple]) -> int:
    """Remember call metadata (id, ts, system, talkgroup) for playback and selection."""
    if not rows:
        return 0
    now = time.time()
    with _lock:
        c = _conn()
        c.executemany("INSERT OR IGNORE INTO radio_recent (key, id, ts, system, talkgroup, seen) "
                      "VALUES (?,?,?,?,?,?)", [(key, int(i), float(ts), s, tg, now) for i, ts, s, tg in rows])
        c.execute("DELETE FROM radio_recent WHERE ts < ?", (now - RECENT_SEC,))
        c.commit()
    return len(rows)


def candidates(since: float, limit: int = 4000, until: float | None = None) -> list[dict]:
    """Recent calls nobody has tried to transcribe yet (or queued), newest first."""
    with _lock:
        rows = _conn().execute(
            "SELECT r.key, r.id, r.ts, r.system, r.talkgroup FROM radio_recent r "
            "LEFT JOIN radio_transcripts t ON t.key = r.key AND t.id = r.id "
            "WHERE r.ts >= ? AND r.ts < ? AND t.id IS NULL ORDER BY r.ts DESC LIMIT ?",
            (since, until if until is not None else 9e18, limit)).fetchall()
    return [dict(r) for r in rows]


def add_job(name: str, model: str, meta: list, now: float | None = None) -> None:
    """A submitted batch transcription job and the calls in each of its requests."""
    with _lock:
        c = _conn()
        c.execute("INSERT OR REPLACE INTO radio_stt_jobs VALUES (?,?,?,?)",
                  (name, now or time.time(), model, json.dumps(meta)))
        c.commit()


def jobs() -> list[dict]:
    with _lock:
        rows = _conn().execute("SELECT * FROM radio_stt_jobs ORDER BY created").fetchall()
    return [{**dict(r), "meta": json.loads(r["meta"])} for r in rows]


def drop_job(name: str) -> None:
    with _lock:
        c = _conn()
        c.execute("DELETE FROM radio_stt_jobs WHERE name = ?", (name,))
        c.commit()


def add_transcript(key: str, call_id: int, ts: float, system, talkgroup, status: str,
                   **fields) -> None:
    """Record an attempt (status done / skipped / failed) and, when done, its redacted
    result. Raw transcripts are never stored."""
    import json as _json
    units = fields.get("units")
    row = (key, int(call_id), float(ts), system, talkgroup, status, fields.get("weight"),
           fields.get("tg_label"), fields.get("tag"), fields.get("kind"), fields.get("incident_type"),
           fields.get("text"), fields.get("summary"), fields.get("location_text"),
           _json.dumps(units) if units is not None else None, fields.get("priority"),
           fields.get("seconds"), fields.get("lat"), fields.get("lon"), time.time())
    with _lock:
        c = _conn()
        c.execute("INSERT OR REPLACE INTO radio_transcripts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row)
        c.execute("DELETE FROM radio_transcripts WHERE ts < ?", (time.time() - TRANSCRIPT_SEC,))
        c.commit()


def _tr(r) -> dict:
    import json as _json
    d = dict(r)
    try:
        d["units"] = _json.loads(d["units"]) if d.get("units") else []
    except ValueError:
        d["units"] = []
    return d


def recent_calls(key: str, system: int, limit: int = 40) -> list[dict]:
    """Newest calls on one system, with the transcript when there is one."""
    with _lock:
        rows = _conn().execute(
            "SELECT r.id, r.ts, r.talkgroup, t.status, t.kind, t.incident_type, t.text, t.summary, "
            "t.location_text, t.priority, t.lat, t.lon FROM radio_recent r "
            "LEFT JOIN radio_transcripts t ON t.key = r.key AND t.id = r.id "
            "WHERE r.key = ? AND r.system = ? ORDER BY r.ts DESC LIMIT ?",
            (key, system, limit)).fetchall()
    return [dict(r) for r in rows]


def feed(since: float, max_calls: int = 400, max_lines: int = 60) -> dict:
    """What happened after `since` (server time): calls the lane newly saw, calls newly
    transcribed, and incident threads newly written. Feeds the map's live indicators."""
    with _lock:
        c = _conn()
        calls = c.execute("SELECT key, id, ts, system, talkgroup FROM radio_recent WHERE seen > ? "
                          "ORDER BY seen, ts LIMIT ?", (since, max_calls)).fetchall()
        lines = c.execute("SELECT * FROM radio_transcripts WHERE done_at > ? AND status = 'done' "
                          "ORDER BY done_at LIMIT ?", (since, max_lines)).fetchall()
        threads = c.execute("SELECT tid FROM radio_threads WHERE updated > ?", (since,)).fetchall()
    return {"calls": [dict(r) for r in calls], "lines": [_tr(r) for r in lines],
            "threads": [r[0] for r in threads]}


def has_call(key: str, call_id: int) -> bool:
    with _lock:
        return _conn().execute("SELECT 1 FROM radio_recent WHERE key = ? AND id = ?",
                               (key, call_id)).fetchone() is not None


def transcripts(max_age_sec: float = 3600.0, located_only: bool = False,
                min_priority: float = 0.0, limit: int = 500) -> list[dict]:
    sql = ("SELECT * FROM radio_transcripts WHERE status = 'done' AND ts >= ? AND "
           "COALESCE(priority, 0) >= ?" + (" AND lat IS NOT NULL" if located_only else "")
           + " ORDER BY ts DESC LIMIT ?")
    with _lock:
        rows = _conn().execute(sql, (time.time() - max_age_sec, min_priority, limit)).fetchall()
    return [_tr(r) for r in rows]


def stt_stats(hours: float = 24.0) -> dict:
    since = time.time() - hours * 3600
    with _lock:
        rows = _conn().execute("SELECT status, COUNT(*), COALESCE(SUM(seconds),0) FROM radio_transcripts "
                               "WHERE done_at >= ? GROUP BY status", (since,)).fetchall()
        located = _conn().execute("SELECT COUNT(*) FROM radio_transcripts WHERE done_at >= ? AND "
                                  "status = 'done' AND lat IS NOT NULL", (since,)).fetchone()[0]
    out = {r[0]: r[1] for r in rows}
    out["audio_sec"] = round(sum(r[2] for r in rows))
    out["located"] = located
    return out


def set_profile(key: str, system: int, area: str | None, state: str | None,
                lat: float | None, lon: float | None, statewide: bool, confidence: float,
                dispatch_tgs: list[int]) -> None:
    import json as _json
    with _lock:
        c = _conn()
        c.execute("INSERT OR REPLACE INTO radio_profiles VALUES (?,?,?,?,?,?,?,?,?,?)",
                  (key, system, area, state, lat, lon, 1 if statewide else 0, confidence,
                   _json.dumps(sorted(set(int(t) for t in dispatch_tgs))), time.time()))
        c.commit()


def profiles() -> dict[tuple, dict]:
    import json as _json
    with _lock:
        rows = _conn().execute("SELECT * FROM radio_profiles").fetchall()
    out = {}
    for r in rows:
        d = dict(r)
        try:
            d["dispatch_tgs"] = set(_json.loads(d.get("dispatch_tgs") or "[]"))
        except ValueError:
            d["dispatch_tgs"] = set()
        out[(d["key"], d["system"])] = d
    return out


def learned_places(min_n: int = 3) -> dict[tuple, dict]:
    """Where each system's located dispatches actually were (median), once there are
    enough of them. Internal only: built from block-level geocodes."""
    with _lock:
        rows = _conn().execute("SELECT key, system, lat, lon FROM radio_transcripts "
                               "WHERE status = 'done' AND lat IS NOT NULL").fetchall()
    pts: dict[tuple, list] = {}
    for k, s, la, lo in rows:
        pts.setdefault((k, s), []).append((la, lo))
    out = {}
    for ks, p in pts.items():
        if len(p) >= min_n:
            lats, lons = sorted(x[0] for x in p), sorted(x[1] for x in p)
            out[ks] = {"lat": lats[len(lats) // 2], "lon": lons[len(lons) // 2], "n": len(p)}
    return out


def talkgroup_stats() -> dict[tuple, tuple[int, int]]:
    """(key, system, talkgroup) -> (transcribed calls, of which dispatch)."""
    with _lock:
        rows = _conn().execute(
            "SELECT key, system, talkgroup, COUNT(*), SUM(CASE WHEN kind = 'dispatch' THEN 1 ELSE 0 END) "
            "FROM radio_transcripts WHERE status = 'done' GROUP BY key, system, talkgroup").fetchall()
    return {(r[0], r[1], r[2]): (r[3], r[4] or 0) for r in rows}


def active_talkgroups(key: str, system: int, hours: float = 6.0, limit: int = 60) -> list[tuple[int, int]]:
    with _lock:
        rows = _conn().execute("SELECT talkgroup, COUNT(*) AS n FROM radio_recent WHERE key = ? AND "
                               "system = ? AND ts >= ? GROUP BY talkgroup ORDER BY n DESC LIMIT ?",
                               (key, system, time.time() - hours * 3600, limit)).fetchall()
    return [(r[0], r[1]) for r in rows]


def active_systems(hours: float = 6.0) -> list[tuple[str, int, int]]:
    with _lock:
        rows = _conn().execute("SELECT key, system, COUNT(*) AS n FROM radio_recent WHERE ts >= ? "
                               "GROUP BY key, system ORDER BY n DESC", (time.time() - hours * 3600,)).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


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


def add_areas(key: str, system: int, calls: int, newest: float, areas: list[dict],
              now: float | None = None) -> None:
    """One area summary of a system's recent transcribed traffic (kept three days)."""
    now = now or time.time()
    with _lock:
        c = _conn()
        c.execute("INSERT OR REPLACE INTO radio_areas VALUES (?, ?, ?, ?, ?, ?)",
                  (key, system, now, calls, newest, json.dumps(areas)))
        c.execute("DELETE FROM radio_areas WHERE ts < ?", (now - 3 * 86400,))
        c.commit()


def latest_areas(max_age_sec: float = 5400.0) -> list[dict]:
    """The newest area summary per system, newer than max_age_sec."""
    with _lock:
        rows = _conn().execute(
            "SELECT a.* FROM radio_areas a JOIN (SELECT key, system, MAX(ts) m FROM radio_areas "
            "GROUP BY key, system) b ON a.key = b.key AND a.system = b.system AND a.ts = b.m "
            "WHERE a.ts >= ? ORDER BY a.ts DESC", (time.time() - max_age_sec,)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["areas"] = json.loads(d["areas"] or "[]")
        except ValueError:
            d["areas"] = []
        out.append(d)
    return out


_THREAD_COLS = ("tid", "key", "system", "first_ts", "last_ts", "type", "title", "activity", "area",
                "state", "priority", "status", "loc_text", "lat", "lon", "precision", "calls",
                "updated", "published")


def save_thread(t: dict) -> None:
    """Insert or replace one incident thread (calls = list of call ids)."""
    row = {**t, "calls": json.dumps(sorted(set(int(c) for c in t.get("calls") or []))),
           "updated": t.get("updated") or time.time(), "published": int(bool(t.get("published")))}
    with _lock:
        c = _conn()
        c.execute(f"INSERT OR REPLACE INTO radio_threads ({', '.join(_THREAD_COLS)}) VALUES "
                  f"({', '.join('?' for _ in _THREAD_COLS)})", tuple(row.get(k) for k in _THREAD_COLS))
        c.execute("DELETE FROM radio_threads WHERE last_ts < ?", (time.time() - 3 * 86400,))
        c.commit()


def _thread(r) -> dict:
    d = dict(r)
    try:
        d["calls"] = json.loads(d.get("calls") or "[]")
    except ValueError:
        d["calls"] = []
    return d


def threads(max_age_sec: float = 10800.0, key: str | None = None, system: int | None = None) -> list[dict]:
    """Incident threads active within max_age_sec, newest activity first."""
    sql, args = "SELECT * FROM radio_threads WHERE last_ts >= ?", [time.time() - max_age_sec]
    if key is not None:
        sql += " AND key = ? AND system = ?"
        args += [key, system]
    with _lock:
        rows = _conn().execute(sql + " ORDER BY last_ts DESC", args).fetchall()
    return [_thread(r) for r in rows]


def thread(tid: str) -> dict | None:
    with _lock:
        r = _conn().execute("SELECT * FROM radio_threads WHERE tid = ?", (tid,)).fetchone()
    return _thread(r) if r else None


def transcripts_for(key: str, ids: list[int]) -> dict[int, dict]:
    """{call id: transcript row} for the given calls of one instance."""
    ids = [int(i) for i in ids][:500]
    if not ids:
        return {}
    with _lock:
        rows = _conn().execute(f"SELECT * FROM radio_transcripts WHERE key = ? AND id IN "
                               f"({', '.join('?' for _ in ids)})", [key, *ids]).fetchall()
    return {r["id"]: _tr(r) for r in rows}
