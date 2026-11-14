"""Feed-silence detection — tell "nothing is happening" apart from "the feed broke".

A lane that returns zero rows, or keeps serving last Tuesday's rows, looks exactly
like a calm city on the map. This store learns each feed's own normal (rows per
fetch, how old its freshest record usually is) and names the feeds that left it:

  failing   the fetch itself errors (the ingest layer is backing off)
  silent    fetches succeed but return nothing, for a feed that normally has rows
  stale     rows arrive but the freshest one is far older than this feed's usual lag
  ok        within its own normal (also while still learning)

Baselines freeze while a feed is in a problem state, so a long outage does not
become the new normal. Lives in the snapshot store's SQLite file; written by the
poller leader once per cycle.
"""
from __future__ import annotations

import sqlite3
import time

from apb.store import snapshots

_lock = snapshots.db_lock
_ready = False

PROBLEM = ("failing", "silent", "stale")
_MIN_SAMPLES = 8              # fetches before a feed has a baseline worth judging
_ALPHA = 0.15                 # EWMA weight for the per-feed normals
_FAILS = 3                    # consecutive fetch failures before "failing"
_SILENT_MIN_SEC = 3600.0      # zero rows must persist this long...
_SILENT_EVIDENCE = 20.0       # ...and typical_rows x zero_run must reach this
_STALE_FLOOR_SEC = 3 * 3600.0
_STALE_FACTOR = 4.0
# Lanes where an empty answer is ordinary (no cyclone, no SIGMET, no squawk 7700).
BURSTY = {"nhc", "spc", "volcano", "sigmet", "hms_smoke", "squawk", "faa_delay",
          "nifc_fire", "gdacs", "usgs_flood", "firms", "faa_tfr", "radar"}


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS feed_health (
            slug TEXT PRIMARY KEY, kind TEXT, name TEXT, samples INTEGER DEFAULT 0,
            typical_rows REAL DEFAULT 0, typical_lag REAL, last_fetch REAL DEFAULT 0,
            last_rows INTEGER, last_nonzero REAL, zero_run INTEGER DEFAULT 0,
            freshest REAL, fails INTEGER DEFAULT 0, state TEXT DEFAULT 'ok',
            state_since REAL, reason TEXT);
        """)
        c.commit()
        _ready = True
    return c


def _judge(row: dict, now: float) -> tuple[str, str]:
    """State + one-line reason from a feed's current record."""
    if row["fails"] >= _FAILS:
        return "failing", f"{row['fails']} consecutive fetch failures"
    if row["samples"] < _MIN_SAMPLES:
        return "ok", "learning"
    if row["kind"] not in BURSTY and row["last_rows"] == 0:
        quiet_for = now - (row["last_nonzero"] or row["state_since"] or now)
        if (row["typical_rows"] >= 1.0 and quiet_for >= _SILENT_MIN_SEC
                and row["typical_rows"] * row["zero_run"] >= _SILENT_EVIDENCE):
            return "silent", (f"0 rows for {quiet_for / 3600:.1f} h; "
                              f"normally ~{row['typical_rows']:.0f} per fetch")
    if row["last_rows"] and row["freshest"] and row["typical_lag"] is not None:
        lag = now - row["freshest"]
        limit = max(_STALE_FLOOR_SEC, _STALE_FACTOR * row["typical_lag"])
        if lag > limit:
            return "stale", (f"freshest record {lag / 3600:.1f} h old; "
                             f"normally {row['typical_lag'] / 3600:.1f} h")
    return "ok", ""


def observe(observations: list[dict], now: float | None = None) -> list[dict]:
    """Fold one cycle of per-feed observations into the store.

    Each observation: {slug, kind, name, fetched_at, rows, freshest, fails}.
    `fetched_at` is the time of the feed's last SUCCESSFUL fetch (0/None if never);
    an unchanged `fetched_at` means "no new fetch since last cycle" and only the
    failure count is refreshed. Returns the state transitions this cycle caused:
    [{slug, kind, name, from, to, reason}].
    """
    now = now or time.time()
    changes: list[dict] = []
    with _lock:
        c = _conn()
        known = {r["slug"]: dict(r) for r in c.execute("SELECT * FROM feed_health")}
        for o in observations:
            slug = o["slug"]
            row = known.get(slug) or {
                "slug": slug, "kind": o.get("kind"), "name": o.get("name"), "samples": 0,
                "typical_rows": 0.0, "typical_lag": None, "last_fetch": 0.0,
                "last_rows": None, "last_nonzero": None, "zero_run": 0, "freshest": None,
                "fails": 0, "state": "ok", "state_since": now, "reason": ""}
            row["kind"], row["name"] = o.get("kind"), o.get("name") or row.get("name")
            row["fails"] = int(o.get("fails") or 0)
            fetched = float(o.get("fetched_at") or 0.0)
            if fetched > (row["last_fetch"] or 0.0) + 1.0:          # a new successful fetch
                rows = int(o.get("rows") or 0)
                fresh = o.get("freshest") or None
                was_ok = row["state"] == "ok"
                row["last_fetch"], row["last_rows"] = fetched, rows
                row["samples"] += 1
                if rows > 0:
                    row["last_nonzero"], row["zero_run"] = fetched, 0
                else:
                    row["zero_run"] += 1
                prev_fresh = row["freshest"]
                if fresh:
                    row["freshest"] = float(fresh)
                # normals move only while healthy (or still learning): an outage must
                # not teach the store that empty is normal
                if was_ok and (rows > 0 or row["samples"] <= _MIN_SAMPLES
                               or row["kind"] in BURSTY):
                    row["typical_rows"] = (rows if row["samples"] == 1 else
                                           (1 - _ALPHA) * row["typical_rows"] + _ALPHA * rows)
                # typical_lag = how old the newest record usually gets BEFORE a newer one
                # shows up. Learned only at the moment freshness advances: measuring it
                # on every fetch lets the baseline chase a frozen feed forever.
                if fresh and rows > 0:
                    if prev_fresh is None:
                        row["typical_lag"] = max(0.0, fetched - float(fresh))
                    elif was_ok and float(fresh) > prev_fresh + 1.0:
                        peak = max(0.0, fetched - prev_fresh)
                        row["typical_lag"] = (peak if row["typical_lag"] is None else
                                              (1 - _ALPHA) * row["typical_lag"] + _ALPHA * peak)
            state, reason = _judge(row, now)
            if state != row["state"]:
                if state in PROBLEM or row["state"] in PROBLEM:
                    changes.append({"slug": slug, "kind": row["kind"], "name": row["name"],
                                    "from": row["state"], "to": state,
                                    "reason": reason or "recovered"})
                row["state"], row["state_since"] = state, now
            row["reason"] = reason
            c.execute("""
                INSERT INTO feed_health (slug, kind, name, samples, typical_rows, typical_lag,
                    last_fetch, last_rows, last_nonzero, zero_run, freshest, fails, state,
                    state_since, reason)
                VALUES (:slug,:kind,:name,:samples,:typical_rows,:typical_lag,:last_fetch,
                    :last_rows,:last_nonzero,:zero_run,:freshest,:fails,:state,:state_since,
                    :reason)
                ON CONFLICT(slug) DO UPDATE SET kind=excluded.kind, name=excluded.name,
                    samples=excluded.samples, typical_rows=excluded.typical_rows,
                    typical_lag=excluded.typical_lag, last_fetch=excluded.last_fetch,
                    last_rows=excluded.last_rows, last_nonzero=excluded.last_nonzero,
                    zero_run=excluded.zero_run, freshest=excluded.freshest,
                    fails=excluded.fails, state=excluded.state,
                    state_since=excluded.state_since, reason=excluded.reason
            """, row)
        # feeds that left the registry stop being reported
        live = {o["slug"] for o in observations}
        gone = [s for s in known if s not in live]
        if gone and observations:
            c.executemany("DELETE FROM feed_health WHERE slug = ?", [(s,) for s in gone])
        c.commit()
    return changes


def summary(limit: int = 60) -> dict:
    """Counts by state and by kind, plus the problem feeds (longest-broken first)."""
    now = time.time()
    with _lock:
        rows = [dict(r) for r in _conn().execute("SELECT * FROM feed_health")]
    by_state: dict[str, int] = {}
    by_kind: dict[str, dict[str, int]] = {}
    learning = 0
    for r in rows:
        by_state[r["state"]] = by_state.get(r["state"], 0) + 1
        k = by_kind.setdefault(r["kind"] or "?", {})
        k[r["state"]] = k.get(r["state"], 0) + 1
        if r["samples"] < _MIN_SAMPLES and r["state"] == "ok":
            learning += 1
    problems = sorted((r for r in rows if r["state"] in PROBLEM),
                      key=lambda r: r["state_since"] or now)
    total = len(rows)
    bad = sum(by_state.get(s, 0) for s in PROBLEM)
    return {
        "feeds": total, "by_state": by_state, "learning": learning,
        "healthy_share": round(1 - bad / total, 3) if total else None,
        "by_kind": {k: v for k, v in sorted(by_kind.items())
                    if any(s in v for s in PROBLEM)},
        "problems": [{
            "slug": r["slug"], "kind": r["kind"], "name": r["name"], "state": r["state"],
            "reason": r["reason"],
            "for_min": round((now - (r["state_since"] or now)) / 60),
            "typical_rows": round(r["typical_rows"] or 0, 1),
            "last_fetch_min": (round((now - r["last_fetch"]) / 60)
                               if r["last_fetch"] else None),
        } for r in problems[:limit]],
        "problems_total": len(problems),
    }


def states() -> dict[str, str]:
    """slug -> state, for callers that only need to know who is blind."""
    with _lock:
        return {r[0]: r[1] for r in _conn().execute("SELECT slug, state FROM feed_health")}

