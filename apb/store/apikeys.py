"""API platform state: applications, keys and daily usage (snapshot SQLite file).

Secrets are never stored: API keys and claim tokens are kept as SHA-256 hashes plus a
short display prefix. A key is shown once, when the approved applicant claims it.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time

from apb.store import snapshots

_lock = snapshots.db_lock
_ready = False

KEY_PREFIX = "pk_"


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS api_applications (
            id TEXT PRIMARY KEY, created REAL NOT NULL, name TEXT, email TEXT, org TEXT,
            website TEXT, use_case TEXT, datasets TEXT, volume TEXT, ip_hash TEXT,
            status TEXT NOT NULL DEFAULT 'pending', tier TEXT, note TEXT, decided_at REAL,
            claim_hash TEXT NOT NULL, key_id TEXT, grant_datasets TEXT, claim_hash2 TEXT);
        CREATE INDEX IF NOT EXISTS idx_apiapp_status ON api_applications(status, created);
        CREATE TABLE IF NOT EXISTS api_keys (
            id TEXT PRIMARY KEY, app_id TEXT, label TEXT, email TEXT, tier TEXT NOT NULL,
            datasets TEXT, key_hash TEXT NOT NULL UNIQUE, prefix TEXT NOT NULL,
            created REAL NOT NULL, revoked REAL, last_used REAL);
        CREATE TABLE IF NOT EXISTS api_usage (
            key_id TEXT NOT NULL, day TEXT NOT NULL, dataset TEXT NOT NULL, n INTEGER NOT NULL,
            PRIMARY KEY (key_id, day, dataset));
        """)
        c.commit()
        _ready = True
    return c


def _h(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def day(now: float | None = None) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(now or time.time()))


def _app(r) -> dict:
    d = dict(r)
    d["datasets"] = json.loads(d["datasets"]) if d.get("datasets") else []       # what was asked for
    d["grant_datasets"] = json.loads(d["grant_datasets"]) if d.get("grant_datasets") else []   # [] = all
    d.pop("claim_hash", None)
    d.pop("claim_hash2", None)
    return d


# ---- applications ---------------------------------------------------------------------

def add_application(fields: dict, ip: str) -> tuple[str, str]:
    """Store a pending application; returns (application id, claim token)."""
    app_id = "app_" + secrets.token_hex(6)
    claim = secrets.token_urlsafe(24)
    with _lock:
        c = _conn()
        c.execute("INSERT INTO api_applications (id, created, name, email, org, website, use_case, datasets, "
                  "volume, ip_hash, claim_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                  (app_id, time.time(), fields["name"], fields["email"], fields.get("org"),
                   fields.get("website"), fields["use_case"], json.dumps(fields.get("datasets") or []),
                   fields.get("volume"), _h("ip:" + ip)[:16], _h(claim)))
        c.commit()
    return app_id, claim


def recent_from_ip(ip: str, since: float) -> int:
    with _lock:
        return _conn().execute("SELECT COUNT(*) FROM api_applications WHERE ip_hash = ? AND created >= ?",
                               (_h("ip:" + ip)[:16], since)).fetchone()[0]


def application(app_id: str) -> dict | None:
    with _lock:
        r = _conn().execute("SELECT * FROM api_applications WHERE id = ?", (app_id,)).fetchone()
    return _app(r) if r else None


def application_by_claim(claim: str) -> dict | None:
    with _lock:
        r = _conn().execute("SELECT * FROM api_applications WHERE claim_hash = ? OR claim_hash2 = ?",
                            (_h(claim), _h(claim))).fetchone()
    return _app(r) if r else None


def new_claim(app_id: str) -> str:
    """A fresh claim link for emailing (approval, reissue). The link shown when the
    application was sent keeps working; only the previous emailed link is replaced."""
    claim = secrets.token_urlsafe(24)
    with _lock:
        c = _conn()
        c.execute("UPDATE api_applications SET claim_hash2 = ? WHERE id = ?", (_h(claim), app_id))
        c.commit()
    return claim


def applications(status: str | None = None, limit: int = 200) -> list[dict]:
    sql = "SELECT * FROM api_applications" + (" WHERE status = ?" if status else "") + " ORDER BY created DESC LIMIT ?"
    with _lock:
        rows = _conn().execute(sql, ((status, limit) if status else (limit,))).fetchall()
    return [_app(r) for r in rows]


def decide(app_id: str, status: str, tier: str | None, note: str | None,
           datasets: list[str] | None = None) -> dict | None:
    with _lock:
        c = _conn()
        c.execute("UPDATE api_applications SET status = ?, tier = ?, note = ?, decided_at = ?, "
                  "grant_datasets = ? WHERE id = ?",
                  (status, tier, note, time.time(), json.dumps(datasets) if datasets else None, app_id))
        c.commit()
    return application(app_id)


def reissue(app_id: str) -> bool:
    """Revoke an approved application's key so the applicant can claim a new one."""
    with _lock:
        c = _conn()
        r = c.execute("SELECT key_id FROM api_applications WHERE id = ? AND status = 'approved'", (app_id,)).fetchone()
        if not r:
            return False
        if r[0]:
            c.execute("UPDATE api_keys SET revoked = ? WHERE id = ?", (time.time(), r[0]))
        c.execute("UPDATE api_applications SET key_id = NULL WHERE id = ?", (app_id,))
        c.commit()
    return True


# ---- keys -----------------------------------------------------------------------------

def issue_key(app: dict) -> str:
    """Create the key for an approved application (once) and return the secret."""
    secret = KEY_PREFIX + secrets.token_urlsafe(30)
    key_id = "key_" + secrets.token_hex(6)
    with _lock:
        c = _conn()
        with c:
            c.execute("BEGIN IMMEDIATE")
            cur = c.execute("SELECT key_id, status FROM api_applications WHERE id = ?", (app["id"],)).fetchone()
            if not cur or cur[0] or cur[1] != "approved":
                return ""
            c.execute("INSERT INTO api_keys (id, app_id, label, email, tier, datasets, key_hash, prefix, created) "
                      "VALUES (?,?,?,?,?,?,?,?,?)",
                      (key_id, app["id"], app.get("org") or app.get("name"), app.get("email"), app["tier"],
                       json.dumps(app.get("grant_datasets") or []), _h(secret), secret[:10], time.time()))
            c.execute("UPDATE api_applications SET key_id = ? WHERE id = ?", (key_id, app["id"]))
    return secret


def key_for(secret: str) -> dict | None:
    if not secret or not secret.startswith(KEY_PREFIX):
        return None
    with _lock:
        r = _conn().execute("SELECT * FROM api_keys WHERE key_hash = ?", (_h(secret),)).fetchone()
    if not r:
        return None
    d = dict(r)
    d["datasets"] = json.loads(d["datasets"]) if d.get("datasets") else []
    return d


def keys(limit: int = 500) -> list[dict]:
    with _lock:
        rows = _conn().execute("SELECT id, app_id, label, email, tier, datasets, prefix, created, revoked, last_used "
                               "FROM api_keys ORDER BY created DESC LIMIT ?", (limit,)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["datasets"] = json.loads(d["datasets"]) if d.get("datasets") else []
        out.append(d)
    return out


def update_key(key_id: str, *, tier: str | None = None, revoke: bool | None = None,
               datasets: list[str] | None = None) -> bool:
    sets, vals = [], []
    if tier:
        sets.append("tier = ?")
        vals.append(tier)
    if revoke is not None:
        sets.append("revoked = ?")
        vals.append(time.time() if revoke else None)
    if datasets is not None:
        sets.append("datasets = ?")
        vals.append(json.dumps(datasets))
    if not sets:
        return False
    with _lock:
        c = _conn()
        n = c.execute(f"UPDATE api_keys SET {', '.join(sets)} WHERE id = ?", (*vals, key_id)).rowcount
        c.commit()
    return n > 0


# ---- usage ----------------------------------------------------------------------------

def add_usage(counts: dict[tuple[str, str, str], int], last_used: dict[str, float]) -> None:
    """Flush buffered request counts: (key id, day, dataset) -> n."""
    if not counts and not last_used:
        return
    with _lock:
        c = _conn()
        c.executemany("INSERT INTO api_usage VALUES (?,?,?,?) ON CONFLICT(key_id, day, dataset) "
                      "DO UPDATE SET n = n + excluded.n", [(k, d, ds, n) for (k, d, ds), n in counts.items()])
        c.executemany("UPDATE api_keys SET last_used = MAX(COALESCE(last_used, 0), ?) WHERE id = ?",
                      [(t, k) for k, t in last_used.items()])
        c.commit()


def usage_today(key_id: str, on: str | None = None) -> int:
    with _lock:
        r = _conn().execute("SELECT COALESCE(SUM(n), 0) FROM api_usage WHERE key_id = ? AND day = ?",
                            (key_id, on or day())).fetchone()
    return int(r[0])


def usage(key_id: str, days: int = 30) -> list[dict]:
    since = day(time.time() - days * 86400)
    with _lock:
        rows = _conn().execute("SELECT day, dataset, n FROM api_usage WHERE key_id = ? AND day >= ? "
                               "ORDER BY day DESC, n DESC", (key_id, since)).fetchall()
    return [dict(r) for r in rows]
