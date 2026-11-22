"""Keyed API platform: apply for a key, owner approval, and /v1 dataset relays.

Flow
  1. An applicant fills the form on /developers/ -> POST /v1/apply. The owner is
     notified (apb/alerts/mail.py); the applicant gets a private claim link.
  2. The owner approves or denies on /developers/admin/ (APB_ADMIN_TOKEN), choosing a
     tier (config/api_tiers.yaml) and optionally limiting the key to some datasets.
  3. The applicant opens the claim link; once approved, the key is generated and shown
     exactly once (only its SHA-256 is stored).
  4. Keyed calls: GET /v1/data/{dataset}[/{id}] with `X-API-Key: pk_...` (or
     `Authorization: Bearer pk_...`).

Datasets (config/api_datasets.yaml) relay internal routes through a short shared cache
with single-flight, so N clients asking for the same thing cost one internal fetch and
upstream sources see no more traffic than the map already causes. Fields that carry
raw text from restricted sources are stripped. Only `enabled` datasets are served.

Throttling per key: a token bucket (per_minute, burst = per_minute) and a daily quota
of request units (UTC day, a dataset's `cost` per request), plus a global cap on
concurrent relay fetches so keyed traffic cannot starve the map.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import os
import re
import secrets
import threading
import time
from pathlib import Path

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

_CONFIG = Path(__file__).resolve().parents[2] / "config"
INTERNAL_TOKEN = secrets.token_urlsafe(24)          # marks the relay's own sub-requests
router = APIRouter()

_registry: dict | None = None
_tiers: dict | None = None
_buckets: dict[str, list[float]] = {}               # key id -> [tokens, last refill]
_daily: dict[str, list] = {}                        # key id -> [day, units]
_pending: dict[tuple[str, str, str], int] = {}      # (key id, day, dataset) -> n, not yet flushed
_pending_used: dict[str, float] = {}
_flushed = [0.0]
_lock = threading.Lock()
_cache: dict[str, tuple[float, int, bytes]] = {}    # internal url -> (expires, status, body)
_inflight: dict[str, asyncio.Lock] = {}
_sem: asyncio.Semaphore | None = None
_stats = {"requests": 0, "relay_hits": 0, "relay_misses": 0, "throttled": 0, "quota": 0,
          "applications": 0, "denied_auth": 0}

EMAIL_RE = re.compile(r"^[^@\s]{1,64}@[^@\s]{1,190}\.[A-Za-z]{2,24}$")


# ---- config ---------------------------------------------------------------------------

def registry() -> dict:
    global _registry
    if _registry is None:
        import yaml
        raw = yaml.safe_load((_CONFIG / "api_datasets.yaml").read_text(encoding="utf-8")) or {}
        ds = {}
        for k, v in (raw.get("datasets") or {}).items():
            v = dict(v)
            v.setdefault("params", ["max_age_hours"])
            v.setdefault("query", {})
            v.setdefault("strip", [])
            v.setdefault("ttl", 60)
            v.setdefault("cost", 1)
            ds[k] = v
        _registry = {"datasets": ds, "excluded": dict(raw.get("excluded") or {})}
    return _registry


def tiers() -> dict:
    global _tiers
    if _tiers is None:
        import yaml
        _tiers = yaml.safe_load((_CONFIG / "api_tiers.yaml").read_text(encoding="utf-8")) or {}
    return _tiers


def tier(name: str | None) -> dict:
    t = tiers().get("tiers") or {}
    return t.get(name or "") or t.get(tiers().get("default_tier", "research")) or {"per_minute": 30, "per_day": 5000,
                                                                                    "datasets": "all"}


def enabled() -> dict:
    return {k: v for k, v in registry()["datasets"].items() if v.get("status") == "enabled"}


def allowed_datasets(key: dict) -> set[str]:
    on = set(enabled())
    t = tier(key.get("tier"))
    base = on if t.get("datasets", "all") == "all" else on & set(t.get("datasets") or [])
    return base & set(key["datasets"]) if key.get("datasets") else base


def public_url() -> str:
    return os.environ.get("APB_PUBLIC_URL", "https://panoptes.run").rstrip("/")


# ---- auth -----------------------------------------------------------------------------

def _secret_from(request: Request) -> str:
    k = request.headers.get("x-api-key", "").strip()
    if not k:
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            k = auth[7:].strip()
    return k


def _key(request: Request) -> dict | None:
    from apb.store import apikeys
    k = apikeys.key_for(_secret_from(request))
    return k if k and not k.get("revoked") else None


def _admin_ok(request: Request) -> bool:
    want = os.environ.get("APB_ADMIN_TOKEN", "").strip()
    got = request.headers.get("authorization", "")
    got = got[7:].strip() if got.lower().startswith("bearer ") else ""
    return bool(want) and len(want) >= 24 and hmac.compare_digest(want.encode(), got.encode())


def _err(status: int, msg: str, **headers) -> JSONResponse:
    return JSONResponse({"error": msg, "docs": public_url() + "/developers/"}, status_code=status,
                        headers={k.replace("_", "-"): str(v) for k, v in headers.items()})


# ---- throttling -----------------------------------------------------------------------

def _flush(force: bool = False) -> None:
    from apb.store import apikeys
    with _lock:
        if not force and time.time() - _flushed[0] < 30:
            return
        counts, used = dict(_pending), dict(_pending_used)
        _pending.clear()
        _pending_used.clear()
        _flushed[0] = time.time()
    try:
        apikeys.add_usage(counts, used)
    except Exception:
        with _lock:                                   # keep the counts for the next try
            for k, n in counts.items():
                _pending[k] = _pending.get(k, 0) + n


def charge(key: dict, dataset: str, cost: int, now: float | None = None) -> tuple[bool, dict]:
    """Take one request from the key's bucket and `cost` units from today's quota."""
    from apb.store import apikeys
    now = now or time.time()
    t = tier(key.get("tier"))
    per_min, per_day = int(t.get("per_minute", 60)), int(t.get("per_day", 20000))
    today = apikeys.day(now)
    with _lock:
        d = _daily.get(key["id"])
    if not d or d[0] != today:
        base = apikeys.usage_today(key["id"], today)
        with _lock:
            d = _daily[key["id"]] = [today, base + sum(n for (k, dd, _), n in _pending.items()
                                                       if k == key["id"] and dd == today)]
    with _lock:
        b = _buckets.setdefault(key["id"], [float(per_min), now])
        b[0] = min(float(per_min), b[0] + (now - b[1]) * per_min / 60.0)
        b[1] = now
        info = {"limit": per_min, "remaining": int(b[0]), "day_limit": per_day,
                "day_remaining": max(0, per_day - d[1]), "reset": int(60 - now % 60)}
        if d[1] + cost > per_day:
            info["why"] = "quota"
            return False, info
        if b[0] < 1.0:
            info["why"] = "rate"
            info["retry"] = max(1, int((1.0 - b[0]) * 60.0 / per_min) + 1)
            return False, info
        b[0] -= 1.0
        d[1] += cost
        k = (key["id"], today, dataset)
        _pending[k] = _pending.get(k, 0) + cost
        _pending_used[key["id"]] = now
        info.update(remaining=int(b[0]), day_remaining=max(0, per_day - d[1]))
    _flush()
    return True, info


def _rl_headers(info: dict) -> dict:
    return {"X-RateLimit-Limit": str(info["limit"]), "X-RateLimit-Remaining": str(info["remaining"]),
            "X-RateLimit-Reset": str(info["reset"]), "X-Quota-Limit": str(info["day_limit"]),
            "X-Quota-Remaining": str(info["day_remaining"])}


# ---- relay ----------------------------------------------------------------------------

def _strip(obj, keys: set[str]):
    if not keys:
        return obj
    if isinstance(obj, dict):
        return {k: _strip(v, keys) for k, v in obj.items() if k not in keys}
    if isinstance(obj, list):
        return [_strip(v, keys) for v in obj]
    return obj


async def _relay(request: Request, url: str, ttl: float) -> tuple[int, bytes, bool]:
    global _sem
    hit = _cache.get(url)
    if hit and hit[0] > time.time():
        _stats["relay_hits"] += 1
        return hit[1], hit[2], True
    lock = _inflight.setdefault(url, asyncio.Lock())
    async with lock:                                   # single flight per url
        hit = _cache.get(url)
        if hit and hit[0] > time.time():
            _stats["relay_hits"] += 1
            return hit[1], hit[2], True
        if _sem is None:
            _sem = asyncio.Semaphore(max(1, int(os.environ.get("APB_API_CONCURRENCY", "6"))))
        async with _sem:
            transport = httpx.ASGITransport(app=request.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://internal", timeout=60) as c:
                r = await c.get(url, headers={"x-apb-internal": INTERNAL_TOKEN})
        _stats["relay_misses"] += 1
        if r.status_code == 200:
            _cache[url] = (time.time() + ttl, r.status_code, r.content)
            if len(_cache) > 600:                      # drop the soonest-expiring half
                for k in sorted(_cache, key=lambda k: _cache[k][0])[:300]:
                    _cache.pop(k, None)
    _inflight.pop(url, None)
    return r.status_code, r.content, False


@router.get("/v1/data/{dataset}")
@router.get("/v1/data/{dataset}/{item_id:path}")
async def v1_data(request: Request, dataset: str, item_id: str | None = None):
    _stats["requests"] += 1
    key = _key(request)
    if not key:
        _stats["denied_auth"] += 1
        return _err(401, "a valid API key is required (X-API-Key header)")
    d = registry()["datasets"].get(dataset)
    if not d or d.get("status") != "enabled":
        why = registry()["excluded"].get(dataset)
        return _err(404, f"no such dataset{': ' + why if why else ''}")
    if dataset not in allowed_datasets(key):
        return _err(403, "this key is not granted that dataset")
    path = d["path"]
    if "{id}" in path:
        if not item_id or len(item_id) > 120 or not re.fullmatch(r"[A-Za-z0-9:._\-]+", item_id):
            return _err(400, "a valid id is required")
        path = path.replace("{id}", item_id)
    elif item_id:
        return _err(404, "this dataset has no items by id")
    ok, info = charge(key, dataset, int(d["cost"]))
    if not ok:
        if info["why"] == "quota":
            _stats["quota"] += 1
            return _err(429, "daily quota used up (resets 00:00 UTC)", **_rl_headers(info))
        _stats["throttled"] += 1
        return _err(429, "rate limited", Retry_After=info["retry"], **_rl_headers(info))
    q = {k: v for k, v in request.query_params.items() if k in d["params"]}
    q.update({k: str(v) for k, v in d["query"].items()})
    url = str(httpx.URL(path, params=sorted(q.items())))
    status, body, cached = await _relay(request, url, float(d["ttl"]))
    if status != 200:
        try:
            detail = json.loads(body)
        except ValueError:
            detail = None
        return JSONResponse({"error": "upstream error" if status >= 500 else "bad request",
                             "detail": detail}, status_code=status if status < 500 else 502,
                            headers=_rl_headers(info))
    data = _strip(json.loads(body), set(d["strip"]))
    out = {"dataset": dataset, "attribution": d.get("attribution"), "licence": d.get("licence"),
           "served_at": time.time(), "data": data}
    return JSONResponse(out, headers={**_rl_headers(info), "X-Cache": "HIT" if cached else "MISS",
                                      "Cache-Control": f"private, max-age={int(d['ttl'])}"})


@router.get("/v1")
def v1_index():
    return {"name": "Panoptes API", "version": 1, "docs": public_url() + "/developers/",
            "terms": public_url() + "/developers/terms/", "datasets": "/v1/datasets", "me": "/v1/me",
            "data": "/v1/data/{dataset}", "auth": "X-API-Key: pk_..."}


@router.get("/v1/datasets")
def v1_datasets():
    """The catalogue: what can be had, under which licence, and what is never served."""
    out = []
    for k, v in registry()["datasets"].items():
        if v.get("status") == "excluded":
            continue
        out.append({"id": k, "title": v.get("title"), "status": v["status"], "class": v.get("class"),
                    "licence": v.get("licence"), "attribution": v.get("attribution"),
                    "params": v["params"] if "{id}" not in v["path"] else ["{id} in the path"],
                    "refresh_seconds": v["ttl"], "cost": v["cost"],
                    "noncommercial": bool(v.get("noncommercial"))})
    return JSONResponse({"datasets": out, "not_available": registry()["excluded"],
                         "tiers": {k: {x: t.get(x) for x in ("title", "per_minute", "per_day")}
                                   for k, t in (tiers().get("tiers") or {}).items()}},
                        headers={"Cache-Control": "public, max-age=300"})


@router.get("/v1/me")
def v1_me(request: Request):
    from apb.store import apikeys
    key = _key(request)
    if not key:
        return _err(401, "a valid API key is required (X-API-Key header)")
    _flush(force=True)
    t = tier(key.get("tier"))
    return {"key": key["prefix"] + "…", "label": key.get("label"), "tier": key.get("tier"),
            "per_minute": t.get("per_minute"), "per_day": t.get("per_day"),
            "used_today": apikeys.usage_today(key["id"]), "datasets": sorted(allowed_datasets(key)),
            "usage_30d": apikeys.usage(key["id"], 30)}


# ---- applications ---------------------------------------------------------------------

def _s(v, n: int) -> str:
    return re.sub(r"[\x00-\x1f\x7f]+", " ", str(v or "")).strip()[:n]


@router.post("/v1/apply")
async def v1_apply(request: Request):
    from apb.alerts import mail
    from apb.store import apikeys
    from apb.api.main import client_ip
    try:
        body = await request.json()
    except ValueError:
        return _err(400, "send JSON")
    if not isinstance(body, dict):
        return _err(400, "send a JSON object")
    if body.get("website2"):                          # honeypot: humans never see it
        return JSONResponse({"ok": True, "status": "pending"}, status_code=202)
    f = {"name": _s(body.get("name"), 120), "email": _s(body.get("email"), 200).lower(),
         "org": _s(body.get("org"), 160), "website": _s(body.get("website"), 300),
         "use_case": _s(body.get("use_case"), 3000), "volume": _s(body.get("volume"), 200),
         "datasets": [d for d in (body.get("datasets") or []) if isinstance(d, str) and d in enabled()][:60]}
    missing = [k for k in ("name", "email", "use_case") if not f[k]]
    if missing:
        return _err(400, "missing: " + ", ".join(missing))
    if not EMAIL_RE.match(f["email"]):
        return _err(400, "that email address does not look right")
    if len(f["use_case"]) < 40:
        return _err(400, "describe how you will use the data (at least a couple of sentences)")
    if body.get("agree_terms") is not True:
        return _err(400, "you must accept the terms of service")
    ip = client_ip(request)
    if apikeys.recent_from_ip(ip, time.time() - 3600) >= 3:
        return _err(429, "too many applications from this address; try again later")
    app_id, claim = apikeys.add_application(f, ip)
    _stats["applications"] += 1
    mail.notify_owner(
        f"[Panoptes API] Key request from {f['name']}" + (f" ({f['org']})" if f["org"] else ""),
        "\n".join([
            f"Name:      {f['name']}", f"Email:     {f['email']}", f"Org:       {f['org'] or '-'}",
            f"Website:   {f['website'] or '-'}", f"Volume:    {f['volume'] or '-'}",
            f"Datasets:  {', '.join(f['datasets']) or 'not specified'}", "", "Intended use:",
            f["use_case"], "", f"Review: {public_url()}/developers/admin/#{app_id}", f"Application id: {app_id}"]))
    mail.send(f["email"], "Your Panoptes API application was received", _letter(f["name"], [
        "Thanks for applying for a Panoptes API key. A person reads every application; you will get",
        "another email when it has been decided.",
        "",
        "Your private link to check on it, and to collect your key once it is approved:",
        f"{public_url()}/developers/#claim={claim}",
        "",
        "Keep this link to yourself. Anyone who has it can collect your key.",
        "",
        f"Application id: {app_id}"]))
    return JSONResponse({"ok": True, "application": app_id, "status": "pending", "claim_token": claim,
                         "claim_url": f"{public_url()}/developers/#claim={claim}"}, status_code=201)


def _letter(name: str | None, lines: list[str]) -> str:
    from apb.alerts import mail
    return "\n".join([f"Hello {name}," if name else "Hello,", "", *lines, "", "— Panoptes API",
                       f"Terms: {public_url()}/developers/terms/", f"Questions: {mail.reply_to()}"])


def _email_decision(a: dict, action: str) -> None:
    """Tell the applicant what was decided; approved / reissued mail carries a fresh link."""
    from apb.alerts import mail
    from apb.store import apikeys
    note = [f"Note from the reviewer: {a['note']}", ""] if a.get("note") else []
    if action == "deny":
        mail.send(a["email"], "Your Panoptes API application", _letter(a.get("name"), [
            "Your application for a Panoptes API key was not approved.", "", *note,
            "You are welcome to reply to this email or apply again with more detail."]))
        return
    claim = apikeys.new_claim(a["id"])
    first = action == "approve"
    mail.send(a["email"], "Your Panoptes API key is ready" if first else "Your new Panoptes API key is ready",
              _letter(a.get("name"), [
                  ("Your application was approved" if first else "Your previous key was revoked and a new one is ready")
                  + f" (tier: {a.get('tier')}).", "", *note,
                  "Open this private link to see your key. It is shown once, so copy it somewhere safe:",
                  f"{public_url()}/developers/#claim={claim}", "",
                  f"Docs and datasets: {public_url()}/developers/",
                  "Send the key in the X-API-Key header. Never put it in a URL or in client-side code."]))


def _claim_app(request: Request) -> dict | None:
    from apb.store import apikeys
    tok = request.headers.get("x-claim-token", "").strip()
    return apikeys.application_by_claim(tok) if 16 <= len(tok) <= 80 else None


@router.get("/v1/apply/status")
def v1_apply_status(request: Request):
    a = _claim_app(request)
    if not a:
        return _err(404, "unknown claim token")
    return {"application": a["id"], "status": a["status"], "tier": a.get("tier"),
            "note": a.get("note"), "key_issued": bool(a.get("key_id"))}


@router.post("/v1/apply/claim")
def v1_apply_claim(request: Request):
    from apb.store import apikeys
    a = _claim_app(request)
    if not a:
        return _err(404, "unknown claim token")
    if a["status"] != "approved":
        return _err(409, f"application is {a['status']}")
    if a.get("key_id"):
        return _err(409, "the key for this application was already shown; ask the owner to reissue it")
    secret = apikeys.issue_key(a)
    if not secret:
        return _err(409, "the key for this application was already shown")
    return JSONResponse({"api_key": secret, "tier": a.get("tier"),
                         "note": "Store this key now — it is not shown again."},
                        headers={"Cache-Control": "no-store"})


# ---- admin ----------------------------------------------------------------------------

def _admin(request: Request) -> JSONResponse | None:
    if not os.environ.get("APB_ADMIN_TOKEN", "").strip():
        return _err(503, "admin is disabled (APB_ADMIN_TOKEN not set)")
    if not _admin_ok(request):
        return _err(401, "admin token required")
    return None


@router.get("/v1/admin/applications")
def admin_applications(request: Request, status: str | None = None):
    from apb.store import apikeys
    if (e := _admin(request)):
        return e
    return JSONResponse({"applications": apikeys.applications(status if status in
                                                                   ("pending", "approved", "denied") else None),
                         "tiers": list((tiers().get("tiers") or {}).keys()), "datasets": sorted(enabled())},
                        headers={"Cache-Control": "no-store"})


@router.post("/v1/admin/applications/{app_id}/{action}")
async def admin_decide(request: Request, app_id: str, action: str):
    from apb.store import apikeys
    if (e := _admin(request)):
        return e
    if action not in ("approve", "deny", "reissue"):
        return _err(404, "action is approve, deny or reissue")
    a = apikeys.application(app_id)
    if not a:
        return _err(404, "no such application")
    if action == "reissue":
        if a.get("key_id"):
            _buckets.pop(a["key_id"], None)
        ok = apikeys.reissue(app_id)
        if ok:
            _email_decision(apikeys.application(app_id), "reissue")
        return JSONResponse({"ok": ok}, headers={"Cache-Control": "no-store"})
    try:
        body = await request.json()
    except ValueError:
        body = {}
    body = body if isinstance(body, dict) else {}
    t = body.get("tier") or tiers().get("default_tier", "research")
    if action == "approve" and t not in (tiers().get("tiers") or {}):
        return _err(400, "unknown tier")
    ds = body.get("datasets")
    ds = [d for d in ds if d in enabled()] if isinstance(ds, list) else None
    out = apikeys.decide(app_id, "approved" if action == "approve" else "denied",
                         t if action == "approve" else None, _s(body.get("note"), 500) or None, ds)
    if body.get("email", True) is not False:          # the admin can decide quietly
        _email_decision(out, action)
    return JSONResponse({"application": out}, headers={"Cache-Control": "no-store"})


@router.get("/v1/admin/keys")
def admin_keys(request: Request):
    from apb.store import apikeys
    if (e := _admin(request)):
        return e
    _flush(force=True)
    rows = apikeys.keys()
    for k in rows:
        k["used_today"] = apikeys.usage_today(k["id"])
    return JSONResponse({"keys": rows, "stats": status()}, headers={"Cache-Control": "no-store"})


@router.post("/v1/admin/keys/{key_id}")
async def admin_key_update(request: Request, key_id: str):
    from apb.store import apikeys
    if (e := _admin(request)):
        return e
    try:
        body = await request.json()
    except ValueError:
        return _err(400, "send JSON")
    t = body.get("tier")
    if t and t not in (tiers().get("tiers") or {}):
        return _err(400, "unknown tier")
    ds = body.get("datasets")
    ds = [d for d in ds if d in enabled()] if isinstance(ds, list) else None
    ok = apikeys.update_key(key_id, tier=t, revoke=body.get("revoke") if isinstance(body.get("revoke"), bool) else None,
                            datasets=ds)
    _buckets.pop(key_id, None)
    return JSONResponse({"ok": ok}, status_code=200 if ok else 404)


def status() -> dict:
    from apb.alerts import mail
    return {**_stats, "relay_cache": len(_cache), "notify": mail.configured(),
            "admin": bool(os.environ.get("APB_ADMIN_TOKEN", "").strip())}
