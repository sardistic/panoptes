"""Query + alerting API over stored incidents (dissemination layer).

Only redacted incident records are exposed here. Raw audio and unredacted
transcripts are intentionally NOT served by this API.

All startup side effects (feed registration, background pollers) run in the
FastAPI lifespan handler — importing this module is side-effect free, so tests,
tooling, and multi-worker servers behave predictably.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import asyncio
import threading
import time as _time
import httpx
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Callable, Literal
from uuid import uuid4

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from apb.context.feeds import feeds_near
from apb.ingest import cad as cad_mod
from apb.ingest.cad import FEEDS as CAD_FEEDS
from apb.ingest.cad import CadIngest
from apb.store import snapshots, state

log = logging.getLogger("apb.api")

# Shared request bounds. Public endpoints fan out to third-party feeds, so accepting
# unbounded windows and limits is both an availability risk and an accidental footgun.
Hours = Annotated[float, Query(ge=0.0, le=24.0 * 31)]
WindowHours = Annotated[float, Query(gt=0.0, le=24.0 * 7)]
Latitude = Annotated[float, Query(ge=-90.0, le=90.0)]
Longitude = Annotated[float, Query(ge=-180.0, le=180.0)]


def _off(flag: str) -> bool:
    """True if an opt-out env flag is set truthy (keyless lanes default ON)."""
    return os.environ.get(flag, "").strip().lower() in ("1", "true", "yes", "on")


_cad = CadIngest()

# Every live-ready lane found by the discovery sweeps (countrywide). Keyed lanes
# (firms, openaq, airnow, acled, ...) register 0 feeds unless their key is set.
_LOADERS: list[tuple[str, Callable[[], int]]] = [
    ("socrata", cad_mod.load_catalog),
    ("arcgis", cad_mod.load_arcgis_catalog),
    ("pulsepoint", cad_mod.load_pulsepoint),
    ("p2c", cad_mod.load_p2c),
    ("southern", cad_mod.load_southern),
    ("hazard", cad_mod.load_hazard),
    ("traffic511", cad_mod.load_traffic511),
    # keyless but heavy (14 polls/cycle); APB_ADSB_OFF to disable
    ("adsb", lambda: 0 if _off("APB_ADSB_OFF") else cad_mod.load_adsb()),
    ("tfr", cad_mod.load_faa_tfr),
    ("fema", cad_mod.load_fema),
    ("firms", cad_mod.load_firms),
    ("odin", cad_mod.load_odin),
    ("flood", cad_mod.load_usgs_flood),
    ("openaq", cad_mod.load_openaq),
    ("volcano", cad_mod.load_volcano),
    ("smoke", cad_mod.load_hms_smoke),
    ("ndbc", cad_mod.load_ndbc),
    ("spc", cad_mod.load_spc),
    ("nhc", cad_mod.load_nhc),
    ("faa_delay", cad_mod.load_faa_delays),
    ("nifc_fire", cad_mod.load_nifc_fire),
    ("airnow", cad_mod.load_airnow),
    ("acled", cad_mod.load_acled),
    ("emsc", cad_mod.load_emsc),
    ("gdacs", cad_mod.load_gdacs),
    ("sigmet", cad_mod.load_sigmet),
    ("chp", cad_mod.load_chp),
    ("amtrak", cad_mod.load_amtrak),
    ("squawk", cad_mod.load_squawk),
]


_poller_beat = {"n": 0, "at": 0.0}      # /status: is the background loop alive?
_poller_stop = threading.Event()
_poller_thread: threading.Thread | None = None
_poller_leader = False
_poller_election_error: str | None = None


def _poller(interval: float = 120.0):
    """Continuously snapshot the national stream into the DB so coverage/history grow,
    persist fused events (lifecycle identity), and fire webhook alerts."""
    snapshots.conn()  # init
    n = 0
    while not _poller_stop.is_set():
        _poller_beat.update(n=n, at=_time.time())
        try:
            wrote = snapshots.record(_cad.overview(limit_per=80, max_age_hours=168))
            n += 1
            if n % 30 == 0:
                snapshots.prune()
            log.info("snapshot %d: +%d rows, db=%s", n, wrote, snapshots.stats())
        except Exception as e:
            log.warning("poller error: %s", e)
        try:
            from apb.alerts.notify import send_pending
            from apb.fusion.cluster import detect
            from apb.fusion.sources import gather_signals
            from apb.store import events as event_store
            fused = detect(gather_signals(_cad, max_age_hours=6.0), min_count=2,
                           min_sources=1, min_score=1.2, max_age_hours=6.0)
            new = event_store.record(fused)
            if n % 30 == 0:
                event_store.prune()
            if new:
                log.info("event registry: +%d new (of %d active)", new, len(fused))
            send_pending()
        except Exception as e:
            log.warning("event-registry error: %s", e)
        _poller_stop.wait(interval)


_started = False


def _startup() -> None:
    """Register feeds and launch workers; shared-state deployments elect one leader."""
    global _started, _poller_thread, _poller_leader, _poller_election_error
    if _started:
        return
    _started = True

    counts: dict[str, int] = {}
    for label, loader in _LOADERS:
        try:
            counts[label] = loader()
        except Exception as e:
            log.warning("lane %s failed to register: %s", label, e)
    log.info("live CAD feeds: %d (%s)", len(CAD_FEEDS),
             ", ".join(f"{k} +{v}" for k, v in counts.items()))

    try:
        _poller_leader = state.acquire_poller_leadership()
        _poller_election_error = None
    except Exception as exc:
        log.error("worker leadership check failed: %s", exc)
        _poller_leader = False
        _poller_election_error = str(exc)

    if not _off("APB_POLLER_OFF"):
        if _poller_leader:
            _poller_stop.clear()
            _poller_thread = threading.Thread(target=_poller, daemon=True,
                                              name="apb-snapshot-poller")
            _poller_thread.start()
        else:
            log.info("shared-state follower: another instance owns the poller")

    # Keyless live Bluesky firehose -> rolling social-signal buffer for /live/fused.
    # Default ON (websockets ships with uvicorn[standard]); APB_BLUESKY_OFF to disable.
    if _poller_leader and not _off("APB_BLUESKY_OFF"):
        try:
            from apb.fusion import social_store
            social_store.start(keep_unplaced=bool(os.environ.get("APB_BLUESKY_KEEP_UNPLACED")))
        except Exception as e:
            log.warning("bluesky lane disabled: %s", e)

    # Keyless news-RSS poller -> rolling news-signal buffer. APB_NEWS_OFF to disable.
    if _poller_leader and not _off("APB_NEWS_OFF"):
        try:
            from apb.fusion import news_store
            news_store.start(keep_unplaced=bool(os.environ.get("APB_NEWS_KEEP_UNPLACED")))
        except Exception as e:
            log.warning("news lane disabled: %s", e)

    # Per-metro Google News incident headlines -> same buffer. APB_GNEWS_OFF to disable.
    if _poller_leader and not _off("APB_GNEWS_OFF"):
        try:
            from apb.fusion import news_store
            news_store.start_gnews()
        except Exception as e:
            log.warning("gnews lane disabled: %s", e)

    # Keyless Reddit/Mastodon RSS poller -> shares the social buffer. APB_SOCIAL_RSS_OFF to disable.
    if _poller_leader and not _off("APB_SOCIAL_RSS_OFF"):
        try:
            from apb.fusion import social_store
            social_store.start_rss(keep_unplaced=bool(os.environ.get("APB_SOCIAL_RSS_KEEP_UNPLACED")))
        except Exception as e:
            log.warning("social-rss lane disabled: %s", e)

    # Optional aisstream maritime firehose -> rolling vessel buffer for /live/maritime.
    # Needs AISSTREAM_KEY + `pip install websockets`; kept out of the lean prod image.
    if _poller_leader and os.environ.get("AISSTREAM_KEY"):
        from apb.fusion import maritime_store
        maritime_store.start()


@asynccontextmanager
async def _lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    _startup()
    try:
        yield
    finally:
        _poller_stop.set()
        from apb.fusion import maritime_store, news_store, social_store
        social_store.stop()
        news_store.stop()
        maritime_store.stop()
        if _poller_thread and _poller_thread.is_alive():
            _poller_thread.join(timeout=5.0)
        state.release_poller_leadership()


# DB stack (sqlalchemy/geoalchemy/psycopg) is imported lazily so the live/map UI and
# /feeds work with only FastAPI installed — no Postgres/PostGIS required to test.
def _db():
    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from apb.store.db import ActivityRow, IncidentRow, engine
    return select, Session, ActivityRow, IncidentRow, engine


app = FastAPI(title="APB", version="0.1.0", lifespan=_lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "OPTIONS"],
    allow_headers=["Accept", "Content-Type"], allow_credentials=False,
)

_WEB_DIR = Path(__file__).resolve().parents[2] / "web"

# ---------------------------------------------------------------------------
# Cheap abuse protection + client caching. The map UI polls /live/* endpoints;
# per-IP throttling keeps one bad client from monopolizing the fetch/cluster
# work, and Cache-Control lets browsers/proxies absorb repeat hits.
try:
    _RATE_PER_MIN = max(0, int(os.environ.get("APB_RATE_LIMIT", "300")))
except ValueError:
    log.warning("invalid APB_RATE_LIMIT; using 300")
    _RATE_PER_MIN = 300
_hits: dict[str, deque] = defaultdict(deque)
_hits_lock = threading.Lock()
_SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
    # The map stack dictates most of this: MapLibre boots its worker from a blob:
    # URL (worker-src), fetches the Carto style JSON + vector tiles/sprites/glyphs,
    # the Terrarium DEM (s3.amazonaws.com) and VIIRS night lights via fetch
    # (connect-src), and feeds its sky-mask image source from data: URLs; Leaflet
    # loads GOES GeoColor and the IEM/NWS NEXRAD mosaic as plain <img> tiles.
    "Content-Security-Policy": " ".join((
        "default-src 'self';", "base-uri 'self';", "object-src 'none';",
        "frame-ancestors 'none';",
        "script-src 'self' 'unsafe-inline' https://unpkg.com;",
        "worker-src 'self' blob:;", "child-src 'self' blob:;",
        "style-src 'self' 'unsafe-inline' https://unpkg.com https://fonts.googleapis.com;",
        "font-src 'self' https://fonts.gstatic.com;",
        "img-src 'self' data: blob: https://*.basemaps.cartocdn.com"
        " https://gibs.earthdata.nasa.gov https://mesonet.agron.iastate.edu;",
        "connect-src 'self' data: https://basemaps.cartocdn.com"
        " https://*.basemaps.cartocdn.com https://s3.amazonaws.com"
        " https://gibs.earthdata.nasa.gov https://mesonet.agron.iastate.edu;",
    )),
}


@app.middleware("http")
async def _throttle_and_cache(request: Request, call_next):
    started_at = _time.perf_counter()
    request_id = request.headers.get("x-request-id", "")[:80]
    if not request_id or not all(c.isalnum() or c in "-_." for c in request_id):
        request_id = uuid4().hex
    if _RATE_PER_MIN and request.url.path != "/health":
        ip = request.client.host if request.client else "?"
        now = _time.time()
        with _hits_lock:
            q = _hits[ip]
            while q and q[0] < now - 60.0:
                q.popleft()
            if len(q) >= _RATE_PER_MIN:
                return JSONResponse({"error": "rate limited"}, status_code=429,
                                    headers={"Retry-After": "30", "X-Request-ID": request_id,
                                             **_SECURITY_HEADERS})
            q.append(now)
            if len(_hits) > 10_000:      # evict stale/old clients without resetting all
                stale = [key for key, hits in _hits.items()
                         if not hits or hits[-1] < now - 60.0]
                for key in stale:
                    _hits.pop(key, None)
                while len(_hits) > 10_000:
                    _hits.pop(next(iter(_hits)))
    resp = await call_next(request)
    if (request.method == "GET" and request.url.path != "/live/stream"
            and "cache-control" not in resp.headers
            and request.url.path.startswith(("/live/", "/baseline/"))):
        resp.headers["Cache-Control"] = "public, max-age=15"
    for header, value in _SECURITY_HEADERS.items():
        resp.headers.setdefault(header, value)
    elapsed_ms = (_time.perf_counter() - started_at) * 1000
    resp.headers.setdefault("X-Request-ID", request_id)
    resp.headers.setdefault("Server-Timing", f"app;dur={elapsed_ms:.1f}")
    if request.url.path not in ("/health", "/health/ready"):
        log.info("request id=%s method=%s path=%s status=%d elapsed_ms=%.1f",
                 request_id, request.method, request.url.path,
                 resp.status_code, elapsed_ms)
    return resp


# Server-side TTL cache for the expensive endpoints (fetch + normalize + cluster
# over thousands of signals). Keyed on endpoint+params so the map's default view
# is computed once per window no matter how many clients poll.
_resp_cache: dict[tuple, tuple[float, object]] = {}
_resp_lock = threading.Lock()


def _cached(key: tuple, ttl: float, fn: Callable[[], object]):
    now = _time.time()
    with _resp_lock:
        hit = _resp_cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = fn()   # computed outside the lock; a rare duplicate compute beats blocking
    with _resp_lock:
        _resp_cache[key] = (now, val)
        if len(_resp_cache) > 256:       # bound memory under param churn
            for k in sorted(_resp_cache, key=lambda k: _resp_cache[k][0])[:128]:
                _resp_cache.pop(k, None)
    return val


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/health/ready")
def readiness():
    """Dependency-aware readiness without making external network calls."""
    reasons: list[str] = []
    try:
        db = snapshots.stats()
    except Exception as exc:
        log.warning("readiness database check failed: %s", exc)
        db = None
        reasons.append("database unavailable")
    poller = {"enabled": not _off("APB_POLLER_OFF"),
              "leader": _poller_leader,
              "election_error": _poller_election_error,
              "thread_alive": bool(_poller_thread and _poller_thread.is_alive()),
              "last_beat_s": (round(_time.time() - _poller_beat["at"])
                              if _poller_beat["at"] else None)}
    if poller["enabled"] and poller["leader"] and not poller["thread_alive"]:
        reasons.append("poller stopped")
    if poller["election_error"]:
        reasons.append("poller election unavailable")
    if (poller["enabled"] and poller["leader"] and poller["last_beat_s"] is not None
            and poller["last_beat_s"] > 600):
        reasons.append("poller stale")
    body = {"status": "ready" if not reasons else "not_ready",
            "reasons": reasons, "database": db, "poller": poller}
    return body if not reasons else JSONResponse(body, status_code=503)


@app.get("/incidents")
def incidents(
    metro: str | None = None,
    incident_type: str | None = None,
    min_threat: float = Query(0.0, ge=0.0, le=1.0),
    minutes: int = Query(60, ge=1, le=60 * 24 * 31),
    limit: int = Query(200, ge=1, le=1000),
):
    """Recent incidents, filterable. Powers the map/list UI."""
    select, Session, _, IncidentRow, engine = _db()
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    stmt = select(IncidentRow).where(
        IncidentRow.extracted_at >= since,
        IncidentRow.threat_score >= min_threat,
    )
    if metro:
        stmt = stmt.where(IncidentRow.metro == metro)
    if incident_type:
        stmt = stmt.where(IncidentRow.incident_type == incident_type)
    stmt = stmt.order_by(IncidentRow.extracted_at.desc()).limit(limit)

    with Session(engine) as s:
        rows = s.scalars(stmt).all()
    return [
        {
            "call_id": r.call_id,
            "metro": r.metro,
            "type": r.incident_type,
            "summary": r.summary,
            "location": r.location_text,
            "sentiment": r.sentiment,
            "threat_score": r.threat_score,
            "emerging": r.is_emerging,
            "at": r.extracted_at.isoformat(),
        }
        for r in rows
    ]


@app.get("/activity")
def activity(metro: str | None = None,
             minutes: int = Query(30, ge=1, le=60 * 24 * 7),
             anomalous_only: bool = False):
    """Activity-first signal: recent per-talkgroup windows. Works on encrypted systems
    (metadata-only). Powers the map's heat/volume layer."""
    select, Session, ActivityRow, _, engine = _db()
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    stmt = select(ActivityRow).where(ActivityRow.window_start >= since)
    if metro:
        stmt = stmt.where(ActivityRow.metro == metro)
    if anomalous_only:
        stmt = stmt.where(ActivityRow.is_anomalous.is_(True))
    stmt = stmt.order_by(ActivityRow.window_start.desc()).limit(1000)
    with Session(engine) as s:
        rows = s.scalars(stmt).all()
    return [
        {
            "metro": r.metro, "system_id": r.system_id, "talkgroup": r.talkgroup,
            "label": r.talkgroup_label, "window_start": r.window_start.isoformat(),
            "call_count": r.call_count, "airtime_sec": r.total_airtime_sec,
            "encrypted": r.encrypted, "baseline": r.baseline_call_count,
            "zscore": r.zscore, "anomalous": r.is_anomalous,
        }
        for r in rows
    ]


@app.get("/emerging")
def emerging(metro: str | None = None,
             minutes: int = Query(30, ge=1, le=60 * 24 * 7)):
    """Incidents flagged by the anomaly/clustering layer as emerging threats."""
    select, Session, _, IncidentRow, engine = _db()
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    stmt = select(IncidentRow).where(
        IncidentRow.extracted_at >= since,
        IncidentRow.is_emerging.is_(True),
    )
    if metro:
        stmt = stmt.where(IncidentRow.metro == metro)
    stmt = stmt.order_by(IncidentRow.threat_score.desc())
    with Session(engine) as s:
        rows = s.scalars(stmt).all()
    return [{"call_id": r.call_id, "metro": r.metro, "type": r.incident_type,
             "summary": r.summary, "threat_score": r.threat_score} for r in rows]


@app.get("/live/incidents")
def live_incidents(metro: str = Query("seattle", min_length=1, max_length=80,
                                     pattern=r"^[a-zA-Z0-9_-]+$"),
                   limit: int = Query(400, ge=1, le=1000),
                   max_age_hours: Hours = 0.0):
    """Recent CAD/911 dispatch from the local collector snapshot.

    Display requests must never wait on an upstream provider.  The background
    poller owns collection; this route is deliberately a fast database read.
    ``0`` retains the old no-window behavior within the bounded snapshot store.
    """
    age = max_age_hours if max_age_hours > 0 else 24.0 * 30
    return snapshots.query(age, metro=metro, limit=limit)


@app.get("/live/overview")
def live_overview(limit_per: int = Query(60, ge=1, le=200),
                  max_age_hours: Hours = 72.0):
    """National aggregate from accumulated history.

    ``limit_per`` remains for API compatibility; collection limits belong to the
    background poller, while interactive reads stay independent of upstream speed.
    """
    def _build():
        return snapshots.query(max_age_hours, limit=8000)
    return _cached(("overview-snapshot", max_age_hours), 10.0, _build)


def _stream_snapshot(metro: str, max_age_hours: float) -> dict:
    rows = snapshots.query(max_age_hours,
                           metro=None if metro == "__all__" else metro,
                           limit=8000 if metro == "__all__" else 400)
    return {"metro": metro, "max_age_hours": max_age_hours,
            "sent_at": _time.time(), "incidents": rows}


@app.get("/live/stream")
async def live_stream(request: Request,
                      metro: str = Query("__all__", min_length=1, max_length=80,
                                         pattern=r"^(?:__all__|[a-zA-Z0-9_-]+)$"),
                      max_age_hours: Hours = 24.0):
    """Server-Sent Events incident snapshots; unchanged cycles become keepalives."""
    async def generate():
        last_digest = ""
        yield "retry: 5000\n\n"
        while not await request.is_disconnected():
            try:
                payload = await asyncio.to_thread(_stream_snapshot, metro, max_age_hours)
                body = json.dumps(payload, separators=(",", ":"), default=str)
                digest = hashlib.sha256(json.dumps(
                    payload["incidents"], separators=(",", ":"), default=str
                ).encode()).hexdigest()
                if digest != last_digest:
                    yield f"event: snapshot\ndata: {body}\n\n"
                    last_digest = digest
                else:
                    yield f": keepalive {_time.time():.0f}\n\n"
            except Exception as exc:
                log.warning("SSE snapshot failed for %s: %s", metro, exc)
                yield "event: upstream_error\ndata: {}\n\n"
            await asyncio.sleep(15.0)

    return StreamingResponse(generate(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache, no-store", "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    })


@app.get("/live/hazards")
def live_hazards(source: str | None = Query(None, max_length=30),
                 max_age_hours: Hours = 24.0):
    """No-key national hazard/event feeds (USGS quakes, NWS alerts, NASA EONET).
    `source` filters to one of: usgs, nws, eonet. These also seed the correlation layer."""
    from apb.ingest.hazard import SOURCES
    keys = [source] if source in SOURCES else list(SOURCES)
    cutoff = _time.time() - max_age_hours * 3600 if max_age_hours > 0 else 0
    out: list[dict] = []
    for k in keys:
        for d in _cad.fetch("hz_" + k):
            if not cutoff or (d.get("ts") and d["ts"] >= cutoff):
                out.append(d)
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


@app.get("/live/environment")
def live_environment():
    """Coarse, model-derived environmental context: thermal moisture, recent rain,
    UV, PM2.5 and US AQI. Shared ten-minute cache keeps this field inexpensive."""
    from apb.ingest.environment import fetch_environment

    def _build():
        try:
            return fetch_environment()
        except (httpx.HTTPError, ValueError, KeyError, IndexError) as exc:
            log.warning("environment field fetch failed: %s", exc)
            return []
    return _cached(("environment",), 600.0, _build)


@app.get("/live/traffic")
def live_traffic(system: str | None = None, include_planned: bool = False,
                 max_age_hours: Hours = 0.0):
    """State DOT 511 traffic incidents (crashes/hazards/closures). `system` filters to a
    state key (e.g. 'ny'). Planned roadwork excluded unless include_planned=true."""
    from apb.ingest.traffic511 import SYSTEMS
    keys = [system] if system in SYSTEMS else list(SYSTEMS)
    cutoff = _time.time() - max_age_hours * 3600 if max_age_hours > 0 else 0
    out: list[dict] = []
    for k in keys:
        for d in _cad.fetch("t511_" + k):
            if include_planned or d.get("threat_score", 0) > 0.2:
                if not cutoff or (d.get("ts") and d["ts"] >= cutoff):
                    out.append(d)
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


@app.get("/live/aircraft")
def live_aircraft():
    """ADS-B loitering rotorcraft (police/medevac orbiting a scene) — an early proxy for
    a major ground incident. Stateful: accuracy improves as the watcher accumulates track
    history. Enable the background watcher with APB_ADSB=1."""
    return _cad.fetch("adsb")


@app.get("/live/declarations")
def live_declarations(source: str | None = Query(None, max_length=30),
                      max_age_hours: Hours = 0.0):
    """Authority event signals: FAA Temporary Flight Restrictions + FEMA disaster
    declarations. `source` filters to 'faa_tfr' or 'fema'. These anchor the
    correlation layer (a TFR/declaration explains a local surge)."""
    keys = [source] if source in ("faa_tfr", "fema") else ["faa_tfr", "fema"]
    cutoff = _time.time() - max_age_hours * 3600 if max_age_hours > 0 else 0
    out: list[dict] = []
    for k in keys:
        for d in _cad.fetch(k):
            if not cutoff or (d.get("ts") and d["ts"] >= cutoff):
                out.append(d)
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


def _live_feed(slug: str, max_age_hours: float) -> list[dict]:
    """Shared helper: fetch one hidden feed, optionally time-filtered, newest first."""
    if slug not in CAD_FEEDS:
        return []
    cutoff = _time.time() - max_age_hours * 3600 if max_age_hours > 0 else 0
    out = [d for d in _cad.fetch(slug)
           if not cutoff or (d.get("ts") and d["ts"] >= cutoff)]
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


@app.get("/live/fire")
def live_fire(max_age_hours: Hours = 0.0):
    """NASA FIRMS active-fire pixels (VIIRS). Empty unless FIRMS_MAP_KEY is configured."""
    return _live_feed("firms", max_age_hours)


@app.get("/live/outages")
def live_outages(max_age_hours: Hours = 0.0):
    """ODIN current power outages (county-level), largest first."""
    return _live_feed("odin", max_age_hours)


@app.get("/live/flood")
def live_flood(max_age_hours: Hours = 0.0):
    """NOAA NWPS river gauges currently at/above flood 'action' stage."""
    return _live_feed("usgs_flood", max_age_hours)


@app.get("/live/airquality")
def live_airquality(max_age_hours: Hours = 0.0):
    """OpenAQ PM2.5 spikes (Unhealthy+). Empty unless OPENAQ_KEY is configured."""
    return _live_feed("openaq", max_age_hours)


@app.get("/live/maritime")
def live_maritime(max_age_hours: Hours = 2.0):
    """Live AIS vessel positions (US coastal). Empty unless AISSTREAM_KEY is configured."""
    from apb.fusion import maritime_store
    out = maritime_store.recent(max_age_hours)
    out.sort(key=lambda d: d.get("ts") or 0, reverse=True)
    return out


@app.get("/live/volcano")
def live_volcano(max_age_hours: Hours = 0.0):
    """USGS elevated-status volcanoes (aviation color code / alert level)."""
    return _live_feed("volcano", max_age_hours)


@app.get("/live/smoke")
def live_smoke(max_age_hours: Hours = 0.0):
    """NOAA HMS satellite smoke plumes (Light/Medium/Heavy)."""
    return _live_feed("hms_smoke", max_age_hours)


@app.get("/live/marine")
def live_marine(max_age_hours: Hours = 0.0):
    """NDBC buoys currently reporting high seas / gale-force winds."""
    return _live_feed("ndbc", max_age_hours)


@app.get("/live/storm_reports")
def live_storm_reports(max_age_hours: Hours = 0.0):
    """SPC preliminary storm reports (observed tornado/hail/wind), today."""
    return _live_feed("spc", max_age_hours)


@app.get("/live/cyclones")
def live_cyclones(max_age_hours: Hours = 0.0):
    """NHC active tropical cyclones (position/intensity). Empty out of season."""
    return _live_feed("nhc", max_age_hours)


@app.get("/live/airport_delays")
def live_airport_delays(max_age_hours: Hours = 0.0):
    """FAA airport ground delays, ground stops, and closures with reason."""
    return _live_feed("faa_delay", max_age_hours)


@app.get("/live/wildfires")
def live_wildfires(max_age_hours: Hours = 0.0):
    """NIFC WFIGS active named wildfire incidents (acreage / % contained)."""
    return _live_feed("nifc_fire", max_age_hours)


@app.get("/live/airnow")
def live_airnow(max_age_hours: Hours = 0.0):
    """AirNow official AQI readings (Unhealthy+). Empty unless AIRNOW_KEY is set."""
    return _live_feed("airnow", max_age_hours)


@app.get("/live/unrest")
def live_unrest(max_age_hours: Hours = 0.0):
    """ACLED civil-unrest events. Empty unless ACLED_KEY + ACLED_EMAIL are set."""
    return _live_feed("acled", max_age_hours)


@app.get("/live/quakes_global")
def live_quakes_global(max_age_hours: Hours = 24.0):
    """EMSC global M4+ earthquakes (multi-agency, often earlier than USGS abroad).
    Quakes USGS already carries are suppressed so the map shows one marker per event."""
    from apb.fusion.dedupe import dedupe_signal_rows
    emsc = _live_feed("emsc", max_age_hours)
    usgs = _cad.fetch("hz_usgs")
    kept = dedupe_signal_rows(usgs + emsc)
    return [d for d in kept if d.get("source") == "emsc"]


@app.get("/live/disasters")
def live_disasters(max_age_hours: Hours = 0.0):
    """GDACS Orange/Red global disaster alerts (EQ/TC/flood/wildfire/volcano)."""
    return _live_feed("gdacs", max_age_hours)


@app.get("/live/sigmets")
def live_sigmets(max_age_hours: Hours = 0.0):
    """AWC SIGMETs in effect: convective/turbulence/icing/ash airspace hazards."""
    return _live_feed("sigmet", max_age_hours)


@app.get("/live/rail")
def live_rail(max_age_hours: Hours = 0.0):
    """Amtrak trains running >= 1h late — a rail-corridor anomaly signal."""
    return _live_feed("amtrak", max_age_hours)


@app.get("/live/squawks")
def live_squawks(max_age_hours: Hours = 0.0):
    """Aircraft currently squawking 7500/7600/7700. Usually empty; never ignorable."""
    return _live_feed("squawk", max_age_hours)


@app.get("/live/hazards/all")
def live_hazards_all(request: Request, max_age_hours: Hours = 24.0):
    """One browser-friendly aggregate for every optional hazard family."""
    def _build():
        rows: list[dict] = []
        rows.extend(live_hazards(source=None, max_age_hours=max_age_hours))
        rows.extend(live_traffic(system=None, include_planned=False,
                                 max_age_hours=max_age_hours))
        rows.extend(live_aircraft())
        rows.extend(live_declarations(source=None, max_age_hours=max_age_hours))
        for fn in (live_fire, live_outages, live_flood, live_airquality,
                   live_maritime, live_volcano, live_smoke, live_marine,
                   live_storm_reports, live_cyclones, live_airport_delays,
                   live_wildfires, live_airnow, live_unrest, live_quakes_global,
                   live_disasters, live_sigmets, live_rail, live_squawks):
            rows.extend(fn(max_age_hours=max_age_hours))
        rows.sort(key=lambda d: d.get("ts") or 0, reverse=True)
        return rows[:10_000]
    rows = _cached(("hazards_all", max_age_hours), 20.0, _build)
    encoded = json.dumps(rows, separators=(",", ":"), default=str).encode()
    etag = '"' + hashlib.sha256(encoded).hexdigest()[:24] + '"'
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers={"ETag": etag})
    return Response(encoded, media_type="application/json", headers={"ETag": etag})


@app.get("/events")
def events(max_age_hours: Hours = 24.0,
           limit: int = Query(200, ge=1, le=1000)):
    """Persisted fused events with lifecycle: stable uid, first_seen/last_seen,
    age_min, peak vs latest score, growing flag. Written by the background poller —
    this is the 'what has been happening' view, vs /live/fused's instantaneous one."""
    from apb.store import events as event_store
    return event_store.query(max_age_hours, limit)


@app.get("/db/stats")
def db_stats():
    """How much history the snapshot poller has accumulated."""
    return snapshots.stats()


@app.get("/status")
def status():
    """Per-lane operational health: registered feeds by kind, backoff state, cache
    freshness of the singleton lanes, buffer sizes, poller heartbeat. This is how a
    silently-degraded lane (empty rows, stale timestamps) gets noticed."""
    from collections import Counter
    from apb.fusion import news_store, social_store
    now = _time.time()
    kinds = Counter(f.kind for f in CAD_FEEDS.values())
    backing_off = sorted(m for m in _cad._fail if _cad._backing_off(m))
    lanes = {}
    for slug, feed in CAD_FEEDS.items():
        if not feed.hidden or feed.kind in ("socrata", "arcgis", "pulsepoint",
                                            "p2c", "southern"):
            continue                       # singletons only; catalogs via `kinds`
        hit = _cad._cache.get(slug)
        rows = hit[1] if hit else None
        freshest = max((d.get("ts") or 0 for d in rows), default=0) if rows else 0
        lanes[slug] = {
            "rows": len(rows) if rows is not None else None,   # None = not yet fetched
            "cache_age_s": round(now - hit[0]) if hit else None,
            "freshest_min": round((now - freshest) / 60) if freshest else None,
            "backing_off": slug in backing_off,
        }
    return {
        "feeds_total": len(CAD_FEEDS),
        "feeds_by_kind": dict(kinds),
        "backing_off": {"count": len(backing_off), "feeds": backing_off[:40]},
        "lanes": lanes,
        "buffers": {"social": social_store.stats(), "news": news_store.stats()},
        "db": snapshots.stats(),
        "poller": {"cycles": _poller_beat["n"],
                   "leader": _poller_leader,
                   "election_error": _poller_election_error,
                   "last_beat_s": round(now - _poller_beat["at"]) if _poller_beat["at"] else None},
        "response_cache": len(_resp_cache),
    }


@app.get("/baseline/anomalies")
def baseline_anomalies(window_hours: WindowHours = 1.0,
                       lookback_hours: WindowHours = 72.0,
                       z: float = Query(2.0, ge=0.0, le=20.0)):
    """Rate-anomaly detection from accumulated history: metros whose incident rate in
    the most recent `window_hours` is >= z std-devs above their own trailing baseline."""
    from apb.infer.baseline import detect_rate_anomalies
    anomalies = detect_rate_anomalies(snapshots.query(lookback_hours, limit=20000),
                                      window_hours=window_hours, z_threshold=z)
    for a in anomalies:                       # attach a center for map rendering
        feed = CAD_FEEDS.get(a["metro"])
        if feed and feed.center:
            a["lat"], a["lon"] = feed.center[0], feed.center[1]
        a["name"] = feed.name if feed else a["metro"]
    return anomalies


@app.get("/live/emerging")
def live_emerging(min_count: int = Query(3, ge=2, le=100),
                  threat_floor: float = Query(0.5, ge=0.0, le=1.0),
                  max_age_hours: Hours = 24.0):
    """Emerging events: spatial clusters of converging, elevated-severity incidents
    from the RECENT live stream (default last 24h)."""
    from apb.infer.cluster import detect

    def _build():
        clusters = detect(snapshots.query(max_age_hours, limit=8000),
                          min_count=min_count, threat_floor=threat_floor)
        return [c.__dict__ for c in clusters]
    return _cached(("emerging-snapshot", min_count, threat_floor, max_age_hours), 20.0,
                   _build)


def _parse_kinds(kinds: str | None) -> set[str] | None:
    """Comma-separated source_kind filter -> set, or None for all. Powers per-family
    filtering across the fusion endpoints (cad, traffic, weather, aircraft, social,
    news, context, radio_metadata, radio_transcript)."""
    if not kinds:
        return None
    return {k.strip() for k in kinds.split(",") if k.strip()}


@app.get("/live/signals")
def live_signals(limit_per: int = Query(60, ge=1, le=200),
                 max_age_hours: Hours = 24.0,
                 include_seed: bool = True, include_live: bool = True,
                 kinds: str | None = Query(None, max_length=200,
                                           pattern=r"^[a-zA-Z0-9_, -]*$")):
    """Normalized source signals across CAD/history plus optional local seed rows.

    This is the source-fusion substrate: every sensor becomes the same shape before
    clustering. `kinds` filters by sensor family (e.g. kinds=cad,social,weather).
    `data/social_seed.jsonl` can be used for offline social/news tests.
    """
    from apb.fusion.signals import dict_signal
    from apb.fusion.sources import gather_signals

    def _build():
        signals = gather_signals(_cad, limit_per=limit_per, max_age_hours=max_age_hours,
                                 include_seed=include_seed, include_live=include_live,
                                 kinds=_parse_kinds(kinds))
        signals.sort(key=lambda s: s.observed_at, reverse=True)
        return [dict_signal(s) for s in signals[:5000]]
    return _cached(("signals", limit_per, max_age_hours, include_seed, include_live,
                    kinds), 20.0, _build)


@app.get("/live/fused")
def live_fused(min_count: int = Query(2, ge=2, le=100),
               min_sources: int = Query(1, ge=1, le=20),
               min_score: float = Query(1.2, ge=0.0, le=100.0),
               max_age_hours: Hours = 24.0,
               limit_per: int = Query(20, ge=1, le=200),
               include_seed: bool = True, include_live: bool = True,
               kinds: str | None = Query(None, max_length=200,
                                         pattern=r"^[a-zA-Z0-9_, -]*$")):
    """Cross-source event clusters ranked by surge score.

    Unlike /live/emerging, this is built for APB's broader goal: radio/CAD/social/
    traffic/news signals can reinforce each other when they converge in place/time.
    `kinds` restricts the clustered substrate to given sensor families.
    """
    from apb.fusion.cluster import detect
    from apb.fusion.sources import gather_signals

    def _build():
        signals = gather_signals(_cad, limit_per=limit_per, max_age_hours=max_age_hours,
                                 include_seed=include_seed, include_live=include_live,
                                 kinds=_parse_kinds(kinds))
        return [e.__dict__ for e in detect(
            signals, min_count=min_count, min_sources=min_sources, min_score=min_score,
            max_age_hours=max_age_hours)]
    return _cached(("fused", min_count, min_sources, min_score, max_age_hours,
                    limit_per, include_seed, include_live, kinds), 20.0, _build)


@app.get("/live/social")
def live_social(max_age_hours: Hours = 24.0,
                limit: int = Query(800, ge=1, le=5000)):
    """Placed social/news signals (seed + live Bluesky buffer) for the map social layer.
    Rendered as their own markers so the firehose is visible even without CAD corroboration."""
    from apb.fusion.signals import dict_signal
    from apb.fusion.sources import seed_recent
    from apb.fusion import news_store, social_store
    out = [s for s in (seed_recent(max_age_hours) + social_store.recent(max_age_hours)
                       + news_store.recent(max_age_hours))
           if s.lat is not None and s.lon is not None]
    out.sort(key=lambda s: s.observed_at, reverse=True)
    return [dict_signal(s) for s in out[:limit]]


@app.get("/live/metros")
def live_metros():
    """Metros with a live CAD feed for the dropdown (hidden bulk feeds excluded)."""
    feeds = sorted((f for f in CAD_FEEDS.values() if not f.hidden),
                   key=lambda f: f.name.lower())
    return [{"metro": f.metro, "name": f.name, "state": f.state,
             "center": list(f.center) if f.center else None} for f in feeds]


@app.get("/feeds")
def feeds(lat: Latitude, lon: Longitude,
          radius_m: float = Query(800.0, ge=50.0, le=20_000.0)):
    """On-demand public feeds near an incident (cameras/traffic). Not stored."""
    return [f.__dict__ for f in feeds_near(lat, lon, radius_m)]


def _local_news(lat: float, lon: float, radius_km: float = 60.0,
                max_age_hours: float = 48.0, limit: int = 4) -> list[dict]:
    """Placed headlines from the live news-RSS buffer near a point — instant and
    immune to GDELT throttling, so the popup always has something to show."""
    import math
    from apb.fusion import news_store
    coslat = max(0.2, math.cos(math.radians(lat)))
    out = []
    for s in news_store.recent(max_age_hours):
        if s.lat is None or s.lon is None:
            continue
        dkm = math.hypot(s.lat - lat, (s.lon - lon) * coslat) * 111.0
        if dkm <= radius_km:
            out.append({"title": s.summary, "url": s.url,
                        "domain": s.source, "at": s.observed_at.isoformat()})
    out.sort(key=lambda d: d["at"] or "", reverse=True)
    return out[:limit]


@app.get("/correlate")
def correlate(lat: Latitude, lon: Longitude,
              types: str | None = Query(None, max_length=200,
                                        pattern=r"^[a-zA-Z0-9_, -]*$"),
              timespan: Literal["1h", "6h", "12h", "1d", "3d", "7d"] = "3d"):
    """Spike -> likely cause: local news-RSS headlines near the point (instant) plus
    recent GDELT news for the place/types (free, no key, heavily rate-limited).
    `types` = comma-separated incident types from the cluster (shapes the news query)."""
    from apb.context.gdelt import correlate as _corr
    tl = sorted({t for t in (types or "").split(",") if t})

    def _build():
        out = _corr(lat, lon, tl or None, timespan)
        out["local"] = _local_news(lat, lon)
        return out
    # rounded key: repeat clicks on the same cluster reuse one GDELT slot
    return _cached(("correlate", round(lat, 2), round(lon, 2), ",".join(tl), timespan),
                   120.0, _build)


# Serve the test UI at / (mounted last so it doesn't shadow API routes).
if _WEB_DIR.exists():
    app.mount("/", StaticFiles(directory=str(_WEB_DIR), html=True), name="web")
