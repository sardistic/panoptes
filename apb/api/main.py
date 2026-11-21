"""Query + alerting API over stored incidents (dissemination layer).

Only redacted incident records are exposed here. Radio transcripts are internal: they
are stored redacted and used only to classify calls and place them in a ~2 km area;
their words and addresses are never served. One exception for audio (2026-11-17, the owner's call): a
call published by a volunteer's public Rdio Scanner can be replayed on request through
/live/radio/audio — relayed, briefly cached, rate-limited per source, never archived.

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
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from apb.context.feeds import feeds_near
from apb.ingest import cad as cad_mod
from apb.ingest.cad import FEEDS as CAD_FEEDS
from apb.ingest.cad import CadIngest
from apb.ingest.cameras import STREAM_HOSTS, CameraRegistry
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
_cameras = CameraRegistry()      # public DOT/traffic cameras; inventories load lazily

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
    ("radar", cad_mod.load_radar),       # Cloudflare Radar; needs a Radar-read token
]


_poller_beat = {"n": 0, "at": 0.0}      # /status: is the background loop alive?
_poller_stop = threading.Event()
_poller_thread: threading.Thread | None = None
_poller_leader = False
_poller_election_error: str | None = None

# A full national sweep is slow: measured at a steady ~870s per cycle (roughly
# 750s of fetching across 314 metros plus the inter-cycle wait), with observed
# outliers past 1100s. The staleness bound has to clear that comfortably, or a
# perfectly healthy poller reports itself stale partway through every cycle.
# Two missed cycles is a real wedge; one long sweep is not.
_POLLER_STALE_AFTER_S = 1800.0


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
            # Beat on completed work, not just on entering the loop body. The
            # sweep above is the long pole, so a start-only beat makes a healthy
            # poller look stale for the back half of every cycle.
            _poller_beat.update(n=n, at=_time.time())
            if n % 30 == 0:
                snapshots.prune()
            log.info("snapshot %d: +%d rows, db=%s", n, wrote, snapshots.stats())
        except Exception as e:
            log.warning("poller error: %s", e)
        try:                    # which feeds went quiet, stale or dead this cycle?
            from apb.alerts.notify import send_ops
            from apb.store import feedhealth
            changes = feedhealth.observe(_feed_observations())
            if changes:
                log.info("feed health: %s", ", ".join(
                    f"{c['slug']} {c['from']}->{c['to']}" for c in changes[:20]))
                send_ops(changes)
        except Exception as e:
            log.warning("feed-health error: %s", e)
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
            if not _off("APB_CAMERAS_OFF"):
                from apb.context import tasking          # go and look at the strongest
                tasking.enqueue(event_store.query(max_age_hours=1.0, limit=300))
            send_pending()
            from apb.alerts.notify import send_digest_if_due
            from apb.fusion import news_store
            from apb.store import alertlog
            alertlog.score_pending(news_store.recent(30.0))   # were the alerts right?
            send_digest_if_due()
            _poller_beat.update(n=n, at=_time.time())
        except Exception as e:
            log.warning("event-registry error: %s", e)
        _poller_stop.wait(interval)


def _feed_observations() -> list[dict]:
    """This cycle's per-feed facts for the feed-health store: every registered lane
    (from the ingest cache — a fetch time that moved means a new successful fetch),
    the social/news buffers, and the camera inventories."""
    now = _time.time()
    obs: list[dict] = []
    for slug, feed in list(CAD_FEEDS.items()):
        hit = _cad._cache.get(slug)
        rows = hit[1] if hit else None
        fresh = max((d["ts"] for d in rows
                     if isinstance(d.get("ts"), (int, float)) and d["ts"] <= now + 900),
                    default=0) if rows else 0
        obs.append({"slug": slug, "kind": feed.kind, "name": feed.name,
                    "fetched_at": hit[0] if hit else 0.0,
                    "rows": len(rows) if rows is not None else 0,
                    "freshest": fresh or None,
                    # a lane paused as a whole is failing, even though its feeds are no
                    # longer being asked (and so stop accumulating failures of their own)
                    "fails": (3 if _cad.kind_paused(feed.kind)
                              else _cad._fail.get(slug, (0, 0.0))[0])})
    try:
        from apb.fusion import news_store, social_store
        for slug, name, store in (("buf_news", "News buffer", news_store),
                                  ("buf_social", "Social buffer", social_store)):
            if not store.stats().get("running"):
                continue
            sigs = store.recent(1.0)
            obs.append({"slug": slug, "kind": "buffer", "name": name, "fetched_at": now,
                        "rows": len(sigs), "fails": 0,
                        "freshest": max((s.observed_at.timestamp() for s in sigs), default=None)})
    except Exception as e:
        log.info("feed observations: buffers unavailable: %s", e)
    try:                                        # one pseudo-feed per radio instance
        from apb.ingest import rdio
        from apb.store import radio as radio_store
        if rdio._instances:
            known = radio_store.states()
            calls: dict[str, int] = {}
            for r in radio_store.latest(3 * rdio.EVERY):
                calls[r["key"]] = calls.get(r["key"], 0) + (r["calls"] or 0)
            for inst in rdio._instances:
                st = known.get(inst["key"]) or {}
                if st.get("state") == "closed":
                    continue
                obs.append({"slug": f"rdio_{inst['key']}", "kind": "rdio",
                            "name": "Radio: " + "; ".join(list(inst["labels"].values())[:2])[:60],
                            "fetched_at": st.get("last_poll") or 0.0,
                            "rows": calls.get(inst["key"], 0), "freshest": None,
                            "fails": st.get("fails") or 0})
    except Exception as e:
        log.info("feed observations: radio unavailable: %s", e)
    if not _off("APB_CAMERAS_OFF"):
        with _cameras._lock:
            cams = [(k, _cameras._at.get(k), len(_cameras._rows.get(k, [])), _cameras._err.get(k))
                    for k in list(_cameras._at)]
        for key, at, n_rows, err in cams:
            obs.append({"slug": f"cam_{key}", "kind": "camera", "name": f"Cameras: {key}",
                        "fetched_at": at or 0.0, "rows": n_rows, "freshest": None,
                        "fails": 3 if err else 0})
    return obs


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

    # Radio activity from open Rdio Scanner instances (call metadata only, through
    # the egress proxy). Starts only when an instance list and APB_RDIO_PROXY exist.
    if _poller_leader and not _off("APB_RDIO_OFF"):
        try:
            from apb.ingest import rdio
            if rdio.start():
                from apb.context import radio_stt
                radio_stt.start()          # transcribes the calls that matter, on a budget
                from apb.context import radio_areas
                radio_areas.start()        # what is going on where, from those transcripts
                from apb.ingest import streams
                streams.start()            # public scanner audio streams, cut into calls
        except Exception as e:
            log.warning("rdio lane disabled: %s", e)

    # Warm the public-camera inventories (background threads, ~20s for all vendors)
    # so the first CAMERAS toggle after a deploy is not an empty layer.
    if not _off("APB_CAMERAS_OFF"):
        _cameras.ensure_loaded()

    # Background samplers (leader only): facts over watched cells for baselines, and
    # structured camera reads. APB_SAMPLER_OFF disables (tests, CI, extra workers).
    if _poller_leader and not _off("APB_SAMPLER_OFF") and not _off("APB_CAMERAS_OFF"):
        from apb.context import sampler
        centers = [tuple(f.center) for f in CAD_FEEDS.values() if f.center and not f.hidden]
        sampler.start(centers, _cameras)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # HTTPX logs full request URLs at INFO, including API keys carried in query
    # parameters by some upstream providers. Keep failures visible without
    # writing credentials into container logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    _startup()
    try:
        yield
    finally:
        _poller_stop.set()
        from apb.fusion import maritime_store, news_store, social_store
        social_store.stop()
        news_store.stop()
        __import__("apb.ingest.rdio", fromlist=["stop"]).stop()
        __import__("apb.context.radio_stt", fromlist=["stop"]).stop()
        __import__("apb.context.radio_areas", fromlist=["stop"]).stop()
        __import__("apb.ingest.streams", fromlist=["stop"]).stop()
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
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
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
        "frame-src https://www.youtube-nocookie.com;",     # broadcast cams (YouTube Live embeds)
        # analytics.sardistic.com is our own Umami instance: it serves the
        # tracker (script-src) and receives the beacon (connect-src). Naming the
        # one origin keeps this narrower than the 'unsafe-inline' the map stack
        # already forces.
        "script-src 'self' 'unsafe-inline' https://unpkg.com https://sardistic.com"
        " https://analytics.sardistic.com;",
        "worker-src 'self' blob:;", "child-src 'self' blob:;",
        "style-src 'self' 'unsafe-inline' https://unpkg.com https://fonts.googleapis.com;",
        "font-src 'self' https://fonts.gstatic.com;",
        "img-src 'self' data: blob: https://*.basemaps.cartocdn.com"
        " https://gibs.earthdata.nasa.gov https://mesonet.agron.iastate.edu"
        " https://server.arcgisonline.com;",         # Esri World Imagery at high zoom
        # camera stills go through /live/cameras/{id}/image (same-origin); only HLS
        # playlists/segments are fetched by hls.js straight from the DOT streamers
        "media-src 'self' blob: " + " ".join(STREAM_HOSTS) + ";",
        "connect-src 'self' data: https://basemaps.cartocdn.com"
        " https://*.basemaps.cartocdn.com https://s3.amazonaws.com"
        " https://gibs.earthdata.nasa.gov https://mesonet.agron.iastate.edu"
        " https://analytics.sardistic.com " + " ".join(STREAM_HOSTS) + ";",
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
            and poller["last_beat_s"] > _POLLER_STALE_AFTER_S):
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
                   live_disasters, live_sigmets, live_rail, live_squawks,
                   live_internet):
            rows.extend(fn(max_age_hours=max_age_hours))
        rows.sort(key=lambda d: d.get("ts") or 0, reverse=True)
        return rows[:10_000]
    rows = _cached(("hazards_all", max_age_hours), 20.0, _build)
    encoded = json.dumps(rows, separators=(",", ":"), default=str).encode()
    etag = '"' + hashlib.sha256(encoded).hexdigest()[:24] + '"'
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers={"ETag": etag})
    return Response(encoded, media_type="application/json", headers={"ETag": etag})


@app.get("/live/cameras")
def live_cameras(bbox: str | None = Query(None, max_length=80),
                 limit: int = Query(2000, ge=1, le=5000),
                 source: str | None = Query(None, max_length=20),
                 include_offline: bool = False):
    """Public live cameras (DOT/traffic first). `bbox=w,s,e,n` scopes to the map
    view; over `limit` rows are hash-sampled so a national view shows coverage spread.
    Stills are served by /live/cameras/{id}/image; `stream_url` is a direct HLS URL."""
    if _off("APB_CAMERAS_OFF"):
        return JSONResponse([], headers={"Cache-Control": "public, max-age=300"})
    box = None
    if bbox:
        try:
            w, s, e, n = (float(x) for x in bbox.split(","))
        except ValueError:
            return JSONResponse({"error": "bbox must be w,s,e,n"}, status_code=400)
        if not (-180 <= w <= e <= 180 and -90 <= s <= n <= 90):
            return JSONResponse({"error": "bbox out of range"}, status_code=400)
        box = (w, s, e, n)
    rows = _cameras.query(box, limit=limit, source=source, online_only=not include_offline)
    return JSONResponse(rows, headers={"Cache-Control": "public, max-age=120"})


@app.get("/live/cameras/sources")
def live_camera_sources():
    """Per-vendor inventory health (rows, age, last error) — the camera lane's /status."""
    return _cameras.stats()


@app.get("/live/cameras/{cam_id}/image")
def live_camera_image(cam_id: str):
    """Snapshot proxy. Only ids present in the registry resolve (never arbitrary
    URLs), so this cannot be used to reach other hosts; upstream bytes are cached
    a few seconds per camera so a busy popup does not hammer the DOT."""
    if _off("APB_CAMERAS_OFF") or len(cam_id) > 120:
        return Response(status_code=404)
    try:
        got = _cameras.snapshot(cam_id)
    except httpx.HTTPError as e:
        log.info("[cameras] snapshot %s failed: %s", cam_id, e)
        return Response(status_code=502, headers={"Cache-Control": "no-store"})
    if got is None:
        return Response(status_code=404)
    body, ctype = got
    return Response(body, media_type=ctype,
                    headers={"Cache-Control": "public, max-age=5"})


class ExplainRequest(BaseModel):
    """Abstracted view the browser sends for the scene explainer. Everything is
    bounded: the server fetches nothing the client names except registry camera ids."""
    bounds: dict[str, float]
    view: dict[str, float] = Field(default_factory=dict)
    local_time: str = Field("", max_length=60)
    metro: str = Field("", max_length=80)
    window: str = Field("", max_length=20)
    type: str = Field("", max_length=60)
    severity: str = Field("", max_length=10)
    layers: list[str] = Field(default_factory=list, max_length=12)
    environment: str = Field("", max_length=400)
    incidents: list[dict] = Field(default_factory=list, max_length=40)
    hazards: list[dict] = Field(default_factory=list, max_length=30)
    surges: list[dict] = Field(default_factory=list, max_length=10)
    clusters: list[dict] = Field(default_factory=list, max_length=10)
    counts: dict[str, int] = Field(default_factory=dict)
    cameras: list[str] = Field(default_factory=list, max_length=12)
    social: list[dict] = Field(default_factory=list, max_length=30)
    stream_cameras: list[str] = Field(default_factory=list, max_length=20)   # names, HLS-only
    frames: list[dict] = Field(default_factory=list, max_length=4)   # {id, data} browser frame grabs
    focus: Literal["overview", "cameras", "incidents", "hazards", "social", "weather",
                   "place", "facts", "street"] = "overview"
    question: str = Field("", max_length=300)


_explain_hits: dict[str, deque] = defaultdict(deque)


@app.post("/explain")
def explain_scene(req: ExplainRequest, request: Request):
    """"What am I looking at?" — Gemini Flash summary of a drawn rectangle: the
    abstracted layers + filters, live weather at its center, and image analysis of up
    to five camera stills inside it. Costs money per call, so it is throttled harder
    than the rest of the API (6/min/IP) and refuses when no key is configured."""
    from apb.context import explain as explainer
    if not explainer.api_key():
        return JSONResponse({"error": "scene explainer is not configured (GEMINI_API_KEY)"},
                            status_code=503)
    ip = request.client.host if request.client else "?"
    now = _time.time()
    q = _explain_hits[ip]
    while q and q[0] < now - 60:
        q.popleft()
    if len(q) >= 6:
        return JSONResponse({"error": "slow down — six looks a minute"}, status_code=429,
                            headers={"Retry-After": "20"})
    q.append(now)
    b = req.bounds
    if not all(k in b for k in ("south", "north", "west", "east")) or b["south"] > b["north"]:
        return JSONResponse({"error": "bounds must be south/north/west/east"}, status_code=400)
    ctx = req.model_dump()
    for key in ("incidents", "hazards", "surges", "clusters", "social"):   # bound prompt size
        ctx[key] = [{k: (str(v)[:200] if isinstance(v, str) else v) for k, v in row.items()
                     if k in ("type", "threat", "ago", "location", "summary", "source", "cat",
                              "event", "headline", "severity", "metro", "count", "score",
                              "types", "zscore", "current", "baseline", "name", "text", "url")}
                    for row in ctx[key][:40] if isinstance(row, dict)]
    view = req.view or {}
    if req.focus in ("social", "place", "overview") and "lat" in view and "lon" in view:
        try:                                    # placed headlines from the live news buffer
            ctx["news"] = [{"title": n["title"], "domain": n["domain"], "at": n["at"]}
                           for n in _local_news(float(view["lat"]), float(view["lon"]),
                                                radius_km=40.0, limit=8)]
        except Exception as e:                  # never let context enrichment sink the answer
            log.info("explain news context failed: %s", e)
    max_stills = 10 if req.focus == "cameras" else 3      # fewer bytes = faster first answer
    if req.focus in ("facts", "place"):                   # every public metric for the box
        from apb.context import facts as facts_mod
        try:
            ctx["facts"] = facts_mod.digest(facts_mod.facts(b))
        except Exception as e:
            log.info("facts unavailable: %s", e)
    ctx["stream_cameras"] = [str(n)[:120] for n in req.stream_cameras]
    from apb.store import looks as look_store
    try:                                        # memory: earlier answers over this box
        ctx["prior"] = [{"ago_min": round((_time.time() - p["ts"]) / 60), "focus": p["focus"],
                         "text": (p["text"] or "")[:500]} for p in look_store.prior(b)]
    except Exception as e:
        log.info("prior looks unavailable: %s", e)
    stills: list[tuple[str, bytes, str]] = []
    used_ids: list[str] = []
    for cam_id in req.cameras[:12]:
        cam = _cameras.get(cam_id)
        if not cam or not cam.get("image_url"):
            continue
        try:
            got = _cameras.snapshot(cam_id)
        except httpx.HTTPError:
            continue
        if got and len(got[0]) <= 900_000:
            stills.append((f"{cam['name']} ({cam['source']})", got[0], got[1]))
            used_ids.append(cam_id)
        if len(stills) >= max_stills:
            break
    # Street-level frames: free providers ride along on overview/place; the Street
    # facet also spends Street View (billed) and sends more.
    from apb.context import street as street_mod
    street_frames: list[dict] = []
    if req.focus in ("street", "overview", "place"):
        try:
            street_frames = street_mod.frames(b, paid=(req.focus == "street"))[: 8 if req.focus == "street" else 3]
        except Exception as e:
            log.info("street frames unavailable: %s", e)
        for fr in street_frames:
            got = None
            try:
                got = street_mod.image(fr["token"])
            except httpx.HTTPError:
                pass
            if got:
                stills.append((f"street level, {fr['provider']} {fr.get('captured') or ''} at {fr['lat']},{fr['lon']}"
                               + (f" ({fr['note']})" if fr.get("note") else ""), got[0], got[1]))
    ctx["street_frames"] = len(street_frames)
    # Frames the browser grabbed from HLS-only cameras (canvas capture). Only ids that
    # are registered stream cameras are accepted, and the bytes must be a real image.
    import base64
    from apb.ingest.cameras import _sniff_image
    for fr in req.frames[:4]:
        cam = _cameras.get(str(fr.get("id", "")))
        if not cam or not cam.get("stream_url") or len(stills) >= max_stills + 3:
            continue
        try:
            raw = base64.b64decode(str(fr.get("data", ""))[:900_000], validate=True)
        except (ValueError, TypeError):
            continue
        mime = _sniff_image(raw)
        if mime and len(raw) <= 650_000:
            stills.append((f"{cam['name']} ({cam['source']}, live stream frame)", raw, mime))
            used_ids.append(cam["id"])
    try:
        out = explainer.explain(ctx, stills)
    # 424 (not 502/503): Cloudflare swaps origin 5xx bodies for its own error page,
    # which would hide the message from the UI.
    except RuntimeError as e:
        log.warning("explain failed: %s", e)
        return JSONResponse({"error": str(e)[:240]}, status_code=424)
    except httpx.HTTPError as e:
        log.warning("explain upstream error: %s", e)
        return JSONResponse({"error": "model upstream unreachable"}, status_code=424)
    out["camera_ids"] = used_ids
    out["street"] = street_frames
    if out.get("camera_obs") and used_ids:            # structured reads off the stills -> history
        try:
            from apb.store import metrics as mstore
            rows = []
            for o in out["camera_obs"]:
                i = int(o.get("i", 0)) - 1
                if 0 <= i < len(used_ids):
                    rows.append({**o, "camera_id": used_ids[i]})
            mstore.record_camera_obs(mstore.cell_for(float(view.get("lat", 0)), float(view.get("lon", 0))), rows)
        except Exception as e:
            log.info("camera obs not recorded: %s", e)
    if req.focus == "street":
        out["street_budget"] = street_mod.budget()
    try:
        out["uid"] = look_store.record(b, req.focus, out, req.counts)
    except Exception as e:                      # persistence must never sink the answer
        log.warning("look not saved: %s", e)
    return JSONResponse(out, headers={"Cache-Control": "no-store"})


@app.get("/facts")
def facts_for_box(bbox: str = Query(..., max_length=80), stream: bool = False):
    """Every public metric we can retrieve for a rectangle (`bbox=w,s,e,n`): place,
    population, economy, terrain, water, sky, air, nature, activity. Keyless sources
    only unless CENSUS_API_KEY / EBIRD_API_KEY are set. ~15s cold, cached 10 min."""
    from apb.context import facts as facts_mod
    try:
        w, s, e, n = (float(x) for x in bbox.split(","))
    except ValueError:
        return JSONResponse({"error": "bbox must be w,s,e,n"}, status_code=400)
    if not (-180 <= w < e <= 180 and -90 <= s < n <= 90) or (n - s) * (e - w) > 25:
        return JSONResponse({"error": "bbox out of range or larger than 25 deg^2"}, status_code=400)
    box = {"south": s, "north": n, "west": w, "east": e}
    if stream:                                   # NDJSON: fastest sources first, appended live
        def gen():
            for ev in facts_mod.facts_stream(box):
                yield json.dumps(ev, ensure_ascii=False, default=str) + "\n"
        return StreamingResponse(gen(), media_type="application/x-ndjson",
                                 headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})
    return JSONResponse(facts_mod.facts(box), headers={"Cache-Control": "public, max-age=300"})


@app.get("/street")
def street_for_box(bbox: str = Query(..., max_length=80), paid: bool = False):
    """Street-level frames sampled across a box (Mapillary / KartaView; Street View when
    `paid=true` and GOOGLE_MAPS_KEY is set). Images are served by /street/{token}."""
    from apb.context import street as street_mod
    try:
        w, s, e, n = (float(x) for x in bbox.split(","))
    except ValueError:
        return JSONResponse({"error": "bbox must be w,s,e,n"}, status_code=400)
    if not (-180 <= w < e <= 180 and -90 <= s < n <= 90) or (n - s) * (e - w) > 4:
        return JSONResponse({"error": "bbox out of range or larger than 4 deg^2"}, status_code=400)
    return JSONResponse({"providers": street_mod.providers(), "budget": street_mod.budget(),
                         "frames": street_mod.frames({"south": s, "north": n, "west": w, "east": e}, paid=paid)},
                        headers={"Cache-Control": "public, max-age=600"})


@app.get("/street/{token}")
def street_image(token: str):
    """Proxy for a street-level frame issued by /street or /explain (token only)."""
    from apb.context import street as street_mod
    if len(token) != 18 or not all(c in "0123456789abcdef" for c in token):
        return Response(status_code=404)
    try:
        got = street_mod.image(token)
    except httpx.HTTPError:
        return Response(status_code=424)
    if not got:
        return Response(status_code=404)
    return Response(got[0], media_type=got[1], headers={"Cache-Control": "public, max-age=600"})


@app.delete("/looks/{uid}")
def delete_look(uid: str):
    """Remove a saved scene explanation (its outline disappears from every map)."""
    from apb.store import looks as look_store
    if len(uid) > 40 or not uid.isalnum():
        return JSONResponse({"error": "bad uid"}, status_code=400)
    return JSONResponse({"deleted": look_store.delete(uid)}, headers={"Cache-Control": "no-store"})


@app.get("/live/notable")
def live_notable(max_age_hours: Hours = 6.0):
    """Cells whose latest facts pass raised notable flags (thresholds or >=95th
    percentile of the cell's own 30-day history). Drawn as the NOTABLE layer."""
    from apb.store import metrics as mstore
    return JSONResponse(mstore.notable_cells(max_age_hours), headers={"Cache-Control": "public, max-age=120"})


@app.get("/live/camera_activity")
def live_camera_activity(bbox: str = Query(..., max_length=80), hours: float = Query(24.0, gt=0, le=168)):
    """Camera-derived activity (vehicles / pedestrians / wet road / visibility) read off
    stills by the sampler and the Cameras facet, aggregated per ~5 km cell in the box."""
    from apb.store import metrics as mstore
    try:
        w, s, e, n = (float(x) for x in bbox.split(","))
    except ValueError:
        return JSONResponse({"error": "bbox must be w,s,e,n"}, status_code=400)
    out = []
    lat = s
    while lat < n:
        lon = w
        while lon < e:
            cell = mstore.cell_for(lat, lon)
            agg = mstore.camera_obs(cell, hours)
            if agg:
                out.append({"cell": cell, **agg, "series": mstore.camera_obs_series(cell, hours)})
            lon += mstore.CELL_DEG
        lat += mstore.CELL_DEG
        if len(out) > 400:
            break
    return JSONResponse(out, headers={"Cache-Control": "public, max-age=300"})


@app.get("/looks")
def looks(max_age_hours: Hours = 24.0, limit: int = Query(200, ge=1, le=500),
          bbox: str | None = Query(None, max_length=80)):
    """Persisted scene explanations (the clip's answers): rectangle, focus, text,
    cameras used, weather. The map draws them as outlines you can re-open."""
    from apb.store import looks as look_store
    box = None
    if bbox:
        try:
            w, s, e, n = (float(x) for x in bbox.split(","))
            box = (w, s, e, n)
        except ValueError:
            return JSONResponse({"error": "bbox must be w,s,e,n"}, status_code=400)
    return JSONResponse(look_store.query(max_age_hours, limit, box),
                        headers={"Cache-Control": "no-store"})


@app.get("/events")
def events(max_age_hours: Hours = 24.0,
           limit: int = Query(200, ge=1, le=1000)):
    """Persisted fused events with lifecycle: stable uid, first_seen/last_seen,
    age_min, peak vs latest score, growing flag. Written by the background poller —
    this is the 'what has been happening' view, vs /live/fused's instantaneous one."""
    from apb.store import events as event_store
    return event_store.query(max_age_hours, limit)


_UID_RE = __import__("re").compile(r"^fev:-?\d{1,3}(\.\d+)?:-?\d{1,3}(\.\d+)?:\d{6,12}$")


@app.get("/events/{uid:path}")
def event_detail(uid: str):
    """One event's whole story: growth curve, tasked camera checks, alerts raised,
    and how it scored against the news."""
    if len(uid) > 80 or not _UID_RE.match(uid):
        return JSONResponse({"error": "bad uid"}, status_code=400)
    from apb.context import tasking
    from apb.store import alertlog
    from apb.store import events as event_store
    e = event_store.get(uid)
    if not e:
        return JSONResponse({"error": "not found"}, status_code=404)
    e["place"] = _place_name(e["lat"], e["lon"])
    e["camera_checks"] = tasking.checks_for(uid)
    e["alerts"] = alertlog.for_event(uid)
    e["outcome"] = alertlog.outcome(uid)
    return JSONResponse(json.loads(json.dumps(e, default=str)),
                        headers={"Cache-Control": "public, max-age=30"})


_place_grid: dict[tuple[int, int], list[tuple[float, float, str, int]]] | None = None


def _place_name(lat: float, lon: float) -> str | None:
    """A name for a point — the board says "Seattle", not 47.61,-122.33: the most
    populous gazetteer city within ~25 km (so a suburb reads as its metro)."""
    import math
    global _place_grid
    if _place_grid is None:
        from apb.fusion.places import _gaz
        grid: dict[tuple[int, int], list[tuple[float, float, str, int]]] = {}
        for name, (plat, plon, pop) in _gaz().items():
            grid.setdefault((math.floor(plat), math.floor(plon)), []).append(
                (plat, plon, name.title(), pop))
        _place_grid = grid
    coslat = max(0.2, math.cos(math.radians(lat)))
    best, best_pop, nearest, nearest_km = None, -1, None, 60.0
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            for plat, plon, name, pop in _place_grid.get((math.floor(lat) + dy, math.floor(lon) + dx), ()):
                km = math.hypot(lat - plat, (lon - plon) * coslat) * 111.0
                if km <= 25.0 and pop > best_pop:
                    best, best_pop = name, pop
                if km < nearest_km:
                    nearest, nearest_km = name, km
    return best or (f"near {nearest}" if nearest else None)


# How much an incident kind matters on the board, independent of how many rows a busy
# city produces: forty traffic stops are a normal evening, four shots-fired calls are not.
_CONCERN = {"shots_fired": 1.6, "pursuit": 1.4, "assault": 1.3, "robbery": 1.25, "fire": 1.2,
            "quake": 1.2, "weather": 1.0, "domestic": 1.0, "medical": 0.9, "suspicious": 0.85,
            "welfare": 0.75, "traffic": 0.7, "other": 0.7, "noise": 0.5}


def _concern(types) -> float:
    if not isinstance(types, dict) or not types:
        return 1.0
    total = sum(types.values()) or 1
    return round(sum(_CONCERN.get(k, 1.0) * v for k, v in types.items()) / total, 2)


def _top_type(types) -> str:
    if not isinstance(types, dict) or not types:
        return "activity"
    ranked = sorted(types.items(), key=lambda kv: -kv[1])
    return next((k for k, _ in ranked if k != "other"), ranked[0][0])


def _board(limit: int) -> dict:
    """Rank what is active right now by how much it deserves attention, not by raw
    size: new beats standing, rising beats flat, corroborated and camera-supported
    beat a single feed's say-so, and a cluster that is always there is scored against
    its own normal."""
    from apb.context import tasking
    from apb.infer.baseline import detect_rate_anomalies
    from apb.store import events as event_store, feedhealth
    from apb.store import metrics as mstore
    now = _time.time()
    rows = [r for r in event_store.query(max_age_hours=2.0, limit=800)
            if now - (r.get("last_seen") or 0) <= 2700]
    checks = tasking.latest_by_uid(6.0)
    items = []
    for r in rows:
        age_h = (r.get("age_min") or 0) / 60.0
        trend = r.get("trend")
        why: list[str] = []
        novelty = 1.3 if age_h < 1 else 1.0 if age_h < 6 else 0.6 if age_h < 24 else 0.4
        if age_h < 1:
            why.append("new")
        if age_h >= 6 and r.get("vs_median"):
            vs = max(0.5, min(2.0, r["vs_median"]))
            novelty *= vs
            if vs >= 1.3:
                why.append(f"{r['vs_median']}x its own normal")
        factor = {"rising": 1.3, "falling": 0.75}.get(trend, 1.0)
        if trend == "rising":
            why.append(f"rising x{r.get('ratio')}")
        kinds = r.get("source_count") or 0
        if kinds >= 2:
            why.append(f"{kinds} source kinds")
        # one feed's say-so is the weakest evidence there is, however long its list
        factor *= 0.6 if kinds <= 1 else 1.0 if kinds == 2 else 1.2
        concern = _concern(r.get("types"))
        factor *= concern
        if concern >= 1.15:
            why.append("serious kind")
        cam = checks.get(r["uid"])
        cam_out = None
        if cam:
            cam_out = {"ts": cam["ts"], "cameras": cam["cameras"], "supports": cam["supports"],
                       "contradicts": cam["contradicts"], "verdict": cam["verdict"],
                       "overall": cam.get("overall")}
            if cam.get("overall") == "yes":
                factor *= 1.3
                why.append("cameras confirm")
            elif cam.get("overall") == "no" and cam["cameras"] >= 3:
                factor *= 0.8
                why.append("cameras look normal")
        heads = r.get("summaries") or []
        items.append({
            "uid": r["uid"], "lat": r["lat"], "lon": r["lon"],
            "place": _place_name(r["lat"], r["lon"]),
            "type": _top_type(r.get("types")), "types": r.get("types"),
            "score": r.get("latest_score"), "peak": r.get("peak_score"),
            "count": r.get("latest_count"), "source_count": r.get("source_count"),
            "sources": r.get("sources"), "trend": trend, "ratio": r.get("ratio"),
            "spark": r.get("spark"), "age_min": r.get("age_min"),
            "standing": age_h >= 6 and trend != "rising",
            "headline": str(heads[0])[:200] if isinstance(heads, list) and heads else None,
            "camera": cam_out, "why": why,
            "rank": round((r.get("latest_score") or 0) * novelty * factor, 2),
        })
    items.sort(key=lambda x: -x["rank"])
    def _anomalies() -> list[dict]:
        out = []
        for a in detect_rate_anomalies(snapshots.query(72.0, limit=20000), z_threshold=2.5)[:6]:
            feed = CAD_FEEDS.get(a["metro"])
            out.append({**a, "name": feed.name if feed else a["metro"],
                        "lat": feed.center[0] if feed and feed.center else None,
                        "lon": feed.center[1] if feed and feed.center else None})
        return out
    anomalies = []
    try:        # 20k rows materialised per call: once every five minutes is plenty
        anomalies = _cached(("board-anomalies",), 300.0, _anomalies)
    except Exception as e:
        log.info("board: anomalies unavailable: %s", e)
    health = feedhealth.summary(limit=6)
    try:
        notable = len(mstore.notable_cells(6.0))
    except Exception:
        notable = 0
    radio_rows: list[dict] = []
    radio_live = 0
    try:
        from apb.ingest import rdio
        act = rdio.activity(3 * rdio.EVERY)
        radio_live = len(act)
        radio_rows = [r for r in act if r["z"] is not None and r["z"] >= 3.0 and r["calls"] >= 8][:6]
    except Exception as e:
        log.info("board: radio unavailable: %s", e)
    heard: list[dict] = []
    try:
        from apb.ingest import rdio
        from apb.store import radio as radio_store
        for t in radio_store.transcripts(1800.0, min_priority=0.4, limit=60):
            if t.get("kind") != "dispatch" or t.get("incident_type") in (None, "noise"):
                continue
            label, area = _system_area(rdio, rdio.instance(t["key"]), t["system"])
            heard.append(_transcript_out(t, label, area))
            if len(heard) >= 8:
                break
    except Exception as e:
        log.info("board: transcripts unavailable: %s", e)
    return {
        "generated_at": now, "active": len(items), "events": items[:limit],
        "anomalies": anomalies, "notable_cells": notable,
        "radio": radio_rows, "radio_systems": radio_live, "heard": heard,
        "internet": [d for d in _live_feed("radar", 0.0) if d.get("ongoing")][:8],
        "health": {"feeds": health["feeds"], "by_state": health["by_state"],
                   "healthy_share": health["healthy_share"],
                   "problems_total": health["problems_total"],
                   "problems": health["problems"],
                   "components": [c for c in _components() if not c["ok"]]},
    }


@app.get("/live/board")
def live_board(limit: int = Query(12, ge=1, le=50)):
    """The "right now" board: the handful of things most worth a look, ranked by
    novelty x trend x corroboration, with rate surges, internet outages and the
    feeds that have gone dark alongside."""
    return _cached(("board", limit), 30.0, lambda: _board(limit))


def _sitrep() -> dict:
    from apb.context import explain
    board = _board(8)
    lines = []
    for i, e in enumerate(board["events"], 1):
        where = e["place"] or f"{round(e['lat'], 2)},{round(e['lon'], 2)}"
        line = (f"{i}. {where}: {e['type']} x{e['count']}, score {e['score']}, {e['trend']}, "
                f"{e['source_count']} source kinds, {round((e['age_min'] or 0) / 60, 1)} h old")
        if e.get("camera") and e["camera"].get("verdict"):
            line += f"; cameras: {e['camera']['verdict']}"
        if e.get("headline"):
            line += f"; e.g. \"{e['headline'][:140]}\""
        lines.append(line)
    for a in board["anomalies"]:
        lines.append(f"Rate surge: {a['name']} {a['current']} calls this hour vs "
                     f"{a['baseline_mean']} normal (z {a['zscore']})")
    for d in board["internet"]:
        lines.append(f"Internet: {d['summary'][:160]}")
    for t in board.get("heard", [])[:5]:
        lines.append(f"Heard on dispatch ({t['system'] or 'radio'}): {t['type']} call, priority "
                     f"{t.get('priority')}" + (f", {t['area']}" if t.get("area") else ""))
    for r in board.get("radio", []):
        lines.append(f"Radio surge: {r['system']} {r['calls']} calls in the last window, "
                     f"{r['per10']}/10min vs {r['normal']} normal (z {r['z']}); busiest: "
                     + ", ".join(r["tags"]))
    h = board["health"]
    if h["problems_total"]:
        lines.append(f"Blind spots: {h['problems_total']} of {h['feeds']} feeds dark, e.g. "
                     + ", ".join(f"{p['name'] or p['slug']} ({p['state']})" for p in h["problems"][:4]))
    if not lines:
        return {"text": "Nothing above the noise floor right now.",
                "generated_at": _time.time(), "items": 0}
    prompt = ("You are the duty officer for a public-safety situational map. Below is the ranked "
              "board of live multi-source event clusters (dispatch, traffic, social, news, cameras), "
              "rate surges, internet outages and feed blind spots. Write a situation report of at most "
              "five short lines: lead with the one or two things that most deserve attention and why, "
              "say plainly when the example text looks unrelated to a real incident (keyword noise), "
              "and end with any blind spot that limits confidence. No preamble, no markdown headings.\n\n"
              + "\n".join(lines))
    return {"text": explain.generate([{"text": prompt}], max_tokens=350, feature="sitrep").strip(),
            "generated_at": _time.time(), "items": len(lines)}


@app.get("/live/sitrep")
def live_sitrep():
    """A five-line situation report over the board, written by the text model.
    Cached 10 minutes for everyone, so it costs at most six calls an hour."""
    from apb.context import explain
    if not explain.api_key():
        return JSONResponse({"error": "GEMINI_API_KEY is not configured"}, status_code=503)
    try:
        return _cached(("sitrep",), 600.0, _sitrep)
    except Exception as e:
        log.warning("sitrep failed: %s", e)
        return JSONResponse({"error": "sitrep unavailable"}, status_code=424)


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
        "lanes_paused": _cad.kind_status(),
        "radio": __import__("apb.ingest.rdio", fromlist=["status"]).status(),
        "radio_stt": __import__("apb.context.radio_stt", fromlist=["status"]).status(),
        "radio_areas": __import__("apb.context.radio_areas", fromlist=["status"]).status(),
        "streams": __import__("apb.ingest.streams", fromlist=["status"]).status(),
        "costs": _costs_brief(),
        "memory": _memory(),
        "lanes": lanes,
        "buffers": {"social": social_store.stats(), "news": news_store.stats()},
        "db": snapshots.stats(),
        "poller": {"cycles": _poller_beat["n"],
                   "leader": _poller_leader,
                   "election_error": _poller_election_error,
                   "last_beat_s": round(now - _poller_beat["at"]) if _poller_beat["at"] else None},
        "response_cache": len(_resp_cache),
        "cameras": _cameras.stats(),
        "streetview_budget": __import__("apb.context.street", fromlist=["budget"]).budget(),
        "quotas": {"youtube_units": __import__("apb.store.spend", fromlist=["used"]).used("youtube_units"),
                   "windy_requests": __import__("apb.store.spend", fromlist=["used"]).used("windy")},
        "sampler": __import__("apb.context.sampler", fromlist=["status"]).status(),
        "feed_health": {k: v for k, v in __import__(
            "apb.store.feedhealth", fromlist=["summary"]).summary(limit=0).items()
            if k in ("feeds", "by_state", "learning", "healthy_share", "problems_total")},
    }


_BOOT_AT = _time.time()


def _costs_brief() -> dict:
    """Today and the 7-day average, from the usage meter, plus the metered spends."""
    from apb.store import usage as usage_store
    try:
        s = usage_store.summary(7)
    except Exception as e:                    # the status page must not fail on the meter
        log.info("costs unavailable: %s", e)
        return {}
    return {"gemini_today_usd": s["today_usd"], "gemini_avg_day_usd": s["avg_day_usd"],
            "gemini_7d_usd": s["total_usd"],
            "top_features_7d": {k: v["usd"] for k, v in list(s["by_feature"].items())[:5]}}


@app.get("/status/costs")
def status_costs(days: int = Query(30, ge=1, le=90)):
    """What the paid APIs cost: Gemini by feature and by day (estimated from token
    counts), Street View (metered), and the free-quota counters."""
    from apb.context import street
    from apb.store import spend
    from apb.store import usage as usage_store
    out = {"gemini": usage_store.summary(days), "streetview": street.budget(),
           "quotas": {"youtube_units": spend.used("youtube_units"), "windy_requests": spend.used("windy"),
                      "radio_audio_sec": spend.used("radio_stt_sec"),
                      "radio_profile_calls": spend.used("radio_profile_calls"),
                      "camera_checks": spend.used("gemini_tasking")}}
    return JSONResponse(out, headers={"Cache-Control": "public, max-age=60"})


def _memory() -> dict:
    """Resident memory against the container's limit. The service was being killed
    at its limit every ~70 minutes with nothing but a bare "Killed" in the log."""
    rss = limit = None
    try:
        with open("/proc/self/status", encoding="ascii") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1]) / 1024.0
                    break
        with open("/sys/fs/cgroup/memory.max", encoding="ascii") as f:
            raw = f.read().strip()
            limit = int(raw) / 1048576.0 if raw.isdigit() else None
    except (OSError, ValueError):
        pass
    return {"rss_mb": round(rss) if rss is not None else None,
            "limit_mb": round(limit) if limit else None,
            "used_share": round(rss / limit, 2) if rss and limit else None,
            "uptime_min": round((_time.time() - _BOOT_AT) / 60)}


def _components() -> list[dict]:
    """The moving parts that are not feeds: are the loops themselves alive?"""
    from apb.context import sampler
    now = _time.time()
    beat = round(now - _poller_beat["at"]) if _poller_beat["at"] else None
    samp = sampler.status()
    mem = _memory()
    paused = _cad.kind_status()
    rdio_st = __import__("apb.ingest.rdio", fromlist=["status"]).status()
    radio_part = []
    if rdio_st.get("listed"):
        down = rdio_st.get("proxy_ok") is False
        open_n = (rdio_st.get("instances") or {}).get("open", 0)
        radio_part = [{
            "name": "radio egress", "ok": not down and open_n > 0,
            "detail": (f"proxy down {rdio_st.get('proxy_down_min')} min — radio lane paused" if down
                       else f"{open_n} of {rdio_st['listed']} scanners answering"),
        }]
    return radio_part + [{
        "name": "memory", "ok": not (mem["used_share"] and mem["used_share"] >= 0.85),
        "detail": (f"{mem['rss_mb']} MB of {mem['limit_mb']} MB, up {mem['uptime_min']} min"
                   if mem["rss_mb"] is not None else "not measurable on this host"),
    }, {
        "name": "shared lanes", "ok": not paused,
        "detail": (", ".join(f"{k} paused {v['paused_min']} min" for k, v in paused.items())
                   or "none paused"),
    }, {
        "name": "poller", "ok": bool(_poller_leader and beat is not None
                                     and beat <= _POLLER_STALE_AFTER_S),
        "detail": (f"cycle {_poller_beat['n']}, last beat {beat}s ago" if beat is not None
                   else "no beat yet" if _poller_leader else "follower (another instance polls)"),
    }, {
        "name": "sampler", "ok": bool(samp.get("running")),
        "detail": f"{samp.get('facts_runs', 0)} facts passes, {samp.get('cams_frames', 0)} frames read, "
                  f"{samp.get('errors', 0)} errors",
    }, {
        "name": "camera tasking", "ok": not samp["tasking"]["off"],
        "detail": f"{samp['tasking']['checks']} checks ({samp['tasking']['supported']} supported), "
                  f"{samp['tasking']['queued']} queued",
    }]


@app.get("/status/health")
def status_health(limit: int = Query(80, ge=0, le=1000)):
    """Feed-silence report: which lanes are failing (fetch errors), silent (zero rows
    where rows are normal) or stale (freshest record far older than usual) — the
    difference between a calm map and a blind one."""
    from apb.store import feedhealth
    out = feedhealth.summary(limit=limit)
    out["components"] = _components()
    return JSONResponse(out, headers={"Cache-Control": "public, max-age=30"})


@app.get("/scorecard")
def scorecard(days: float = Query(30.0, gt=0.0, le=90.0)):
    """Were the alerts right? Share matched by news, how often we were first, and the
    median lead time over the first headline — per rule and overall."""
    from apb.store import alertlog
    return JSONResponse(alertlog.scorecard(days), headers={"Cache-Control": "public, max-age=60"})


@app.get("/alerts/rules")
def alert_rules():
    """The active alert rules (webhook URLs are never returned)."""
    from apb.alerts.rules import load_rules
    return [{"name": r.name, "min_score": r.min_score, "min_sources": r.min_sources,
             "types": sorted(r.types) if r.types else None, "bbox": r.bbox,
             "center": r.center, "radius_km": r.radius_km if r.center else None,
             "max_age_hours": r.max_age_hours, "escalate": r.escalate,
             "own_webhook": bool(r.webhook)} for r in load_rules()]


@app.get("/live/radio")
def live_radio(max_age_minutes: float = Query(30.0, gt=0.0, le=720.0),
               min_z: float = Query(-99.0, ge=-99.0, le=20.0)):
    """Radio activity per public scanner system: calls in the last poll window, the
    system's own normal for this time of day, and how surprising the count is (z).
    Call counts and talkgroup tags only — no audio, and no operator addresses."""
    from apb.ingest import rdio
    rows = [r for r in rdio.activity(max_age_minutes * 60.0)
            if min_z <= -99.0 or (r["z"] is not None and r["z"] >= min_z)]
    return JSONResponse(rows[:500], headers={"Cache-Control": "public, max-age=60"})


_RADIO_ID_RE = __import__("re").compile(r"^[0-9a-f]{10}:-?\d{1,9}$")
_RADIO_KEY_RE = __import__("re").compile(r"^[0-9a-f]{10}$")
_audio_cache: "dict[tuple, tuple[bytes, str]]" = {}
_audio_cache_bytes = 0
_AUDIO_CACHE_MAX = 24 * 1024 * 1024
_audio_hits: dict[str, deque] = defaultdict(deque)
_AUDIO_PER_MIN = 60


def _transcript_out(t: dict, label: str | None = None, area: str | None = None) -> dict:
    """What leaves the server about a transcribed call: what kind of incident, how
    urgent, the system, and the ~2 km area it is in. Never the words, the address or
    the units — those stay internal and only serve to place the area."""
    from apb.context.radio_stt import coarse
    cell = coarse((t["lat"], t["lon"])) if t.get("lat") is not None else None
    return {"id": f"{t['key']}:{t['system']}", "call": t["id"], "ts": t["ts"],
            "system": label, "talkgroup": t.get("tg_label"), "tag": t.get("tag"),
            "kind": t.get("kind"), "type": t.get("incident_type"), "priority": t.get("priority"),
            "area": area, "lat": cell[0] if cell else None, "lon": cell[1] if cell else None,
            "audio": f"/live/radio/audio/{t['key']}/{t['id']}"}


def _system_area(rdio, inst, system) -> tuple[str | None, str | None]:
    if not inst:
        return None, None
    label = rdio._label(inst, system)
    pl = rdio.place_info(inst, label, system)
    return label, pl.get("area") or (pl.get("state") or None)


@app.get("/live/radio/transcripts")
def live_radio_transcripts(max_age_minutes: float = Query(60.0, gt=0.0, le=4320.0),
                           located_only: bool = False,
                           min_priority: float = Query(0.0, ge=0.0, le=1.0),
                           limit: int = Query(200, ge=1, le=500)):
    """Calls the smart-radio worker classified: incident type, urgency, system and the
    ~2 km area. The transcript text and addresses are never published."""
    from apb.ingest import rdio
    from apb.store import radio as radio_store
    out = []
    for t in radio_store.transcripts(max_age_minutes * 60.0, located_only, min_priority, limit):
        label, area = _system_area(rdio, rdio.instance(t["key"]), t["system"])
        out.append(_transcript_out(t, label, area))
    return JSONResponse(out, headers={"Cache-Control": "public, max-age=30"})


@app.get("/live/radio/areas")
def live_radio_areas(max_age_minutes: float = Query(90.0, gt=0.0, le=4320.0)):
    """What is going on where, per radio system: one line per area summarised from the
    system's recent transcribed calls. Activity only — no addresses, names or units;
    an area is placed on a ~2 km cell, or not at all."""
    from apb.context.radio_areas import public
    return JSONResponse(public(max_age_minutes * 60.0), headers={"Cache-Control": "public, max-age=60"})


@app.get("/live/radio/threads")
def live_radio_threads(max_age_minutes: float = Query(180.0, gt=0.0, le=4320.0)):
    """Incidents heard on the radio: calls linked into one thread per event (dispatch,
    units responding, updates, clearing), each placed on a ~2 km cell with how it was
    placed (address / intersection / town). Cleaned lines only — no addresses, names,
    unit numbers or medical details."""
    from apb.context.radio_areas import public_threads
    return JSONResponse(public_threads(max_age_minutes * 60.0)[:400],
                        headers={"Cache-Control": "public, max-age=30"})


def _radio_line(t: dict) -> dict | None:
    """A transcribed call as the map shows it: cleaned words, kind, type, urgency and the
    ~2 km cell when placed — or None for noise and mostly-[inaudible] calls."""
    from apb.context.radio_areas import clean
    from apb.context.radio_stt import coarse
    line = clean(t.get("text") or t.get("summary"))[:240]
    heard = __import__("re").sub(r"\[?inaudible\]?|[^A-Za-z0-9]+", " ", line, flags=2).split()
    if len(heard) < 3 or t.get("incident_type") == "noise" or t.get("kind") in ("noise", "tones") \
            or t.get("system") is None:
        return None
    cell = coarse((t["lat"], t["lon"])) if t.get("lat") is not None else None
    return {"sys": f"{t['key']}:{t['system']}", "id": t["id"], "ts": t["ts"],
            "talkgroup": t.get("tg_label"), "kind": t.get("kind"), "type": t.get("incident_type"),
            "priority": t.get("priority"), "line": line,
            "lat": cell[0] if cell else None, "lon": cell[1] if cell else None,
            "audio": f"/live/radio/audio/{t['key']}/{t['id']}"}


@app.get("/live/radio/lines")
def live_radio_lines(max_age_minutes: float = Query(60.0, gt=0.0, le=360.0),
                     limit: int = Query(400, ge=1, le=1000)):
    """Recent transcribed calls as cleaned lines (newest first) — the map's chat
    bubbles. Same cleaning as /calls and /feed: no house numbers, units or names."""
    from apb.store import radio as radio_store
    out = [x for x in (_radio_line(t) for t in radio_store.transcripts(max_age_minutes * 60.0, limit=limit)) if x]
    return JSONResponse(out, headers={"Cache-Control": "public, max-age=30"})


@app.get("/live/radio/feed")
def live_radio_feed(since: float | None = Query(None, ge=0.0)):
    """Live radio indicators: what the radio lanes saw after `since` (the `now` of the
    previous reply; omit it for the last 90 s). New calls (system, talkgroup, weight — no
    content), newly transcribed calls (cleaned line, kind, ~2 km cell when placed) and
    incident threads written since. Poll every few seconds."""
    from apb.context.radio_stt import weight_for
    from apb.ingest import rdio
    from apb.store import radio as radio_store
    now = _time.time()
    since = max(now - 600.0, min(now, since if since is not None else now - 90.0))
    got = radio_store.feed(since)
    calls, lines = [], []
    for c in got["calls"]:
        if c["system"] is None:
            continue
        tg = rdio.talkgroup(c["key"], int(c["system"]), c["talkgroup"])
        w, _ = weight_for(c["key"], int(c["system"]), c["talkgroup"], tg)
        calls.append({"sys": f"{c['key']}:{c['system']}", "id": c["id"], "ts": c["ts"],
                      "talkgroup": tg["label"], "weight": w})
    for t in got["lines"]:
        x = _radio_line(t)
        if x:
            lines.append(x)
    return JSONResponse({"now": now, "calls": calls, "lines": lines, "threads": got["threads"]},
                        headers={"Cache-Control": "no-store"})


@app.get("/live/radio/{rid}/calls")
def live_radio_calls(rid: str, limit: int = Query(30, ge=1, le=100)):
    """Newest calls on one radio system: time, talkgroup, how much it matters, a
    playable audio link, and the incident kind when the worker classified it."""
    if not _RADIO_ID_RE.match(rid):
        return JSONResponse({"error": "bad id"}, status_code=400)
    from apb.context.radio_stt import weight_for
    from apb.ingest import rdio
    from apb.store import radio as radio_store
    key, _, system = rid.partition(":")
    if not rdio.instance(key):
        return JSONResponse({"error": "unknown system"}, status_code=404)
    from apb.context.radio_areas import clean
    in_thread = {cid: (t["tid"], clean(t.get("title")))
                 for t in radio_store.threads(4 * 3600.0, key, int(system)) for cid in t["calls"]}
    out = []
    for c in radio_store.recent_calls(key, int(system), limit):
        tg = rdio.talkgroup(key, int(system), c["talkgroup"])
        weight, family = weight_for(key, int(system), c["talkgroup"], tg)
        row = {"id": c["id"], "ts": c["ts"], "talkgroup": tg["label"], "tag": tg["tag"],
               "group": tg.get("group"), "weight": weight, "family": family,
               "audio": f"/live/radio/audio/{key}/{c['id']}"}
        if c.get("status") == "done":             # cleaned words: no numbers, units or conditions
            row.update(kind=c.get("kind"), type=c.get("incident_type"),
                       priority=c.get("priority"), located=c.get("lat") is not None,
                       line=clean(c.get("text") or c.get("summary"))[:240] or None)
        if c["id"] in in_thread:
            row["thread"], row["thread_title"] = in_thread[c["id"]]
        out.append(row)
    return JSONResponse(out, headers={"Cache-Control": "public, max-age=8"})


@app.get("/live/radio/audio/{key}/{call_id}")
def live_radio_audio(request: Request, key: str, call_id: int):
    """One call's audio, relayed through the egress proxy from the instance that
    published it and cached briefly. Only calls the lane itself has listed can be
    asked for (no arbitrary hosts, no enumeration); every request counts against the
    instance's per-minute allowance, so a popular page cannot flood a home server."""
    global _audio_cache_bytes
    if not _RADIO_KEY_RE.match(key) or not 0 < call_id < 10**13:
        return JSONResponse({"error": "bad id"}, status_code=400)
    from apb.ingest import rdio
    from apb.store import radio as radio_store
    inst = rdio.instance(key)
    if not inst or not radio_store.has_call(key, call_id):
        return JSONResponse({"error": "unknown call"}, status_code=404)
    hit = _audio_cache.get((key, call_id))
    if hit is None:
        ip = request.client.host if request.client else "?"
        now = _time.time()
        with _hits_lock:
            q = _audio_hits[ip]
            while q and q[0] < now - 60.0:
                q.popleft()
            if len(q) >= _AUDIO_PER_MIN:
                return JSONResponse({"error": "slow down"}, status_code=429,
                                    headers={"Retry-After": "20"})
            q.append(now)
        try:
            got = rdio.fetch_calls(inst, [call_id])
        except Exception as e:
            log.info("radio audio %s/%s: %s", key, call_id, str(e)[:120])
            return JSONResponse({"error": "source unavailable"}, status_code=424)
        if call_id not in got:
            return JSONResponse({"error": "source busy or call gone"}, status_code=503,
                                headers={"Retry-After": "20"})
        hit = got[call_id]
        _audio_cache[(key, call_id)] = hit
        _audio_cache_bytes += len(hit[0])
        while _audio_cache_bytes > _AUDIO_CACHE_MAX and _audio_cache:
            old = next(iter(_audio_cache))
            _audio_cache_bytes -= len(_audio_cache.pop(old)[0])
    data, mime = hit
    headers = {"Cache-Control": "private, max-age=3600", "Accept-Ranges": "bytes"}
    rng = request.headers.get("range", "")
    if rng.startswith("bytes="):
        try:                                   # Safari asks for byte ranges before playing
            a, _, b = rng[6:].split(",")[0].partition("-")
            start = int(a) if a else max(0, len(data) - int(b))
            end = min(len(data) - 1, int(b)) if (a and b) else len(data) - 1
            if start > end or start >= len(data):
                raise ValueError
            return Response(data[start:end + 1], status_code=206, media_type=mime, headers={
                **headers, "Content-Range": f"bytes {start}-{end}/{len(data)}"})
        except ValueError:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{len(data)}"})
    return Response(data, media_type=mime, headers=headers)


@app.get("/live/radio/{rid}")
def live_radio_series(rid: str, hours: float = Query(24.0, gt=0.0, le=168.0)):
    """One system's call-rate history (calls per 10 minutes, with each point's z)."""
    if not _RADIO_ID_RE.match(rid):
        return JSONResponse({"error": "bad id"}, status_code=400)
    from apb.store import radio as radio_store
    key, _, system = rid.partition(":")
    return JSONResponse(radio_store.series(key, int(system), hours),
                        headers={"Cache-Control": "public, max-age=120"})


@app.get("/live/internet")
def live_internet(max_age_hours: Hours = 0.0):
    """Cloudflare Radar internet outages / traffic anomalies by country and network.
    Empty unless a Radar-read token is configured."""
    return _live_feed("radar", max_age_hours)


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
