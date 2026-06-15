"""Query + alerting API over stored incidents (dissemination layer).

Only redacted incident records are exposed here. Raw audio and unredacted
transcripts are intentionally NOT served by this API.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from apb.context.feeds import feeds_near
from apb.ingest.cad import FEEDS as CAD_FEEDS
from apb.ingest.cad import (CadIngest, load_arcgis_catalog, load_catalog,
                            load_p2c, load_pulsepoint, load_southern)

import threading
import time as _time

from apb.store import snapshots

_cad = CadIngest()
# Auto-register every live-ready feed found by the discovery sweeps (countrywide).
_a = load_catalog()
_b = load_arcgis_catalog()
_c = load_pulsepoint()
_d = load_p2c()
_e = load_southern()
print(f"[api] live CAD feeds: {len(CAD_FEEDS)} "
      f"(socrata +{_a}, arcgis +{_b}, pulsepoint +{_c}, p2c +{_d}, southern +{_e})")


def _poller(interval: float = 120.0):
    """Continuously snapshot the national stream into the DB so coverage/history grow."""
    snapshots.conn()  # init
    n = 0
    while True:
        try:
            wrote = snapshots.record(_cad.overview(limit_per=80, max_age_hours=168))
            n += 1
            if n % 30 == 0:
                snapshots.prune()
            print(f"[poller] snapshot {n}: +{wrote} rows, db={snapshots.stats()}")
        except Exception as e:
            print(f"[poller] error: {e}")
        _time.sleep(interval)


threading.Thread(target=_poller, daemon=True).start()

# DB stack (sqlalchemy/geoalchemy/psycopg) is imported lazily so the live/map UI and
# /feeds work with only FastAPI installed — no Postgres/PostGIS required to test.
def _db():
    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from apb.store.db import ActivityRow, IncidentRow, engine
    return select, Session, ActivityRow, IncidentRow, engine

app = FastAPI(title="APB", version="0.1.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

_WEB_DIR = Path(__file__).resolve().parents[2] / "web"


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/incidents")
def incidents(
    metro: str | None = None,
    incident_type: str | None = None,
    min_threat: float = 0.0,
    minutes: int = 60,
    limit: int = 200,
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
def activity(metro: str | None = None, minutes: int = 30, anomalous_only: bool = False):
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
def emerging(metro: str | None = None, minutes: int = 30):
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
def live_incidents(metro: str = "seattle", limit: int = 400,
                   max_age_hours: float = 0.0):
    """Real live CAD/911 dispatch (geocoded). max_age_hours>0 keeps only incidents
    within that window (0 = no time filter)."""
    rows = _cad.fetch(metro, limit)
    if max_age_hours > 0:
        import time as _t
        cutoff = _t.time() - max_age_hours * 3600
        rows = [d for d in rows if d.get("ts") and d["ts"] >= cutoff]
    return rows


@app.get("/live/overview")
def live_overview(limit_per: int = 60, max_age_hours: float = 72.0):
    """National aggregate: live fetch UNIONed with accumulated DB history (dedup), so
    coverage grows over time instead of being limited to each feed's latest page."""
    live = _cad.overview(limit_per, max_age_hours)
    merged = {(d["metro"], str(d["call_id"])): d for d in snapshots.query(max_age_hours)}
    for d in live:                       # live wins on conflict (freshest)
        merged[(d["metro"], str(d["call_id"]))] = d
    return list(merged.values())


@app.get("/db/stats")
def db_stats():
    """How much history the snapshot poller has accumulated."""
    return snapshots.stats()


@app.get("/baseline/anomalies")
def baseline_anomalies(window_hours: float = 1.0, lookback_hours: float = 72.0,
                       z: float = 2.0):
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
def live_emerging(min_count: int = 3, threat_floor: float = 0.5,
                  max_age_hours: float = 24.0):
    """Emerging events: spatial clusters of converging, elevated-severity incidents
    from the RECENT live stream (default last 24h)."""
    from apb.infer.cluster import detect
    clusters = detect(_cad.overview(max_age_hours=max_age_hours),
                      min_count=min_count, threat_floor=threat_floor)
    return [c.__dict__ for c in clusters]


@app.get("/live/metros")
def live_metros():
    """Metros with a live CAD feed for the dropdown (hidden bulk feeds excluded)."""
    feeds = sorted((f for f in CAD_FEEDS.values() if not f.hidden),
                   key=lambda f: f.name.lower())
    return [{"metro": f.metro, "name": f.name, "state": f.state,
             "center": list(f.center) if f.center else None} for f in feeds]


@app.get("/feeds")
def feeds(lat: float, lon: float, radius_m: float = 800.0):
    """On-demand public feeds near an incident (cameras/traffic). Not stored."""
    return [f.__dict__ for f in feeds_near(lat, lon, radius_m)]


@app.get("/correlate")
def correlate(lat: float, lon: float, types: str | None = None,
              timespan: str = "3d"):
    """Spike -> likely cause: recent GDELT news near a spike's place/time. Free, no key.
    `types` = comma-separated incident types from the cluster (shapes the news query)."""
    from apb.context.gdelt import correlate as _corr
    tl = [t for t in (types or "").split(",") if t]
    return _corr(lat, lon, tl or None, timespan)


# Serve the test UI at / (mounted last so it doesn't shadow API routes).
if _WEB_DIR.exists():
    app.mount("/", StaticFiles(directory=str(_WEB_DIR), html=True), name="web")
