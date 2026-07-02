"""OpenAQ air-quality spikes — smoke/hazmat proxy signal (free key, opt-in).

Sharp PM2.5 spikes track wildfire smoke plumes, industrial releases, and large fires
better than any dispatch feed, so an air-quality cluster corroborates a fire/hazmat
event in the same place/time. OpenAQ v3 requires a free API key; opt-in via the
OPENAQ_KEY env var (mirrors FIRMS/ADS-B), so the lean core never depends on it.

We pull the latest PM2.5 reading per sensor nationwide and emit only locations above
the EPA "Unhealthy" threshold, scaling threat by AQI band.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="openaq"; see apb.ingest.cad.load_openaq.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import httpx

import logging

log = logging.getLogger(__name__)

# OpenAQ v3: latest values for PM2.5 (parameters_id=2), with coordinates.
_API = "https://api.openaq.org/v3/parameters/2/latest"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]
_UNHEALTHY = 55.5          # µg/m³ — EPA "Unhealthy" PM2.5 24h breakpoint


def api_key() -> str | None:
    return os.environ.get("OPENAQ_KEY")


def _threat(pm25: float) -> float:
    if pm25 >= 250.5:      # Hazardous
        return 0.95
    if pm25 >= 150.5:      # Very Unhealthy
        return 0.8
    if pm25 >= 55.5:       # Unhealthy
        return 0.6
    return 0.4


def _epoch(dt: dict | str | None) -> float | None:
    s = dt.get("utc") if isinstance(dt, dict) else dt
    if not s:
        return None
    try:
        d = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return (d if d.tzinfo else d.replace(tzinfo=timezone.utc)).timestamp()
    except ValueError:
        return None


class OpenAQIngest:
    """Fetches latest PM2.5 spikes; mirrors the CadIngest fetch contract."""

    def __init__(self, limit: int = 1000):
        self._limit = limit
        self._client = httpx.Client(timeout=25.0, follow_redirects=True, headers={
            "User-Agent": "apb/0.1 (panoptes.run)", "X-API-Key": api_key() or ""})

    def fetch(self) -> list[dict]:
        if not api_key():
            return []
        try:
            results = self._client.get(
                _API, params={"limit": self._limit}).json().get("results", [])
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"fetch failed: {e}")
            return []
        out: list[dict] = []
        for r in results:
            try:
                pm = float(r.get("value"))
            except (TypeError, ValueError):
                continue
            if pm < _UNHEALTHY:
                continue
            coords = r.get("coordinates") or {}
            lat, lon = coords.get("latitude"), coords.get("longitude")
            if lat is None or lon is None:
                continue
            threat = _threat(pm)
            ts = _epoch(r.get("datetime"))
            out.append({
                "call_id": f"openaq:{r.get('sensorsId') or r.get('locationsId')}",
                "metro": "openaq", "type": "other",
                "summary": f"Air quality: PM2.5 {pm:.0f} µg/m³ (Unhealthy+)",
                "location": None, "source": "openaq",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": pm >= 250.5,
                "lat": float(lat), "lon": float(lon),
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
