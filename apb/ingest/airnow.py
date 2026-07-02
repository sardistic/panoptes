"""AirNow official AQI — EPA/NOAA air-quality index (free key, opt-in).

AirNow is the authoritative US AQI network (EPA + state/local agencies), distinct
from OpenAQ's sensor aggregation — different coverage and an official AQI category.
Like OpenAQ it's a smoke/hazmat proxy: an Unhealthy+ reading corroborates a nearby
fire/hazmat event. Requires a free AirNow API key; opt-in via the AIRNOW_KEY env var
(mirrors FIRMS/OpenAQ), so the lean core never depends on it.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="airnow"; see apb.ingest.cad.load_airnow.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_API = "https://www.airnowapi.org/aq/data/"
_BBOX = "-125,24,-66,50"          # CONUS

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]
_MIN_AQI = 151                    # EPA "Unhealthy"


def api_key() -> str | None:
    return os.environ.get("AIRNOW_KEY")


def _threat(aqi: float) -> float:
    if aqi >= 301:                # Hazardous
        return 0.95
    if aqi >= 201:                # Very Unhealthy
        return 0.8
    if aqi >= 151:                # Unhealthy
        return 0.6
    return 0.4


def _epoch(s: str | None) -> float | None:
    if not s:
        return None
    try:                          # AirNow UTC e.g. "2026-06-21T00:00"
        d = datetime.fromisoformat(str(s).replace("Z", ""))
        return d.replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


class AirNowIngest:
    """Fetches current Unhealthy+ AQI readings; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=25.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        key = api_key()
        if not key:
            return []
        now = datetime.now(timezone.utc)
        params = {
            "startDate": now.strftime("%Y-%m-%dT%H"),
            "endDate": now.strftime("%Y-%m-%dT%H"),
            "parameters": "PM25,OZONE,PM10", "BBOX": _BBOX,
            "dataType": "A",       # AQI value
            "format": "application/json", "verbose": 0, "API_KEY": key,
        }
        try:
            rows = self._client.get(_API, params=params).json()
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"fetch failed: {e}")
            return []
        out: list[dict] = []
        for r in rows if isinstance(rows, list) else []:
            try:
                aqi = float(r.get("Value"))
                lat, lon = float(r["Latitude"]), float(r["Longitude"])
            except (TypeError, ValueError, KeyError):
                continue
            if aqi < _MIN_AQI:
                continue
            threat = _threat(aqi)
            param = r.get("Parameter") or "AQI"
            ts = _epoch(r.get("UTC"))
            out.append({
                "call_id": f"airnow:{lat:.3f}:{lon:.3f}:{param}",
                "metro": "airnow", "type": "other",
                "summary": f"Air quality: {param} AQI {aqi:.0f} (Unhealthy+)",
                "location": None, "source": "airnow",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": aqi >= 301,
                "lat": lat, "lon": lon,
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
