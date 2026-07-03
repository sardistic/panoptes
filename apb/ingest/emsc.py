"""EMSC (seismicportal.eu) earthquakes — keyless global seismic feed.

Complements the USGS quake lane: EMSC aggregates dozens of national agencies
(BMKG, JMA, AFAD, ...) and often publishes non-US events minutes earlier. The
FDSN event API returns GeoJSON, no key.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered
as a hidden feed of kind="emsc"; see apb.ingest.cad.load_emsc.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_URL = "https://www.seismicportal.eu/fdsnws/event/1/query"
_MIN_MAG = 4.0               # below this it's routine background seismicity

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]


def _threat(mag: float) -> float:
    """Same scale intuition as the USGS lane: M4 ~ 0.35, M6 ~ 0.7, M7+ ~ 0.9."""
    return max(0.2, min(0.95, (mag - 2.0) / 5.5))


class EmscIngest:
    """Fetches recent M4+ events; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)

    def fetch(self, limit: int = 150) -> list[dict]:
        try:
            r = self._client.get(_URL, params={
                "format": "json", "limit": limit, "minmag": _MIN_MAG,
                "orderby": "time"})
            r.raise_for_status()
            feats = r.json().get("features", [])
        except (httpx.HTTPError, ValueError) as e:
            log.warning("fetch failed: %s", e)
            return []
        out: list[dict] = []
        for f in feats:
            p = f.get("properties", {})
            lat, lon, mag = p.get("lat"), p.get("lon"), p.get("mag")
            if lat is None or lon is None or mag is None:
                continue
            ts = None
            try:
                ts = datetime.fromisoformat(
                    str(p.get("time", "")).replace("Z", "+00:00")).timestamp()
            except ValueError:
                pass
            threat = _threat(float(mag))
            region = (p.get("flynn_region") or "").title()
            out.append({
                "call_id": f"emsc:{p.get('unid') or p.get('source_id')}",
                "metro": "emsc", "type": "weather",
                "summary": f"M{mag} earthquake — {region} "
                           f"(depth {p.get('depth', '?')} km, {p.get('auth', 'EMSC')})",
                "location": region, "source": "emsc", "magnitude": mag,
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": float(mag) >= 6.0,
                "lat": float(lat), "lon": float(lon),
                "at": p.get("time"), "ts": ts,
            })
        return out
