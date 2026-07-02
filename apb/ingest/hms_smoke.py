"""NOAA HMS satellite smoke plumes — keyless wildfire-smoke signal.

The Hazard Mapping System digitizes smoke plumes from satellite imagery as polygons
classified Light / Medium / Heavy. This complements FIRMS active-fire pixels (where
fire is burning) and OpenAQ (ground PM2.5) with the overhead plume footprint, and
explains air-quality spikes downwind. Served keyless as an ArcGIS Feature Service.

Polygons are reduced to their centroid for a map point. Returns the same normalized
incident dict the CAD/hazard layers emit. Registered as a hidden feed of
kind="hms_smoke"; see apb.ingest.cad.load_hms_smoke.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_URL = ("https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/"
        "NOAA_Satellite_Smoke_Detection_(v1)/FeatureServer/0/query")

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]
_DENSITY = {"light": 0.3, "medium": 0.55, "heavy": 0.8}


def _centroid(geom: dict | None):
    if not geom:
        return None, None
    xs: list[float] = []
    ys: list[float] = []

    def _walk(c):
        if (isinstance(c, (list, tuple)) and len(c) >= 2
                and all(isinstance(n, (int, float)) for n in c[:2])):
            xs.append(float(c[0]))
            ys.append(float(c[1]))
        elif isinstance(c, (list, tuple)):
            for sub in c:
                _walk(sub)

    _walk(geom.get("coordinates"))
    if not xs:
        return None, None
    return sum(ys) / len(ys), sum(xs) / len(xs)


def _epoch(v) -> float | None:
    if v in (None, ""):
        return None
    if isinstance(v, (int, float)):
        return float(v) / 1000.0 if v > 1e12 else float(v)
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except ValueError:
        return None


class HmsSmokeIngest:
    """Fetches current HMS smoke polygons; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=25.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        params = {"where": "1=1", "outFields": "*", "f": "geojson",
                  "resultRecordCount": 500}
        try:
            feats = self._client.get(_URL, params=params).json().get("features", [])
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"fetch failed: {e}")
            return []
        out: list[dict] = []
        for f in feats:
            p = f.get("properties") or {}
            lat, lon = _centroid(f.get("geometry"))
            if lat is None or lon is None:
                continue
            density = str(p.get("Density") or "").strip()
            threat = _DENSITY.get(density.lower(), 0.4)
            ts = _epoch(p.get("End_") or p.get("Start"))
            out.append({
                "call_id": f"smoke:{p.get('FID')}:{p.get('Satellite','')}",
                "metro": "hms_smoke", "type": "fire",
                "summary": f"{density or 'Smoke'} smoke plume ({p.get('Satellite','satellite')})",
                "location": None, "source": "hms_smoke",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": False,
                "lat": lat, "lon": lon,
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
