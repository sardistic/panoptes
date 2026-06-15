"""Geocoding for the map UI — self-hosted Nominatim, no per-lookup cost or limits.

Dispatch locations are messy ("100 block of Elm", "5th and Main"), so we constrain
every lookup to the metro's bounding box, which disambiguates common street names and
keeps results local. Falls back to the metro centroid with low confidence.

Run your own Nominatim (docker: mediagis/nominatim) and point APB_NOMINATIM_URL at it.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

import httpx

from apb.common.config import MetroSystem

NOMINATIM_URL = os.environ.get("APB_NOMINATIM_URL", "http://localhost:8080")


@dataclass
class GeoResult:
    lat: float
    lon: float
    confidence: float        # 1.0 = exact geocode, ~0.2 = centroid fallback
    matched: str | None      # display name Nominatim returned, or None for fallback


class Geocoder:
    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or NOMINATIM_URL).rstrip("/")
        self._client = httpx.Client(timeout=10.0, headers={"User-Agent": "apb/0.1"})
        self._cache: dict[str, GeoResult] = {}

    def geocode(self, location_text: str | None, system: MetroSystem) -> GeoResult | None:
        centroid = system.centroid
        fallback = (
            GeoResult(centroid[0], centroid[1], 0.2, None) if centroid else None
        )
        if not location_text:
            return fallback

        cache_key = f"{system.metro}|{location_text.lower()}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        params = {
            "q": f"{location_text}, {system.metro}",
            "format": "jsonv2",
            "limit": 1,
            "addressdetails": 0,
        }
        bbox = getattr(system, "bbox", None)
        if bbox:  # [min_lon, min_lat, max_lon, max_lat]
            params["viewbox"] = ",".join(map(str, bbox))
            params["bounded"] = 1

        try:
            resp = self._client.get(f"{self.base_url}/search", params=params)
            resp.raise_for_status()
            hits = resp.json()
        except (httpx.HTTPError, ValueError):
            return fallback

        if not hits:
            return fallback

        h = hits[0]
        importance = float(h.get("importance", 0.5))
        result = GeoResult(
            lat=float(h["lat"]), lon=float(h["lon"]),
            # blend Nominatim importance into a 0.5..1.0 confidence band for real hits
            confidence=round(0.5 + 0.5 * min(importance, 1.0), 2),
            matched=h.get("display_name"),
        )
        self._cache[cache_key] = result
        return result
