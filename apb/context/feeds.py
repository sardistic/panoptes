"""Lazy contextual-feed resolution for incident click-through.

Feeds (public/official cameras, traffic data) are NOT ingested or stored — they are
resolved on demand when the UI asks "what's near this incident?", fetched at view-time
and briefly cached. Private cameras (Ring etc.) are intentionally excluded.

Add a source by implementing FeedProvider.feeds_near(); register an instance in
REGISTRY. Each provider owns one data source.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Protocol

import logging

log = logging.getLogger(__name__)


@dataclass
class Feed:
    provider: str
    kind: str               # "camera" | "traffic" | "weather"
    name: str
    lat: float
    lon: float
    distance_m: float
    url: str | None = None      # stream / snapshot / info url
    snapshot: str | None = None  # still-image url if available


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class FeedProvider(Protocol):
    name: str
    def feeds_near(self, lat: float, lon: float, radius_m: float) -> list[Feed]: ...


class _CachingProvider:
    """Base with a short TTL cache so repeated clicks don't re-hit sources."""

    name = "base"
    ttl_sec = 60.0

    def __init__(self):
        self._cache: dict[str, tuple[float, list[Feed]]] = {}

    def _cached(self, key: str):
        hit = self._cache.get(key)
        if hit and (time.time() - hit[0]) < self.ttl_sec:
            return hit[1]
        return None

    def _store(self, key: str, feeds: list[Feed]) -> list[Feed]:
        self._cache[key] = (time.time(), feeds)
        return feeds


class DotCameraProvider(_CachingProvider):
    """Public DOT/511 traffic cameras.

    Many states/cities publish an open camera catalog (GeoJSON/JSON). Point
    `catalog_url` at one; we filter to those within radius. Catalog is fetched once
    and cached. (Catalog wiring left as a config step — schema varies per agency.)
    """

    name = "dot-cameras"

    def __init__(self, catalog: list[dict] | None = None):
        super().__init__()
        # catalog item shape: {"name","lat","lon","image"/"stream"}
        self._catalog = catalog or []

    def feeds_near(self, lat: float, lon: float, radius_m: float) -> list[Feed]:
        key = f"{lat:.4f},{lon:.4f},{int(radius_m)}"
        cached = self._cached(key)
        if cached is not None:
            return cached
        out: list[Feed] = []
        for c in self._catalog:
            d = haversine_m(lat, lon, c["lat"], c["lon"])
            if d <= radius_m:
                out.append(Feed(
                    provider=self.name, kind="camera", name=c.get("name", "camera"),
                    lat=c["lat"], lon=c["lon"], distance_m=round(d),
                    url=c.get("stream"), snapshot=c.get("image"),
                ))
        out.sort(key=lambda f: f.distance_m)
        return self._store(key, out)


# Registry of active providers. Append real providers (with catalogs) here.
REGISTRY: list[FeedProvider] = [
    DotCameraProvider(),
]


def feeds_near(lat: float, lon: float, radius_m: float = 800.0, limit: int = 20) -> list[Feed]:
    """Resolve nearby public feeds across all registered providers (on demand)."""
    found: list[Feed] = []
    for provider in REGISTRY:
        try:
            found.extend(provider.feeds_near(lat, lon, radius_m))
        except Exception as e:  # one bad provider shouldn't break the click
            log.warning(f"{provider.name} failed: {e}")
    found.sort(key=lambda f: f.distance_m)
    return found[:limit]
