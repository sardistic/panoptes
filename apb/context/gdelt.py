"""Spike → cause correlation via GDELT (free global news/event index, no API key).

When the anomaly layer flags a spike (apb/infer/cluster.py, baseline.py), this answers
"what's the news saying happened HERE, NOW?" — turning a bare activity spike into an
explained event. GDELT's DOC 2.0 API indexes worldwide news every 15 minutes; we reverse
-geocode the spike to a place, build a query from the spike's incident types, and return
the most recent matching articles as candidate causes.

GDELT throttles to ~1 request / 5s, so calls are rate-limited and cached. Reverse geocode
is BigDataCloud's free no-key endpoint. Nothing here costs money or needs credentials —
the paid radio firehose (apb/ingest/broadcastify.py) is a separate, optional layer.

Usage:
  from apb.context.gdelt import correlate
  correlate(37.80, -122.27, types=["shots_fired","assault"])
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

import httpx

_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"
_REVGEO = "https://api.bigdatacloud.net/data/reverse-geocode-client"
_MIN_INTERVAL = 5.2          # GDELT: keep to ~1 request / 5s
_CACHE_TTL = 300.0

# normalized incident type (apb.ingest.cad.classify) -> news search terms
_THEME_TERMS: dict[str, str] = {
    "shots_fired": "shooting OR gunfire OR \"shots fired\" OR active shooter",
    "assault": "shooting OR stabbing OR assault OR violence OR homicide",
    "robbery": "robbery OR burglary OR looting OR theft",
    "fire": "fire OR explosion OR blaze OR evacuation OR wildfire",
    "pursuit": "\"police chase\" OR pursuit OR manhunt OR standoff",
    "traffic": "crash OR collision OR \"car accident\" OR pileup OR derailment",
    "medical": "overdose OR \"mass casualty\" OR outbreak OR collapse",
    "domestic": "police OR violence OR incident",
    "suspicious": "police OR threat OR evacuation OR \"bomb threat\"",
    "welfare": "police OR missing OR rescue",
}
_GENERIC = "police OR shooting OR protest OR fire OR crash OR emergency OR evacuation"


class GDELT:
    def __init__(self):
        self._client = httpx.Client(timeout=20.0,
                                    headers={"User-Agent": "Mozilla/5.0 apb-correlate/0.1"})
        self._lock = threading.Lock()
        self._last = 0.0
        self._geo_cache: dict[tuple, tuple] = {}
        self._art_cache: dict[str, tuple[float, list]] = {}

    # ── reverse geocode (free, no key) ───────────────────────────────────────
    def place(self, lat: float, lon: float) -> tuple[str | None, str | None]:
        key = (round(lat, 3), round(lon, 3))
        if key in self._geo_cache:
            return self._geo_cache[key]
        try:
            j = self._client.get(_REVGEO, params={"latitude": lat, "longitude": lon,
                                                  "localityLanguage": "en"}).json()
            city = j.get("locality") or j.get("city")
            state = j.get("principalSubdivision")
            out = (city or None, state or None)
        except (httpx.HTTPError, ValueError):
            out = (None, None)
        self._geo_cache[key] = out
        return out

    # ── GDELT DOC query (rate-limited + cached) ──────────────────────────────
    def articles(self, query: str, timespan: str = "3d", maxrecords: int = 8) -> list[dict]:
        ck = f"{query}|{timespan}|{maxrecords}"
        hit = self._art_cache.get(ck)
        if hit and time.time() - hit[0] < _CACHE_TTL:
            return hit[1]
        with self._lock:                       # serialize + throttle to GDELT's limit
            wait = _MIN_INTERVAL - (time.time() - self._last)
            if wait > 0:
                time.sleep(min(wait, _MIN_INTERVAL))
            self._last = time.time()
            try:
                r = self._client.get(_DOC, params={
                    "query": query, "mode": "artlist", "maxrecords": maxrecords,
                    "timespan": timespan, "format": "json", "sort": "datedesc"})
            except httpx.HTTPError:
                return hit[1] if hit else []
        if r.status_code != 200 or not r.text.strip().startswith("{"):
            return hit[1] if hit else []       # 429/empty -> serve stale or nothing
        arts = [{
            "title": a.get("title"), "url": a.get("url"), "domain": a.get("domain"),
            "seendate": a.get("seendate"), "at": _iso(a.get("seendate")),
        } for a in r.json().get("articles", [])]
        self._art_cache[ck] = (time.time(), arts)
        return arts

    def correlate(self, lat: float, lon: float, types: list[str] | None = None,
                  timespan: str = "3d", maxrecords: int = 6) -> dict:
        """Spike (lat/lon + incident types) -> place + candidate-cause articles."""
        city, state = self.place(lat, lon)
        terms = _terms_for(types)
        place_q = f'"{city}"' if city else (f'"{state}"' if state else "")
        query = (f"{place_q} ({terms})" if place_q else terms).strip()
        arts = self.articles(query, timespan, maxrecords)
        return {"place": city, "state": state, "query": query, "articles": arts}


def _terms_for(types: list[str] | None) -> str:
    if not types:
        return _GENERIC
    seen, parts = set(), []
    for t in types:
        term = _THEME_TERMS.get(t)
        if term and term not in seen:
            seen.add(term)
            parts.append(f"({term})")
        if len(parts) >= 3:
            break
    return " OR ".join(parts) if parts else _GENERIC


def _iso(seendate: str | None) -> str | None:
    """GDELT seendate 'YYYYMMDDTHHMMSSZ' -> ISO 8601."""
    if not seendate:
        return None
    try:
        dt = datetime.strptime(seendate, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        return None


_gdelt: GDELT | None = None


def correlate(lat: float, lon: float, types: list[str] | None = None,
              timespan: str = "3d", maxrecords: int = 6) -> dict:
    """Module-level singleton entry point (one shared rate-limiter/cache)."""
    global _gdelt
    if _gdelt is None:
        _gdelt = GDELT()
    return _gdelt.correlate(lat, lon, types, timespan, maxrecords)
