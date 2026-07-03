"""Spike → cause correlation via GDELT (free global news/event index, no API key).

When the anomaly layer flags a spike (apb/infer/cluster.py, baseline.py), this answers
"what's the news saying happened HERE, NOW?" — turning a bare activity spike into an
explained event. GDELT's DOC 2.0 API indexes worldwide news every 15 minutes; we reverse
-geocode the spike to a place, build a query from the spike's incident types, and return
the most recent matching articles as candidate causes.

Query shape matters: GDELT rejects nested OR groups — `((a OR b) OR (c OR d))` returns
a plain-text "keywords too short/common (orclauseid:N)" error, NOT articles. All terms
must be flattened into ONE parenthesized OR group.

GDELT throttles hard (~1 request / 5s per IP; violations get a penalty window), so calls
are rate-limited, cached, and failures are negative-cached so the UI can't hammer it.
Reverse geocode is BigDataCloud's free no-key endpoint. Nothing here costs money or needs
credentials.

Usage:
  from apb.context.gdelt import correlate
  correlate(37.80, -122.27, types=["shots_fired","assault"])
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

import httpx

log = logging.getLogger(__name__)

_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"
_GNEWS = "https://news.google.com/rss/search"
_REVGEO = "https://api.bigdatacloud.net/data/reverse-geocode-client"
_MIN_INTERVAL = 6.0          # GDELT: ~1 request / 5s; stay comfortably under
_CACHE_TTL = 300.0
_ERR_TTL = 600.0             # negative cache: GDELT 429 penalties last many minutes

# Google News mixes in sports/entertainment "fire vs storm" noise — skip the obvious.
_NOISE_DOMAINS = {"espn.com", "si.com", "cbssports.com", "foxsports.com",
                  "bleacherreport.com", "mlb.com", "nba.com", "nfl.com"}

# normalized incident type (apb.ingest.cad.classify) -> news search terms.
# Kept as lists so multiple types can merge into a single flat OR group.
_THEME_TERMS: dict[str, list[str]] = {
    "shots_fired": ["shooting", "gunfire", '"shots fired"', '"active shooter"'],
    "assault": ["shooting", "stabbing", "assault", "homicide"],
    "robbery": ["robbery", "burglary", "looting", "theft"],
    "fire": ["fire", "explosion", "blaze", "evacuation", "wildfire"],
    "pursuit": ['"police chase"', "pursuit", "manhunt", "standoff"],
    "traffic": ["crash", "collision", '"car accident"', "pileup", "derailment"],
    "medical": ["overdose", '"mass casualty"', "outbreak", "collapse"],
    "domestic": ["police", "violence", "incident"],
    "suspicious": ["police", "threat", "evacuation", '"bomb threat"'],
    "welfare": ["police", "missing", "rescue"],
}
_GENERIC = ["police", "shooting", "protest", "fire", "crash", "emergency", "evacuation"]
_MAX_TERMS = 12              # keep the query well under GDELT's complexity limits


class GDELT:
    def __init__(self):
        self._client = httpx.Client(timeout=12.0,
                                    headers={"User-Agent": "Mozilla/5.0 apb-correlate/0.1"})
        self._lock = threading.Lock()
        self._last = 0.0
        self._geo_cache: dict[tuple, tuple] = {}
        self._art_cache: dict[str, tuple[float, list]] = {}
        self._err_until = 0.0          # penalty window after a 429/error response

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

    # ── GDELT DOC query (rate-limited + cached, negative-cached on error) ────
    def articles(self, query: str, timespan: str = "3d", maxrecords: int = 8) -> list[dict]:
        ck = f"{query}|{timespan}|{maxrecords}"
        hit = self._art_cache.get(ck)
        if hit and time.time() - hit[0] < _CACHE_TTL:
            return hit[1]
        if time.time() < self._err_until:      # in penalty window: stale or nothing
            return hit[1] if hit else []
        with self._lock:                       # serialize + throttle to GDELT's limit
            wait = _MIN_INTERVAL - (time.time() - self._last)
            if wait > 0:
                time.sleep(min(wait, _MIN_INTERVAL))
            self._last = time.time()
            try:
                r = self._client.get(_DOC, params={
                    "query": query, "mode": "artlist", "maxrecords": maxrecords,
                    "timespan": timespan, "format": "json", "sort": "hybridrel"})
            except httpx.HTTPError as e:
                log.warning("GDELT request failed: %s", e)
                self._err_until = time.time() + _ERR_TTL
                return hit[1] if hit else []
        if r.status_code != 200 or not r.text.strip().startswith("{"):
            # 429 or a plain-text query error ("keywords too short/common", ...)
            log.warning("GDELT non-JSON reply (HTTP %s): %s",
                        r.status_code, r.text.strip()[:120])
            self._err_until = time.time() + _ERR_TTL
            return hit[1] if hit else []
        arts = [{
            "title": a.get("title"), "url": a.get("url"), "domain": a.get("domain"),
            "seendate": a.get("seendate"), "at": _iso(a.get("seendate")),
        } for a in r.json().get("articles", [])]
        self._art_cache[ck] = (time.time(), arts)
        return arts

    # ── Google News RSS fallback (keyless, generous limits) ──────────────────
    def gnews(self, query: str, timespan: str = "3d", maxrecords: int = 6) -> list[dict]:
        """Same OR-query syntax as GDELT; carries the popup when GDELT is throttled
        (its 429 penalties can last long past the nominal 5s window)."""
        import xml.etree.ElementTree as ET
        from email.utils import parsedate_to_datetime
        from urllib.parse import urlparse
        ck = f"gnews|{query}|{timespan}"
        hit = self._art_cache.get(ck)
        if hit and time.time() - hit[0] < _CACHE_TTL:
            return hit[1]
        try:
            r = self._client.get(_GNEWS, params={"q": f"{query} when:{timespan}",
                                                 "hl": "en-US", "gl": "US",
                                                 "ceid": "US:en"})
            r.raise_for_status()
            root = ET.fromstring(r.text)
        except (httpx.HTTPError, ET.ParseError) as e:
            log.warning("google news fallback failed: %s", e)
            return hit[1] if hit else []
        arts = []
        for item in root.findall(".//item"):
            src = item.find("source")
            url = src.get("url", "") if src is not None else ""
            domain = urlparse(url).netloc.removeprefix("www.")
            if domain in _NOISE_DOMAINS:
                continue
            title = (item.findtext("title") or "").rsplit(" - ", 1)[0]
            try:
                at = parsedate_to_datetime(item.findtext("pubDate") or "").isoformat()
            except (TypeError, ValueError):
                at = None
            arts.append({"title": title, "url": item.findtext("link"),
                         "domain": domain or (src.text if src is not None else None),
                         "seendate": None, "at": at})
            if len(arts) >= maxrecords:
                break
        arts.sort(key=lambda a: a["at"] or "", reverse=True)
        self._art_cache[ck] = (time.time(), arts)
        return arts

    def correlate(self, lat: float, lon: float, types: list[str] | None = None,
                  timespan: str = "3d", maxrecords: int = 6) -> dict:
        """Spike (lat/lon + incident types) -> place + candidate-cause articles.
        Google News RSS first — its news-search ranking beats GDELT's full-text
        matching for "what happened in <city>" and it doesn't 429; GDELT fills in
        only when Google returns nothing."""
        city, state = self.place(lat, lon)
        terms = _terms_for(types)
        place_q = f'"{city}"' if city else (f'"{state}"' if state else "")
        query = (f"{place_q} ({terms})" if place_q else f"({terms})").strip()
        arts = self.gnews(query, timespan, maxrecords)
        provider = "gnews"
        if not arts:
            arts = self.articles(query, timespan, maxrecords)
            provider = "gdelt"
        return {"place": city, "state": state, "query": query, "articles": arts,
                "provider": provider,
                "throttled": time.time() < self._err_until and not arts}


def _terms_for(types: list[str] | None) -> str:
    """Flatten all types' terms into ONE deduped OR list (GDELT forbids nested groups)."""
    terms: list[str] = []
    for t in (types or []):
        for term in _THEME_TERMS.get(t, []):
            if term not in terms:
                terms.append(term)
    if not terms:
        terms = list(_GENERIC)
    return " OR ".join(terms[:_MAX_TERMS])


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
