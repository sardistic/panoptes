"""No-key national hazard/event geo-feeds — direct-observation signals.

Unlike CAD (reported incidents) these are sensor/authority feeds that publish
geocoded events directly, free and without auth:

- USGS earthquakes  (real-time GeoJSON, M2.5+ past day)
- NWS active alerts (severe weather / hazards, CAP GeoJSON)
- NASA EONET        (open natural events: wildfires, storms, volcanoes)

Each source function returns the same normalized incident dict the CAD layer
emits (lat/lon/type/summary/ts/threat_score), so hazards flow straight into the
national overview, the snapshot poller, and the map. Registered as hidden feeds
of kind="hazard"; see apb.ingest.cad.load_hazard.

These also double as CAUSE signals for the correlation layer — a quake or red-flag
warning explains an incident surge in the same place/time.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import httpx

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}

# USGS magnitude feed: M2.5+ in the past day (good signal/noise nationally).
_USGS = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"
# NWS active alerts (actual alerts only; warnings/watches with geometry).
_NWS = "https://api.weather.gov/alerts/active"
# NASA EONET open natural events in the last week.
_EONET = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&days=7"

# Source key -> human label, used by load_hazard to register feeds.
SOURCES = {
    "usgs": "USGS Earthquakes (M2.5+)",
    "nws": "NWS Severe-Weather Alerts",
    "eonet": "NASA EONET Natural Events",
}


def _centroid(geom: dict | None) -> tuple[float | None, float | None]:
    """Average all coordinate pairs in a GeoJSON geometry -> (lat, lon)."""
    if not geom:
        return None, None
    coords = geom.get("coordinates")
    if coords is None:
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

    _walk(coords)
    if not xs:
        return None, None
    return sum(ys) / len(ys), sum(xs) / len(xs)


# ── NWS severity (CAP) -> threat 0..1 ─────────────────────────────────────────
_NWS_SEV = {"Extreme": 0.95, "Severe": 0.75, "Moderate": 0.5, "Minor": 0.3,
            "Unknown": 0.3}
_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]


def _row(call_id, source, itype, summary, location, lat, lon, threat, ts):
    return {
        "call_id": f"{source}:{call_id}", "metro": source, "type": itype,
        "summary": summary, "location": location, "source": source,
        "sentiment": _SENTIMENT[min(4, int(threat * 5))],
        "threat_score": round(threat, 2), "emerging": threat >= 0.9,
        "lat": lat, "lon": lon, "at": ts, "ts": _epoch(ts),
    }


def _epoch(ts) -> float | None:
    if ts in (None, ""):
        return None
    if isinstance(ts, (int, float)):
        return float(ts) / 1000.0 if ts > 1e12 else float(ts)
    try:
        s = str(ts).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except ValueError:
        return None


def _iso(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


class HazardIngest:
    """Fetches one hazard source by key; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)

    def fetch(self, source: str) -> list[dict]:
        try:
            if source == "usgs":
                return self._usgs()
            if source == "nws":
                return self._nws()
            if source == "eonet":
                return self._eonet()
        except (httpx.HTTPError, ValueError, KeyError) as e:
            print(f"[hazard] {source} fetch failed: {e}")
        return []

    def _usgs(self) -> list[dict]:
        feats = self._client.get(_USGS).json().get("features", [])
        out = []
        for f in feats:
            p = f.get("properties") or {}
            lon, lat = (f.get("geometry") or {}).get("coordinates", [None, None])[:2]
            if lat is None or lon is None:
                continue
            mag = p.get("mag") or 0.0
            # magnitude -> threat: M3 ~0.4, M5 ~0.7, M6.5+ ~0.95
            threat = max(0.2, min(0.97, (mag - 1.0) / 6.0))
            out.append(_row(f.get("id"), "usgs", "quake",
                            p.get("title") or f"M{mag} earthquake",
                            p.get("place"), float(lat), float(lon), threat,
                            p.get("time")))
        return out

    def _nws(self) -> list[dict]:
        feats = self._client.get(_NWS, params={"status": "actual",
                                               "message_type": "alert"}).json().get("features", [])
        out = []
        for f in feats:
            p = f.get("properties") or {}
            lat, lon = _centroid(f.get("geometry"))
            if lat is None or lon is None:
                continue                      # many alerts are zone-only; skip those
            threat = _NWS_SEV.get(p.get("severity"), 0.3)
            event = p.get("event") or "Weather Alert"
            out.append(_row(p.get("id") or f.get("id"), "nws", "weather",
                            event, p.get("areaDesc"), lat, lon, threat,
                            p.get("effective") or p.get("sent")))
        return out

    def _eonet(self) -> list[dict]:
        events = self._client.get(_EONET).json().get("events", [])
        out = []
        for e in events:
            geoms = e.get("geometry") or []
            if not geoms:
                continue
            g = geoms[-1]                     # latest position
            lat, lon = _centroid(g)
            if lat is None or lon is None:
                continue
            cats = [c.get("title", "") for c in (e.get("categories") or [])]
            cat = (cats[0] if cats else "").lower()
            itype = "fire" if "wildfire" in cat else "weather"
            threat = 0.7 if itype == "fire" else 0.5
            out.append(_row(e.get("id"), "eonet", itype,
                            e.get("title") or cat.title(),
                            ", ".join(cats), lat, lon, threat,
                            g.get("date")))
        return out
