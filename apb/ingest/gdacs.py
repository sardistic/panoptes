"""GDACS (gdacs.org) — keyless global multi-hazard disaster alerts.

The UN/EC Global Disaster Alert and Coordination System scores every significant
disaster (earthquake, tropical cyclone, flood, wildfire, volcano, drought) with a
Green/Orange/Red humanitarian-impact level. We keep only Orange/Red — Green is
routine background — which gives the map a curated "world's active disasters"
layer with report links.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered
as a hidden feed of kind="gdacs"; see apb.ingest.cad.load_gdacs.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_URL = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/MAP"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]
_LEVEL_THREAT = {"Red": 0.9, "Orange": 0.65}
_TYPE_LABEL = {"EQ": "Earthquake", "TC": "Tropical cyclone", "FL": "Flood",
               "WF": "Wildfire", "VO": "Volcano", "DR": "Drought", "TS": "Tsunami"}
_TYPE_NORM = {"EQ": "weather", "TC": "weather", "FL": "weather",
              "WF": "fire", "VO": "weather", "DR": "weather", "TS": "weather"}


def _point(geom: dict | None) -> tuple[float, float] | None:
    """(lat, lon) from a GeoJSON geometry; polygons collapse to their first vertex."""
    c = (geom or {}).get("coordinates")
    while isinstance(c, list) and c and isinstance(c[0], list):
        c = c[0]
    if isinstance(c, list) and len(c) >= 2 and isinstance(c[0], (int, float)):
        return float(c[1]), float(c[0])
    return None


def _ts(iso: str | None) -> float | None:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return None


class GdacsIngest:
    """Fetches current Orange/Red GDACS events; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=25.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        try:
            r = self._client.get(_URL)
            r.raise_for_status()
            feats = r.json().get("features", [])
        except (httpx.HTTPError, ValueError) as e:
            log.warning("fetch failed: %s", e)
            return []
        out: dict[str, dict] = {}          # eventid -> row (latest episode wins)
        for f in feats:
            p = f.get("properties", {})
            level = p.get("alertlevel")
            if level not in _LEVEL_THREAT:
                continue
            pt = _point(f.get("geometry"))
            if pt is None:
                continue
            etype = p.get("eventtype", "?")
            threat = _LEVEL_THREAT[level]
            ts = _ts(p.get("todate")) or _ts(p.get("fromdate"))
            key = f"{etype}:{p.get('eventid')}"
            report = (p.get("url") or {}).get("report")
            out[key] = {
                "call_id": f"gdacs:{key}",
                "metro": "gdacs", "type": _TYPE_NORM.get(etype, "weather"),
                "event_type": etype, "alert_level": level,
                "summary": f"GDACS {level}: {p.get('name') or p.get('description') or _TYPE_LABEL.get(etype, etype)}",
                "location": p.get("country") or None,
                "source": "gdacs", "url": report,
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": threat, "emerging": level == "Red",
                "lat": pt[0], "lon": pt[1],
                "at": p.get("todate") or p.get("fromdate"), "ts": ts,
            }
        return list(out.values())
