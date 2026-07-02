"""OpenFEMA disaster declarations — authoritative event boundaries (keyless).

A federal declaration is a high-confidence anchor for the correlation layer: when a
region is declared for a fire/flood/hurricane, CAD/news/social spikes there are
explained. The API is keyless JSON but county/state-granular with no geometry, so we
geolocate the `designatedArea` via the shared gazetteer, falling back to the state
centroid.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="fema"; see apb.ingest.cad.load_fema.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_API = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

# FEMA incidentType -> (incident_type, threat).
_TYPE = {
    "fire": ("fire", 0.8), "flood": ("weather", 0.7), "hurricane": ("weather", 0.85),
    "severe storm": ("weather", 0.7), "tornado": ("weather", 0.8),
    "earthquake": ("quake", 0.85), "snowstorm": ("weather", 0.6),
    "severe ice storm": ("weather", 0.6), "coastal storm": ("weather", 0.7),
    "mud/landslide": ("weather", 0.7), "volcanic eruption": ("weather", 0.85),
    "tropical storm": ("weather", 0.75), "winter storm": ("weather", 0.6),
    "typhoon": ("weather", 0.85), "dam/levee break": ("weather", 0.8),
    "drought": ("weather", 0.45), "tsunami": ("weather", 0.9),
}
_PAREN = re.compile(r"\s*\(.*?\)\s*$")     # strip "(County)" / "(Reservation)"


def _classify(incident_type: str | None):
    return _TYPE.get((incident_type or "").strip().lower(), ("other", 0.55))


def _resolve(area: str | None, state: str | None):
    from apb.common.states import state_centroid
    from apb.fusion.places import resolve_place
    if area:
        p = resolve_place(_PAREN.sub("", area).strip())
        if p:
            return p.lat, p.lon
    return state_centroid(state)


def _epoch(s: str | None) -> float | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except ValueError:
        return None


class FemaIngest:
    """Fetches recent FEMA disaster declarations; mirrors the CadIngest fetch contract."""

    def __init__(self, top: int = 300):
        self._client = httpx.Client(timeout=25.0, headers=_UA, follow_redirects=True)
        self._top = top

    def fetch(self) -> list[dict]:
        params = {"$top": self._top, "$orderby": "declarationDate desc",
                  "$select": ("disasterNumber,state,declarationType,declarationDate,"
                              "incidentType,declarationTitle,designatedArea,"
                              "incidentBeginDate")}
        try:
            rows = self._client.get(_API, params=params).json().get(
                "DisasterDeclarationsSummaries", [])
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"fetch failed: {e}")
            return []
        out: list[dict] = []
        seen: set[str] = set()
        for r in rows:
            area, state = r.get("designatedArea"), r.get("state")
            key = f"{r.get('disasterNumber')}:{area}"
            if key in seen:
                continue
            seen.add(key)
            lat, lon = _resolve(area, state)
            if lat is None or lon is None:
                continue
            itype, threat = _classify(r.get("incidentType"))
            ts = _epoch(r.get("incidentBeginDate") or r.get("declarationDate"))
            title = r.get("declarationTitle") or r.get("incidentType") or "Declaration"
            out.append({
                "call_id": key, "metro": "fema", "type": itype,
                "summary": f"FEMA {r.get('declarationType','')} {title} "
                           f"({area}, {state})".strip()[:280],
                "location": f"{area}, {state}" if area else state, "source": "fema",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": False,
                "lat": lat, "lon": lon,
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
