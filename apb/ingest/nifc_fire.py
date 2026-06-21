"""NIFC WFIGS active wildfire incidents — keyless named-fire signal.

FIRMS gives satellite fire *pixels* and HMS gives smoke *plumes*; this gives the
authoritative *named incident* (acreage, % contained, cause, discovery date) from the
interagency Wildland Fire Interagency Geospatial Services current-incident point layer.
Unlike the WFIGS *perimeter* service (now token-gated), the incident-location points
are public.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="nifc_fire"; see apb.ingest.cad.load_nifc_fire.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_URL = ("https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
        "WFIGS_Incident_Locations_Current/FeatureServer/0/query")

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]


def _threat(acres: float | None, contained: float | None) -> float:
    a = acres or 0.0
    base = 0.9 if a >= 10000 else (0.7 if a >= 1000 else (0.5 if a >= 100 else 0.35))
    if contained and contained >= 90:      # mostly contained -> de-escalate
        base = max(0.3, base - 0.2)
    return base


def _f(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


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


class NifcFireIngest:
    """Fetches current WFIGS wildfire incidents; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=30.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        params = {"where": "1=1", "outFields": "*", "f": "geojson",
                  "resultRecordCount": 1000}
        try:
            feats = self._client.get(_URL, params=params).json().get("features", [])
        except (httpx.HTTPError, ValueError) as e:
            print(f"[nifc_fire] fetch failed: {e}")
            return []
        out: list[dict] = []
        for ft in feats:
            geom = ft.get("geometry") or {}
            coords = geom.get("coordinates") or [None, None]
            lon, lat = coords[0], coords[1]
            if lat is None or lon is None:
                continue
            p = ft.get("properties") or {}
            name = p.get("IncidentName") or "Wildfire"
            acres = _f(p.get("DailyAcres")) or _f(p.get("FinalAcres")) \
                or _f(p.get("DiscoveryAcres"))
            contained = _f(p.get("PercentContained"))
            threat = _threat(acres, contained)
            ts = _epoch(p.get("FireDiscoveryDateTime") or p.get("ModifiedOnDateTime"))
            bits = []
            if acres:
                bits.append(f"{acres:,.0f} ac")
            if contained is not None:
                bits.append(f"{contained:.0f}% contained")
            out.append({
                "call_id": f"nifc:{p.get('UniqueFireIdentifier') or p.get('OBJECTID') or name}",
                "metro": "nifc_fire", "type": "fire",
                "summary": (f"{name} Fire" + (" — " + ", ".join(bits) if bits else "")
                            + (f" ({p.get('POOState','').replace('US-','')})"
                               if p.get('POOState') else "")).strip()[:280],
                "location": p.get("POOLandownerCategory") or name, "source": "nifc_fire",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2),
                "emerging": bool(acres and acres >= 10000 and (contained or 0) < 25),
                "lat": float(lat), "lon": float(lon),
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
