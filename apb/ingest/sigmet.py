"""FAA/NWS Aviation Weather Center SIGMETs — keyless hazardous-airspace weather.

SIGMETs flag airspace-scale weather hazards in effect right now: convective
storms, severe turbulence, icing, volcanic ash. They're issued for areas (a
polygon), so we place each at the polygon centroid. A convective SIGMET over a
metro corroborates ground weather/traffic signals in the fusion layer.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered
as a hidden feed of kind="sigmet"; see apb.ingest.cad.load_sigmet.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import httpx

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_URL = "https://aviationweather.gov/api/data/airsigmet"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]
_HAZ_THREAT = {"CONVECTIVE": 0.6, "TS": 0.6, "TURB": 0.45, "ICE": 0.45,
               "IFR": 0.35, "MTN OBSCN": 0.3, "ASH": 0.85}


class SigmetIngest:
    """Fetches currently-valid SIGMETs; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        try:
            r = self._client.get(_URL, params={"format": "json"})
            r.raise_for_status()
            rows = r.json()
        except (httpx.HTTPError, ValueError) as e:
            log.warning("fetch failed: %s", e)
            return []
        if not isinstance(rows, list):
            return []
        now = time.time()
        out: list[dict] = []
        for a in rows:
            coords = a.get("coords") or []
            pts = [(c.get("lat"), c.get("lon")) for c in coords
                   if c.get("lat") is not None and c.get("lon") is not None]
            if not pts:
                continue
            valid_to = a.get("validTimeTo")
            if valid_to and valid_to < now:
                continue                    # expired
            lat = sum(p[0] for p in pts) / len(pts)
            lon = sum(p[1] for p in pts) / len(pts)
            hazard = (a.get("hazard") or "").upper()
            threat = _HAZ_THREAT.get(hazard, 0.4)
            ts = a.get("validTimeFrom")
            kind = a.get("airSigmetType") or "SIGMET"
            tops = a.get("altitudeHi1")
            out.append({
                "call_id": f"sigmet:{a.get('icaoId')}:{a.get('seriesId')}:{ts}",
                "metro": "sigmet", "type": "weather", "hazard": hazard,
                "summary": (f"{kind} {a.get('seriesId') or ''} — {hazard.title()}"
                            f"{f' to FL{tops // 100}' if tops else ''}").strip(),
                "location": None, "source": "sigmet",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": threat, "emerging": hazard == "ASH",
                "lat": round(lat, 4), "lon": round(lon, 4),
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
