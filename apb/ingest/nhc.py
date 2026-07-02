"""NHC active tropical cyclones — keyless hurricane-tracking signal.

The National Hurricane Center publishes currently-active Atlantic/East-Pacific
systems as keyless JSON (position, classification, intensity). A named storm is a
strong regional anchor — CAD/outage/flood/evac spikes in its path correlate to it.
Empty most of the year; lights up in hurricane season.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="nhc"; see apb.ingest.cad.load_nhc.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_URL = "https://www.nhc.noaa.gov/CurrentStorms.json"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

# Classification code -> (threat, label).
_CLASS = {
    "HU": (0.9, "Hurricane"), "MH": (0.95, "Major Hurricane"),
    "TS": (0.7, "Tropical Storm"), "TD": (0.55, "Tropical Depression"),
    "STS": (0.7, "Subtropical Storm"), "SD": (0.55, "Subtropical Depression"),
    "PTC": (0.5, "Potential Tropical Cyclone"), "STD": (0.5, "Post-Tropical"),
}


def _epoch(s: str | None) -> float | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except ValueError:
        return None


class NhcIngest:
    """Fetches active tropical cyclones; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        try:
            storms = self._client.get(_URL).json().get("activeStorms", [])
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"fetch failed: {e}")
            return []
        out: list[dict] = []
        for s in storms:
            try:
                lat, lon = float(s.get("latitudeNumeric")), float(s.get("longitudeNumeric"))
            except (TypeError, ValueError):
                continue
            threat, label = _CLASS.get((s.get("classification") or "").upper(),
                                       (0.6, "Cyclone"))
            name = s.get("name") or "Unnamed"
            wind = s.get("intensity")
            ts = _epoch(s.get("lastUpdate"))
            out.append({
                "call_id": f"nhc:{s.get('id') or name}", "metro": "nhc",
                "type": "weather",
                "summary": (f"{label} {name}"
                            + (f" — {wind} kt winds" if wind else ""))[:280],
                "location": f"{label} {name}", "source": "nhc",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": threat >= 0.9,
                "lat": lat, "lon": lon,
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
