"""State DOT 511 traffic-incident feeds — the largest CAD-class lane after open CAD.

Every state runs a 511 system; many expose a public "events" JSON with lat/lon, event
type, severity and description. Shapes vary by vendor, so this is a REGISTRY (mirrors the
vendor_dork pattern): add a state = one SystemSpec entry + the parser for its shape.

Seeded with the common Carmanah/511 "events array" shape (verified keyless on 511NY,
~2.3k live events). Crashes/incidents map to high-threat traffic; long-planned roadwork
is downweighted (and optionally filtered) so it doesn't drown the live incident signal.

Returns the normalized incident-dict shape, registered as hidden kind="traffic511" feeds.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

# EventType (lowercased) -> (normalized type, threat). Incidents > planned work.
_EVENT_THREAT = {
    "accident": ("traffic", 0.6), "crash": ("traffic", 0.6),
    "incident": ("traffic", 0.55), "hazard": ("traffic", 0.45),
    "disabledvehicle": ("traffic", 0.3), "weather": ("weather", 0.4),
    "closure": ("traffic", 0.35), "roadwork": ("traffic", 0.2),
    "construction": ("traffic", 0.2), "specialevent": ("other", 0.25),
}


@dataclass
class SystemSpec:
    key: str            # slug, e.g. "ny"
    name: str
    url: str
    shape: str = "carmanah"   # parser family
    state: str | None = None


# Verified-keyless seed. Add states here (most others need a per-state API key).
SYSTEMS: dict[str, SystemSpec] = {
    "ny": SystemSpec("ny", "511 New York Traffic",
                     "https://511ny.org/api/getevents?format=json", state="NY"),
}


def _parse_dt(s: str | None) -> float | None:
    """511 timestamps are 'DD/MM/YYYY HH:MM:SS' (day-first, verified 28/05/...)."""
    if not s:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s.strip(), fmt).replace(
                tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    return None


class Traffic511:
    def __init__(self):
        self._client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)

    def fetch(self, key: str, include_planned: bool = False) -> list[dict]:
        spec = SYSTEMS.get(key)
        if not spec:
            return []
        try:
            data = self._client.get(spec.url).json()
        except (httpx.HTTPError, ValueError) as e:
            print(f"[511] {key} fetch failed: {e}")
            return []
        if spec.shape == "carmanah":
            return self._carmanah(data, spec, include_planned)
        return []

    def _carmanah(self, data, spec: SystemSpec, include_planned: bool) -> list[dict]:
        out = []
        for r in data if isinstance(data, list) else []:
            lat, lon = r.get("Latitude"), r.get("Longitude")
            if lat in (None, "", 0) or lon in (None, "", 0):
                continue
            etype = str(r.get("EventType") or "").lower().replace(" ", "")
            itype, threat = _EVENT_THREAT.get(etype, ("traffic", 0.3))
            if not include_planned and threat <= 0.2:
                continue                       # drop long-running planned roadwork
            sev = str(r.get("Severity") or "").lower()
            if sev in ("severe", "major"):
                threat = min(0.85, threat + 0.2)
            ts = _parse_dt(r.get("Reported")) or _parse_dt(r.get("LastUpdated"))
            desc = r.get("Description") or r.get("RoadwayName") or itype
            out.append({
                "call_id": f"{spec.key}:{r.get('ID')}", "metro": f"t511_{spec.key}",
                "type": itype, "summary": str(desc)[:280], "source": "511",
                "location": r.get("Location") or r.get("RoadwayName"),
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": threat >= 0.9,
                "lat": float(lat), "lon": float(lon),
                "at": r.get("Reported") or r.get("LastUpdated"), "ts": ts,
            })
        return out
