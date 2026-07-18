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

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

# EventType (lowercased) -> (normalized type, threat). Incidents > planned work.
_EVENT_THREAT = {
    "accident": ("traffic", 0.6), "crash": ("traffic", 0.6),
    "incident": ("traffic", 0.55), "hazard": ("traffic", 0.45),
    "disabledvehicle": ("traffic", 0.3), "weather": ("weather", 0.4),
    "closure": ("traffic", 0.35), "roadwork": ("traffic", 0.2),
    "construction": ("traffic", 0.2), "specialevent": ("other", 0.25),
    # PennDOT RCRS event types (statewide, richer than 511pa)
    "damagedroadway": ("traffic", 0.5), "debrisonroadway": ("traffic", 0.45),
    "flooding": ("weather", 0.55), "bridgeoutage": ("traffic", 0.35),
    "utilitywork": ("traffic", 0.2), "winterweather": ("weather", 0.5),
}


@dataclass
class SystemSpec:
    key: str            # slug, e.g. "ny"
    name: str
    url: str            # may contain {key}, filled from env `env_key`
    shape: str = "carmanah"   # parser family
    state: str | None = None
    env_key: str | None = None   # env var holding a (free) API key; None = keyless


# NY is verified keyless. The other states run the same platform ("carmanah"
# /api/getevents shape) but require a free API key — register at each site's
# "developer resources" page and set the env var to unlock the lane.
SYSTEMS: dict[str, SystemSpec] = {
    "ny": SystemSpec("ny", "511 New York Traffic",
                     "https://511ny.org/api/getevents?format=json", state="NY"),
    "ga": SystemSpec("ga", "511 Georgia Traffic",
                     "https://511ga.org/api/getevents?key={key}&format=json",
                     state="GA", env_key="T511_GA_KEY"),
    "la": SystemSpec("la", "511 Louisiana Traffic",
                     "https://511la.org/api/getevents?key={key}&format=json",
                     state="LA", env_key="T511_LA_KEY"),
    "pa": SystemSpec("pa", "511 Pennsylvania Traffic",
                     "https://www.511pa.com/api/getevents?key={key}&format=json",
                     state="PA", env_key="T511_PA_KEY"),
    "id": SystemSpec("id", "511 Idaho Traffic",
                     "https://511.idaho.gov/api/getevents?key={key}&format=json",
                     state="ID", env_key="T511_ID_KEY"),
    "fl": SystemSpec("fl", "FL511 Florida Traffic",
                     "https://fl511.com/api/getevents?key={key}&format=json",
                     state="FL", env_key="T511_FL_KEY"),
    # one system covers CT/ME/MA/NH/RI/VT
    "ne6": SystemSpec("ne6", "New England 511 Traffic",
                      "https://newengland511.org/api/getevents?key={key}&format=json",
                      state=None, env_key="T511_NE_KEY"),
    # PennDOT RCRS_Event_Data web service — statewide live events, HTTP Basic auth
    # with a CWOPA service account (PENNDOT_RCRS_USER / PENNDOT_RCRS_PW). Docs:
    # pa.gov "Developer Resources Documentation/API". Richer than the keyed 511pa
    # lane: fatality/hazmat/school-bus flags, lane status, verified timestamps.
    "pa_rcrs": SystemSpec("pa_rcrs", "PennDOT RCRS Live Events",
                          "https://eventsdata.dot.pa.gov/liveEvents",
                          shape="rcrs", state="PA", env_key="PENNDOT_RCRS_USER"),
}


def available() -> dict[str, SystemSpec]:
    """Systems usable right now: keyless ones plus keyed ones whose env key is set."""
    import os
    return {k: s for k, s in SYSTEMS.items()
            if not s.env_key or os.environ.get(s.env_key, "").strip()}


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
        self._rcrs_client = None   # built lazily; needs env credentials

    def fetch(self, key: str, include_planned: bool = False) -> list[dict]:
        import os
        spec = SYSTEMS.get(key)
        if not spec:
            return []
        if spec.shape == "rcrs":
            user = os.environ.get("PENNDOT_RCRS_USER", "").strip()
            pw = os.environ.get("PENNDOT_RCRS_PW", "").strip()
            if not user or not pw:
                return []
            # PennDOT terminates TLS with a Commonwealth-internal CA (self-signed
            # root, verified 2026-07-13), so certifi can't validate this host.
            if self._rcrs_client is None:
                self._rcrs_client = httpx.Client(timeout=20.0, headers=_UA,
                                                 auth=(user, pw), verify=False)
            try:
                data = self._rcrs_client.get(spec.url).json()
            except (httpx.HTTPError, ValueError) as e:
                log.warning(f"[511] {key} fetch failed: {e}")
                return []
            return self._rcrs(data, spec, include_planned)
        url = spec.url
        if spec.env_key:
            api_key = os.environ.get(spec.env_key, "").strip()
            if not api_key:
                return []
            url = url.format(key=api_key)
        try:
            data = self._client.get(url).json()
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"[511] {key} fetch failed: {e}")
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

    def _rcrs(self, data, spec: SystemSpec, include_planned: bool) -> list[dict]:
        """PennDOT RCRS liveEvents: {"values":[...]}; coords as "lat,lon" strings.
        Timestamps are Pennsylvania local time, so they are parsed as Eastern —
        treating them as UTC would age every event by 4-5 h and starve the
        short live windows."""
        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")

        def local_ts(s):
            if not s:
                return None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    return datetime.strptime(s.strip(), fmt).replace(
                        tzinfo=eastern).timestamp()
                except ValueError:
                    continue
            return None

        out = []
        for r in (data.get("values") or []) if isinstance(data, dict) else []:
            ll = r.get("incidentLocLatLong") or r.get("fromLocLatLong") or ""
            try:
                lat, lon = (float(x) for x in str(ll).split(","))
            except (TypeError, ValueError):
                continue                       # ~3/4 of records carry no point
            etype = str(r.get("eventType") or "").lower()
            itype, threat = _EVENT_THREAT.get(etype.replace(" ", ""), ("traffic", 0.3))
            if not include_planned and threat <= 0.2:
                continue                       # roadwork/utility: planned noise
            if r.get("isHazmat") == "T":
                threat = min(0.9, threat + 0.25)
            if r.get("isFatality") == "T":
                threat = max(threat, 0.85)
            when = r.get("lastUpdate") or r.get("dateTimeNotified")
            desc = r.get("description") or etype
            loc = " ".join(x for x in (r.get("facility"), r.get("incidentMuniName")
                                       or r.get("countyName")) if x) or None
            out.append({
                "call_id": f"{spec.key}:{r.get('eventID')}", "metro": f"t511_{spec.key}",
                "type": itype, "summary": str(desc)[:280], "source": "511",
                "location": loc,
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": threat >= 0.9,
                "lat": lat, "lon": lon,
                "at": when, "ts": local_ts(when),
            })
        return out
