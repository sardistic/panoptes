"""Cloudflare Radar — internet outages and traffic anomalies by country / network.

Radar's Outage Center annotates confirmed outages (government shutdowns, cable cuts,
power failures, cyclone damage) and its anomaly detector flags countries and networks
whose traffic left the expected range. Connectivity dropping out is one of the
earliest machine-readable signs that something large happened somewhere, and it
complements the county-level IODA lane in the facts box with a global one.

Needs an API token with the account permission "Radar: Read" in `CLOUDFLARE_RADAR_TOKEN`
(or `CLOUDFLARE_API_TOKEN`). Use a token that has ONLY that permission for the
deployed service — a zone-editing token does not belong in an app's environment.

Rows are placed at the country's coordinates as Radar reports them. Registered as a
hidden feed of kind="radar"; see apb.ingest.cad.load_radar.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import httpx

log = logging.getLogger(__name__)

_BASE = "https://api.cloudflare.com/client/v4/radar"
_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]
_CAUSE_THREAT = {"GOVERNMENT_DIRECTED": 0.75, "CABLE_CUT": 0.65, "POWER_OUTAGE": 0.7,
                 "WEATHER": 0.7, "NATURAL_DISASTER": 0.8, "MILITARY_ACTION": 0.85,
                 "CYBERATTACK": 0.7, "FIRE": 0.65, "TECHNICAL_PROBLEM": 0.45,
                 "MAINTENANCE": 0.3, "UNKNOWN": 0.5}
_LOC_TTL = 7 * 86400.0


def api_token() -> str:
    return (os.environ.get("CLOUDFLARE_RADAR_TOKEN", "").strip()
            or os.environ.get("CLOUDFLARE_API_TOKEN", "").strip())


def _ts(iso: str | None) -> float | None:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _words(code: str | None) -> str:
    return (code or "unknown").replace("_", " ").lower()


class RadarIngest:
    """Outage annotations + verified traffic anomalies; CadIngest fetch contract,
    except that upstream failures RAISE so the ingest layer backs off and the
    feed-health store can tell a broken token from a quiet internet."""

    def __init__(self):
        self._client = httpx.Client(timeout=20.0, follow_redirects=True,
                                    headers={"User-Agent": "apb/0.1 (panoptes.run)"})
        self._locs: dict[str, tuple[float, float, str]] = {}
        self._locs_at = 0.0

    def _get(self, path: str, **params) -> dict:
        r = self._client.get(f"{_BASE}/{path}", params={"format": "json", **params},
                             headers={"Authorization": f"Bearer {api_token()}"})
        r.raise_for_status()
        body = r.json()
        if not body.get("success", True):
            raise ValueError(f"radar {path}: {body.get('errors')}")
        return body.get("result") or {}

    def _locations(self) -> dict[str, tuple[float, float, str]]:
        if self._locs and time.time() - self._locs_at < _LOC_TTL:
            return self._locs
        out: dict[str, tuple[float, float, str]] = {}
        for loc in self._get("entities/locations", limit=300).get("locations", []):
            try:
                out[loc["alpha2"]] = (float(loc["latitude"]), float(loc["longitude"]),
                                      loc.get("name") or loc["alpha2"])
            except (KeyError, TypeError, ValueError):
                continue
        if out:
            self._locs, self._locs_at = out, time.time()
        return self._locs

    def fetch(self) -> list[dict]:
        if not api_token():
            return []
        locs = self._locations()
        out: dict[str, dict] = {}

        def add(key: str, code: str | None, summary: str, threat: float, start: str | None,
                ongoing: bool, **extra) -> None:
            place = locs.get(code or "")
            if not place:
                return                              # global / platform-only: nowhere to draw it
            ts = _ts(start)
            out[key] = {
                "call_id": key, "metro": "radar", "type": "other", "source": "radar",
                "summary": summary[:280], "location": place[2],
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": ongoing and threat >= 0.7,
                "lat": place[0], "lon": place[1], "at": start, "ts": ts,
                "country": code, "ongoing": ongoing, **extra,
            }

        for a in self._get("annotations/outages", dateRange="7d", limit=100).get("annotations", []):
            if (a.get("eventType") or "OUTAGE") != "OUTAGE":
                continue
            o = a.get("outage") or {}
            cause = o.get("outageCause") or "UNKNOWN"
            threat = _CAUSE_THREAT.get(cause, 0.5)
            ongoing = not a.get("endDate")
            scope = f" ({a['scope']})" if a.get("scope") else ""
            desc = (a.get("description") or "").strip()
            for code in a.get("locations") or []:
                name = locs.get(code, (0, 0, code))[2]
                add(f"radar:outage:{a.get('id')}:{code}", code,
                    f"Internet outage — {name}{scope}: {_words(cause)}, "
                    f"{_words(o.get('outageType'))}" + (f". {desc}" if desc else ""),
                    threat if ongoing else threat * 0.7, a.get("startDate"), ongoing,
                    cause=cause, url=a.get("linkedUrl"))

        for t in self._get("traffic_anomalies", dateRange="2d", limit=100,
                           status="VERIFIED").get("trafficAnomalies", []):
            ongoing = not t.get("endDate")
            loc, asn = t.get("locationDetails") or {}, t.get("asnDetails") or {}
            if loc.get("code"):
                add(f"radar:anomaly:{t.get('uuid')}", loc["code"],
                    f"Internet traffic anomaly — {loc.get('name') or loc['code']} (country-wide)",
                    0.6 if ongoing else 0.4, t.get("startDate"), ongoing)
            elif (asn.get("location") or {}).get("code"):
                code = asn["location"]["code"]
                add(f"radar:anomaly:{t.get('uuid')}", code,
                    f"Internet traffic anomaly — {asn.get('name') or 'AS' + str(asn.get('asn'))} "
                    f"(AS{asn.get('asn')}, {asn['location'].get('name') or code})",
                    0.45 if ongoing else 0.3, t.get("startDate"), ongoing, asn=asn.get("asn"))
        return list(out.values())
