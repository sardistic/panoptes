"""ODIN power-outage feed (ORNL / DOE) — keyless infrastructure-disruption signal.

The Outage Data Initiative Nationwide aggregates near-real-time utility outages at
county granularity (refreshed continuously). A large outage is both an event in its
own right and a corroborating cause signal — storms, fires, and crashes knock out
power, so an outage cluster explains a CAD/weather spike in the same place/time.

Keyless JSON via the ORNL OpenDataSoft Explore API. Each record carries the affected
county polygon (`geom`), so we centroid it for a map point — no FIPS table needed.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="odin"; see apb.ingest.cad.load_odin.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_API = ("https://ornl.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
        "odin-real-time-outages-county/records")

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]
_MIN_CUSTOMERS = 100        # skip trivial outages so the buffer stays high-signal


def _centroid(geom: dict | None):
    """Average every coordinate pair in a GeoJSON geometry -> (lat, lon)."""
    if not geom:
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

    _walk(geom.get("coordinates"))
    if not xs:
        return None, None
    return sum(ys) / len(ys), sum(xs) / len(xs)


def _threat(meters: int) -> float:
    if meters >= 20000:
        return 0.8
    if meters >= 5000:
        return 0.6
    if meters >= 1000:
        return 0.45
    return 0.3


def _epoch(s: str | None) -> float | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except ValueError:
        return None


class OdinIngest:
    """Fetches the largest current power outages; mirrors the CadIngest fetch contract."""

    def __init__(self, limit: int = 100):     # OpenDataSoft caps page size at 100
        self._client = httpx.Client(timeout=25.0, headers=_UA, follow_redirects=True)
        self._limit = limit

    def fetch(self) -> list[dict]:
        params = {"limit": self._limit, "order_by": "metersaffected DESC"}
        try:
            rows = self._client.get(_API, params=params).json().get("results", [])
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"fetch failed: {e}")
            return []
        out: list[dict] = []
        for r in rows:
            meters = r.get("metersaffected") or 0
            if meters < _MIN_CUSTOMERS:
                continue
            lat, lon = _centroid((r.get("geom") or {}).get("geometry"))
            if lat is None or lon is None:
                continue
            threat = _threat(meters)
            county, state = r.get("county"), r.get("state")
            ert = None
            try:
                ert = json.loads(r.get("estimatedrestorationtime") or "{}").get("ert")
            except (ValueError, AttributeError):
                pass
            ts = _epoch(r.get("reportedstarttime"))
            out.append({
                "call_id": f"odin:{r.get('utility_id')}:{r.get('communitydescriptor')}",
                "metro": "odin", "type": "other",
                "summary": (f"Power outage: {meters:,} customers — "
                            f"{county}, {state}"
                            + (f" (est. restore {ert})" if ert else ""))[:280],
                "location": f"{county}, {state}" if county else state,
                "source": "odin",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": False,
                "lat": lat, "lon": lon,
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
