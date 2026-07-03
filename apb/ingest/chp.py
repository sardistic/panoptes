"""CHP (California Highway Patrol) live incident feed — keyless statewide CAD.

media.chp.ca.gov/sa_xml/sa.xml is the official public feed behind CHP's Traffic
Incident Information Page: every active CHP incident statewide with dispatch
center, type code, location text, and micro-degree coordinates. It's the single
largest keyless CAD source in the country.

Feed quirks handled here:
- the server truncates the document at ~248 KB, so it is NOT valid XML — we
  regex-extract complete <Log> blocks and drop the trailing partial one;
- LATLON is "38661084:121369020" (degrees * 1e6, longitude unsigned → negate
  for the western hemisphere); "0:0" means no fix;
- LogTime is US-Pacific local time;
- <LogDetails> contains raw dispatch chatter including license plates — we
  deliberately never read it (PII policy: type + location only).

Returns the normalized incident dict, registered as a hidden feed of kind="chp".
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_URL = "https://media.chp.ca.gov/sa_xml/sa.xml"
_TZ = ZoneInfo("America/Los_Angeles")

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

_LOG_RE = re.compile(r'<Log ID = "([^"]+)">(.*?)</Log>', re.S)
_FIELD_RE = re.compile(r'<(LogTime|LogType|Location|LocationDesc|Area)>"([^"]*)"</')

# CHP type-code prefixes -> (normalized type, threat). Fallback: keyword classify.
_CODE_THREAT = {
    "1179": ("traffic", 0.7),   # collision - ambulance responding
    "1180": ("traffic", 0.75),  # collision - major injuries
    "1181": ("traffic", 0.6),   # collision - minor injuries
    "1182": ("traffic", 0.5),   # collision - property damage
    "1183": ("traffic", 0.55),  # collision - unknown injuries
    "1125": ("traffic", 0.4),   # traffic hazard
    "1166": ("traffic", 0.45),  # defective signal
    "20001": ("traffic", 0.8),  # hit & run - injuries
    "20002": ("traffic", 0.5),  # hit & run - property
    "23114": ("traffic", 0.4),  # object flying from vehicle
    "1144": ("medical", 0.9),   # fatality
    "10851": ("robbery", 0.6),  # stolen vehicle
    "1199": ("assault", 0.9),   # officer needs help
}


def _parse_ts(s: str) -> float | None:
    """'Jul  2 2026  5:55PM' (US-Pacific) -> UTC epoch."""
    try:
        return datetime.strptime(" ".join(s.split()), "%b %d %Y %I:%M%p").replace(
            tzinfo=_TZ).timestamp()
    except ValueError:
        return None


def _latlon(s: str) -> tuple[float, float] | None:
    try:
        lat_u, lon_u = s.split(":")
        lat, lon = int(lat_u) / 1e6, int(lon_u) / 1e6
    except (ValueError, AttributeError):
        return None
    if abs(lat) < 1e-3 or abs(lon) < 1e-3:
        return None
    return lat, -abs(lon)          # feed longitudes are unsigned west


class ChpIngest:
    """Fetches all active CHP incidents; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=30.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        from apb.ingest.cad import classify
        try:
            raw = self._client.get(_URL).text
        except httpx.HTTPError as e:
            log.warning("fetch failed: %s", e)
            return []
        out: list[dict] = []
        for log_id, body in _LOG_RE.findall(raw):
            f = dict(_FIELD_RE.findall(body))
            ll_m = re.search(r'<LATLON>"([^"]*)"</LATLON>', body)
            ll = _latlon(ll_m.group(1)) if ll_m else None
            if ll is None:
                continue
            logtype = f.get("LogType", "")
            code = logtype.split("-", 1)[0].strip()
            itype, threat = _CODE_THREAT.get(code) or classify(logtype)
            loc = ", ".join(x for x in (f.get("Location"), f.get("Area")) if x)
            out.append({
                "call_id": f"chp:{log_id}",
                "metro": "chp", "type": itype,
                "summary": logtype or itype,
                "location": loc or None, "source": "chp",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": threat >= 0.85,
                "lat": ll[0], "lon": ll[1],
                "at": f.get("LogTime"), "ts": _parse_ts(f.get("LogTime", "")),
            })
        return out
