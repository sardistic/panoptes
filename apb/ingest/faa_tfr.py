"""FAA Temporary Flight Restrictions — a keyless airspace-activity signal.

TFRs are issued over wildfires, disasters, major incidents, and VIP/security
movements, so a fresh TFR is an early "something is happening here" marker that
pairs naturally with the ADS-B loiter watcher. The FAA list endpoint is keyless
JSON; it carries no geometry, but each row's `description` leads with a place
("Reading, PA, ..." / "9NM NW KENDALL, FL, ..."), which we resolve to a point via
the shared gazetteer, falling back to the state centroid.

Returns the same normalized incident dict the CAD/hazard layers emit, so TFRs flow
into the overview, the snapshot poller, and the map. Registered as a hidden feed of
kind="faa_tfr"; see apb.ingest.cad.load_faa_tfr.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_LIST = "https://tfr.faa.gov/tfrapi/exportTfrList"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

# TFR type -> (incident_type, threat). HAZARDS often = active wildfire/disaster TFR.
_TYPE = {
    "HAZARDS": ("fire", 0.7),
    "SECURITY": ("suspicious", 0.6),
    "VIP": ("suspicious", 0.55),
    "SPECIAL": ("other", 0.4),
    "SPACE": ("other", 0.35),
}

# "..., PA," — capture the place text before a trailing 2-letter state code.
_PLACE_ST = re.compile(r"^(.*?),\s*([A-Z]{2}),", re.S)
# distance/bearing prefixes the FAA prepends ("9NM NW KENDALL" -> "KENDALL").
_DIST = re.compile(r"^\s*\d+\s*NM\s+[NSEW]{1,3}\s+", re.I)


def _resolve(desc: str, state: str | None):
    """Best-effort point for a TFR: gazetteer match on the leading place, else state."""
    from apb.common.states import state_centroid
    from apb.fusion.places import resolve_place
    place_txt, st = None, state
    m = _PLACE_ST.match(desc or "")
    if m:
        place_txt = _DIST.sub("", m.group(1)).strip()
        st = m.group(2)
    if place_txt:
        p = resolve_place(place_txt)
        if p:
            return p.lat, p.lon, place_txt, st
    lat, lon = state_centroid(st)
    return lat, lon, (place_txt or st), st


def _epoch_from_creation(s: str | None) -> float | None:
    if not s:
        return None
    try:                              # FAA list date is "MM/DD/YYYY"
        return datetime.strptime(s.strip(), "%m/%d/%Y").replace(
            tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


class FaaTfrIngest:
    """Fetches the national TFR list; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        try:
            rows = self._client.get(_LIST).json()
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"fetch failed: {e}")
            return []
        out: list[dict] = []
        for r in rows if isinstance(rows, list) else []:
            desc = r.get("description") or ""
            itype, threat = _TYPE.get((r.get("type") or "").upper(), ("other", 0.4))
            lat, lon, loc, _st = _resolve(desc, r.get("state"))
            if lat is None or lon is None:
                continue
            nid = r.get("notam_id") or desc[:24]
            ts = _epoch_from_creation(r.get("creation_date"))
            out.append({
                "call_id": f"tfr:{nid}", "metro": "faa_tfr", "type": itype,
                "summary": f"TFR {r.get('type','')}: {desc}".strip()[:280],
                "location": loc, "source": "faa_tfr",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": False,
                "lat": lat, "lon": lon,
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
