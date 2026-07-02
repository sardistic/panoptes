"""USGS Volcano Hazards notices — keyless direct-observation hazard signal.

The Hazard Notification System (HANS) publishes elevated-status volcanoes with an
aviation color code (GREEN/YELLOW/ORANGE/RED) and alert level (NORMAL/ADVISORY/
WATCH/WARNING). Eruptive unrest drives Volcano Observatory Notices for Aviation and
often a TFR, so this pairs with the FAA TFR + ADS-B lanes.

The HANS API returns volcano *names* but not coordinates, so we map names to points
with a curated table of monitored US volcanoes (only a handful are ever elevated).

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="volcano"; see apb.ingest.cad.load_volcano.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_ELEVATED = "https://volcanoes.usgs.gov/hans-public/api/volcano/getElevatedVolcanoes"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

# Aviation color code -> threat.
_COLOR = {"GREEN": 0.25, "YELLOW": 0.45, "ORANGE": 0.7, "RED": 0.95, "UNASSIGNED": 0.3}

# Curated coordinates for monitored US volcanoes (the set that ever goes elevated).
VOLCANOES: dict[str, tuple[float, float]] = {
    "Great Sitkin": (52.076, -176.130), "Shishaldin": (54.756, -163.970),
    "Kupreanof": (56.013, -159.797), "Cleveland": (52.825, -169.945),
    "Pavlof": (55.417, -161.894), "Veniaminof": (56.170, -159.388),
    "Trident": (58.236, -155.103), "Atka": (52.331, -174.139),
    "Semisopochnoi": (51.930, 179.580), "Tanaga": (51.885, -178.146),
    "Gareloi": (51.790, -178.794), "Okmok": (53.397, -168.166),
    "Akutan": (54.134, -165.986), "Makushin": (53.890, -166.923),
    "Redoubt": (60.485, -152.742), "Spurr": (61.299, -152.251),
    "Iliamna": (60.032, -153.090), "Augustine": (59.363, -153.435),
    "Kilauea": (19.421, -155.287), "Mauna Loa": (19.475, -155.608),
    "Hualalai": (19.692, -155.870), "Mount St. Helens": (46.200, -122.188),
    "Mount Rainier": (46.853, -121.760), "Mount Hood": (45.374, -121.695),
    "Mount Shasta": (41.409, -122.193), "Lassen": (40.488, -121.505),
    "Long Valley": (37.700, -118.872), "Yellowstone": (44.428, -110.588),
    "Mount Baker": (48.777, -121.813), "Glacier Peak": (48.112, -121.114),
    "Three Sisters": (44.103, -121.768), "Newberry": (43.722, -121.229),
    "Crater Lake": (42.944, -122.108), "Medicine Lake": (41.611, -121.554),
}


def _epoch(s) -> float | None:
    if isinstance(s, (int, float)):
        return float(s)
    if not s:
        return None
    try:
        dt = datetime.strptime(str(s), "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


class VolcanoIngest:
    """Fetches elevated-status volcanoes; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        try:
            rows = self._client.get(_ELEVATED).json()
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"fetch failed: {e}")
            return []
        out: list[dict] = []
        for r in rows if isinstance(rows, list) else []:
            name = r.get("volcano_name") or ""
            coords = VOLCANOES.get(name)
            if not coords:
                continue                       # unmapped volcano; skip rather than guess
            color = (r.get("color_code") or "").upper()
            threat = _COLOR.get(color, 0.4)
            ts = _epoch(r.get("sent_unixtime") or r.get("sent_utc"))
            out.append({
                "call_id": f"volcano:{r.get('vnum') or name}",
                "metro": "volcano", "type": "weather",
                "summary": (f"Volcano {name}: {r.get('alert_level','')} / "
                            f"{color} ({r.get('obs_abbr','').upper()})").strip()[:280],
                "location": name, "source": "volcano",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": color == "RED",
                "lat": coords[0], "lon": coords[1],
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
