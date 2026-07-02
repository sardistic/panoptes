"""FAA airport delays & closures — keyless ground-disruption signal.

The FAA NAS status feed reports active ground-delay programs, ground stops, arrival/
departure delays, and airport closures with a plain-English reason ("thunderstorms",
"volume", "snow/ice"). It's an aviation-side mirror of surface disruption and ties
weather to its operational impact. Keyless XML; airports geolocated via a curated
coordinate table of the busy fields that actually appear in the feed.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="faa_delay"; see apb.ingest.cad.load_faa_delays.
"""
from __future__ import annotations

from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_URL = "https://nasstatus.faa.gov/api/airport-status-information"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

# Delay-type Name -> (threat, short label).
_TYPE = {
    "Airport Closures": (0.7, "Airport closure"),
    "Ground Stop Programs": (0.65, "Ground stop"),
    "Ground Delay Programs": (0.5, "Ground delay"),
    "General Arrival/Departure Delay Info": (0.4, "Arr/dep delay"),
}

# Curated coords for the airports that appear in the NAS status feed (IATA -> lat,lon).
AIRPORTS: dict[str, tuple[float, float]] = {
    "ATL": (33.6407, -84.4277), "LAX": (33.9416, -118.4085), "ORD": (41.9742, -87.9073),
    "DFW": (32.8998, -97.0403), "DEN": (39.8561, -104.6737), "JFK": (40.6413, -73.7781),
    "SFO": (37.6213, -122.3790), "SEA": (47.4502, -122.3088), "LAS": (36.0840, -115.1537),
    "MCO": (28.4312, -81.3081), "EWR": (40.6895, -74.1745), "MIA": (25.7959, -80.2870),
    "PHX": (33.4342, -112.0116), "IAH": (29.9902, -95.3368), "BOS": (42.3656, -71.0096),
    "MSP": (44.8848, -93.2223), "DTW": (42.2162, -83.3554), "FLL": (26.0742, -80.1506),
    "LGA": (40.7769, -73.8740), "PHL": (39.8744, -75.2424), "CLT": (35.2140, -80.9431),
    "BWI": (39.1754, -76.6683), "SLC": (40.7899, -111.9791), "DCA": (38.8512, -77.0402),
    "SAN": (32.7338, -117.1933), "IAD": (38.9531, -77.4565), "TPA": (27.9755, -82.5332),
    "PDX": (45.5898, -122.5951), "HNL": (21.3187, -157.9225), "AUS": (30.1975, -97.6664),
    "STL": (38.7487, -90.3700), "MDW": (41.7868, -87.7522), "BNA": (36.1245, -86.6782),
    "SMF": (38.6951, -121.5908), "SJC": (37.3639, -121.9289), "OAK": (37.7126, -122.2197),
    "MSY": (29.9934, -90.2580), "RDU": (35.8801, -78.7880), "SAT": (29.5337, -98.4698),
    "PIT": (40.4915, -80.2329), "CLE": (41.4117, -81.8498), "CVG": (39.0489, -84.6678),
    "IND": (39.7173, -86.2944), "CMH": (39.9980, -82.8919), "MCI": (39.2976, -94.7139),
    "SDF": (38.1744, -85.7360), "JAX": (30.4941, -81.6879), "RSW": (26.5362, -81.7552),
    "PBI": (26.6832, -80.0956), "BUF": (42.9405, -78.7322), "ABQ": (35.0402, -106.6092),
    "OKC": (35.3931, -97.6007), "OMA": (41.3032, -95.8941), "MEM": (35.0424, -89.9767),
    "RIC": (37.5052, -77.3197), "ORF": (36.8946, -76.2012), "PVD": (41.7240, -71.4282),
    "YUL": (45.4706, -73.7408), "YYZ": (43.6777, -79.6248), "YVR": (49.1967, -123.1815),
}


class FaaDelayIngest:
    """Fetches active NAS delays/closures; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        try:
            root = ET.fromstring(self._client.get(_URL).text)
        except (httpx.HTTPError, ET.ParseError) as e:
            log.warning(f"fetch failed: {e}")
            return []
        ts = datetime.now(timezone.utc).timestamp()
        out: list[dict] = []
        seen: set[str] = set()
        for dt in root.findall("Delay_type"):
            name = dt.findtext("Name") or "Delay"
            threat, label = _TYPE.get(name, (0.45, "Delay"))
            for entry in dt.iter():            # entries directly holding an ARPT code
                code = entry.findtext("ARPT") if list(entry) else None
                if not code:
                    continue
                coords = AIRPORTS.get(code.strip().upper())
                if not coords:
                    continue
                key = f"{code}:{name}"
                if key in seen:
                    continue
                seen.add(key)
                reason = (entry.findtext("Reason") or "").strip()
                out.append({
                    "call_id": f"faa:{key}", "metro": "faa_delay", "type": "traffic",
                    "summary": f"{label} {code}" + (f": {reason}" if reason else ""),
                    "location": code, "source": "faa_delay",
                    "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                    "threat_score": round(threat, 2), "emerging": False,
                    "lat": coords[0], "lon": coords[1],
                    "at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                    "ts": ts,
                })
        return out
