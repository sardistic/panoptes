"""NDBC marine buoys — keyless high-seas / gale observation signal.

The National Data Buoy Center publishes the latest observation from every buoy as a
single fixed-width text file. Raw met readings aren't events, so we threshold: only
buoys reporting high seas (significant wave height) or gale-force winds are emitted,
giving a marine-hazard layer that complements NWS Special Marine Warnings.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="ndbc"; see apb.ingest.cad.load_ndbc.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_URL = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]
# Column order in latest_obs.txt (after STN/LAT/LON/date).
_WVHT_MIN = 4.0      # m — significant wave height (high seas)
_WSPD_MIN = 17.2     # m/s — gale force (~34 kt)


def _f(v: str) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class NdbcIngest:
    """Fetches buoys currently reporting high seas / gales; mirrors the fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=25.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        try:
            text = self._client.get(_URL).text
        except httpx.HTTPError as e:
            print(f"[ndbc] fetch failed: {e}")
            return []
        out: list[dict] = []
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            c = line.split()
            if len(c) < 13:
                continue
            stn, lat, lon = c[0], _f(c[1]), _f(c[2])
            if lat is None or lon is None:
                continue
            wspd, wvht = _f(c[9]), _f(c[11])
            if (wvht is None or wvht < _WVHT_MIN) and (wspd is None or wspd < _WSPD_MIN):
                continue
            big_sea = wvht is not None and wvht >= 6.0
            big_wind = wspd is not None and wspd >= 24.0
            threat = 0.7 if (big_sea or big_wind) else 0.5
            ts = None
            try:
                ts = datetime(int(c[3]), int(c[4]), int(c[5]), int(c[6]), int(c[7]),
                              tzinfo=timezone.utc).timestamp()
            except (ValueError, IndexError):
                pass
            parts = []
            if wvht is not None:
                parts.append(f"seas {wvht:.1f} m")
            if wspd is not None:
                parts.append(f"wind {wspd:.0f} m/s")
            out.append({
                "call_id": f"ndbc:{stn}", "metro": "ndbc", "type": "weather",
                "summary": f"Marine hazard buoy {stn}: " + ", ".join(parts),
                "location": f"Buoy {stn}", "source": "ndbc",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": False,
                "lat": lat, "lon": lon,
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
