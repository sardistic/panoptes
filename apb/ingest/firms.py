"""NASA FIRMS active-fire detections — per-pixel wildfire signal (free key).

VIIRS thermal-anomaly pixels are near-real-time and far finer than EONET's
event-level wildfire markers, so they pin an active fire front to a point. Requires a
free FIRMS MAP_KEY (https://firms.modaps.eosdis.nasa.gov/api/map_key/); opt-in via the
FIRMS_MAP_KEY env var, mirroring the ADS-B/Bluesky opt-in pattern, so the lean core
never depends on it.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="firms"; see apb.ingest.cad.load_firms.
"""
from __future__ import annotations

import csv
import io
import os
from datetime import datetime, timezone

import httpx

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
# CSV area endpoint: {key}/{source}/{west,south,east,north}/{days}. CONUS bbox, 1 day.
_API = ("https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
        "{key}/VIIRS_SNPP_NRT/-125,24,-66,50/1")

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]
_CONF = {"l": 0.45, "n": 0.6, "h": 0.8}     # VIIRS confidence class -> base threat


def map_key() -> str | None:
    return os.environ.get("FIRMS_MAP_KEY")


def _ts(date: str, hhmm: str) -> float | None:
    try:                                # acq_date "YYYY-MM-DD", acq_time "HHMM" UTC
        h, m = int(hhmm[:-2] or 0), int(hhmm[-2:])
        dt = datetime.strptime(date, "%Y-%m-%d").replace(
            hour=h, minute=m, tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, IndexError):
        return None


class FirmsIngest:
    """Fetches CONUS VIIRS active-fire pixels; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=30.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        key = map_key()
        if not key:
            return []
        try:
            r = self._client.get(_API.format(key=key))
            r.raise_for_status()
        except httpx.HTTPError as e:
            print(f"[firms] fetch failed: {e}")
            return []
        out: list[dict] = []
        for row in csv.DictReader(io.StringIO(r.text)):
            try:
                lat, lon = float(row["latitude"]), float(row["longitude"])
            except (KeyError, ValueError):
                continue
            conf = _CONF.get(str(row.get("confidence", "")).lower(), 0.5)
            try:                        # fire radiative power nudges threat up
                conf = min(0.95, conf + min(0.15, float(row.get("frp", 0)) / 200.0))
            except ValueError:
                pass
            ts = _ts(row.get("acq_date", ""), str(row.get("acq_time", "")))
            out.append({
                "call_id": f"firms:{row.get('acq_date')}:{lat:.4f}:{lon:.4f}",
                "metro": "firms", "type": "fire",
                "summary": f"Active fire pixel (VIIRS, FRP {row.get('frp','?')})",
                "location": None, "source": "firms",
                "sentiment": _SENTIMENT[min(4, int(conf * 5))],
                "threat_score": round(conf, 2), "emerging": conf >= 0.9,
                "lat": lat, "lon": lon,
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
