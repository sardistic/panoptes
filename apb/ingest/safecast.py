"""Safecast real-time radiation sensors (tt.safecast.org) — keyless, CC0.

One request returns every real-time device Safecast knows (~2,100 worldwide, a few
hundred reporting on any day): position, capture time and counts per minute from one
or more Geiger tubes. CPM becomes ambient dose rate (µSv/h) with Safecast's per-tube
calibration. Readings older than MAX_AGE are dropped; devices at 0,0 are skipped.

Conditional GET (ETag / Last-Modified) keeps an unchanged poll nearly free.
"""
from __future__ import annotations

import logging
from datetime import datetime

import httpx

log = logging.getLogger(__name__)

URL = "https://tt.safecast.org/devices"
_UA = {"User-Agent": "apb/0.1 (panoptes.run; public radiation map)"}
MAX_AGE = 6 * 3600.0

# CPM per µSv/h (Safecast's conversion factors). Preferred tube first.
TUBES = (("lnd_7318u", 334.0), ("lnd_7318c", 334.0), ("lnd_7128ec", 108.0), ("lnd_712u", 108.0))

_client = httpx.Client(timeout=40.0, headers=_UA, follow_redirects=True)
_cond: dict[str, str] = {}
_last: list[dict] = []


def _ts(s) -> float | None:
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def normalize(raw: list, now: float) -> list[dict]:
    out = []
    for d in raw if isinstance(raw, list) else []:
        if not isinstance(d, dict):
            continue
        lat, lon, ts = d.get("loc_lat"), d.get("loc_lon"), _ts(d.get("when_captured"))
        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)) or ts is None:
            continue
        if (abs(lat) < 0.01 and abs(lon) < 0.01) or abs(lat) > 90 or abs(lon) > 180 or now - ts > MAX_AGE:
            continue
        for field, factor in TUBES:
            cpm = d.get(field)
            if isinstance(cpm, (int, float)) and 0 < cpm < 100_000:
                out.append({"device": str(d.get("device_urn") or d.get("device") or "")[:80],
                            "lat": float(lat), "lon": float(lon), "ts": ts, "cpm": float(cpm),
                            "tube": field[4:], "usvh": round(cpm / factor, 4)})
                break
    return [r for r in out if r["device"]]


def fetch(now: float) -> tuple[list[dict], bool]:
    """(fresh readings, changed). On 304 the previous readings come back with changed=False."""
    global _last
    r = _client.get(URL, headers={k: v for k, v in _cond.items() if v})
    if r.status_code == 304:
        return _last, False
    r.raise_for_status()
    _cond["If-None-Match"] = r.headers.get("etag", "")
    _cond["If-Modified-Since"] = r.headers.get("last-modified", "")
    _last = normalize(r.json(), now)
    return _last, True
