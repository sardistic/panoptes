"""Safecast bGeigie drive surveys — approved drive logs, read from their raw files (CC0).

The import list (api.safecast.org/bgeigie_imports.json, 25 per page) names each uploaded
log and links its raw file on S3. The measurements API pages 25 rows at a time, far too
slow for ~25k readings a day, so logs are read directly:

  probe(url)   the first few KB (HTTP Range) -> the first GPS fix, to decide whether a
               log is worth downloading (backfill keeps North America only)
  fetch(url)   the whole log (median ~150 KB)
  parse(text)  every "$xxRDD" sentence (bGeigie Nano $BNRDD, other bGeigie-format devices
               such as $CZRDD) with a valid reading AND a valid GPS fix

Sentence: $BNRDD,device,ISO time,CPM,counts/5s,total,rad valid(A/V),DDMM.mmmm,N/S,
DDDMM.mmmm,E/W,altitude,GPS valid(A/V),HDOP/sats,...*checksum. CPM -> µSv/h with 334
(LND 7317, the bGeigie tube); other device types are converted the same way and keep
their sentence kind so they can be told apart.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime

import httpx

log = logging.getLogger(__name__)

LIST = "https://api.safecast.org/bgeigie_imports.json"
FACTOR = 334.0
_RDD = re.compile(r"^\$([A-Z]{2})RDD,")
_client = httpx.Client(timeout=60.0, follow_redirects=True,
                       headers={"User-Agent": "apb/0.1 (panoptes.run; public radiation map)"})


def page(n: int, newest_first: bool = False) -> list[dict]:
    params = {"page": n}
    if newest_first:
        params["order"] = "created_at desc"
    r = _client.get(LIST, params=params)
    r.raise_for_status()
    d = r.json()
    return d if isinstance(d, list) else []


def _deg(v: str, hemi: str) -> float:
    x = float(v)
    d = int(x / 100)
    out = d + (x - d * 100) / 60.0
    return -out if hemi in ("S", "W") else out


def _ts(s: str) -> float | None:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def parse(text: str) -> list[dict]:
    out = []
    for line in text.splitlines():
        m = _RDD.match(line)
        if not m:
            continue
        p = line.split(",")
        if len(p) < 13 or p[6] != "A" or p[12][:1] != "A":
            continue
        try:
            cpm = float(p[3])
            lat, lon = _deg(p[7], p[8]), _deg(p[9], p[10])
        except ValueError:
            continue
        ts = _ts(p[2])
        if ts is None or not (0 < cpm < 1_000_000) or not (-90 <= lat <= 90 and -180 <= lon <= 180) \
                or (abs(lat) < 0.01 and abs(lon) < 0.01):
            continue
        out.append({"ts": ts, "lat": lat, "lon": lon, "cpm": cpm, "usvh": cpm / FACTOR,
                    "kind": m.group(1), "device": p[1]})
    return out


def probe(url: str, nbytes: int = 8192) -> tuple[float, float] | None:
    """The first GPS-valid point in the first `nbytes` of a log (twice as far on a miss)."""
    for n in (nbytes, nbytes * 4):
        r = _client.get(url, headers={"Range": f"bytes=0-{n - 1}"})
        if r.status_code not in (200, 206):
            r.raise_for_status()
        lines = r.text.splitlines()
        pts = parse("\n".join(lines[:-1] if r.status_code == 206 else lines))   # last line may be cut
        if pts:
            return pts[0]["lat"], pts[0]["lon"]
        if r.status_code == 200:
            return None
    return None


def fetch(url: str) -> str:
    r = _client.get(url)
    r.raise_for_status()
    return r.text


def region(lat: float, lon: float) -> str:
    if 14 < lat < 72 and -170 < lon < -50:
        return "north_america"
    if 24 < lat < 46 and 122 < lon < 146:
        return "japan"
    if 34 < lat < 72 and -25 < lon < 45:
        return "europe"
    return "other"
