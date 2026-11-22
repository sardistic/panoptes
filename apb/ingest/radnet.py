"""EPA RadNet fixed air monitors — hourly gamma dose rate, keyless, U.S. Government work.

About 140 monitors (data/radnet_stations.json, built by apb.discover.radnet_stations).
Each station's data is only published as a whole-year CSV (~0.7-0.8 MB, no range
requests, no compression, no ETag), so the radiation lane refreshes a few stations per
cycle in rotation rather than all of them every time. Rows: hourly "DOSE EQUIVALENT
RATE (nSv/h)" in UTC; rejected / invalid / suspect rows are dropped.
"""
from __future__ import annotations

import csv
import io
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import httpx

log = logging.getLogger(__name__)

STATIONS = Path(__file__).resolve().parents[2] / "data" / "radnet_stations.json"
CSV = "https://radnet.epa.gov/cdx-radnet-rest/api/rest/csv/{year}/fixed/{st}/{city}"
_client = httpx.Client(timeout=60.0, follow_redirects=True,
                       headers={"User-Agent": "Mozilla/5.0 (compatible; apb/0.1; panoptes.run public radiation map)"})
_stations: dict | None = None


def device_id(key: str) -> str:
    """URL-safe sensor id: "TX:EL PASO" -> "radnet:TX:EL_PASO", "NY:NYC (EML)" -> "radnet:NY:NYC_EML"."""
    import re
    return "radnet:" + re.sub(r"[^A-Za-z0-9:.]+", "_", key).strip("_")


def stations() -> dict[str, dict]:
    global _stations
    if _stations is None:
        try:
            _stations = json.loads(STATIONS.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            log.warning("radnet stations unreadable: %s", e)
            _stations = {}
    return _stations


def parse(text: str, key: str, st: dict, since: float) -> list[dict]:
    out = []
    reader = csv.reader(io.StringIO(text))
    head = next(reader, None) or []
    try:
        i_t = head.index("SAMPLE COLLECTION TIME")
        i_d = next(i for i, h in enumerate(head) if h.startswith("DOSE EQUIVALENT RATE"))
    except (ValueError, StopIteration):
        return []
    i_s = head.index("STATUS") if "STATUS" in head else None
    for r in reader:
        if len(r) <= max(i_t, i_d):
            continue
        status = (r[i_s] if i_s is not None and i_s < len(r) else "").upper()
        if any(w in status for w in ("REJECT", "INVALID", "SUSPECT")):
            continue
        try:
            ts = datetime.strptime(r[i_t], "%m/%d/%Y %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            nsv = float(r[i_d])
        except ValueError:
            continue
        if ts > since and 0 < nsv < 100_000:
            out.append({"device": device_id(key), "lat": st["lat"], "lon": st["lon"], "ts": ts,
                        "cpm": None, "tube": "radnet", "usvh": round(nsv / 1000.0, 4), "name": st["name"]})
    out.sort(key=lambda r: r["ts"])
    return out


def fetch_station(key: str, year: int, since: float) -> list[dict]:
    st = stations()[key]
    url = CSV.format(year=year, st=st["state"], city=quote(st["city"]))
    r = _client.get(url)
    r.raise_for_status()
    return parse(r.text, key, st, since)
