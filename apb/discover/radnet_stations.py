"""Build data/radnet_stations.json: EPA RadNet fixed monitors with coordinates.

RadNet's per-station CSVs carry no coordinates, only "ST: CITY". This reads the station
list from EPA's CSV download page (the current year's links), geocodes each "City, ST"
once with OpenStreetMap Nominatim (1 request/s, as its policy asks), checks the point is
in the named state (Census), and writes the result. Rerun when stations change:

    python -m apb.discover.radnet_stations
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import unquote

import httpx

PAGE = "https://www.epa.gov/radnet/radnet-csv-file-downloads"
OUT = Path(__file__).resolve().parents[2] / "data" / "radnet_stations.json"
UA = {"User-Agent": "apb-radnet-stations/1.0 (public radiation map)"}
_FIX = {"FT.": "Fort", "ST.": "Saint", "PT.": "Port", "MT.": "Mount"}
# names Nominatim cannot place
_MANUAL = {"NY:NYC (EML)": {"state": "NY", "city": "NYC (EML)", "name": "New York (EML), NY", "lat": 40.728, "lon": -74.005}}


NRT = "https://www.epa.gov/radnet/radnet-near-real-time-air-data"
CSV = "https://radnet.epa.gov/cdx-radnet-rest/api/rest/csv/{year}/fixed/{st}/{city}"
_ABBR = {"fort": "FT.", "saint": "ST.", "st": "ST.", "port": "PT.", "mount": "MT."}


def _exists(year: int, st: str, city: str) -> bool:
    try:
        return httpx.head(CSV.format(year=year, st=st, city=city), headers=_BROWSER, timeout=30).status_code == 200
    except httpx.HTTPError:
        return False


_BROWSER = {"User-Agent": "Mozilla/5.0 " + UA["User-Agent"]}


def stations(year: int) -> list[tuple[str, str]]:
    """Every fixed monitor with a CSV for `year`: the CSV download page (any year's link)
    plus the near-real-time station pages ("albany-ny"), checked against the CSV endpoint."""
    html = httpx.get(PAGE, headers=_BROWSER, timeout=40).text
    out = {(st, unquote(city).strip()) for st, city in re.findall(r"/csv/\d{4}/fixed/([A-Z]{2})/([^\"]+)\"", html)}
    nrt = httpx.get(NRT, headers=_BROWSER, timeout=40).text + html
    known = {(st, re.sub(r"[^a-z0-9]+", "-", c.lower()).strip("-")) for st, c in out}
    for slug in sorted(set(re.findall(r"radnet-near-real-time-air-data-([a-z0-9-]+)", nrt))):
        words, st = slug.rsplit("-", 1)[0].split("-"), slug.rsplit("-", 1)[-1].upper()
        if len(st) != 2 or (st, "-".join(words)) in known:
            continue
        for cand in (" ".join(words).upper(), " ".join([_ABBR.get(words[0], words[0]).upper()] + words[1:]).upper()):
            if _exists(year, st, cand):
                out.add((st, cand))
                break
        else:
            print(f"no {year} CSV for page {slug}", flush=True)
    return sorted(out)


def nice(city: str) -> str:
    return " ".join(_FIX.get(w, w.title()) for w in city.split())


def main() -> int:
    from apb.context.radio_stt import state_at
    year = time.gmtime().tm_year
    old = json.loads(OUT.read_text(encoding="utf-8")) if OUT.exists() else {}
    out, missing = {}, []
    for st, city in stations(year):
        key = f"{st}:{city}"
        if key in old or key in _MANUAL:
            out[key] = old.get(key) or _MANUAL[key]
            continue
        q = f"{nice(city)}, {st}, USA"
        r = httpx.get("https://nominatim.openstreetmap.org/search", headers=UA, timeout=30,
                      params={"q": q, "format": "json", "limit": 3, "countrycodes": "us"})
        time.sleep(1.1)
        pt = None
        for hit in r.json() if r.status_code == 200 else []:
            lat, lon = float(hit["lat"]), float(hit["lon"])
            if state_at(lat, lon) == st:
                pt = (round(lat, 3), round(lon, 3))
                break
        if pt:
            out[key] = {"state": st, "city": city, "name": f"{nice(city)}, {st}", "lat": pt[0], "lon": pt[1]}
        else:
            missing.append(q)
        print(f"{'ok ' if pt else 'MISS'} {q} {pt or ''}", flush=True)
    OUT.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"{len(out)} stations written to {OUT.name}; not placed: {missing}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
