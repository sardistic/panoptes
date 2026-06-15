"""ArcGIS discovery sweep — find emergency-dispatch FeatureServer layers with real
POINT geometry, the polite way (official ArcGIS Online search API).

Socrata covers many big cities; ArcGIS Hub covers thousands more (cities/counties on
Esri). We search, then PROBE each candidate's layer 0 for actual point geometry +
recent updates, keeping only feeds usable as-is (no geocoding needed).

Output: data/arcgis_catalog.json — auto-loaded by apb.ingest.cad.load_arcgis_catalog().

Usage: python -m apb.discover.arcgis_sweep
"""
from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

import httpx

SEARCH = "https://www.arcgis.com/sharing/rest/search"

TERMS = [
    "police calls for service", "active dispatch", "police incidents",
    "fire incidents", "crime incidents", "911 calls", "cad calls",
    "police dispatch", "fire dispatch", "calls for service",
    "law enforcement incidents", "sheriff calls", "crime reports",
    "fire department incidents", "ems calls", "emergency incidents",
    "real time crime", "current incidents", "public safety incidents",
    "shots fired", "traffic crashes", "arrests",
]
INCLUDE = ("police", "fire", "crime", "dispatch", "incident", "911", "calls",
           "sheriff", "ems", "emergency")
EXCLUDE = ("permit", "energov", "parcel", "zoning", "boundary", "hydrant",
           "station", "district", "beat", "precinct", "school", "camera",
           "address", "mark43", "wfl1", "plan", "311")
_TIME_HINT = re.compile(r"(date|time|received|reported|created|occur)", re.I)


def _search(client: httpx.Client, term: str, num=100) -> list[dict]:
    params = {"q": f'{term} type:"Feature Service"', "f": "json", "num": num,
              "sortField": "numviews", "sortOrder": "desc"}
    r = client.get(SEARCH, params=params)
    r.raise_for_status()
    return r.json().get("results", [])


def _probe(client: httpx.Client, url: str) -> dict | None:
    """Check layer 0 has point geometry; return field hints + sample if usable."""
    base = url.rstrip("/")
    q = f"{base}/0/query"
    try:
        r = client.get(q, params={"where": "1=1", "outFields": "*", "f": "geojson",
                                  "resultRecordCount": 1})
        r.raise_for_status()
        feats = r.json().get("features", [])
    except (httpx.HTTPError, ValueError):
        return None
    if not feats:
        return None
    geom = feats[0].get("geometry") or {}
    if geom.get("type") != "Point" or not geom.get("coordinates"):
        return None
    props = feats[0].get("properties") or {}
    time_field = next((k for k in props if _TIME_HINT.search(k)), None)
    lon, lat = geom["coordinates"][0], geom["coordinates"][1]
    return {"layer_url": f"{base}/0", "time_field": time_field,
            "sample_lat": lat, "sample_lon": lon, "fields": list(props)[:12]}


def sweep() -> list[dict]:
    client = httpx.Client(timeout=30.0, headers={"User-Agent": "apb-discover/0.1"})
    seen: dict[str, dict] = {}
    for term in TERMS:
        try:
            results = _search(client, term)
        except httpx.HTTPError as e:
            print(f"[arcgis] '{term}' failed: {e}")
            continue
        print(f"[arcgis] '{term}': {len(results)} items")
        for it in results:
            url = it.get("url")
            title = (it.get("title") or "").strip()
            tl = title.lower()
            if not url or url in seen:
                continue
            if not any(x in tl for x in INCLUDE) or any(x in tl for x in EXCLUDE):
                continue
            probe = _probe(client, url)
            if not probe:
                continue
            seen[url] = {
                "name": title, "owner": it.get("owner"),
                "url": probe["layer_url"], "time_field": probe["time_field"],
                "geocoded": True, "fields": probe["fields"],
                "sample": [probe["sample_lat"], probe["sample_lon"]],
            }
            print(f"  ✓ {title[:50]}  geo=({probe['sample_lat']:.3f},{probe['sample_lon']:.3f})")
            time.sleep(0.15)
        time.sleep(0.3)
    return list(seen.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/arcgis_catalog.json")
    args = ap.parse_args()
    cands = sweep()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(cands, indent=2), encoding="utf-8")
    print(f"\n[arcgis] {len(cands)} point-geometry emergency feeds -> {out}")


if __name__ == "__main__":
    main()
