"""ArcGIS Hub dataset sweep — the broadest source-discovery pass.

The ArcGIS Hub v3 datasets API (hub.arcgis.com/api/v3/datasets) indexes far more
open-data Feature Layers than the arcgis.com sharing search. We page through it,
keep emergency Feature Layers, and PROBE each for (a) point geometry and (b) a recent
timestamp — so giant archival sets ("911 Calls 2024", millions of rows) are dropped.

Merges results into data/arcgis_catalog.json (dedup by layer url), which
apb.ingest.cad.load_arcgis_catalog() auto-loads.

Usage: python -m apb.discover.hub_sweep
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import httpx

HUB = "https://hub.arcgis.com/api/v3/datasets"
TERMS = [
    "calls for service", "police calls for service", "active police calls",
    "911 calls", "cad calls", "police incidents", "fire incidents",
    "crime incidents", "police dispatch", "fire dispatch", "ems incidents",
    "sheriff calls", "active dispatch", "real time crime", "shots fired",
    "police activity", "current incidents", "emergency calls",
    "law enforcement calls", "service calls", "incident reports", "crime data",
    "police service calls", "fire department calls", "ems calls",
    "dispatch log", "call log", "crime mapping", "police reports",
    "arrests", "citations", "traffic crashes", "accident reports",
    "fire ems incidents", "public safety", "crime incidents recent",
    # Canada-flavoured phrasings (hub.arcgis.com is global)
    "police occurrences", "calls for service ontario", "police incidents canada",
    "appels de service police", "rcmp calls for service", "service de police incidents",
]
INCLUDE = ("police", "fire", "crime", "dispatch", "incident", "911", "calls",
           "sheriff", "ems", "emergency", "shots")
EXCLUDE = ("permit", "parcel", "zoning", "boundary", "hydrant", "station",
           "district", "beat", "precinct", "school", "camera", "phone", "box",
           "wildfire", "neighborhood", "summary", "statistic", "yearly", "annual",
           "monthly", "dashboard", "heat map", "heatmap", "density")
_TIME_HINT = re.compile(r"(date|time|received|reported|created|occur|call)", re.I)
_PAST_YEARS = tuple(str(y) for y in range(2001, time.gmtime().tm_year))
MAX_AGE_DAYS = 21.0


def _archival(name: str) -> bool:
    n = name.lower()
    return (any(w in n for w in ("legacy", "archive", "historical", " old", "to present"))
            or any(y in name for y in _PAST_YEARS))


def _layer_url(url: str) -> str:
    url = url.rstrip("/")
    return url if re.search(r"/\d+$", url) else url + "/0"


def _probe(client: httpx.Client, url: str) -> dict | None:
    base = _layer_url(url)
    q = base + "/query"
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
    tfield = next((k for k in props if _TIME_HINT.search(k)), None)
    if not tfield:
        return None
    # freshness: newest record by that field
    try:
        r2 = client.get(q, params={"where": "1=1", "outFields": tfield, "f": "json",
                                   "resultRecordCount": 1,
                                   "orderByFields": f"{tfield} DESC"})
        val = r2.json().get("features", [{}])[0].get("attributes", {}).get(tfield)
    except (httpx.HTTPError, ValueError, IndexError):
        return None
    ts = _parse(val)
    if ts is None or (time.time() - ts) > MAX_AGE_DAYS * 86400:
        return None
    return {"layer_url": base, "time_field": tfield, "fresh_days": round((time.time()-ts)/86400, 1)}


def _parse(v):
    if v in (None, ""):
        return None
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
        n = float(v)
        return n / 1000.0 if n > 1e12 else n
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except ValueError:
        return None


def sweep() -> list[dict]:
    client = httpx.Client(timeout=30.0, headers={"User-Agent": "apb-discover/0.1"})
    seen: dict[str, dict] = {}
    for term in TERMS:
        data = []
        for page in range(1, 4):     # paginate deeper: up to 3 pages × 100 per term
            try:
                r = client.get(HUB, params={"q": term, "filter[type]": "Feature Layer",
                                            "page[size]": 100, "page[number]": page})
                r.raise_for_status()
                batch = r.json().get("data", [])
            except httpx.HTTPError as e:
                print(f"[hub] '{term}' p{page} failed: {e}")
                break
            data.extend(batch)
            if len(batch) < 100:
                break
            time.sleep(0.2)
        print(f"[hub] '{term}': {len(data)} datasets")
        for x in data:
            a = x.get("attributes", {})
            name, url = (a.get("name") or "").strip(), a.get("url")
            nl = name.lower()
            if not url or url in seen:
                continue
            if not any(w in nl for w in INCLUDE) or any(w in nl for w in EXCLUDE):
                continue
            if _archival(name):
                continue
            probe = _probe(client, url)
            if not probe:
                continue
            seen[url] = {"name": name, "url": probe["layer_url"], "geocoded": True,
                         "time_field": probe["time_field"]}
            print(f"  ✓ {name[:48]:48} fresh={probe['fresh_days']}d")
            time.sleep(0.1)
        time.sleep(0.25)
    return list(seen.values())


def main():
    cands = sweep()
    out = Path("data/arcgis_catalog.json")
    existing = json.loads(out.read_text(encoding="utf-8")) if out.exists() else []
    by_url = {c["url"]: c for c in existing}
    added = 0
    for c in cands:
        if c["url"] not in by_url:
            by_url[c["url"]] = c
            added += 1
    out.write_text(json.dumps(list(by_url.values()), indent=2), encoding="utf-8")
    print(f"\n[hub] {len(cands)} hub feeds probed OK, +{added} new "
          f"-> {len(by_url)} total in {out}")


if __name__ == "__main__":
    main()
