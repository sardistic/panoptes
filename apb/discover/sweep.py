"""Source discovery sweep — find publicly available emergency-dispatch data feeds
across the US, the polite way: query official open-data catalog APIs, not raw crawls.

Primary catalog: the Socrata Discovery API (api.us.socrata.com), which indexes every
Socrata open-data portal nationwide. We search emergency-dispatch terms, dedupe, and
score each dataset for whether it's GEOCODED and LIVE (recently updated) — i.e. usable
as a CAD feed in apb/ingest/cad.py.

Output: data/sources_catalog.json — ranked candidates + ready-to-wire CadFeed hints.

Usage:
  python -m apb.discover.sweep                 # sweep + write catalog
  python -m apb.discover.sweep --min-score 6   # only strong candidates
"""
from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

DISCOVERY = "https://api.us.socrata.com/api/catalog/v1"

# Search terms that surface CAD / calls-for-service / dispatch datasets. Freshness
# scoring filters archival/statistical hits, so breadth here is cheap — more call
# categories + agency-type phrasings catch portals the generic terms miss.
TERMS = [
    "911 dispatch", "computer aided dispatch", "calls for service",
    "fire incidents", "police incidents", "real time fire", "emergency dispatch",
    "police calls", "fire dispatch", "crime incidents", "ems response",
    "police dispatch", "fire department calls", "active calls", "cad calls",
    "law incidents", "sheriff calls", "incident dispatch", "service calls",
    "fire ems", "police activity", "crime data", "dispatched calls",
    # specific high-signal call categories
    "shots fired", "shotspotter", "gunfire", "shooting incidents", "officer involved",
    "use of force", "traffic crashes", "vehicle collisions", "traffic accidents",
    "arrests", "citations", "field interviews", "overdose", "narcotics arrests",
    # agency / record-type phrasings
    "police department calls", "sheriff dispatch", "fire rescue incidents",
    "emergency medical services", "dispatch log", "call log", "incident log",
    "police reports", "crime reports", "daily incidents", "current incidents",
    "public safety incidents", "law enforcement calls", "response times",
]

# Socrata datatypes / field-name hints that indicate a mappable point.
_GEO_TYPES = {"point", "location", "multipoint"}
_GEO_HINTS = ("latitude", "longitude", "lat", "lon", "lng", "point", "geocoded",
              "x_coord", "y_coord", "location_1")
_TYPE_HINTS = ("type", "call_type", "category", "description", "nature",
               "final_call", "event", "offense", "incident_type")
_TIME_TYPES = {"calendar date", "floating timestamp", "date"}


def _first(cols: list[str], dtypes: list[str], hint_names, hint_types=None) -> str | None:
    for name, dt in zip(cols, dtypes):
        n, d = name.lower(), dt.lower()
        if hint_types and d in hint_types:
            return name
        if any(h in n for h in hint_names):
            return name
    return None


def _geo_fields(cols, fields, dtypes):
    """Return (lat_field, lon_field) or (point_field, None) or (None, None)."""
    low = [f.lower() for f in fields]
    # explicit lat/lon pair
    lat = next((f for f, l in zip(fields, low) if l in ("latitude", "lat", "y")), None)
    lon = next((f for f, l in zip(fields, low) if l in ("longitude", "lon", "lng", "x")), None)
    if lat and lon:
        return lat, lon
    # a single point/location column
    for f, dt in zip(fields, dtypes):
        if dt.lower() in _GEO_TYPES:
            return f, None
    for f, l in zip(fields, low):
        if any(h in l for h in _GEO_HINTS):
            return f, None
    return None, None


def _days_since(iso: str | None) -> float | None:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 86400
    except ValueError:
        return None


def _score(geo_ok: bool, fresh_days: float | None, has_type: bool,
           has_time: bool, views: int) -> float:
    s = 0.0
    s += 3 if geo_ok else 0
    if fresh_days is not None:
        s += 3 if fresh_days <= 7 else 1 if fresh_days <= 90 else 0
    s += 1 if has_type else 0
    s += 1 if has_time else 0
    s += min(2.0, math.log10(views + 1) / 2)  # popularity nudge
    return round(s, 2)


def sweep(terms=TERMS, per_term=200, page=100) -> list[dict]:
    client = httpx.Client(timeout=30.0, headers={"User-Agent": "apb-discover/0.1"})
    seen: dict[tuple[str, str], dict] = {}

    for term in terms:
        results: list[dict] = []
        for offset in range(0, per_term, page):  # page through the discovery API
            try:
                resp = client.get(DISCOVERY, params={
                    "q": term, "only": "dataset", "limit": page, "offset": offset})
                resp.raise_for_status()
                batch = resp.json().get("results", [])
            except httpx.HTTPError as e:
                print(f"[sweep] '{term}' offset {offset} failed: {e}")
                break
            results.extend(batch)
            if len(batch) < page:
                break
            time.sleep(0.2)
        print(f"[sweep] '{term}': {len(results)} datasets")

        for r in results:
            res = r["resource"]
            domain = r["metadata"]["domain"]
            key = (domain, res["id"])
            if key in seen:
                seen[key]["matched_terms"].append(term)
                continue

            cols = res.get("columns_name", []) or []
            fields = res.get("columns_field_name", []) or []
            dtypes = res.get("columns_datatype", []) or []

            lat_f, lon_f = _geo_fields(cols, fields, dtypes)
            geo_ok = lat_f is not None
            type_f = _first(fields, dtypes, _TYPE_HINTS)
            time_f = _first(fields, dtypes, (), _TIME_TYPES)
            fresh = _days_since(res.get("data_updated_at"))
            views = int(res.get("page_views", {}).get("page_views_total", 0)
                        if isinstance(res.get("page_views"), dict) else 0)

            seen[key] = {
                "name": res["name"], "domain": domain, "id": res["id"],
                "url": f"https://{domain}/resource/{res['id']}.json",
                "permalink": r.get("permalink"),
                "lat_field": lat_f, "lon_field": lon_f,
                "type_field": type_f, "time_field": time_f,
                "geocoded": geo_ok, "fresh_days": round(fresh, 1) if fresh else None,
                "page_views": views, "matched_terms": [term],
                "score": _score(geo_ok, fresh, bool(type_f), bool(time_f), views),
            }
        time.sleep(0.3)  # be polite to the discovery API

    return sorted(seen.values(), key=lambda c: c["score"], reverse=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-score", type=float, default=0.0)
    ap.add_argument("--out", default="data/sources_catalog.json")
    args = ap.parse_args()

    candidates = [c for c in sweep() if c["score"] >= args.min_score]
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(candidates, indent=2), encoding="utf-8")

    live = [c for c in candidates if c["geocoded"]
            and c["fresh_days"] is not None and c["fresh_days"] <= 7]
    print(f"\n[sweep] {len(candidates)} candidates -> {out}")
    print(f"[sweep] {len(live)} are GEOCODED + updated within 7 days (live-ready):\n")
    for c in live[:25]:
        print(f"  {c['score']:>4}  {c['name'][:42]:42}  {c['domain']:28}  "
              f"{c['fresh_days']}d  geo={c['lat_field']}/{c['lon_field']}")


if __name__ == "__main__":
    main()
