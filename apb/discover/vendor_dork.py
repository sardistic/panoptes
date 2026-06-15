"""Vendor-agnostic source discovery by web-search dorking.

Generalizes the old P2C-only google_dork into a registry: each Vendor declares the
dorks that surface its public CAD/incident endpoints, how to pull the agency
host/endpoint out of a result URL, and how to validate that the endpoint actually
serves live geocoded data before we keep it. Results merge into the same catalogs
apb.ingest.cad auto-loads, so a discovered feed goes live with no further wiring.

Backend is apb.discover.websearch.search (Google CSE if keys are set, else keyless
DuckDuckGo) — so this runs with no API keys.

Adding a vendor = one Vendor entry below. Two are seeded because they validate against
ingest we already have:
  - p2c     : more PoliceToCitizen (CentralSquare) police agencies
  - arcgis  : agency-hosted ArcGIS FeatureServers NOT in the Hub index (hub_sweep
              only sees hub.arcgis.com; dorking finds self-hosted rest/services)

Usage:
  python -m apb.discover.vendor_dork --list
  python -m apb.discover.vendor_dork --vendor p2c
  python -m apb.discover.vendor_dork --all
"""
from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import httpx

from apb.discover.websearch import backend, search

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")


@dataclass
class Vendor:
    name: str
    dorks: list[str]
    # result URL -> stable candidate key (host/endpoint), or None to skip
    extract: Callable[[str], str | None]
    # candidate key -> catalog record (any JSON shape), or None if it doesn't validate
    validate: Callable[[str], object | None]
    catalog: str
    # record -> identity for catalog dedup (default: the record itself, e.g. a string)
    key: Callable[[object], object] = field(default=lambda r: r)


# ── P2C (PoliceToCitizen / CentralSquare) ─────────────────────────────────────
_P2C_RE = re.compile(r"https?://([a-z0-9-]+)\.policetocitizen\.com", re.I)
_P2C_DORKS = [
    # keyword forms outperform inurl: on Google CSE (and still work on DDG)
    "site:policetocitizen.com CADCalls",
    "site:policetocitizen.com CAD calls police department",
    "site:policetocitizen.com CAD calls sheriff office",
    "site:policetocitizen.com \"calls for service\"",
    "site:policetocitizen.com active calls dispatch",
    "site:policetocitizen.com CADCalls city police Texas",
    "site:policetocitizen.com CADCalls county sheriff Florida",
    "site:policetocitizen.com CADCalls Carolina OR Georgia OR Ohio",
    "site:policetocitizen.com \"recent calls\" OR \"current calls\"",
    "policetocitizen.com CADCalls police",
    "policetocitizen.com CADCalls sheriff county",
]
_p2c_client = None


def _p2c_extract(url: str) -> str | None:
    m = _P2C_RE.match(url.lower())
    if not m or m.group(1) in ("www", "api"):
        return None
    return m.group(1)


def _p2c_validate(sub: str):
    global _p2c_client
    if _p2c_client is None:
        from apb.ingest.p2c import P2C
        _p2c_client = P2C()
    try:
        return sub if _p2c_client.incidents(sub) else None
    except Exception:
        return None


# ── ArcGIS FeatureServer (self-hosted, beyond the Hub index) ──────────────────
_ARC_RE = re.compile(r"(https?://[^\s\"'<>]+?/(?:Feature|Map)Server)(/\d+)?", re.I)
_ARC_DORKS = [
    "inurl:FeatureServer \"calls for service\"",
    "inurl:FeatureServer police dispatch incidents",
    "inurl:FeatureServer active calls police",
    "inurl:FeatureServer sheriff calls for service",
    "inurl:FeatureServer fire incidents active",
    "inurl:MapServer \"calls for service\"",
    "inurl:rest/services calls for service police",
    "inurl:rest/services fire incidents active",
    "inurl:arcgis/rest/services police calls for service",
    "\"FeatureServer/0\" calls for service dispatch -site:hub.arcgis.com",
]
_arc_client = None


def _arc_extract(url: str) -> str | None:
    m = _ARC_RE.search(url)
    if not m:
        return None
    return (m.group(1) + (m.group(2) or "")).split("?")[0]


def _arc_validate(url: str):
    global _arc_client
    if _arc_client is None:
        _arc_client = httpx.Client(timeout=25.0, headers={"User-Agent": _UA})
    from apb.discover.hub_sweep import _probe
    probe = _probe(_arc_client, url)
    if not probe:
        return None
    # name from the layer's own metadata; fall back to a slug of the URL
    name = None
    try:
        meta = _arc_client.get(probe["layer_url"], params={"f": "json"}).json()
        name = meta.get("name")
    except (httpx.HTTPError, ValueError):
        pass
    if not name:
        name = re.sub(r"^https?://|/(Feature|Map)Server.*$", "", url)[:60]
    return {"name": name, "url": probe["layer_url"], "geocoded": True,
            "time_field": probe["time_field"]}


# ── Southern Software "Citizen Connect" (multi-tenant CAD, AgencyID-keyed) ────
_SS_RE = re.compile(r"cc\.southernsoftware\.com/.*?AgencyID=([A-Za-z0-9_]+)", re.I)
_SS_DORKS = [
    "site:cc.southernsoftware.com CADCFS_Public",
    "site:cc.southernsoftware.com \"calls for service\"",
    "site:cc.southernsoftware.com AgencyID police OR sheriff",
    "cc.southernsoftware.com CADCFS_Public AgencyID",
]
_ss_client = None


def _ss_extract(url: str) -> str | None:
    m = _SS_RE.search(url)
    return m.group(1) if m else None


def _ss_validate(aid: str):
    global _ss_client
    if _ss_client is None:
        from apb.ingest.southern_software import SouthernSoftware
        _ss_client = SouthernSoftware()
    try:
        return aid if _ss_client.incidents(aid) else None
    except Exception:
        return None


VENDORS: dict[str, Vendor] = {
    "p2c": Vendor("p2c", _P2C_DORKS, _p2c_extract, _p2c_validate,
                  "data/p2c_agencies.json"),
    "arcgis": Vendor("arcgis", _ARC_DORKS, _arc_extract, _arc_validate,
                     "data/arcgis_catalog.json", key=lambda r: r["url"]),
    "southern": Vendor("southern", _SS_DORKS, _ss_extract, _ss_validate,
                       "data/southern_agencies.json"),
}


def harvest(v: Vendor, max_per_dork: int = 30, delay: float = 0.4) -> list:
    """Dork -> extract candidates -> validate -> list of catalog records."""
    cands: set[str] = set()
    for d in v.dorks:
        for url in search(d, max_results=max_per_dork):
            c = v.extract(url)
            if c:
                cands.add(c)
        print(f"[{v.name}] '{d[:44]}' -> {len(cands)} distinct candidates so far")
    print(f"[{v.name}] validating {len(cands)} candidates...")
    records = []
    for c in sorted(cands):
        rec = v.validate(c)
        if rec is not None:
            records.append(rec)
            print(f"  ✓ {c}")
        time.sleep(delay)
    return records


def merge(v: Vendor, records: list) -> tuple[int, int]:
    """Merge records into the vendor catalog (dedup by v.key). Returns (added, total)."""
    out = Path(v.catalog)
    existing = json.loads(out.read_text(encoding="utf-8")) if out.exists() else []
    by_key = {v.key(r): r for r in existing}
    added = 0
    for r in records:
        if v.key(r) not in by_key:
            by_key[v.key(r)] = r
            added += 1
    merged = list(by_key.values())
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(merged, indent=1), encoding="utf-8")
    return added, len(merged)


def run(name: str, max_per_dork: int):
    v = VENDORS[name]
    records = harvest(v, max_per_dork)
    added, total = merge(v, records)
    print(f"\n[{name}] +{added} new, {total} total -> {v.catalog}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vendor", choices=list(VENDORS))
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--max", type=int, default=30, help="max results per dork")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()

    if args.list:
        print(f"search backend: {backend()}")
        for n, v in VENDORS.items():
            print(f"  {n:8} {len(v.dorks)} dorks -> {v.catalog}")
        return
    targets = list(VENDORS) if args.all else ([args.vendor] if args.vendor else [])
    if not targets:
        ap.error("pass --vendor NAME, --all, or --list")
    print(f"[vendor_dork] backend={backend()}")
    for n in targets:
        run(n, args.max)


if __name__ == "__main__":
    main()
