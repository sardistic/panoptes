"""Discover PulsePoint agencies by enumerating the (dense, sequential) EMS#### id
space via the metadata endpoint, harvesting those with coordinates.

Writes data/pulsepoint_agencies.json, auto-loaded by apb.ingest.cad.load_pulsepoint().
Polite: small delay between probes, bounded range.

Usage: python -m apb.discover.pulsepoint_discover --start 1 --end 2700
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import httpx

from apb.ingest.pulsepoint import decrypt

META = "https://api.pulsepoint.org/v1/webapp?resource=agencies&agencyid=EMS"


def discover(start: int, end: int, delay: float = 0.05) -> list[dict]:
    client = httpx.Client(timeout=12.0, headers={"User-Agent": "apb/0.1"})
    found: list[dict] = []
    for n in range(start, end):
        aid = f"EMS{n}"
        try:
            d = decrypt(client.get(META + str(n)).json())
            rows = d.get("agencies") if isinstance(d, dict) else d
        except Exception:
            rows = None
        if rows:
            a = rows[0]
            lat, lon = a.get("agency_latitude"), a.get("agency_longitude")
            if lat and lon:
                found.append({
                    "agencyid": aid, "name": a.get("agencyname") or aid,
                    "city": a.get("city"), "state": a.get("state"),
                    "type": a.get("agencytype"),
                    "lat": float(lat), "lon": float(lon),
                })
        if n % 200 == 0:
            print(f"[pp] probed EMS{n}, found {len(found)} so far")
        time.sleep(delay)
    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--end", type=int, default=2700)
    ap.add_argument("--out", default="data/pulsepoint_agencies.json")
    args = ap.parse_args()

    agencies = discover(args.start, args.end)
    # keep US agencies with sane coords
    agencies = [a for a in agencies if a["lat"] and -170 < a["lon"] < -60 and 15 < a["lat"] < 72]
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # merge with any existing catalog (dedup by agencyid) so ranges accumulate
    merged = {}
    if out.exists():
        for a in json.loads(out.read_text(encoding="utf-8")):
            merged[a["agencyid"]] = a
    new = 0
    for a in agencies:
        if a["agencyid"] not in merged:
            new += 1
        merged[a["agencyid"]] = a
    out.write_text(json.dumps(list(merged.values()), indent=2), encoding="utf-8")
    print(f"\n[pp] +{new} new ({len(agencies)} probed OK), {len(merged)} total -> {out}")


if __name__ == "__main__":
    main()
