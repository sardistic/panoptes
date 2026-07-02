"""Discover PulsePoint agencies by enumerating the (dense, sequential) EMS#### id
space via the metadata endpoint, harvesting those with coordinates.

Writes data/pulsepoint_agencies.json, auto-loaded by apb.ingest.cad.load_pulsepoint().
Polite: small delay between probes, bounded range.

Usage:
  python -m apb.discover.pulsepoint_discover --resume --end 6000
  python -m apb.discover.pulsepoint_discover --directory --skip-range
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import httpx

from apb.ingest.pulsepoint import AGENCIES_URL, decrypt

META = "https://api.pulsepoint.org/v1/webapp?resource=agencies&agencyid=EMS"
TYPES = ("fire", "ems", "law")


def _first(row: dict, *names: str):
    for name in names:
        v = row.get(name)
        if v not in (None, ""):
            return v
    return None


def _normalize(row: dict, fallback_type: str | None = None) -> dict | None:
    aid = _first(row, "agencyid", "id", "AgencyID", "ID")
    lat = _first(row, "agency_latitude", "agencyLatitude", "latitude", "Latitude")
    lon = _first(row, "agency_longitude", "agencyLongitude", "longitude", "Longitude")
    if not aid or not lat or not lon:
        return None
    try:
        lat_f, lon_f = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    return {
        "agencyid": str(aid),
        "name": _first(row, "agencyname", "agency_name", "name", "Name") or str(aid),
        "city": _first(row, "city", "City"),
        "state": _first(row, "state", "State"),
        "type": _first(row, "agencytype", "agency_type", "type", "Type") or fallback_type,
        "lat": lat_f,
        "lon": lon_f,
    }


def _rows(payload) -> list[dict]:
    if isinstance(payload, dict):
        rows = payload.get("agencies", payload.get("data", []))
    else:
        rows = payload
    return rows if isinstance(rows, list) else []


def discover_directory(delay: float = 0.15, types: tuple[str, ...] = TYPES) -> list[dict]:
    """Try the public directory endpoint by agency family.

    PulsePoint currently returns status-only responses for this route in some
    environments, so this is opt-in and the EMS id probe remains the reliable path.
    """
    client = httpx.Client(timeout=20.0, headers={"User-Agent": "apb/0.1"})
    found: dict[str, dict] = {}
    for typ in types:
        try:
            data = decrypt(client.get(AGENCIES_URL + "&type=" + typ).json())
        except Exception as e:
            print(f"[pp] directory type={typ} failed: {e}")
            continue
        if isinstance(data, dict) and data.get("StatusCode"):
            print(f"[pp] directory type={typ}: {data.get('StatusCode')} "
                  f"{data.get('StatusMessage', '')}".strip())
            continue
        n = 0
        for row in _rows(data):
            agency = _normalize(row, typ)
            if not agency:
                continue
            found[agency["agencyid"]] = agency
            n += 1
        print(f"[pp] directory type={typ}: {n} agencies")
        time.sleep(delay)
    return list(found.values())


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
            agency = _normalize(rows[0], "ems")
            if agency:
                agency["agencyid"] = aid
                found.append(agency)
        if (n - start) % 200 == 0:
            print(f"[pp] probed EMS{n}, found {len(found)} so far")
        time.sleep(delay)
    return found


def _sane_us(a: dict) -> bool:
    return bool(a["lat"]) and -170 < a["lon"] < -60 and 15 < a["lat"] < 72


def _resume_start(path: Path) -> int:
    if not path.exists():
        return 1
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return 1
    nums = []
    for a in rows:
        aid = str(a.get("agencyid", ""))
        if aid.startswith("EMS") and aid[3:].isdigit():
            nums.append(int(aid[3:]))
    return max(nums, default=0) + 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--end", type=int, default=2700)
    ap.add_argument("--delay", type=float, default=0.05)
    ap.add_argument("--out", default="data/pulsepoint_agencies.json")
    ap.add_argument("--directory", action="store_true",
                    help="also try PulsePoint's agency-directory endpoint")
    ap.add_argument("--skip-range", action="store_true",
                    help="only use the optional agency-directory endpoint")
    ap.add_argument("--resume", action="store_true",
                    help="start at one past the highest EMS id already in --out")
    args = ap.parse_args()

    out = Path(args.out)
    if args.resume:
        args.start = max(args.start, _resume_start(out))

    agencies: list[dict] = []
    if args.directory:
        agencies.extend(discover_directory())
    if not args.skip_range:
        agencies.extend(discover(args.start, args.end, args.delay))
    # keep US agencies with sane coords
    agencies = [a for a in agencies if _sane_us(a)]
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
