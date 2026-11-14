"""Shodan discovery sweep — find more of the services APB already ingests.

Shodan is used here as a search engine for software that publishes on purpose:
scanner-audio web apps, police-to-citizen portals, 511 traveller sites, open-data
servers. It is NOT used to find cameras or devices exposed by accident; nothing here
connects to a result — the output is a candidate list for a human to review and then
feed to the existing vendor discoverers (p2c_discover, southern_discover, camera_sniff).

    python -m apb.discover.shodan_sweep                 # counts only (no query credits)
    python -m apb.discover.shodan_sweep --search        # fetch hosts (1 credit / query page)
    python -m apb.discover.shodan_sweep --only rdio,p2c --search --country US

Needs SHODAN_API_KEY. A free key can run the counts; `--search` with filters needs
query credits (the paid membership). Results go to data/shodan_candidates.json.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import httpx

_API = "https://api.shodan.io"

# label -> (query, what a hit is, which tool takes it next)
QUERIES: dict[str, tuple[str, str, str]] = {
    "rdio": ('http.title:"Rdio Scanner"',
             "Rdio Scanner web app (public scanner audio + talkgroup metadata)",
             "radio activity lane — check each instance is meant to be public (no access code)"),
    "trunk": ('http.html:"trunk-recorder"',
              "trunk-recorder status / call pages",
              "radio activity lane"),
    "openmhz": ('http.html:"openmhz"',
                "self-hosted OpenMHz-style call archives",
                "radio activity lane"),
    "trunking": ('http.title:"Trunking Recorder"',
                 "Trunking Recorder (Windows) web call players",
                 "radio activity lane"),
    "openwebrx": ('http.title:"OpenWebRX"',
                  "OpenWebRX public SDR receivers (some cover VHF/UHF public safety bands)",
                  "radio activity lane — receiver sites to tune, not dispatch audio by themselves"),
    "icecast": ('product:"Icecast" http.html:"scanner"',
                "Icecast servers with scanner mounts",
                "radio activity lane"),
    "p2c": ('http.title:"Police to Citizen"',
            "CentralSquare Police-to-Citizen portals",
            "apb.discover.p2c_discover"),
    "southern": ('http.html:"Southern Software" http.html:"Citizen Connect"',
                 "Southern Software Citizen Connect CAD portals",
                 "apb.discover.southern_discover"),
    "traveliq": ('http.html:"/map/mapIcons/Cameras"',
                 "Carmanah / TravelIQ 511 sites (camera list route)",
                 "apb.discover.camera_sniff / cameras.TRAVELIQ"),
    "activecad": ('http.title:"Active 911" OR http.title:"Active Calls" http.html:"dispatch"',
                  "agency 'active calls' pages",
                  "apb.discover.vendor_dork"),
    "arcgis": ('http.title:"ArcGIS REST Services Directory" http.html:"FeatureServer"',
               "public ArcGIS servers",
               "apb.discover.arcgis_sweep"),
}


def _get(client: httpx.Client, path: str, key: str, **params) -> dict:
    r = client.get(f"{_API}{path}", params={"key": key, **params})
    if r.status_code in (401, 403):
        raise PermissionError(r.json().get("error", r.text)[:200] if r.content else str(r.status_code))
    r.raise_for_status()
    return r.json()


def slim(match: dict) -> dict:
    """The fields worth keeping from one Shodan banner."""
    loc = match.get("location") or {}
    http = match.get("http") or {}
    return {
        "ip": match.get("ip_str"), "port": match.get("port"),
        "hostnames": (match.get("hostnames") or [])[:4],
        "org": match.get("org"), "title": (http.get("title") or "")[:120],
        "city": loc.get("city"), "region": loc.get("region_code"),
        "country": loc.get("country_code"),
        "lat": loc.get("latitude"), "lon": loc.get("longitude"),
        "ssl": bool(match.get("ssl")), "seen": match.get("timestamp"),
    }


def sweep(key: str, labels: list[str], search: bool, country: str | None,
          pages: int = 1, client: httpx.Client | None = None) -> dict:
    own = client is None
    client = client or httpx.Client(timeout=30.0, headers={"User-Agent": "apb-discover/0.1"})
    out: dict = {"generated_at": int(time.time()), "queries": {}}
    try:
        try:
            info = _get(client, "/api-info", key)
            out["plan"] = {"plan": info.get("plan"), "query_credits": info.get("query_credits")}
        except PermissionError as e:
            out["plan"] = {"error": str(e)}
        for label in labels:
            query, what, nxt = QUERIES[label]
            q = f"{query} country:{country}" if country else query
            entry: dict = {"query": q, "what": what, "next": nxt}
            try:
                count = _get(client, "/shodan/host/count", key, query=q, facets="country:8,org:8")
                entry["total"] = count.get("total")
                entry["facets"] = {k: [(f["value"], f["count"]) for f in v]
                                   for k, v in (count.get("facets") or {}).items()}
            except (PermissionError, httpx.HTTPError) as e:
                entry["count_error"] = str(e)[:200]
            if search:
                hosts: dict[str, dict] = {}
                try:
                    for page in range(1, pages + 1):
                        res = _get(client, "/shodan/host/search", key, query=q, page=page, minify="false")
                        for m in res.get("matches") or []:
                            row = slim(m)
                            hosts[f"{row['ip']}:{row['port']}"] = row
                        if len(res.get("matches") or []) < 100:
                            break
                        time.sleep(1.1)                       # Shodan allows ~1 request/second
                    entry["hosts"] = list(hosts.values())
                except (PermissionError, httpx.HTTPError) as e:
                    entry["search_error"] = str(e)[:200]
            out["queries"][label] = entry
            time.sleep(1.1)
    finally:
        if own:
            client.close()
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--only", help="comma-separated labels: " + ",".join(QUERIES))
    ap.add_argument("--search", action="store_true", help="fetch hosts (uses query credits)")
    ap.add_argument("--country", help="two-letter country filter, e.g. US")
    ap.add_argument("--pages", type=int, default=1, help="result pages per query (100 hosts each)")
    ap.add_argument("--out", default="data/shodan_candidates.json")
    args = ap.parse_args(argv)
    key = os.environ.get("SHODAN_API_KEY", "").strip()
    if not key:
        print("SHODAN_API_KEY is not set", file=sys.stderr)
        return 2
    labels = [x.strip() for x in (args.only or ",".join(QUERIES)).split(",") if x.strip()]
    unknown = [x for x in labels if x not in QUERIES]
    if unknown:
        print(f"unknown label(s): {', '.join(unknown)}", file=sys.stderr)
        return 2
    result = sweep(key, labels, args.search, args.country, max(1, args.pages))
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=1), encoding="utf-8")
    print(f"plan: {result['plan']}")
    for label, e in result["queries"].items():
        note = e.get("count_error") or e.get("search_error") or ""
        print(f"{label:10} total={e.get('total')} hosts={len(e.get('hosts', []))} {note}")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
