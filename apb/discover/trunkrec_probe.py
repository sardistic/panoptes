"""One polite look at each Trunking Recorder candidate: is its call list public?

For each Shodan hit (data/shodan_*.json, query label "trunking") this POSTs one
DataTables request for a single call to `/calljson` (https first, then http) and asks
`/systemsjson` for the system names. An instance that answers 401 needs a login: it is
recorded as `closed` and never retried; no credentials are ever tried. Run it through the
egress proxy so it does not originate from the home address:

    python -m apb.discover.trunkrec_probe data/shodan_more.json --proxy socks5h://warp-egress:1080 \\
        --out state/trunkrec_instances.json
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path

import httpx

UA = "apb-discover/0.1 (+https://panoptes.run; public-safety map; one list request)"
_DC = ("amazon", "google", "microsoft", "digitalocean", "linode", "akamai", "ovh", "hetzner",
       "vultr", "oracle", "cloudflare")


def targets(shodan: dict, label: str = "trunking") -> list[dict]:
    out, seen = [], set()
    for h in (shodan.get("queries", {}).get(label) or {}).get("hosts", []):
        names = [n for n in h.get("hostnames") or [] if not n.replace(".", "").isdigit()]
        host = names[0] if names and any(w in names[0] for w in ("listen", "scan", "radio")) else h["ip"]
        k = (host, h["port"])
        if k in seen:
            continue
        seen.add(k)
        place = ", ".join(x for x in (h.get("city"), h.get("region")) if x) or None
        out.append({"host": host, "port": h["port"], "place": place,
                    "dc": any(w in (h.get("org") or "").lower() for w in _DC)})
    return out


def probe(t: dict, client: httpx.Client) -> dict:
    body = json.dumps({"draw": 1, "start": 0, "length": 1, "order": [{"column": 0, "dir": "desc"}],
                       "columns": [{"data": "CallTime"}], "search": {"value": ""}, "SmartSort": False})
    for scheme in ("https", "http"):
        default = 443 if scheme == "https" else 80
        base = f"{scheme}://{t['host']}" + ("" if t["port"] == default else f":{t['port']}")
        try:
            r = client.post(f"{base}/calljson", content=body,
                            headers={"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"})
        except httpx.HTTPError:
            continue
        if r.status_code == 401:
            return {**t, "base": base, "state": "closed"}
        try:
            d = r.json()
        except ValueError:
            continue
        if r.status_code == 200 and "recordsTotal" in d:
            systems = []
            try:
                s = client.post(f"{base}/systemsjson", content="{}", headers={"Content-Type": "application/json"}).json()
                systems = [x.get("id") for x in s.get("results") or [] if x.get("id")]
            except (httpx.HTTPError, ValueError):
                pass
            return {**t, "base": base, "state": "open", "calls": d.get("recordsTotal"), "systems": systems[:20]}
    return {**t, "state": "unreachable"}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("shodan", nargs="+", help="shodan_sweep output files")
    ap.add_argument("--proxy", help="SOCKS proxy, e.g. socks5h://warp-egress:1080")
    ap.add_argument("--out", default="state/trunkrec_instances.json")
    args = ap.parse_args(argv)
    ts = []
    for f in args.shodan:
        ts += targets(json.loads(Path(f).read_text(encoding="utf-8")))
    with httpx.Client(timeout=15.0, headers={"User-Agent": UA}, proxy=args.proxy, verify=False,
                      follow_redirects=True) as c:
        results = [probe(t, c) for t in ts]
    try:
        old = {r["base"] if r.get("base") else f"{r['host']}:{r['port']}": r
               for r in json.loads(Path(args.out).read_text(encoding="utf-8")).get("instances", [])}
    except (OSError, ValueError):
        old = {}
    for r in results:
        old[r.get("base") or f"{r['host']}:{r['port']}"] = r
    rows = list(old.values())
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(args.out + ".tmp")
    tmp.write_text(json.dumps({"probed_at": int(time.time()), "via_proxy": bool(args.proxy),
                               "states": dict(Counter(r["state"] for r in rows)), "instances": rows}, indent=1),
                   encoding="utf-8")
    tmp.replace(args.out)
    print(dict(Counter(r["state"] for r in results)), "->", args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
