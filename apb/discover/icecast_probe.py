"""Public-safety mounts on Icecast servers found by Shodan (outside the Xiph directory).

For each Icecast host in a shodan_sweep output (labels icecast_*), this reads the
server's public status listing `/status-json.xsl` once (http, then https), keeps MP3
mounts whose name / description / genre say police, fire, EMS, dispatch or scanner (and
not music, ham, weather, test), and writes them in the streams lane's list format to
state/streams_extra.json, which the lane merges with the directory list. Run through the
egress proxy:

    python -m apb.discover.icecast_probe data/shodan_more2.json --proxy socks5h://warp-egress:1080 \\
        --out state/streams_extra.json
"""
from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

import httpx

UA = "apb-discover/0.1 (+https://panoptes.run; public-safety map; one status request)"


def hosts(shodan: dict) -> list[tuple[str, int]]:
    out, seen = [], set()
    for label, q in (shodan.get("queries") or {}).items():
        if not label.startswith("icecast"):
            continue
        for h in q.get("hosts") or []:
            k = (h["ip"], h["port"])
            if k not in seen:
                seen.add(k)
                out.append(k)
    return out


def mounts(status: dict) -> list[dict]:
    src = ((status or {}).get("icestats") or {}).get("source") or []
    return [src] if isinstance(src, dict) else [s for s in src if isinstance(s, dict)]


def wanted(m: dict) -> bool:
    from apb.ingest.streams import _SAFETY, _SKIP
    name = f"{m.get('server_name') or ''} {m.get('server_description') or ''} {m.get('listenurl') or ''}"
    genre = str(m.get("genre") or "")
    text = f"{name} {genre}"
    mp3 = "mpeg" in str(m.get("server_type") or m.get("content-type") or "audio/mpeg")
    return bool(mp3 and _SAFETY.search(text) and not _SKIP.search(text)
                and not re.search(r"music|rock|jazz|hits|\bpop\b|talk radio|christian|country", genre, re.I))


def probe(ip: str, port: int, client: httpx.Client) -> list[dict]:
    from apb.ingest.streams import key_of, tag_for
    for scheme in ("http", "https"):
        base = f"{scheme}://{ip}:{port}"
        try:
            r = client.get(f"{base}/status-json.xsl")
            data = r.json() if r.status_code == 200 else None
        except (httpx.HTTPError, ValueError):
            continue
        if not data:
            continue
        out = []
        for m in mounts(data):
            if not wanted(m):
                continue
            path = re.sub(r"^https?://[^/]+", "", str(m.get("listenurl") or ""))
            if not path.startswith("/"):
                continue
            url = base + path                          # the reachable host, not the server's own name
            name = str(m.get("server_name") or m.get("server_description") or path.strip("/"))[:80]
            try:
                br = int(m.get("bitrate") or 0)
            except (TypeError, ValueError):
                br = 0
            out.append({"key": key_of(url), "name": re.sub(r"\s+", " ", name), "genre": str(m.get("genre") or "")[:60],
                        "url": url, "bitrate": br, "tag": tag_for(name), "via": "shodan"})
        return out
    return []


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("shodan", nargs="+")
    ap.add_argument("--proxy")
    ap.add_argument("--out", default="state/streams_extra.json")
    args = ap.parse_args(argv)
    hs = []
    for f in args.shodan:
        hs += hosts(json.loads(Path(f).read_text(encoding="utf-8")))
    found: dict[str, dict] = {}
    with httpx.Client(timeout=12.0, headers={"User-Agent": UA}, proxy=args.proxy, verify=False,
                      follow_redirects=True) as c:
        for ip, port in hs:
            for m in probe(ip, port, c):
                found.setdefault(m["name"].lower(), m)
    rows = list(found.values())
    tmp = Path(args.out + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps({"fetched": time.time(), "streams": rows}, indent=1), encoding="utf-8")
    tmp.replace(args.out)
    print(f"{len(hs)} servers -> {len(rows)} public-safety mounts -> {args.out}")
    for r in rows:
        print("  ", r["name"][:60], "|", r["genre"][:30], "|", r["bitrate"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
