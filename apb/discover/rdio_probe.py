"""One polite look at each Rdio Scanner candidate: is it open, and what does it cover?

For every target in data/rdio_candidates.json this opens the instance's websocket,
sends VER and CFG once, and records either the system/talkgroup inventory (open) or
that it answered PIN (access code required — recorded and never retried, no codes are
ever tried). No audio is requested. Four targets at a time, one attempt each.

    python -m apb.discover.rdio_probe --proxy socks5h://warp-egress:1080

`--proxy` routes every connection through a SOCKS proxy (needs `python-socks`); run it
that way so the probe does not originate from the home address. Results are written
to data/rdio_instances.json.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import Counter
from pathlib import Path

UA = "apb-discover/0.1 (+https://panoptes.run; public-safety map; one config request)"


def targets(cands: dict) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []

    def add(host: str, port: int | None, place: str | None, dc: bool) -> None:
        key = f"{host}:{port or 443}"
        if key in seen:
            return
        seen.add(key)
        out.append({"host": host, "port": port, "place": place, "dc": dc})

    for name in cands.get("named", []):
        host, _, port = name.partition(":")
        add(host, int(port) if port else None, None, False)
    for h in cands.get("hosts", []) + cands.get("multi_instance_operators", []):
        for port in h.get("ports", []):
            add(h["ip"], port, h.get("place"), bool(h.get("dc")))
    return out


def summarize(cfg: dict) -> dict:
    systems = []
    tags: Counter = Counter()
    for s in cfg.get("systems") or []:
        tgs = s.get("talkgroups") or []
        groups = Counter(str(t.get("group") or "") for t in tgs)
        for t in tgs:
            tags[str(t.get("tag") or "")] += 1
        systems.append({"id": s.get("id"), "label": str(s.get("label") or "")[:80],
                        "talkgroups": len(tgs),
                        "groups": [g for g, _ in groups.most_common(8) if g and g != "Unknown"]})
    return {"systems": systems, "talkgroups": sum(s["talkgroups"] for s in systems),
            "tags": dict(tags.most_common(10))}


async def probe(t: dict, proxy: str | None) -> dict:
    import websockets
    out = {k: t[k] for k in ("host", "port", "place", "dc")}
    port = t["port"]
    schemes = ("wss", "ws") if port in (None, 443, 8443) else ("ws", "wss")
    for scheme in schemes:
        url = f"{scheme}://{t['host']}" + (f":{port}" if port else "") + "/"
        kw = {"open_timeout": 12, "max_size": 16_000_000, "user_agent_header": UA}
        if proxy:
            kw["proxy"] = proxy
        if scheme == "wss" and t["host"].replace(".", "").isdigit():
            import ssl
            ctx = ssl.create_default_context()          # bare IPs have no matching certificate
            ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE
            kw["ssl"] = ctx
        try:
            async with websockets.connect(url, **kw) as ws:
                await ws.send(json.dumps(["VER"]))
                await ws.send(json.dumps(["CFG"]))
                out["scheme"] = scheme
                try:
                    for _ in range(5):
                        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=8))
                        cmd = msg[0] if isinstance(msg, list) and msg else None
                        body = msg[1] if isinstance(msg, list) and len(msg) > 1 else None
                        if cmd == "VER" and isinstance(body, dict):
                            out["version"] = body.get("version")
                            if body.get("branding"):
                                out["branding"] = str(body["branding"])[:80]
                        elif cmd == "PIN":
                            out["state"] = "access_code"
                            return out
                        elif cmd == "CFG" and isinstance(body, dict):
                            out["state"] = "open"
                            if body.get("branding"):
                                out["branding"] = str(body["branding"])[:80]
                            out.update(summarize(body))
                            return out
                except (asyncio.TimeoutError, ValueError):
                    pass
                out["state"] = "no_config"
                return out
        except Exception as e:
            out[f"{scheme}_error"] = f"{type(e).__name__}: {e}"[:120]
    out["state"] = "unreachable"
    return out


async def run(cands: dict, proxy: str | None, parallel: int = 4) -> list[dict]:
    sem = asyncio.Semaphore(parallel)

    async def one(t):
        async with sem:
            r = await probe(t, proxy)
            await asyncio.sleep(0.5)
            return r
    return await asyncio.gather(*(one(t) for t in targets(cands)))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--candidates", default="data/rdio_candidates.json")
    ap.add_argument("--out", default="data/rdio_instances.json")
    ap.add_argument("--proxy", help="SOCKS proxy URL, e.g. socks5h://warp-egress:1080")
    args = ap.parse_args(argv)
    cands = json.loads(Path(args.candidates).read_text(encoding="utf-8"))
    results = asyncio.run(run(cands, args.proxy))
    states = Counter(r["state"] for r in results)
    Path(args.out).write_text(json.dumps({
        "probed_at": int(time.time()), "via_proxy": bool(args.proxy),
        "states": dict(states), "instances": results}, indent=1), encoding="utf-8")
    print(dict(states))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
