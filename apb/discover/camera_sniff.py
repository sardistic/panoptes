"""Camera-feed sniffer — find the hidden endpoint behind any traffic-camera map.

Most DOT / 511 sites draw their cameras from an internal JSON endpoint the page
calls after load (often gated by a Referer or a session, rarely documented). This
tool opens each seed URL in headless Chromium, records every JSON/XML response,
clicks anything labelled like a camera layer, then looks through the captured
payloads for the *shape* of a camera list: an array of >= 15 records that carry a
latitude/longitude pair and an image/stream URL. The best candidate per site is
validated by fetching one still, and written as a generic parser spec that
`apb.ingest.cameras` loads at runtime (`data/camera_discoveries.json`).

    python -m apb.discover.camera_sniff                 # all seeds
    python -m apb.discover.camera_sniff --only tx,mi    # some
    python -m apb.discover.camera_sniff --url https://example.gov/map --key xx

Build-time only (needs `playwright` + Chromium); prod just reads the JSON.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import httpx

OUT = Path("data/camera_discoveries.json")
DUMP = False
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"

# state/region key -> map page(s) to open. Only places not already covered keyless.
SEEDS: dict[str, list[str]] = {
    "tx": ["https://drivetexas.org/", "https://its.txdot.gov/ITS_WEB/FrontEnd/default.html?r=SAT",
           "https://traffic.houstontranstar.org/layers/"],
    "mi": ["https://mdotjboss.state.mi.us/MiDrive/map"],
    "nc": ["https://drivenc.gov/"],
    "va": ["https://www.511virginia.org/"],
    "mo": ["https://traveler.modot.org/map/"],
    "ks": ["https://www.kandrive.gov/"],
    "ne": ["https://511.nebraska.gov/"],
    "ok": ["https://oktraffic.org/"],
    "ar": ["https://www.idrivearkansas.com/"],
    "ms": ["https://www.mdottraffic.com/"],
    "nm": ["https://www.nmroads.com/"],
    "mt": ["https://www.mdt.mt.gov/travinfo/map/"],
    "wy": ["https://map.wyoroad.info/"],
    "nd": ["https://travel.dot.nd.gov/"],
    "sd": ["https://sd511.org/"],
    "ia": ["https://511ia.org/"],
    "mn": ["https://511mn.org/"],
    "in": ["https://511in.org/"],
    "ky": ["https://goky.ky.gov/"],
    "tn": ["https://smartway.tn.gov/traffic"],
    "sc": ["https://www.511sc.org/"],
    "wv": ["https://www.wv511.org/"],
    "co": ["https://www.cotrip.org/"],
    "nj": ["https://www.511nj.org/"],
    "ma": ["https://mass511.com/"],
    "hi": ["https://goakamai.org/"],
    "bc": ["https://www.drivebc.ca/"],
    "qc": ["https://www.quebec511.info/en/Carte/Default.aspx"],
    "mb": ["https://www.manitoba511.ca/"],
    "ab": ["https://511.alberta.ca/"],
    "uk": ["https://www.trafficengland.com/", "https://trafficscotland.org/", "https://traffic.wales/"],
    "au": ["https://www.livetraffic.com/", "https://qldtraffic.qld.gov.au/", "https://traffic.vicroads.vic.gov.au/"],
    "no": ["https://www.vegvesen.no/trafikkinformasjon/reiseinformasjon/webkamera/"],
}

# Where to zoom for each seed (a busy metro): tile-loaded maps only fetch cameras
# once you are close enough. lat, lon.
FOCUS: dict[str, tuple[float, float]] = {
    "tx": (29.76, -95.37), "mi": (42.33, -83.05), "nc": (35.78, -78.64), "va": (37.54, -77.44),
    "mo": (38.63, -90.20), "ks": (37.69, -97.34), "ne": (41.26, -95.94), "ok": (35.47, -97.52),
    "ar": (34.75, -92.29), "ms": (32.30, -90.18), "nm": (35.08, -106.65), "mt": (46.59, -112.02),
    "wy": (41.14, -104.82), "nd": (46.81, -100.78), "sd": (43.54, -96.73), "ia": (41.59, -93.62),
    "mn": (44.98, -93.27), "in": (39.77, -86.16), "ky": (38.25, -85.76), "tn": (36.16, -86.78),
    "sc": (34.00, -81.03), "wv": (38.35, -81.63), "co": (39.74, -104.99), "nj": (40.73, -74.17),
    "ma": (42.36, -71.06), "hi": (21.31, -157.86), "bc": (49.28, -123.12), "qc": (45.50, -73.57),
    "mb": (49.90, -97.14), "ab": (51.05, -114.07), "uk": (51.51, -0.13), "au": (-33.87, 151.21),
    "no": (59.91, 10.75),
}

_LAT = re.compile(r"^(lat|latitude|y|lat_deg|ycoord|latdd)$", re.I)
_LON = re.compile(r"^(lon|lng|long|longitude|x|lon_deg|xcoord|londd)$", re.I)
_IMG = re.compile(r"\.(jpe?g|png|gif|webp)(\?|$)|/image|snapshot|cctv|camera|\.m3u8|/stream|/rtplive/", re.I)
_ICON = re.compile(r"\.svg|/icons?/|/images/tg_|marker|sprite", re.I)
_URLISH = re.compile(r"^(https?:)?//|^/", re.I)


def _walk_arrays(obj, path="$", depth=0):
    """Yield (path, list) for every list of dicts in a JSON document."""
    if depth > 6:
        return
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            yield path, obj
        for i, v in enumerate(obj[:3]):
            yield from _walk_arrays(v, f"{path}[{i}]", depth + 1)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk_arrays(v, f"{path}.{k}", depth + 1)


def _find_keys(rec: dict, prefix=""):
    """Flatten one record to {dotted.key: value} two levels deep."""
    out = {}
    for k, v in rec.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict) and len(prefix) < 40:
            out.update(_find_keys(v, key + "."))
        elif isinstance(v, list) and v and isinstance(v[0], dict) and len(prefix) < 40:
            out.update(_find_keys(v[0], key + "[0]."))
        else:
            out[key] = v
    return out


def _numeric(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def score_list(rows: list[dict]) -> dict | None:
    """Does this list look like cameras? Return the field mapping if so."""
    if len(rows) < 8:
        return None
    sample = rows[: min(40, len(rows))]
    flat = [_find_keys(r) for r in sample if isinstance(r, dict)]
    keys = set().union(*[f.keys() for f in flat])
    lat_k = lon_k = img_k = name_k = id_k = None
    for k in keys:
        leaf = k.rsplit(".", 1)[-1]
        vals = [f.get(k) for f in flat]
        nums = [_numeric(v) for v in vals]
        if _LAT.match(leaf) and sum(1 for n in nums if n is not None and -90 <= n <= 90) > len(flat) * .7:
            lat_k = lat_k or k
        if _LON.match(leaf) and sum(1 for n in nums if n is not None and -180 <= n <= 180) > len(flat) * .7:
            lon_k = lon_k or k
        strs = [v for v in vals if isinstance(v, str)]
        if strs and sum(1 for v in strs if _IMG.search(v) and not _ICON.search(v)) > len(flat) * .5 \
                and (img_k is None or "url" in k.lower()):
            img_k = k
        if leaf.lower() in ("name", "title", "description", "location", "locationname", "label", "desc") and strs:
            name_k = name_k or k
        if leaf.lower() in ("id", "cameraid", "camera_id", "camid", "key", "objectid", "uid"):
            id_k = id_k or k
    if not (lat_k and lon_k and img_k):
        return None
    return {"lat": lat_k, "lon": lon_k, "image": img_k, "name": name_k or img_k, "id": id_k or name_k or img_k,
            "count": len(rows)}


def _get(d, dotted):
    cur = d
    for part in re.split(r"\.(?![^\[]*\])", dotted):
        m = re.match(r"([^\[]+)(?:\[(\d+)\])?$", part)
        if not m or not isinstance(cur, dict):
            return None
        cur = cur.get(m.group(1))
        if m.group(2) is not None and isinstance(cur, list):
            cur = cur[int(m.group(2))] if cur else None
    return cur


def _items(doc, path):
    cur = doc
    for part in path.split(".")[1:]:
        m = re.match(r"([^\[]+)(?:\[(\d+)\])?$", part)
        cur = cur.get(m.group(1)) if isinstance(cur, dict) else None
        if m.group(2) is not None and isinstance(cur, list):
            cur = cur[int(m.group(2))]
    return cur if isinstance(cur, list) else []


async def sniff(key: str, urls: list[str], timeout_s: float = 45.0) -> dict | None:
    from playwright.async_api import async_playwright
    captured: list[dict] = []
    captured_cookies = ""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=_UA, viewport={"width": 1400, "height": 900})
        page = await ctx.new_page()

        async def on_response(resp):
            try:
                ct = resp.headers.get("content-type", "")
                if not any(t in ct for t in ("json", "javascript", "xml", "text/plain")):
                    return
                if resp.status != 200:
                    return
                body = await resp.body()
                if not (2000 < len(body) < 60_000_000):
                    return
                captured.append({"url": resp.url, "ct": ct, "body": body, "req_headers": resp.request.headers,
                                 "method": resp.request.method, "post_data": resp.request.post_data})
            except Exception:
                pass
        page.on("response", on_response)
        for url in urls:
            try:
                await page.goto(url, wait_until="load", timeout=int(timeout_s * 1000))
                await page.wait_for_timeout(6000)
                # 1. open anything that looks like a layers / legend / menu panel
                for sel in ("text=/^layers?$/i", "text=/map layers/i", "text=/legend/i", "text=/filters?/i",
                            "[aria-label*='ayer']", "[title*='ayer']", "button:has-text('Menu')",
                            "[aria-label*='enu']"):
                    try:
                        loc = page.locator(sel)
                        for i in range(min(await loc.count(), 2)):
                            await loc.nth(i).click(timeout=1200, force=True)
                            await page.wait_for_timeout(800)
                    except Exception:
                        pass
                # 2. poke anything that looks like a camera layer toggle
                for sel in ("text=/camera/i", "text=/cctv/i", "text=/webcam/i", "[title*='amera']",
                            "[aria-label*='amera']", "label:has-text('Camera')", "input[type=checkbox]"):
                    try:
                        loc = page.locator(sel)
                        for i in range(min(await loc.count(), 5)):
                            await loc.nth(i).click(timeout=1200, force=True)
                            await page.wait_for_timeout(900)
                    except Exception:
                        pass
                await page.wait_for_timeout(3000)
                # 3. zoom into the focus city: tile-loaded maps only fetch cameras up close.
                #    Try the common map libraries' JS handles first, then wheel-zoom.
                fx = FOCUS.get(key)
                if fx:
                    await page.evaluate("""([lat,lon])=>{
                      const tryAll=()=>{
                        for(const k of Object.keys(window)){
                          const o=window[k]; if(!o||typeof o!=='object') continue;
                          try{
                            if(o.setView&&o.getZoom){ o.setView([lat,lon],12); return 'leaflet:'+k; }
                            if(o.jumpTo&&o.getZoom){ o.jumpTo({center:[lon,lat],zoom:11}); return 'maplibre:'+k; }
                            if(o.setCenter&&o.setZoom&&o.getBounds){ o.setCenter({lat,lng:lon}); o.setZoom(12); return 'gmaps:'+k; }
                            if(o.view&&o.view.goTo){ o.view.goTo({center:[lon,lat],zoom:12}); return 'arcgis:'+k; }
                          }catch(_){}
                        }
                        return null;
                      };
                      return tryAll();
                    }""", [fx[0], fx[1]])
                    box = await page.evaluate("(()=>{const m=document.querySelector('.leaflet-container,.maplibregl-map,.mapboxgl-map,.esri-view,#map,.map,[class*=map]');if(!m) return null;const r=m.getBoundingClientRect();return [r.left+r.width/2,r.top+r.height/2];})()")
                    if box:
                        for _ in range(6):
                            await page.mouse.move(box[0], box[1])
                            await page.mouse.wheel(0, -600)
                            await page.wait_for_timeout(700)
                        for dx, dy in ((300, 0), (-600, 0), (300, 250), (0, -500)):
                            await page.mouse.move(box[0], box[1]); await page.mouse.down()
                            await page.mouse.move(box[0] + dx, box[1] + dy, steps=8); await page.mouse.up()
                            await page.wait_for_timeout(1500)
                await page.wait_for_timeout(4000)
                cookies = await ctx.cookies()
                captured_cookies = "; ".join(f"{c['name']}={c['value']}" for c in cookies if c.get("name"))
            except Exception as e:
                print(f"  [{key}] {url}: {type(e).__name__}", file=sys.stderr)
        await browser.close()

    if DUMP:
        Path("data/sniff_captures").mkdir(exist_ok=True)
        for i, cap in enumerate(captured):
            print(f"    cap {len(cap['body']):>8}B {cap['method']} {cap['url'][:140]}", file=sys.stderr)
            if len(cap["body"]) > 20000 and "googleapis" not in cap["url"] and not cap["url"].endswith(".js"):
                hdr = f"{cap['method']} {cap['url']}" + chr(10) + (cap["post_data"] or "") + chr(10) * 2
                Path(f"data/sniff_captures/{key}_{i}.txt").write_bytes(hdr.encode() + cap["body"])
    best = None
    for cap in captured:
        text = cap["body"].decode("utf-8", "replace")
        doc = None
        try:
            doc = json.loads(text)
        except ValueError:
            m = re.search(r"[\[{]", text)                 # JSONP / JS-wrapped
            if m:
                try:
                    doc = json.loads(text[m.start(): text.rindex("}") + 1] if text.rstrip().endswith((")", ";")) else text[m.start():])
                except ValueError:
                    doc = None
        if doc is None:
            continue
        for path, rows in _walk_arrays(doc):
            sc = score_list(rows)
            if sc and (best is None or sc["count"] > best["fields"]["count"]):
                best = {"url": cap["url"], "items_path": path, "fields": sc, "method": cap["method"],
                        "post_data": cap["post_data"], "referer": cap["req_headers"].get("referer"),
                        "headers": cap["req_headers"], "sample": rows[0]}
    if not best:
        return None
    # validate one still, resolving relative image URLs against the endpoint
    img = _get(best["sample"], best["fields"]["image"])
    img_url = urljoin(best["url"], str(img)) if img else None
    ok, kind = False, "still"
    if img_url:
        try:
            r = httpx.get(img_url, headers={"User-Agent": _UA, "Referer": best.get("referer") or urls[0],
                                            "Cookie": captured_cookies[:4000]},
                          timeout=20, follow_redirects=True)
            ct = r.headers.get("content-type", "")
            if ".m3u8" in img_url or "mpegurl" in ct:
                ok, kind = r.status_code == 200 and r.text.startswith("#EXTM3U"), "hls"
            else:
                ok = r.status_code == 200 and ct.startswith("image/") and len(r.content) > 3000
        except httpx.HTTPError:
            ok = False
    replay_ok = False
    try:
        hdr = {"User-Agent": _UA, "Referer": best.get("referer") or urls[0], "Cookie": captured_cookies[:4000],
               **{k: v for k, v in (best.get("headers") or {}).items()
                  if k.lower() in ("accept", "x-requested-with", "authorization", "origin", "content-type")}}
        if best.get("method") == "POST":
            rr = httpx.post(best["url"], content=best.get("post_data") or "", headers=hdr, timeout=25)
        else:
            rr = httpx.get(best["url"], headers=hdr, timeout=25, follow_redirects=True)
        replay_ok = rr.status_code == 200 and len(rr.content) > 1000 and (
            best["fields"]["count"] and str(best["sample"].get(next(iter(best["sample"]))))[:20] in rr.text
            or rr.text.count("{") > 8)
    except httpx.HTTPError:
        replay_ok = False
    return {"key": key, "endpoint": best["url"], "items_path": best["items_path"], "fields": best["fields"],
            "replay_verified": replay_ok,
            "method": best.get("method", "GET"), "post_data": best.get("post_data"), "media": kind,
            "cookies": captured_cookies[:4000],
            "headers": {k: v for k, v in (best.get("headers") or {}).items()
                        if k.lower() in ("accept", "x-requested-with", "authorization", "origin", "content-type")},
            "referer": best.get("referer") or urls[0], "sample_image": img_url, "image_verified": ok,
            "token_in_url": bool(re.search(r"(key|token|apikey|sig)=", best["url"], re.I)),
            "found_at": time.strftime("%Y-%m-%d")}


async def main_async(only: list[str] | None, extra: tuple[str, str] | None):
    seeds = dict(SEEDS)
    if extra:
        seeds = {extra[1]: [extra[0]]}
    elif only:
        seeds = {k: v for k, v in seeds.items() if k in only}
    existing = json.loads(OUT.read_text()) if OUT.exists() else {}
    sem = asyncio.Semaphore(3)

    async def one(k, urls):
        async with sem:
            print(f"[{k}] sniffing {urls[0]}", file=sys.stderr)
            try:
                res = await asyncio.wait_for(sniff(k, urls), timeout=150)
            except Exception as e:
                res = None
                print(f"  [{k}] failed: {type(e).__name__}: {e}", file=sys.stderr)
            if res:
                print(f"  [{k}] FOUND {res['fields']['count']} records at {res['endpoint'][:90]} "
                      f"image_verified={res['image_verified']}", file=sys.stderr)
                existing[k] = {**res, "enabled": res["image_verified"] and res["replay_verified"],
                               "label": existing.get(k, {}).get("label"), "state": existing.get(k, {}).get("state") or k.upper()}
                print(f"      replay_verified={res['replay_verified']}", file=sys.stderr)
            else:
                print(f"  [{k}] nothing camera-shaped captured", file=sys.stderr)
    await asyncio.gather(*(one(k, v) for k, v in seeds.items()))
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(existing, indent=1, default=str), encoding="utf-8")
    found = [k for k, v in existing.items() if v.get("enabled")]
    print(f"\n{len(found)} verified sources in {OUT}: {', '.join(found)}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated seed keys")
    ap.add_argument("--url"); ap.add_argument("--key")
    ap.add_argument("--dump", action="store_true", help="list every captured response")
    a = ap.parse_args()
    global DUMP
    DUMP = a.dump
    asyncio.run(main_async(a.only.split(",") if a.only else None, (a.url, a.key) if a.url else None))


if __name__ == "__main__":
    main()
