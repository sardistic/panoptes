"""Street-level imagery for a drawn box — the static ground truth behind a look.

Three providers, sampled on a coarse grid across the box so a handful of frames
show the whole area rather than one corner:

  mapillary   crowd-sourced, dense in cities; free token (MAPILLARY_TOKEN)
  kartaview   OSM-community imagery; keyless but its API is fragile (best-effort)
  streetview  Google Street View Static API; GOOGLE_MAPS_KEY with billing. The free
              *metadata* call is made first so only points with coverage are billed
              (~$0.007 per image after the monthly credit)

Frames are handed to the explainer as image parts and shown in the bubble through
/street/{token} — a short-lived server-side token→URL map, so provider URLs (and the
Google key) never reach the browser and the CSP stays same-origin.
"""
from __future__ import annotations

import hashlib
import logging
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone

import httpx

log = logging.getLogger(__name__)
_UA = {"User-Agent": "Mozilla/5.0 (compatible; apb/0.1; +https://panoptes.run)"}
_client = httpx.Client(timeout=15.0, headers=_UA, follow_redirects=True)
_lock = threading.Lock()
_cache: dict[tuple, tuple[float, list[dict]]] = {}     # rounded bbox -> frames
_tokens: dict[str, tuple[float, str]] = {}             # token -> (at, provider url)
_bytes: dict[str, tuple[float, bytes, str]] = {}       # token -> (at, image, mime)
_TTL = 6 * 3600.0
# Street View Static is $7 per 1,000 images. A hard $10/month ceiling = 1,428 images;
# 1,400/month with a 60/day smoothing cap, counted at the fetch (the billable event).
SV_MONTHLY_CAP = int(os.environ.get("STREETVIEW_MONTHLY_CAP", "1400"))
SV_DAILY_CAP = int(os.environ.get("STREETVIEW_DAILY_CAP", "60"))
SV_UNIT_USD = 0.007


def providers() -> dict[str, bool]:
    return {"mapillary": bool(os.environ.get("MAPILLARY_TOKEN", "").strip()),
            "kartaview": True,
            "streetview": bool(os.environ.get("GOOGLE_MAPS_KEY", "").strip())}


def budget() -> dict:
    """Street View spend so far vs the caps (what /status and the bubble show)."""
    from apb.store import spend
    u = spend.used("streetview")
    return {"month_images": u["month"], "month_cap": SV_MONTHLY_CAP, "day_images": u["day"], "day_cap": SV_DAILY_CAP,
            "month_usd": round(u["month"] * SV_UNIT_USD, 2), "cap_usd": round(SV_MONTHLY_CAP * SV_UNIT_USD, 2),
            "exhausted": u["month"] >= SV_MONTHLY_CAP or u["day"] >= SV_DAILY_CAP}


def _token(url: str) -> str:
    t = hashlib.blake2b(url.encode(), digest_size=9).hexdigest()
    with _lock:
        _tokens[t] = (time.time(), url)
        if len(_tokens) > 5000:
            for k in sorted(_tokens, key=lambda k: _tokens[k][0])[:2500]:
                _tokens.pop(k, None)
    return t


def _grid(bounds: dict, cols: int = 3, rows: int = 2) -> list[tuple[float, float]]:
    s, n, w, e = (float(bounds[k]) for k in ("south", "north", "west", "east"))
    return [(s + (n - s) * (j + .5) / rows, w + (e - w) * (i + .5) / cols)
            for j in range(rows) for i in range(cols)]


def _frame(provider, url, lat, lon, when=None, heading=None, note=None) -> dict:
    return {"provider": provider, "token": _token(url), "lat": round(float(lat), 5), "lon": round(float(lon), 5),
            "captured": when, "heading": heading, "note": note}


# ── providers ─────────────────────────────────────────────────────────────────
def mapillary(bounds: dict, limit: int = 6) -> list[dict]:
    """One small query per grid cell (the Graph API rejects big or dense boxes):
    a ~250 m window around each sample point, newest frame wins."""
    tok = os.environ.get("MAPILLARY_TOKEN", "").strip()
    if not tok:
        return []
    def cell(pt):
        lat, lon = pt
        d = 0.0012                                  # ~130 m half-window: dense cities still answer
        try:
            r = _client.get("https://graph.mapillary.com/images", params={
                "access_token": tok, "fields": "id,thumb_1024_url,captured_at,compass_angle,geometry,is_pano",
                "bbox": f"{lon - d:.5f},{lat - d:.5f},{lon + d:.5f},{lat + d:.5f}", "limit": 12}, timeout=12.0)
            if r.status_code != 200:
                return None
            imgs = [im for im in r.json().get("data", []) if im.get("thumb_1024_url")]
        except (httpx.HTTPError, ValueError):
            return None
        if not imgs:
            return None
        im = max(imgs, key=lambda x: x.get("captured_at") or 0)
        c = (im.get("geometry") or {}).get("coordinates") or [lon, lat]
        when = datetime.fromtimestamp(im["captured_at"] / 1000, timezone.utc).strftime("%Y-%m-%d") if im.get("captured_at") else None
        return _frame("mapillary", im["thumb_1024_url"], c[1], c[0], when, im.get("compass_angle"),
                      "360° pano" if im.get("is_pano") else None)
    with ThreadPoolExecutor(6) as ex:               # cells in parallel: ~3 s instead of ~18 s
        return [f for f in ex.map(cell, _grid(bounds)[:limit]) if f]


def kartaview(bounds: dict, limit: int = 4) -> list[dict]:
    """Best-effort: the public API frequently answers 'Restricted access' / timeouts."""
    out = []
    for lat, lon in _grid(bounds)[:limit]:
        try:
            r = _client.get("https://api.openstreetcam.org/2.0/photo/", params={
                "lat": f"{lat:.5f}", "lng": f"{lon:.5f}", "radius": 80, "itemsPerPage": 1}, timeout=8.0)
            data = (r.json().get("result") or {}).get("data") or [] if r.status_code == 200 else []
        except (httpx.HTTPError, ValueError):
            data = []
        for p in data[:1]:
            url = p.get("fileurlLTh") or p.get("fileurlProc") or p.get("fileurl")
            if url:
                out.append(_frame("kartaview", url, p.get("lat", lat), p.get("lng", lon),
                                  (p.get("shotDate") or "")[:10] or None, p.get("heading")))
    return out


def streetview(bounds: dict, limit: int = 6) -> list[dict]:
    key = os.environ.get("GOOGLE_MAPS_KEY", "").strip()
    if not key or budget()["exhausted"]:
        return []
    out = []
    for lat, lon in _grid(bounds)[:limit]:
        try:                                          # free metadata call: only bill covered points
            m = _client.get("https://maps.googleapis.com/maps/api/streetview/metadata", params={
                "location": f"{lat:.5f},{lon:.5f}", "radius": 150, "source": "outdoor", "key": key}).json()
        except (httpx.HTTPError, ValueError):
            continue
        if m.get("status") != "OK":
            continue
        loc = m.get("location") or {}
        url = ("https://maps.googleapis.com/maps/api/streetview?size=640x400&fov=90&pitch=0"
               f"&location={loc.get('lat', lat)},{loc.get('lng', lon)}&source=outdoor&key={key}")
        out.append(_frame("streetview", url, loc.get("lat", lat), loc.get("lng", lon), m.get("date"), None,
                          f"pano {m.get('pano_id', '')[:8]}"))
    return out


# ── collector ─────────────────────────────────────────────────────────────────
def frames(bounds: dict, paid: bool = False, timeout: float = 12.0) -> list[dict]:
    """Free providers always; Street View (billed) only when `paid`. Cached 6 h."""
    key = tuple(round(float(bounds[k]), 3) for k in ("south", "north", "west", "east")) + (paid,)
    now = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < _TTL:
            return hit[1]
    ex = ThreadPoolExecutor(3)
    futs = {ex.submit(mapillary, bounds): "mapillary", ex.submit(kartaview, bounds): "kartaview"}
    if paid:
        futs[ex.submit(streetview, bounds)] = "streetview"
    done, _ = wait(list(futs), timeout=timeout)
    out: list[dict] = []
    for f in done:
        try:
            out.extend(f.result())
        except Exception as e:                        # one provider down never sinks the strip
            log.info("street %s failed: %s", futs[f], e)
    ex.shutdown(wait=False)
    order = {"streetview": 0, "mapillary": 1, "kartaview": 2}
    out.sort(key=lambda f_: (order.get(f_["provider"], 9), f_.get("captured") or "", ))
    with _lock:
        _cache[key] = (now, out)
    return out


def image(token: str) -> tuple[bytes, str] | None:
    """Fetch (cached 10 min) the frame behind a token. None for unknown tokens."""
    with _lock:
        hit = _bytes.get(token)
        if hit and time.time() - hit[0] < (86400 if "googleapis" in (_tokens.get(token) or (0, ""))[1] else 600):
            return hit[1], hit[2]
        url = (_tokens.get(token) or (0, None))[1]
    if not url:
        return None
    if "maps.googleapis.com/maps/api/streetview?" in url:
        from apb.store import spend
        if not spend.allow("streetview", 1, SV_MONTHLY_CAP, SV_DAILY_CAP):
            log.info("street view budget exhausted; frame %s not fetched", token)
            return None
    r = _client.get(url, timeout=20.0)
    r.raise_for_status()
    ctype = r.headers.get("content-type", "image/jpeg").split(";")[0]
    if not ctype.startswith("image/") or len(r.content) > 3_000_000:
        return None
    with _lock:
        _bytes[token] = (time.time(), r.content, ctype)
        if len(_bytes) > 900:
            for k in sorted(_bytes, key=lambda k: _bytes[k][0])[:300]:
                _bytes.pop(k, None)
    return r.content, ctype
