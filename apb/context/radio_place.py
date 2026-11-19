"""Placing radio incidents: the most precise place a call names, resolved internally.

Order of evidence (best first):
  address       — a call in the thread was geocoded to a block address (Census), or the
                  thread's place is a block address
  intersection  — "NC 280 at Deep Woods": the node where two named OpenStreetMap roads
                  meet near the town (Overpass, keyless)
  town          — the town's gazetteer point

Whatever the precision, what leaves the server is the ~2 km cell (radio_stt.coarse)
plus the precision word — never the address or the intersection itself.
"""
from __future__ import annotations

import logging
import math
import re

import httpx

log = logging.getLogger(__name__)

_http = httpx.Client(timeout=16.0, headers={"User-Agent": "apb/0.1 (panoptes.run; radio placement)"})
_cache: dict[tuple, tuple | None] = {}

_SPLIT = re.compile(r"\s+(?:and|&|at|/|near|off|x|by)\s+", re.I)
_SUFFIX = re.compile(r"\s+(?:road|rd|street|st|avenue|ave|av|drive|dr|lane|ln|court|ct|cove|cv|"
                     r"boulevard|blvd|way|place|pl|circle|cir|terrace|ter|trail|trl|loop|parkway|"
                     r"pkwy|pike|highway|hwy|extension|ext)\.?$", re.I)
_ROUTE = re.compile(r"^(?:(?:NC|SC|VA|GA|TN|US|SR|CR|I|IH|Interstate|Highway|Hwy|Route|Rte|State Route|"
                    r"State Road|County Road|[A-Z]{2})\s*-?\s*)(\d{1,4}[A-Z]?)\b", re.I)


def split_where(where: str | None) -> tuple[str, str] | None:
    """'NC 280 at Deep Woods Dr' -> ('NC 280', 'Deep Woods Dr'); anything else None."""
    if not where or "[REDACTED]" in where:
        return None
    parts = [p.strip(" .,") for p in _SPLIT.split(where.strip(), maxsplit=1)]
    if len(parts) != 2 or not all(parts) or any(re.match(r"^\d+(?!st|nd|rd|th)\b", p, re.I) for p in parts):
        return None
    return parts[0], parts[1]


def road_filter(name: str) -> str | None:
    """An Overpass tag filter for a spoken road name: a route number matches `ref`,
    anything else matches the start of `name` (suffix dropped: "Deep Woods Dr" ->
    ^Deep Woods)."""
    name = re.sub(r"^the\s+", "", name.strip(), flags=re.I)
    m = _ROUTE.match(name)
    if m:
        num = re.escape(m.group(1))
        return f'["ref"~"(^|[^0-9]){num}($|[^0-9])",i]'
    base = _SUFFIX.sub("", name).strip()
    base = re.sub(r"^(?:north|south|east|west|n|s|e|w)\.?\s+", "", base, flags=re.I)
    if len(base) < 3 or not re.search(r"[A-Za-z]{3}", base):
        return None
    esc = re.sub(r'([\\.^$|?*+()\[\]{}"])', r"\\\1", base)
    return f'["name"~"^(north |south |east |west |n |s |e |w )?{esc}",i]'


def cross(a: str, b: str, anchor: tuple, radius_m: int, overpass_url: str, fetch=None) -> tuple | None:
    """(lat, lon) where roads a and b share a node within radius of anchor, or None."""
    fa, fb = road_filter(a), road_filter(b)
    if not fa or not fb:
        return None
    key = (fa.lower(), fb.lower(), round(anchor[0], 2), round(anchor[1], 2), radius_m)
    if key in _cache:
        return _cache[key]
    lat, lon = anchor
    q = (f"[out:json][timeout:12];"
         f"way(around:{radius_m},{lat:.5f},{lon:.5f})[\"highway\"]{fa}->.a;"
         f"way(around:{radius_m},{lat:.5f},{lon:.5f})[\"highway\"]{fb}->.b;"
         f"node(w.a)(w.b);out 5;")
    try:
        if fetch:
            data = fetch(q)
        else:
            r = _http.post(overpass_url, data={"data": q}, headers={"Accept": "application/json"})
            r.raise_for_status()
            data = r.json()
    except (httpx.HTTPError, ValueError) as e:
        log.info("radio place: overpass: %s", str(e)[:120])
        return None                                  # not cached: retry next time
    nodes = [e for e in (data or {}).get("elements", []) if e.get("type") == "node" and "lat" in e]
    pt = None
    if nodes:
        nodes.sort(key=lambda e: math.hypot(e["lat"] - lat, (e["lon"] - lon) * math.cos(math.radians(lat))))
        pt = (float(nodes[0]["lat"]), float(nodes[0]["lon"]))
    _cache[key] = pt
    return pt


def town_point(town: str | None, near: tuple | None, max_km: float = 250.0) -> tuple | None:
    """The gazetteer point of a town, if it is within max_km of the system (when known)."""
    from apb.context.radio_stt import _km
    t = str(town or "").strip().lower().replace(".", "")
    if not t or t == "unknown":
        return None
    try:
        from apb.fusion.places import _gaz
        hit = _gaz().get(t)
    except Exception:
        hit = None
    if not hit or (near and _km((hit[0], hit[1]), near) > max_km):
        return None
    return float(hit[0]), float(hit[1])


def place(where: str | None, town: str | None, state: str | None, call_rows: list[dict],
          inst_area: str | None, near: tuple | None, approx: bool,
          geocode=None, crosser=None) -> tuple[tuple | None, str | None]:
    """((lat, lon) exact, precision) for an incident — internal; publish coarse() of it."""
    from apb.context import facts, radio_stt
    pts = [(r["lat"], r["lon"]) for r in call_rows if r.get("lat") is not None]
    if pts:
        return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts)), "address"
    geocode = geocode or radio_stt.geocode
    try:
        pt = geocode(where, town, inst_area, near, approx, state)
    except Exception as e:
        log.info("radio place: geocode: %s", str(e)[:120])
        pt = None
    if pt:
        return pt, "address"
    tpt = town_point(town, near)
    pair = split_where(where)
    if pair:
        anchor, radius = (tpt, 15000) if tpt else ((near, 30000) if near and not approx else (None, 0))
        if anchor:
            pt = (crosser or (lambda a, b, an, r: cross(a, b, an, r, facts.OVERPASS)))(pair[0], pair[1], anchor, radius)
            if pt:
                return pt, "intersection"
    if tpt:
        return tpt, "town"
    return None, None
