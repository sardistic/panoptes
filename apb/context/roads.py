"""Where two named roads meet — for placing radio incidents like "NC 280 at Deep Woods".

Source: the Census Bureau's TIGERweb road layers (keyless ArcGIS REST, US-wide, answers
in well under a second). Public Overpass was tried first and dropped: on 2026-11-19 every
mirror timed out or refused prod (406 / "at capacity"), per incident and per tile alike.

A lookup asks TIGERweb for both roads inside the search box around the anchor (the
town, or the system's location), computes where their centre lines cross, and keeps the
crossing nearest the anchor. Answers — including "no crossing" — are cached in
state/roads.sqlite (hits 90 days, misses 7 days); network failures are not cached, so the
incident is simply retried on a later pass. Spoken names and TIGER names are reduced to
the same key: "Deep Woods Dr" / BASENAME "Deep Woods" -> "n:deep woods"; "NC 280" /
BASENAME "280" on a state route -> "r:280". Nothing here is published — callers publish
a ~2 km cell.
"""
from __future__ import annotations

import logging
import math
import os
import re
import sqlite3
import threading
import time

import httpx

log = logging.getLogger(__name__)

TIGER = os.environ.get("APB_TIGERWEB_URL",
                       "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Transportation/MapServer")
LAYERS = (2, 6, 8)              # primary roads, secondary roads (finest scale), local roads
HIT_TTL = 90 * 86400.0
MISS_TTL = 7 * 86400.0
_ROUTE_TYPES = {"I", "U", "S", "C", "O"}
_lock = threading.RLock()
_conn: sqlite3.Connection | None = None
_conn_path = None
_stats = {"lookups": 0, "cached": 0, "found": 0, "errors": 0, "last_error": None}
_http = httpx.Client(timeout=20.0, headers={"User-Agent": "apb/0.1 (panoptes.run; radio placement)"})

_SUFFIX = re.compile(r"\s+(?:road|rd|street|st|avenue|ave|av|drive|dr|lane|ln|court|ct|cove|cv|"
                     r"boulevard|blvd|way|place|pl|circle|cir|terrace|ter|trail|trl|loop|parkway|"
                     r"pkwy|pike|highway|hwy|extension|ext)\.?$", re.I)
_DIR = re.compile(r"^(?:north|south|east|west|n|s|e|w)\.?\s+", re.I)
_ROUTE = re.compile(r"^(?:(?:NC|SC|VA|GA|TN|US|SR|CR|I|IH|Interstate|Highway|Hwy|Route|Rte|State Route|"
                    r"State Road|State Highway|County Road|[A-Z]{2})\s*-?\s*)(\d{1,4}[A-Z]?)\b", re.I)


def name_key(name: str | None) -> str | None:
    """A spoken or mapped road name -> its lookup key, or None if too vague."""
    s = re.sub(r"^the\s+", "", str(name or "").strip(), flags=re.I)
    m = _ROUTE.match(s)
    if m:
        return "r:" + m.group(1).lower()
    s = _DIR.sub("", _SUFFIX.sub("", s.replace(".", "")).strip())
    s = re.sub(r"[^a-z0-9 ]+", " ", s.lower())
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) < 3 and not re.fullmatch(r"\d+(st|nd|rd|th)?", s):
        return None
    if not re.search(r"[a-z0-9]{3}|\d", s):
        return None
    return "n:" + s


def tiger_key(attrs: dict) -> str | None:
    """The key of one TIGERweb road feature."""
    base = str(attrs.get("BASENAME") or "").strip()
    if not base:
        return None
    if str(attrs.get("RTTYP") or "").upper() in _ROUTE_TYPES and re.fullmatch(r"\d{1,4}[A-Za-z]?", base):
        return "r:" + base.lower()
    return name_key(base)


def _where(key: str) -> str:
    val = key[2:].upper().replace("'", "''")
    if key.startswith("r:"):
        return f"(BASENAME = '{val}' AND RTTYP IN ('I','U','S','C','O'))"
    return f"(UPPER(BASENAME) = '{val}')"


def _db() -> sqlite3.Connection:
    global _conn, _conn_path
    from apb.store import snapshots
    path = snapshots.DB_PATH.parent / "roads.sqlite"
    with _lock:
        if _conn is None or _conn_path != path:
            path.parent.mkdir(parents=True, exist_ok=True)
            _conn = sqlite3.connect(str(path), check_same_thread=False, timeout=30)
            _conn_path = path
            _conn.executescript("""
            CREATE TABLE IF NOT EXISTS road_cross (
                a TEXT NOT NULL, b TEXT NOT NULL, cell TEXT NOT NULL, radius INTEGER NOT NULL,
                lat REAL, lon REAL, ts REAL NOT NULL, PRIMARY KEY (a, b, cell, radius));
            """)
        return _conn


def _xy(lat0: float, lat: float, lon: float, lon0: float) -> tuple[float, float]:
    return (lon - lon0) * 111_320.0 * math.cos(math.radians(lat0)), (lat - lat0) * 110_574.0


def crossings(paths_a: list, paths_b: list, anchor: tuple) -> list[tuple[float, float]]:
    """Points (lat, lon) where any segment of a meets any segment of b (or touches within 12 m)."""
    lat0, lon0 = anchor

    def segs(paths):
        out = []
        for p in paths:
            pts = [_xy(lat0, y, x, lon0) for x, y in p]
            out.extend(zip(pts, pts[1:]))
        return out
    sa, sb = segs(paths_a), segs(paths_b)
    hits = []
    for (p1, p2) in sa:
        minx, maxx = min(p1[0], p2[0]) - 12, max(p1[0], p2[0]) + 12
        miny, maxy = min(p1[1], p2[1]) - 12, max(p1[1], p2[1]) + 12
        for (q1, q2) in sb:
            if max(q1[0], q2[0]) < minx or min(q1[0], q2[0]) > maxx or \
               max(q1[1], q2[1]) < miny or min(q1[1], q2[1]) > maxy:
                continue
            pt = _seg_meet(p1, p2, q1, q2)
            if pt:
                hits.append((lat0 + pt[1] / 110_574.0,
                             lon0 + pt[0] / (111_320.0 * math.cos(math.radians(lat0)))))
    return hits


def _seg_meet(p1, p2, q1, q2, tol: float = 12.0):
    rx, ry = p2[0] - p1[0], p2[1] - p1[1]
    sx, sy = q2[0] - q1[0], q2[1] - q1[1]
    den = rx * sy - ry * sx
    if abs(den) > 1e-9:
        t = ((q1[0] - p1[0]) * sy - (q1[1] - p1[1]) * sx) / den
        u = ((q1[0] - p1[0]) * ry - (q1[1] - p1[1]) * rx) / den
        if -1e-9 <= t <= 1 + 1e-9 and -1e-9 <= u <= 1 + 1e-9:
            return p1[0] + t * rx, p1[1] + t * ry
    for a in (p1, p2):                                 # end points that touch (T-junctions)
        for b in (q1, q2):
            if math.hypot(a[0] - b[0], a[1] - b[1]) <= tol:
                return (a[0] + b[0]) / 2, (a[1] + b[1]) / 2
    return None


def _fetch(layer: int, params: dict) -> dict:
    r = _http.get(f"{TIGER}/{layer}/query", params=params)
    r.raise_for_status()
    j = r.json()
    if j.get("error"):
        raise ValueError(str(j["error"])[:120])
    return j


def cross(a: str, b: str, anchor: tuple, radius_m: float, now: float | None = None, fetch=None) -> tuple | None:
    """(lat, lon) where roads a and b meet nearest anchor within radius_m, or None."""
    ka, kb = name_key(a), name_key(b)
    if not ka or not kb or ka == kb or not anchor:
        return None
    now = now or time.time()
    ka, kb = min(ka, kb), max(ka, kb)
    lat, lon = anchor
    cell, rad = f"{lat:.2f},{lon:.2f}", int(radius_m)
    with _lock:
        row = _db().execute("SELECT lat, lon, ts FROM road_cross WHERE a=? AND b=? AND cell=? AND radius=?",
                            (ka, kb, cell, rad)).fetchone()
    if row and now - row[2] < (HIT_TTL if row[0] is not None else MISS_TTL):
        _stats["cached"] += 1
        return (row[0], row[1]) if row[0] is not None else None
    _stats["lookups"] += 1
    dlat = radius_m / 110_574.0
    dlon = radius_m / (111_320.0 * max(0.2, math.cos(math.radians(lat))))
    params = {"where": f"{_where(ka)} OR {_where(kb)}",
              "geometry": f"{lon - dlon:.5f},{lat - dlat:.5f},{lon + dlon:.5f},{lat + dlat:.5f}",
              "geometryType": "esriGeometryEnvelope", "inSR": 4326, "outSR": 4326,
              "spatialRel": "esriSpatialRelIntersects", "outFields": "BASENAME,RTTYP",
              "returnGeometry": "true", "f": "json"}
    paths: dict[str, list] = {ka: [], kb: []}
    try:
        for layer in LAYERS:
            for f in (fetch or _fetch)(layer, params).get("features") or []:
                k = tiger_key(f.get("attributes") or {})
                if k in paths:
                    paths[k].extend((f.get("geometry") or {}).get("paths") or [])
    except (httpx.HTTPError, ValueError) as e:
        _stats["errors"] += 1
        _stats["last_error"] = str(e)[:160]
        log.info("roads: tigerweb: %s", str(e)[:160])
        return None                                    # not cached: retried on a later pass
    best = None
    if paths[ka] and paths[kb]:
        pts = crossings(paths[ka], paths[kb], anchor)
        near = [(math.hypot(*_xy(lat, p[0], p[1], lon)), p) for p in pts]
        near = [x for x in near if x[0] <= radius_m]
        if near:
            best = min(near)[1]
            best = (round(best[0], 6), round(best[1], 6))
    with _lock:
        c = _db()
        c.execute("INSERT OR REPLACE INTO road_cross VALUES (?,?,?,?,?,?,?)",
                  (ka, kb, cell, rad, best[0] if best else None, best[1] if best else None, now))
        c.commit()
    if best:
        _stats["found"] += 1
    return best


STREET_SPAN_M = 3000.0          # a street placed alone must fit in about one 2 km cell


def street(name: str, anchor: tuple, radius_m: float, now: float | None = None, fetch=None) -> tuple | None:
    """The middle of a named street near anchor, when its stretch inside the search box is
    short enough to stand for one place (<= STREET_SPAN_M across); else None."""
    k = name_key(name)
    if not k or k.startswith("r:") or not anchor:
        return None
    now = now or time.time()
    lat, lon = anchor
    cell, rad = f"{lat:.2f},{lon:.2f}", int(radius_m)
    with _lock:
        row = _db().execute("SELECT lat, lon, ts FROM road_cross WHERE a=? AND b='#street' AND cell=? AND radius=?",
                            (k, cell, rad)).fetchone()
    if row and now - row[2] < (HIT_TTL if row[0] is not None else MISS_TTL):
        _stats["cached"] += 1
        return (row[0], row[1]) if row[0] is not None else None
    _stats["lookups"] += 1
    dlat = radius_m / 110_574.0
    dlon = radius_m / (111_320.0 * max(0.2, math.cos(math.radians(lat))))
    params = {"where": _where(k),
              "geometry": f"{lon - dlon:.5f},{lat - dlat:.5f},{lon + dlon:.5f},{lat + dlat:.5f}",
              "geometryType": "esriGeometryEnvelope", "inSR": 4326, "outSR": 4326,
              "spatialRel": "esriSpatialRelIntersects", "outFields": "BASENAME,RTTYP",
              "returnGeometry": "true", "f": "json"}
    pts = []
    try:
        for layer in LAYERS:
            for f in (fetch or _fetch)(layer, params).get("features") or []:
                if tiger_key(f.get("attributes") or {}) == k:
                    for p in (f.get("geometry") or {}).get("paths") or []:
                        pts.extend((y, x) for x, y in p
                                   if abs(y - lat) <= dlat and abs(x - lon) <= dlon)
    except (httpx.HTTPError, ValueError) as e:
        _stats["errors"] += 1
        _stats["last_error"] = str(e)[:160]
        return None
    best = None
    if pts:
        y0, y1 = min(p[0] for p in pts), max(p[0] for p in pts)
        x0, x1 = min(p[1] for p in pts), max(p[1] for p in pts)
        span = math.hypot(*_xy(y0, y1, x1, x0))
        if span <= STREET_SPAN_M:
            best = (round((y0 + y1) / 2, 6), round((x0 + x1) / 2, 6))
    with _lock:
        c = _db()
        c.execute("INSERT OR REPLACE INTO road_cross VALUES (?,?,?,?,?,?,?)",
                  (k, "#street", cell, rad, best[0] if best else None, best[1] if best else None, now))
        c.commit()
    if best:
        _stats["found"] += 1
    return best


def status() -> dict:
    return dict(_stats)
