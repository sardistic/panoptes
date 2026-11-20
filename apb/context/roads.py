"""Local road-junction index: where two named roads meet, without a live map query.

Placing a radio incident by "NC 280 at Deep Woods" needs the point where those two roads
cross. Asking a public Overpass server per incident proved unreliable (every mirror timed
out or answered "at capacity" on 2026-11-19), so this keeps its own index instead:

  * the map is cut into 0.1-degree tiles; a tile is fetched once (one Overpass query:
    named / numbered roads with their node ids and coordinates) when an incident first
    needs it, and refreshed after 90 days;
  * only junctions are kept — nodes shared by two differently named roads — as
    (road key a, road key b, lat, lon) rows in state/roads.sqlite next to the main DB;
  * road keys are normalised the same way for OSM names and spoken names:
    "Deep Woods Drive" / "Deep Woods Dr" -> "n:deep woods", ref "NC 280;US 64" / "NC 280"
    -> "r:280" / "r:64".

Tiles download in the background, one request at a time, with backoff and mirror rotation;
until a tile is in, lookups there miss and the incident falls back to its town (and is
re-placed on a later pass). Nothing here is published — callers publish a ~2 km cell.
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

TILE = 0.1
MAX_AGE = 90 * 86400.0
_HW = ("^(motorway|trunk|primary|secondary|tertiary|unclassified|residential|living_street|"
       "service|road)(_link)?$")
_lock = threading.RLock()
_conn: sqlite3.Connection | None = None
_conn_path = None
_stop = threading.Event()
_wake = threading.Event()
_thread: threading.Thread | None = None
_stats = {"fetched": 0, "errors": 0, "junctions": 0, "last_error": None, "last": 0.0}

_SUFFIX = re.compile(r"\s+(?:road|rd|street|st|avenue|ave|av|drive|dr|lane|ln|court|ct|cove|cv|"
                     r"boulevard|blvd|way|place|pl|circle|cir|terrace|ter|trail|trl|loop|parkway|"
                     r"pkwy|pike|highway|hwy|extension|ext)\.?$", re.I)
_DIR = re.compile(r"^(?:north|south|east|west|n|s|e|w)\.?\s+", re.I)
_ROUTE = re.compile(r"^(?:(?:NC|SC|VA|GA|TN|US|SR|CR|I|IH|Interstate|Highway|Hwy|Route|Rte|State Route|"
                    r"State Road|State Highway|County Road|[A-Z]{2})\s*-?\s*)(\d{1,4}[A-Z]?)\b", re.I)


def mirrors() -> list[str]:
    env = os.environ.get("APB_OVERPASS_URLS")
    if env:
        return [u.strip() for u in env.split(",") if u.strip()]
    from apb.context import facts
    out = [facts.OVERPASS, "https://overpass-api.de/api/interpreter",
           "https://overpass.private.coffee/api/interpreter"]
    return list(dict.fromkeys(out))


def name_key(name: str | None) -> str | None:
    """A spoken or mapped road name -> its index key, or None if too vague."""
    s = re.sub(r"^the\s+", "", str(name or "").strip(), flags=re.I)
    m = _ROUTE.match(s)
    if m:
        return "r:" + m.group(1).lower()
    s = _DIR.sub("", _SUFFIX.sub("", s.replace(".", "")).strip())
    s = re.sub(r"[^a-z0-9 ]+", " ", s.lower())
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) < 3 or not re.search(r"[a-z0-9]{3}|\d", s):
        return None
    return "n:" + s


def way_keys(tags: dict) -> set[str]:
    out = set()
    k = name_key(tags.get("name"))
    if k:
        out.add(k)
    for ref in re.split(r"[;,]", str(tags.get("ref") or "")):
        m = re.search(r"(\d{1,4}[A-Za-z]?)\b", ref)
        if m:
            out.add("r:" + m.group(1).lower())
    return out


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
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS road_tiles (
                ty INTEGER NOT NULL, tx INTEGER NOT NULL, status TEXT NOT NULL, wanted REAL,
                fetched REAL, tries INTEGER DEFAULT 0, next_try REAL DEFAULT 0, junctions INTEGER,
                PRIMARY KEY (ty, tx));
            CREATE TABLE IF NOT EXISTS road_junctions (
                a TEXT NOT NULL, b TEXT NOT NULL, node INTEGER NOT NULL, lat REAL, lon REAL,
                ty INTEGER, tx INTEGER, PRIMARY KEY (a, b, node)) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_rj_tile ON road_junctions(ty, tx);
            """)
        return _conn


def tile_of(lat: float, lon: float) -> tuple[int, int]:
    return math.floor(lat / TILE), math.floor(lon / TILE)


def tiles_around(anchor: tuple, radius_m: float) -> list[tuple[int, int]]:
    lat, lon = anchor
    dlat = radius_m / 111_000.0
    dlon = dlat / max(0.2, math.cos(math.radians(lat)))
    y0, x0 = tile_of(lat - dlat, lon - dlon)
    y1, x1 = tile_of(lat + dlat, lon + dlon)
    return [(y, x) for y in range(y0, y1 + 1) for x in range(x0, x1 + 1)]


def want(tiles: list[tuple[int, int]], now: float | None = None) -> list[tuple[int, int]]:
    """Queue tiles that are missing or stale; returns the ones not loaded yet."""
    now = now or time.time()
    missing = []
    with _lock:
        c = _db()
        for ty, tx in tiles:
            row = c.execute("SELECT status, fetched FROM road_tiles WHERE ty=? AND tx=?", (ty, tx)).fetchone()
            if row is None:
                c.execute("INSERT INTO road_tiles (ty, tx, status, wanted) VALUES (?,?,'want',?)", (ty, tx, now))
                missing.append((ty, tx))
            else:
                c.execute("UPDATE road_tiles SET wanted=? WHERE ty=? AND tx=?", (now, ty, tx))
                if row[0] != "ok":
                    missing.append((ty, tx))
                elif (row[1] or 0) < now - MAX_AGE:
                    c.execute("UPDATE road_tiles SET status='stale' WHERE ty=? AND tx=?", (ty, tx))
        c.commit()
    if missing:
        _wake.set()
    return missing


def junctions_from(elements: list[dict], tile: tuple[int, int]) -> list[tuple]:
    """Overpass `out body geom` ways -> (a, b, node, lat, lon) rows for junction nodes in tile."""
    at: dict[int, list] = {}
    for w in elements:
        if w.get("type") != "way":
            continue
        keys = way_keys(w.get("tags") or {})
        geo = w.get("geometry") or []
        if not keys or len(geo) != len(w.get("nodes") or []):
            continue
        for nid, g in zip(w["nodes"], geo):
            if not g or tile_of(g["lat"], g["lon"]) != tile:
                continue
            ent = at.setdefault(nid, [g["lat"], g["lon"], []])
            if keys not in ent[2]:
                ent[2].append(keys)
    out = []
    for nid, (lat, lon, sets) in at.items():
        if len(sets) < 2:
            continue
        pairs = set()
        for i, ka in enumerate(sets):
            for kb in sets[i + 1:]:
                for a in ka:
                    for b in kb:
                        if a != b:
                            pairs.add((min(a, b), max(a, b)))
        out.extend((a, b, nid, round(lat, 6), round(lon, 6)) for a, b in pairs)
    return out


def store_tile(tile: tuple[int, int], elements: list[dict], now: float | None = None) -> int:
    rows = junctions_from(elements, tile)
    with _lock:
        c = _db()
        c.execute("DELETE FROM road_junctions WHERE ty=? AND tx=?", tile)
        c.executemany("INSERT OR IGNORE INTO road_junctions (a, b, node, lat, lon, ty, tx) VALUES (?,?,?,?,?,?,?)",
                      [(*r, *tile) for r in rows])
        c.execute("INSERT INTO road_tiles (ty, tx, status, wanted, fetched, tries, next_try, junctions) "
                  "VALUES (?,?,'ok',?,?,0,0,?) ON CONFLICT(ty, tx) DO UPDATE SET status='ok', "
                  "fetched=excluded.fetched, tries=0, next_try=0, junctions=excluded.junctions",
                  (*tile, now or time.time(), now or time.time(), len(rows)))
        c.commit()
    return len(rows)


def _query(tile: tuple[int, int]) -> str:
    s, w = tile[0] * TILE, tile[1] * TILE
    bb = f"({s:.4f},{w:.4f},{s + TILE:.4f},{w + TILE:.4f})"
    return (f'[out:json][timeout:120];(way["highway"~"{_HW}"]["name"]{bb};'
            f'way["highway"~"{_HW}"]["ref"]{bb};);out body geom qt;')


_http = httpx.Client(timeout=150.0, headers={"User-Agent": "apb/0.1 (panoptes.run; road junction index)"})


def fetch_next(now: float | None = None, fetch=None) -> bool:
    """Download the most recently wanted due tile. True if one was attempted."""
    now = now or time.time()
    with _lock:
        row = _db().execute(
            "SELECT ty, tx, tries FROM road_tiles WHERE status IN ('want','error','stale') AND next_try <= ? "
            "ORDER BY wanted DESC LIMIT 1", (now,)).fetchone()
    if not row:
        return False
    tile, tries = (row[0], row[1]), row[2] or 0
    urls = mirrors()
    url = urls[tries % len(urls)]
    try:
        if fetch:
            data = fetch(_query(tile), url)
        else:
            r = _http.post(url, data={"data": _query(tile)}, headers={"Accept": "application/json"})
            r.raise_for_status()
            data = r.json()
        if (data or {}).get("remark") and "error" in str(data["remark"]).lower():
            raise ValueError(str(data["remark"])[:120])
        n = store_tile(tile, (data or {}).get("elements") or [], now)
        _stats["fetched"] += 1
        _stats["junctions"] += n
        _stats["last"] = now
    except (httpx.HTTPError, ValueError) as e:
        _stats["errors"] += 1
        _stats["last_error"] = f"{url.split('/')[2]}: {str(e)[:120]}"
        with _lock:
            c = _db()
            c.execute("UPDATE road_tiles SET status=CASE status WHEN 'stale' THEN 'stale' ELSE 'error' END, "
                      "tries=tries+1, next_try=? WHERE ty=? AND tx=?",
                      (now + min(6 * 3600.0, 120.0 * 2 ** min(tries, 8)), *tile))
            c.commit()
        raise
    return True


def cross(a: str, b: str, anchor: tuple, radius_m: float, now: float | None = None) -> tuple | None:
    """(lat, lon) of the junction of roads a and b nearest anchor within radius, from the
    local index. Queues the tiles it needs; None until they are in."""
    ka, kb = name_key(a), name_key(b)
    if not ka or not kb or ka == kb or not anchor:
        return None
    want(tiles_around(anchor, radius_m), now)
    lat, lon = anchor
    dlat = radius_m / 111_000.0
    dlon = dlat / max(0.2, math.cos(math.radians(lat)))
    with _lock:
        rows = _db().execute(
            "SELECT lat, lon FROM road_junctions WHERE a=? AND b=? AND lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?",
            (min(ka, kb), max(ka, kb), lat - dlat, lat + dlat, lon - dlon, lon + dlon)).fetchall()
    if not rows:
        return None
    best = min(rows, key=lambda p: math.hypot(p[0] - lat, (p[1] - lon) * math.cos(math.radians(lat))))
    if math.hypot(best[0] - lat, (best[1] - lon) * math.cos(math.radians(lat))) * 111_000.0 > radius_m:
        return None
    return float(best[0]), float(best[1])


def _loop() -> None:
    while not _stop.is_set():
        try:
            did = fetch_next()
            _stop.wait(4.0 if did else 0.0)
        except Exception as e:
            log.info("roads: tile fetch: %s", str(e)[:160])
            _stop.wait(60.0)                       # a busy server: back off before the next tile
            continue
        if not did:
            _wake.clear()
            _wake.wait(600.0)


def start() -> bool:
    global _thread
    if _thread or os.environ.get("APB_ROADS_OFF", "").lower() in ("1", "true", "yes"):
        return False
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="road-tiles")
    _thread.start()
    return True


def stop() -> None:
    _stop.set()
    _wake.set()


def status() -> dict:
    try:
        with _lock:
            tiles = dict(_db().execute("SELECT status, COUNT(*) FROM road_tiles GROUP BY status").fetchall())
    except sqlite3.Error:
        tiles = {}
    return {**_stats, "tiles": tiles, "running": bool(_thread and _thread.is_alive())}
