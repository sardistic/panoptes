"""Live CAD / 911 dispatch ingest from public open-data portals (Socrata) — countrywide.

No-key, no-hardware live source: cities publish near-real-time computer-aided-dispatch
feeds with lat/lon + type + time, already geocoded so they map directly.

Two feed kinds:
- curated CadFeed: exact field mapping (a few hand-verified flagships).
- adaptive feeds auto-loaded from data/sources_catalog.json (produced by
  apb.discover.sweep). Adaptive feeds ignore configured field names and detect
  coordinates/type/time/address per-row, so hundreds of cities work without tuning.

Radio (Broadcastify/own SDR) remains primary long-term per [[apb-source-strategy]];
CAD is the fastest countrywide live layer.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path

import httpx


_TS_MIN = 1262304000.0   # 2010-01-01 — older parses are almost certainly junk


def parse_ts(v) -> float | None:
    """Parse a CAD timestamp (ISO string, or epoch seconds/millis) -> epoch seconds.

    Rejects implausible timestamps (pre-2010 or future-dated) -> None. Some feeds emit
    garbage/future dates; left unfiltered they corrupt recency sorting (sort to the top
    as 'most recent', rendering as '0s ago') and freshness windows."""
    if v in (None, ""):
        return None
    ts: float | None = None
    # numeric epoch (ArcGIS uses millis)
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
        n = float(v)
        ts = n / 1000.0 if n > 1e12 else n
    else:
        try:
            s = str(v).replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts = dt.timestamp()
        except ValueError:
            return None
    if ts is None or not (_TS_MIN <= ts <= time.time() + 86400):
        return None          # allow ~1 day skew for tz quirks; reject real garbage
    return ts

# ── incident type → (normalized type, base threat 0..1) ──────────────────────
_TYPE_RULES: list[tuple[str, tuple[str, float]]] = [
    # weapons / violence (most specific first)
    ("shoot", ("shots_fired", 0.95)), ("shots fired", ("shots_fired", 0.97)),
    ("shotspotter", ("shots_fired", 0.9)), ("gun", ("shots_fired", 0.85)),
    ("stab", ("assault", 0.9)), ("cutting", ("assault", 0.85)),
    ("aggravated", ("assault", 0.88)), ("assault", ("assault", 0.85)),
    ("battery", ("assault", 0.8)), ("weapon", ("assault", 0.85)),
    ("armed", ("assault", 0.85)), ("fight", ("assault", 0.55)),
    ("homicide", ("assault", 0.98)), ("kidnap", ("assault", 0.9)),
    ("rape", ("assault", 0.95)), ("sexual", ("assault", 0.85)),
    # robbery / theft / property
    ("robbery", ("robbery", 0.8)), ("carjack", ("robbery", 0.9)),
    ("burglary", ("robbery", 0.6)), ("burglar", ("suspicious", 0.55)),
    ("larceny", ("robbery", 0.45)), ("theft", ("robbery", 0.5)),
    ("stolen", ("robbery", 0.5)), ("shoplif", ("robbery", 0.4)),
    ("vandal", ("suspicious", 0.4)), ("trespass", ("suspicious", 0.4)),
    # pursuit / vehicle
    ("pursuit", ("pursuit", 0.85)), ("chase", ("pursuit", 0.85)),
    ("eluding", ("pursuit", 0.8)), ("mva", ("traffic", 0.5)),
    ("collision", ("traffic", 0.5)), ("crash", ("traffic", 0.5)),
    ("accident", ("traffic", 0.45)), ("hit and run", ("traffic", 0.55)),
    ("hit & run", ("traffic", 0.55)), ("dui", ("traffic", 0.55)),
    ("reckless", ("traffic", 0.45)), ("traffic", ("traffic", 0.35)),
    ("vehicle", ("traffic", 0.35)),
    # fire
    ("explos", ("fire", 0.95)), ("structure fire", ("fire", 0.85)),
    ("structure", ("fire", 0.8)), ("smoke", ("fire", 0.55)),
    ("hazmat", ("fire", 0.8)), ("gas leak", ("fire", 0.7)),
    ("fire", ("fire", 0.7)), ("brush", ("fire", 0.5)),
    # medical
    ("cardiac", ("medical", 0.8)), ("overdose", ("medical", 0.75)),
    ("od ", ("medical", 0.7)), ("unconscious", ("medical", 0.75)),
    ("not breathing", ("medical", 0.85)), ("cpr", ("medical", 0.85)),
    ("seizure", ("medical", 0.6)), ("rescue", ("medical", 0.6)),
    ("injury", ("medical", 0.5)), ("injured", ("medical", 0.5)),
    ("medic", ("medical", 0.55)), ("medical", ("medical", 0.5)),
    ("ems", ("medical", 0.5)), ("aid resp", ("medical", 0.4)),
    ("aid", ("medical", 0.4)), ("sick", ("medical", 0.4)),
    ("fall", ("medical", 0.4)),
    # social / disturbance
    ("domestic", ("domestic", 0.7)), ("dv ", ("domestic", 0.7)),
    ("suicid", ("medical", 0.8)), ("mental", ("medical", 0.55)),
    ("missing", ("welfare", 0.5)), ("welfare", ("welfare", 0.35)),
    ("check", ("welfare", 0.3)), ("disturbance", ("suspicious", 0.45)),
    ("noise", ("noise", 0.2)), ("suspicious", ("suspicious", 0.45)),
    ("prowler", ("suspicious", 0.5)), ("alarm", ("suspicious", 0.3)),
    ("drug", ("suspicious", 0.45)), ("narcotic", ("suspicious", 0.45)),
    ("disorder", ("suspicious", 0.4)), ("loiter", ("suspicious", 0.3)),
    # broader category strings seen across CAD feeds
    ("violent crime", ("assault", 0.75)), ("property crime", ("robbery", 0.45)),
    ("intimidation", ("assault", 0.55)), ("harass", ("suspicious", 0.45)),
    ("threat", ("suspicious", 0.55)), ("nuisance", ("suspicious", 0.3)),
    ("warrant", ("suspicious", 0.5)), ("wanted", ("suspicious", 0.55)),
    ("onview", ("suspicious", 0.35)), ("vice", ("suspicious", 0.45)),
    ("ift", ("medical", 0.4)), ("transfer", ("medical", 0.4)),
    ("public service", ("welfare", 0.25)), ("public assist", ("welfare", 0.3)),
    ("community caretaking", ("welfare", 0.3)), ("citizen assist", ("welfare", 0.3)),
    ("road clos", ("traffic", 0.2)), ("road_closure", ("traffic", 0.2)),
    ("roadwork", ("traffic", 0.2)), ("closure", ("traffic", 0.2)),
    ("hazard", ("traffic", 0.35)), ("abandoned", ("suspicious", 0.25)),
    ("juvenile", ("welfare", 0.3)), ("missing person", ("welfare", 0.55)),
]
_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]


# Learned mappings (raw type string -> [type, threat]) produced by
# apb.infer.learn_types via Claude. Loaded once; lets us classify agency-specific
# strings the keyword rules miss, without an LLM call at request time.
_LEARNED: dict[str, tuple[str, float]] = {}
_UNKNOWN: set[str] = set()           # raw strings we couldn't classify (for the learner)


def _load_learned(path: str = "data/type_map.json") -> None:
    p = Path(path)
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            _LEARNED.update({k.lower(): tuple(v) for k, v in data.items()})
        except (ValueError, OSError):
            pass


_load_learned()


def classify(raw_type: str) -> tuple[str, float]:
    t = (raw_type or "").strip().lower()
    if not t:
        return ("other", 0.3)
    if t in _LEARNED:                 # exact agency string learned via LLM
        return _LEARNED[t]
    for kw, result in _TYPE_RULES:    # keyword rules
        if kw in t:
            return result
    _UNKNOWN.add(t)                   # remember for the offline learner
    return ("other", 0.3)


# ── adaptive field detection (used by auto-loaded feeds) ──────────────────────
_TYPE_KEYS = ("call_type", "final_call_type", "initial_type", "incident_type",
              "cfd_incident_type", "type_english", "primary_type", "nature",
              "offense", "crimetype", "category", "description", "type")
_TIME_RE = re.compile(r"(datetime|date|time|received|reported|created|occur)")
_ADDR_KEYS = ("address", "full_address", "block_address", "location", "block",
              "incident_address")
_LAT_RE = re.compile(r"(^|_)(lat|latitude|y_coord|y)($|_)")
_LON_RE = re.compile(r"(^|_)(lon|lng|long|longitude|x_coord|x)($|_)")


def _detect_type_key(row: dict) -> str | None:
    """Pick the most descriptive type field. Prefer human-readable name/description
    fields with string values; avoid numeric code/id fields."""
    best, best_score = None, -1
    for k, v in row.items():
        kl = k.lower()
        if not any(h in kl for h in _TYPE_KEYS):
            continue
        score = 0
        if any(w in kl for w in ("name", "desc", "nature", "english", "text")):
            score += 3
        if any(w in kl for w in ("code", "id", "num", "_no", "objectid")):
            score -= 3
        if isinstance(v, str) and not v.strip().isdigit():
            score += 2          # real text value, not a numeric code
        if score > best_score:
            best, best_score = k, score
    return best


def _detect_time_key(row: dict) -> str | None:
    for k in row:
        if _TIME_RE.search(k.lower()):
            return k
    return None


def _detect_addr_key(row: dict) -> str | None:
    low = {k.lower(): k for k in row}
    for cand in _ADDR_KEYS:
        if cand in low:
            return low[cand]
    return None


def _coords_from_row(row: dict) -> tuple[float | None, float | None]:
    """Find (lat, lon) in an arbitrary CAD row: pair fields, or a Point/dict column."""
    lat = lon = None
    for k, v in row.items():
        kl = k.lower()
        if isinstance(v, dict):  # Point / location object
            if v.get("coordinates") and len(v["coordinates"]) >= 2:
                return float(v["coordinates"][1]), float(v["coordinates"][0])
            if v.get("latitude") and v.get("longitude"):
                return float(v["latitude"]), float(v["longitude"])
            continue
        if v in (None, ""):
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if lat is None and _LAT_RE.search(kl) and -90 <= fv <= 90:
            lat = fv
        elif lon is None and _LON_RE.search(kl) and -180 <= fv <= 180:
            lon = fv
    return lat, lon


@dataclass
class CadFeed:
    metro: str
    name: str
    url: str
    type_field: str | None = None
    time_field: str | None = None
    lat_field: str = "latitude"
    lon_field: str = "longitude"
    point_field: str | None = None
    addr_field: str | None = None
    id_field: str = "incident_number"
    center: tuple[float, float] | None = None
    adaptive: bool = False         # detect fields per-row instead of using config
    state: str | None = None
    kind: str = "socrata"          # "socrata"|"arcgis"|"pulsepoint"|"p2c"|"southern"
    hidden: bool = False           # in overview/poller but not the metro dropdown


# Hand-verified flagship feeds (exact mappings).
FEEDS: dict[str, CadFeed] = {
    "seattle": CadFeed(
        metro="seattle", name="Seattle Fire/EMS Real-Time 911", state="WA",
        url="https://data.seattle.gov/resource/kzjm-xkqj.json",
        type_field="type", time_field="datetime", addr_field="address",
        center=(47.6062, -122.3321)),
    "montgomery_md": CadFeed(
        metro="montgomery_md", name="Montgomery County MD Police Dispatched",
        state="MD", url="https://data.montgomerycountymd.gov/resource/98cc-bc7d.json",
        type_field="initial_type", time_field="start_time", addr_field="address",
        id_field="incident_id", center=(39.1377, -77.2036)),
    "oakland": CadFeed(
        metro="oakland", name="Oakland CrimeWatch (90-day)", state="CA",
        url="https://data.oaklandca.gov/resource/ym6k-rx7a.json",
        type_field="description", time_field="datetime", point_field="location_1",
        addr_field="address", id_field="casenumber", center=(37.8044, -122.2712)),
}


_PAST_YEARS = tuple(str(y) for y in range(2001, datetime.now().year))


def _is_archival(name: str) -> bool:
    """Heuristic: a year in the name (e.g. '... 2019', 'Crimes 2001-2018') or words
    that mark a historical/legacy dataset rather than a live feed."""
    n = name.lower()
    if any(w in n for w in ("legacy", "archive", "historical", " old", "to present")):
        return True
    return any(y in name for y in _PAST_YEARS)


_DEMO_RE = re.compile(r"\bdemo\b", re.I)


def _is_demo(name: str = "", url: str = "", domain: str = "") -> bool:
    """Esri/Socrata sample & staging feeds carry FAKE data (e.g. 'Crime_Map_Demo',
    'Fire Incidents Demo', *.demo.socrata.com). Word-boundary match so it never trips on
    'demographic'. Keeps synthetic incidents off the live map."""
    if domain and ".demo." in domain.lower():
        return True
    u = (url or "").lower()
    if "/demo" in u or "_demo" in u or "demo_" in u:
        return True
    return bool(_DEMO_RE.search(name or ""))


def load_catalog(path: str | Path = "data/sources_catalog.json",
                 max_fresh_days: float = 30.0, min_score: float = 5.0) -> int:
    """Auto-register adaptive feeds from a discovery-sweep catalog. Returns count added."""
    p = Path(path)
    if not p.exists():
        return 0
    # whitelist: only emergency dispatch / crime / fire feeds (name must match)
    include = ("dispatch", "911", "calls for service", "call for service", "fire",
               "police", "sheriff", "crime", "ems", "law incident", "shooting",
               "pursuit", "arrest", "homicide", "assault", "incident", "cad",
               "emergency", "burglary", "robbery")
    # ...but never these (transport agencies, code/permits, complaints, archival, etc.)
    exclude = ("311", "service request", "rail", "aviation", "drivers", "permit",
               "code enforcement", "complaint", "address information", "utility",
               "bus ", "older adult", "budget", "parking", "inspection", "vendor",
               "salaries", "consumer", "hit ticket",
               "legacy", "archive", "historical", " old", "2001", "to present")
    added = 0
    for c in json.loads(p.read_text(encoding="utf-8")):
        if not c.get("geocoded"):
            continue
        name = c["name"].lower()
        if not any(x in name for x in include) or any(x in name for x in exclude):
            continue
        if _is_archival(c["name"]) or _is_demo(c["name"], c.get("url", ""), c.get("domain", "")):
            continue
        if c.get("score", 0) < min_score:
            continue
        fd = c.get("fresh_days")
        if fd is None or fd > max_fresh_days:
            continue
        slug = re.sub(r"[^a-z0-9]+", "_",
                      f"{c['domain'].split('.')[1] if '.' in c['domain'] else c['domain']}_{c['id']}".lower())
        if slug in FEEDS:
            continue
        FEEDS[slug] = CadFeed(
            metro=slug, name=c["name"], url=c["url"], adaptive=True,
            type_field=c.get("type_field"), time_field=c.get("time_field"),
        )
        added += 1
    return added


def load_arcgis_catalog(path: str | Path = "data/arcgis_catalog.json") -> int:
    """Auto-register ArcGIS point-geometry feeds found by apb.discover.arcgis_sweep."""
    p = Path(path)
    if not p.exists():
        return 0
    skip = ("phone", "call box", "callbox", "wildfire", "hydrant", "boundary",
            "shelter", "facilit", "hospital", "station", "summary", "statistic",
            "dashboard", "density", "annual", "yearly", "monthly")
    added = 0
    for c in json.loads(p.read_text(encoding="utf-8")):
        if not c.get("geocoded"):
            continue
        if (any(x in c["name"].lower() for x in skip) or _is_archival(c["name"])
                or _is_demo(c["name"], c.get("url", ""))):
            continue
        slug = "ag_" + re.sub(r"[^a-z0-9]+", "_", c["name"].lower())[:40]
        if slug in FEEDS:
            continue
        FEEDS[slug] = CadFeed(
            metro=slug, name=c["name"], url=c["url"], kind="arcgis", adaptive=True,
            time_field=c.get("time_field"),
        )
        added += 1
    return added


def load_pulsepoint(path: str | Path = "data/pulsepoint_agencies.json") -> int:
    """Register discovered PulsePoint agencies as hidden feeds (fire/EMS coverage).
    Hidden = included in national overview + poller, but kept out of the dropdown to
    avoid thousands of entries."""
    p = Path(path)
    if not p.exists():
        return 0
    added = 0
    for a in json.loads(p.read_text(encoding="utf-8")):
        slug = "pp_" + str(a["agencyid"]).lower()
        if slug in FEEDS:
            continue
        nm = a.get("name") or a["agencyid"]
        FEEDS[slug] = CadFeed(
            metro=slug, name=f"{nm} ({a.get('city','')},{a.get('state','')})",
            url=a["agencyid"], kind="pulsepoint", hidden=True,
            center=(a["lat"], a["lon"]), state=a.get("state"),
        )
        added += 1
    return added


def load_p2c(path: str | Path = "data/p2c_agencies.json") -> int:
    """Register PoliceToCitizen agencies (police CAD) as hidden feeds. Resolves each
    agency's id/name/center lazily on first fetch."""
    p = Path(path)
    if not p.exists():
        return 0
    added = 0
    for sub in json.loads(p.read_text(encoding="utf-8")):
        slug = "p2c_" + str(sub).lower()
        if slug in FEEDS:
            continue
        FEEDS[slug] = CadFeed(metro=slug, name=f"P2C {sub}", url=sub,
                              kind="p2c", hidden=True)
        added += 1
    return added


def load_southern(path: str | Path = "data/southern_agencies.json") -> int:
    """Register Southern Software 'Citizen Connect' agencies (police/sheriff CAD) as
    hidden feeds. feed.url = AgencyID; resolved lazily on first fetch."""
    p = Path(path)
    if not p.exists():
        return 0
    added = 0
    for aid in json.loads(p.read_text(encoding="utf-8")):
        slug = "ss_" + str(aid).lower()
        if slug in FEEDS:
            continue
        FEEDS[slug] = CadFeed(metro=slug, name=f"SoSoftware {aid}", url=str(aid),
                              kind="southern", hidden=True)
        added += 1
    return added


class CadIngest:
    ttl_sec = 20.0

    def __init__(self):
        self._client = httpx.Client(timeout=15.0, headers={"User-Agent": "apb/0.1"})
        self._cache: dict[str, tuple[float, list[dict]]] = {}
        self._pp = None
        self._p2c = None
        self._ss = None
        self._rot = 0           # rotating cursor for bounded overview polling

    def fetch(self, metro: str, limit: int = 400) -> list[dict]:
        feed = FEEDS.get(metro)
        if not feed:
            return []
        hit = self._cache.get(metro)
        if hit and time.time() - hit[0] < self.ttl_sec:
            return hit[1]

        try:
            if feed.kind == "pulsepoint":
                out = self._fetch_pulsepoint(feed)
                self._cache[metro] = (time.time(), out)
                return out
            if feed.kind == "p2c":
                out = self._fetch_p2c(feed)
                self._cache[metro] = (time.time(), out)
                return out
            if feed.kind == "southern":
                out = self._fetch_southern(feed)
                self._cache[metro] = (time.time(), out)
                return out
            if feed.kind == "arcgis":
                rows = self._fetch_arcgis(feed, limit)
            else:
                params = {"$limit": limit}
                if feed.time_field:
                    params["$order"] = f"{feed.time_field} DESC"
                resp = self._client.get(feed.url, params=params)
                resp.raise_for_status()
                rows = resp.json()
        except (httpx.HTTPError, ValueError) as e:
            print(f"[cad] {metro} fetch failed: {e}")
            return hit[1] if hit else []

        out = self._normalize(rows, feed)
        self._cache[metro] = (time.time(), out)
        return out

    def _fetch_pulsepoint(self, feed: CadFeed) -> list[dict]:
        """Fetch + normalize one PulsePoint agency's incidents (feed.url = agencyid)."""
        if self._pp is None:
            from apb.ingest.pulsepoint import PulsePoint
            self._pp = PulsePoint()
        out = []
        for i in self._pp.incidents(feed.url):
            if abs(i["lat"]) < 1e-6 and abs(i["lon"]) < 1e-6:
                continue          # some PulsePoint EMS feeds emit (0,0) null-island coords
            itype, threat = classify(i.get("type_raw", ""))
            out.append({
                "call_id": i["call_id"], "metro": feed.metro, "type": itype,
                "summary": i.get("type_raw") or itype, "location": i.get("address"),
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": threat >= 0.9,
                "lat": i["lat"], "lon": i["lon"], "at": i.get("at"),
                "ts": parse_ts(i.get("at")),
            })
        return out

    def _fetch_p2c(self, feed: CadFeed) -> list[dict]:
        """Fetch + normalize one PoliceToCitizen agency (feed.url = subdomain)."""
        if self._p2c is None:
            from apb.ingest.p2c import P2C
            self._p2c = P2C()
        out = []
        for i in self._p2c.incidents(feed.url):
            itype, threat = classify(i.get("type_raw", ""))
            out.append({
                "call_id": f"{feed.url}:{i['call_id']}", "metro": feed.metro,
                "type": itype, "summary": i.get("type_raw") or itype,
                "location": i.get("address"),
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": threat >= 0.9,
                "lat": i["lat"], "lon": i["lon"], "at": i.get("at"),
                "ts": parse_ts(i.get("at")),
            })
        return out

    def _fetch_southern(self, feed: CadFeed) -> list[dict]:
        """Fetch + normalize one Southern Software agency (feed.url = AgencyID)."""
        if self._ss is None:
            from apb.ingest.southern_software import SouthernSoftware
            self._ss = SouthernSoftware()
        out = []
        for i in self._ss.incidents(feed.url):
            itype, threat = classify(i.get("type_raw", ""))
            out.append({
                "call_id": f"{feed.url}:{i['call_id']}", "metro": feed.metro,
                "type": itype, "summary": i.get("type_raw") or itype,
                "location": i.get("address"),
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": threat >= 0.9,
                "lat": i["lat"], "lon": i["lon"], "at": i.get("at"),
                "ts": parse_ts(i.get("at")),
            })
        return out

    def _fetch_arcgis(self, feed: CadFeed, limit: int) -> list[dict]:
        """Query an ArcGIS FeatureServer layer as GeoJSON; embed geometry per row so
        the adaptive coordinate detector picks it up."""
        params = {"where": "1=1", "outFields": "*", "f": "geojson",
                  "resultRecordCount": limit}
        if feed.time_field:
            params["orderByFields"] = f"{feed.time_field} DESC"
        url = feed.url.rstrip("/")
        if not url.endswith("/query"):
            url += "/query"
        resp = self._client.get(url, params=params)
        resp.raise_for_status()
        feats = resp.json().get("features", [])
        return [{**(f.get("properties") or {}), "geometry": f.get("geometry")}
                for f in feats]

    def _normalize(self, rows: list[dict], feed: CadFeed) -> list[dict]:
        out: list[dict] = []
        # detect field names once per batch for adaptive feeds
        tkey = feed.type_field
        timekey = feed.time_field
        akey = feed.addr_field
        if feed.adaptive and rows:
            tkey = tkey or _detect_type_key(rows[0])
            timekey = timekey or _detect_time_key(rows[0])
            akey = akey or _detect_addr_key(rows[0])

        for r in rows:
            if feed.adaptive:
                lat, lon = _coords_from_row(r)
            else:
                lat, lon = self._coords(r, feed)
            if lat is None or lon is None or (abs(lat) < 1e-6 and abs(lon) < 1e-6):
                continue          # drop null-island (0,0) garbage coords
            raw_type = (r.get(tkey) if tkey else "") or ""
            itype, threat = classify(str(raw_type))
            at = r.get(timekey) if timekey else None
            out.append({
                "call_id": str(r.get(feed.id_field) or r.get("id") or len(out)),
                "metro": feed.metro, "type": itype,
                "summary": str(raw_type) or itype,
                "location": (r.get(akey) if akey else None),
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": threat >= 0.9,
                "lat": lat, "lon": lon, "at": at, "ts": parse_ts(at),
            })
        return out

    def overview(self, limit_per: int = 60, max_age_hours: float = 72.0,
                 pp_per_cycle: int = 150) -> list[dict]:
        """Aggregate RECENT incidents (national view). Polls all regular feeds plus a
        rotating slice of the (large) PulsePoint set each call — the DB poller + merge
        retain the rest, so load stays bounded and respectful. Cached ~60s."""
        ck = f"__overview__{max_age_hours}"
        hit = self._cache.get(ck)
        if hit and time.time() - hit[0] < 60.0:
            return hit[1]

        from concurrent.futures import ThreadPoolExecutor

        regular = [m for m, f in FEEDS.items() if f.kind != "pulsepoint"]
        pp = [m for m, f in FEEDS.items() if f.kind == "pulsepoint"]
        if pp:
            self._rot = (self._rot + pp_per_cycle) % len(pp)
            window = (pp + pp)[self._rot:self._rot + pp_per_cycle]
        else:
            window = []
        targets = regular + window

        def _safe(m):
            try:
                return self.fetch(m, limit_per)
            except Exception as e:           # one bad feed must not sink the overview
                print(f"[cad] overview: {m} failed: {e}")
                return []

        cutoff = time.time() - max_age_hours * 3600
        out: list[dict] = []
        with ThreadPoolExecutor(max_workers=16) as ex:
            for chunk in ex.map(_safe, targets):
                out.extend(d for d in chunk if d.get("ts") and d["ts"] >= cutoff)
        self._cache[ck] = (time.time(), out)
        return out

    @staticmethod
    def _coords(row: dict, feed: CadFeed) -> tuple[float | None, float | None]:
        if feed.point_field:
            pt = row.get(feed.point_field)
            if isinstance(pt, dict):
                if pt.get("coordinates"):
                    return float(pt["coordinates"][1]), float(pt["coordinates"][0])
                if pt.get("latitude") and pt.get("longitude"):
                    return float(pt["latitude"]), float(pt["longitude"])
            return None, None
        lat, lon = row.get(feed.lat_field), row.get(feed.lon_field)
        if lat in (None, "") or lon in (None, ""):
            return None, None
        try:
            return float(lat), float(lon)
        except (TypeError, ValueError):
            return None, None
