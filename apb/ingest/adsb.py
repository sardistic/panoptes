"""ADS-B aircraft activity — a novel direct-observation signal nobody else fuses.

Police/medevac helicopters orbiting a scene, or fixed-wing surveillance holds, are a
strong proxy for a major ground incident BEFORE it hits CAD/news. adsb.lol's public API
is keyless. We watch a set of metro centers, keep a short per-aircraft track history, and
flag LOITERING low-and-slow rotorcraft (tight position spread + wide heading spread =
circling) as event signals.

Single-fetch heuristic alone (low alt + low speed helicopter) is a weak hint; the
stateful tracker across polls is what makes "circling on-scene" detectable. Returns the
normalized incident-dict shape, registered as a hidden kind="adsb" feed.
"""
from __future__ import annotations

import math
import threading
import time
from collections import defaultdict, deque

import httpx

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_POINT = "https://api.adsb.lol/v2/point/{lat}/{lon}/{radius}"
_MIL = "https://api.adsb.lol/v2/mil"
# Same community v2 API on an independent aggregator; used when adsb.lol errors.
_POINT_ALT = "https://api.airplanes.live/v2/point/{lat}/{lon}/{radius}"
_MIL_ALT = "https://api.airplanes.live/v2/mil"
_MIN_INTERVAL = 1.3      # adsb.lol throttles rapid polling; keep ~1 req/1.3s
_PER_SCAN = 3            # metros polled per scan (rotates through WATCH)
_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

# Metros to watch (lat, lon). Radius in nautical miles (API max 250).
WATCH = [
    ("nyc", 40.7128, -74.0060), ("la", 34.0522, -118.2437),
    ("chicago", 41.8781, -87.6298), ("houston", 29.7604, -95.3698),
    ("phoenix", 33.4484, -112.0740), ("philadelphia", 39.9526, -75.1652),
    ("dc", 38.9072, -77.0369), ("atlanta", 33.7490, -84.3880),
    ("miami", 25.7617, -80.1918), ("dallas", 32.7767, -96.7970),
    ("seattle", 47.6062, -122.3321), ("sf", 37.7749, -122.4194),
    ("denver", 39.7392, -104.9903), ("vegas", 36.1716, -115.1391),
]
_RADIUS = 40

# Loiter thresholds
_MAX_ALT = 4000        # ft AGL-ish; on-scene rotor work is low
_MAX_GS = 130          # kt; circling is slow
_MIN_TRACK_SPREAD = 120  # deg of heading change across history = turning/orbiting
_MAX_POS_SPREAD_KM = 6   # stays within a tight box
_HISTORY = 6           # track points kept per aircraft
_HIST_TTL = 1800.0     # forget aircraft unseen for 30 min


def _is_rotor(ac: dict) -> bool:
    # category A7 = rotorcraft; many police/medevac also typed by ICAO type code.
    if ac.get("category") == "A7":
        return True
    t = str(ac.get("t") or "").upper()
    return t in {"EC35", "EC45", "A139", "AS50", "H60", "B407", "B429", "R44",
                 "S76", "AS65", "EC30", "B06", "H500", "MD52"}


def _spread(vals: list[float]) -> float:
    return max(vals) - min(vals) if vals else 0.0


def _km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class AdsbIngest:
    """Stateful across calls: each fetch updates per-aircraft history and emits loiterers."""

    def __init__(self):
        self._client = httpx.Client(timeout=25.0, headers=_UA, follow_redirects=True)
        # hex -> deque[(ts, lat, lon, track, alt, gs)]
        self._hist: dict[str, deque] = defaultdict(lambda: deque(maxlen=_HISTORY))
        self._meta: dict[str, dict] = {}
        self._last_seen: dict[str, float] = {}
        self._cursor = 0          # rotates through WATCH so each scan polls a few
        self._last_req = 0.0
        self._lock = threading.Lock()

    def _throttle(self) -> None:
        with self._lock:
            wait = _MIN_INTERVAL - (time.time() - self._last_req)
            if wait > 0:
                time.sleep(wait)
            self._last_req = time.time()

    def _get_ac(self, url: str, fallback: str) -> list[dict]:
        """Fetch an aircraft list, failing over to the alternate aggregator."""
        for u in (url, fallback):
            try:
                r = self._client.get(u)
                r.raise_for_status()
                return r.json().get("ac", []) or []
            except (httpx.HTTPError, ValueError):
                continue
        return []

    def _poll_point(self, lat: float, lon: float) -> list[dict]:
        self._throttle()
        return self._get_ac(_POINT.format(lat=lat, lon=lon, radius=_RADIUS),
                            _POINT_ALT.format(lat=lat, lon=lon, radius=_RADIUS))

    def _poll_mil(self) -> list[dict]:
        """Single cheap nationwide call: military aircraft (often surveillance holds)."""
        self._throttle()
        return self._get_ac(_MIL, _MIL_ALT)

    def _prune(self, now: float) -> None:
        for hx in [h for h, t in self._last_seen.items() if now - t > _HIST_TTL]:
            self._hist.pop(hx, None)
            self._meta.pop(hx, None)
            self._last_seen.pop(hx, None)

    def _ingest(self, ac: dict, metro: str, now: float) -> None:
        if not _is_rotor(ac):
            return
        hx = ac.get("hex")
        aclat, aclon = ac.get("lat"), ac.get("lon")
        if not hx or aclat is None or aclon is None:
            return
        alt = ac.get("alt_baro")
        self._hist[hx].append((now, float(aclat), float(aclon),
                               float(ac.get("track") or 0),
                               float(alt) if isinstance(alt, (int, float)) else 0.0,
                               float(ac.get("gs") or 0)))
        self._meta[hx] = {"flight": (ac.get("flight") or "").strip(),
                          "r": ac.get("r"), "t": ac.get("t"), "metro": metro}
        self._last_seen[hx] = now

    def scan(self) -> list[dict]:
        """Poll a ROTATING slice of watched metros (rate-limited) + the nationwide mil
        feed, update tracks, return loitering rotorcraft. History accumulates across calls
        so the full WATCH list is covered every few poller cycles."""
        now = time.time()
        window = (WATCH + WATCH)[self._cursor:self._cursor + _PER_SCAN]
        self._cursor = (self._cursor + _PER_SCAN) % len(WATCH)
        for metro, lat, lon in window:
            for ac in self._poll_point(lat, lon):
                self._ingest(ac, metro, now)
        for ac in self._poll_mil():
            self._ingest(ac, "mil", now)
        self._prune(now)
        return self._loiterers(now)

    def _loiterers(self, now: float) -> list[dict]:
        out = []
        for hx, hist in self._hist.items():
            if len(hist) < 3:
                continue
            lats = [p[1] for p in hist]
            lons = [p[2] for p in hist]
            tracks = [p[3] for p in hist]
            alts = [p[4] for p in hist]
            gss = [p[5] for p in hist]
            pos_spread = _km(min(lats), min(lons), max(lats), max(lons))
            # circular heading spread (handle wrap via two framings)
            t_spread = min(_spread(tracks), _spread([(t + 180) % 360 for t in tracks]))
            mean_alt = sum(alts) / len(alts)
            mean_gs = sum(gss) / len(gss)
            if not (pos_spread <= _MAX_POS_SPREAD_KM and t_spread >= _MIN_TRACK_SPREAD
                    and mean_alt <= _MAX_ALT and mean_gs <= _MAX_GS):
                continue
            m = self._meta.get(hx, {})
            label = m.get("flight") or m.get("r") or hx
            threat = 0.65 if pos_spread <= 3 else 0.5
            out.append({
                "call_id": f"adsb:{hx}", "metro": "adsb", "type": "suspicious",
                "summary": f"Helicopter loitering ({label}, {m.get('t','?')}) — "
                           f"orbit ~{pos_spread:.1f}km @ {int(mean_alt)}ft",
                "source": "adsb", "location": m.get("metro"),
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": False,
                "lat": sum(lats) / len(lats), "lon": sum(lons) / len(lons),
                "at": None, "ts": now,
            })
        return out
