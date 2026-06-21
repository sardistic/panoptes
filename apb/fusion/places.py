"""Lightweight place resolution for unstructured public text.

Most social/news snippets do not carry coordinates. This module only resolves
explicit place mentions to coarse metro centroids so those posts can corroborate
official CAD/radio signals without pretending to know an exact incident address.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache

from apb.common.config import load_metros


@dataclass(frozen=True)
class Place:
    metro: str
    name: str
    lat: float
    lon: float
    confidence: float = 0.35


_EXTRA_PLACES: tuple[tuple[str, str, float, float, tuple[str, ...]], ...] = (
    ("seattle", "Seattle", 47.6062, -122.3321, ("sea",)),
    ("sf_bay", "San Francisco", 37.7749, -122.4194, ("sf", "san francisco")),
    ("oakland", "Oakland", 37.8044, -122.2712, ()),
    ("dc", "Washington DC", 38.9072, -77.0369, ("washington dc", "d.c.", "dc")),
    ("atlanta", "Atlanta", 33.7490, -84.3880, ()),
    ("miami", "Miami", 25.7617, -80.1918, ()),
    ("dallas", "Dallas", 32.7767, -96.7970, ()),
    ("denver", "Denver", 39.7392, -104.9903, ()),
    ("las_vegas", "Las Vegas", 36.1716, -115.1391, ("north las vegas", "vegas")),
    ("boston", "Boston", 42.3601, -71.0589, ()),
    ("philadelphia", "Philadelphia", 39.9526, -75.1652, ("philly",)),
    ("toronto", "Toronto", 43.6532, -79.3832, ()),
    ("vancouver_bc", "Vancouver BC", 49.2827, -123.1207, ("vancouver",)),
    ("calgary", "Calgary", 51.0447, -114.0719, ()),
    ("edmonton", "Edmonton", 53.5461, -113.4938, ()),
    ("winnipeg", "Winnipeg", 49.8951, -97.1384, ()),
    ("ottawa", "Ottawa", 45.4215, -75.6972, ()),
    ("montreal", "Montreal", 45.5019, -73.5674, ("montréal",)),
    ("cdmx", "Mexico City", 19.4326, -99.1332, ("mexico city", "ciudad de mexico", "cdmx")),
    ("guadalajara", "Guadalajara", 20.6597, -103.3496, ()),
    ("monterrey", "Monterrey", 25.6866, -100.3161, ()),
    ("tijuana", "Tijuana", 32.5149, -117.0382, ()),
)


@lru_cache(maxsize=1)
def places() -> list[tuple[Place, tuple[str, ...]]]:
    out: list[tuple[Place, tuple[str, ...]]] = []
    try:
        for metro, systems in load_metros().items():
            system = systems[0] if systems else None
            if not system or not system.centroid:
                continue
            name = metro.replace("_", " ").title()
            aliases = (metro, name.lower())
            out.append((Place(metro, name, system.centroid[0], system.centroid[1]), aliases))
    except Exception:
        pass
    for metro, name, lat, lon, aliases in _EXTRA_PLACES:
        all_aliases = (name.lower(), metro.replace("_", " "), *aliases)
        out.append((Place(metro, name, lat, lon), tuple(dict.fromkeys(all_aliases))))
    return out


# ── GeoNames gazetteer (worldwide cities >=15k pop) ──────────────────────────
_GAZ_PATH = "data/gazetteer.tsv"
_WORD = re.compile(r"[a-z][a-z'’-]+")
# common English words that are ALSO city names — never match as a lone token
_COMMON = frozenset((
    "nice split reading mobile general best most union march may bar man same born "
    "of as is are why how who industry mission liberty victory hope eden surprise "
    "boring normal paradise sandwich average"
).split())


@lru_cache(maxsize=1)
def _gaz() -> dict[str, tuple[float, float, int]]:
    """name -> (lat, lon, population). Periods stripped so 'st louis' matches."""
    out: dict[str, tuple[float, float, int]] = {}
    try:
        with open(_GAZ_PATH, encoding="utf-8") as f:
            for line in f:
                p = line.rstrip("\n").split("\t")
                if len(p) < 4:
                    continue
                name = p[0].replace(".", "").strip()
                try:
                    out[name] = (float(p[1]), float(p[2]), int(p[3]))
                except ValueError:
                    continue
    except OSError:
        pass
    return out


def _gaz_match(text: str) -> Place | None:
    """Scan text for the most salient city mention (prefer multi-word, then population)."""
    gaz = _gaz()
    if not gaz:
        return None
    toks = _WORD.findall(text.lower())
    n = len(toks)
    best = None  # (score, lat, lon, name, pop, size)
    for i in range(n):
        for size in (3, 2, 1):
            if i + size > n:
                continue
            phrase = " ".join(toks[i:i + size])
            hit = gaz.get(phrase)
            if not hit:
                continue
            lat, lon, pop = hit
            if size == 1 and (len(phrase) < 4 or phrase in _COMMON or pop < 50000):
                continue                       # lone common/small names are too noisy
            score = size * 10_000_000 + pop     # multi-word wins, then bigger city
            if best is None or score > best[0]:
                best = (score, lat, lon, phrase, pop, size)
    if not best:
        return None
    _, lat, lon, name, pop, size = best
    conf = round(min(0.55, 0.28 + 0.08 * size + min(0.15, pop / 3_000_000)), 2)
    return Place("geo_" + name.replace(" ", "_"), name.title(), lat, lon, conf)


def resolve_place(text: str) -> Place | None:
    """Coarse place match: curated metros (high confidence) then the GeoNames gazetteer
    (worldwide). Returns None when no city is named."""
    t = f" {text.lower()} "
    for place, aliases in places():
        for alias in aliases:
            a = alias.strip().lower()
            if a and re.search(rf"(?<![a-z0-9]){re.escape(a)}(?![a-z0-9])", t):
                return place
    return _gaz_match(text)
