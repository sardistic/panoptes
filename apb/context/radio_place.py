"""Placing radio incidents: the most precise place a call names, resolved internally.

Order of evidence (best first):
  address       — a call in the thread was geocoded to a block address (Census), or the
                  thread's place is a block address
  intersection  — "NC 280 at Deep Woods": the node where two named OpenStreetMap roads
                  meet near the town (local junction index, apb.context.roads)
  town          — the town's gazetteer point

Whatever the precision, what leaves the server is the ~2 km cell (radio_stt.coarse)
plus the precision word — never the address or the intersection itself.
"""
from __future__ import annotations

import logging
import re

log = logging.getLogger(__name__)

_SPLIT = re.compile(r"\s+(?:and|&|at|/|near|off|x|by|between|(?:north|south|east|west)\s+of)\s+", re.I)
_TRAVEL = re.compile(r"\b(?:north|south|east|west)bound\b|\b(?:[nsew]b)\b|\b(?:on-?|off-?)?ramps?\b|"
                     r"\bexit\b|\binterchange\b|\bthe\s+", re.I)
_ROADY = re.compile(r"\b(?:road|rd|street|st|avenue|ave|drive|dr|lane|ln|court|ct|boulevard|blvd|way|"
                    r"place|pl|circle|cir|terrace|trail|parkway|pkwy|pike|highway|hwy|route|rte)\.?$|"
                    r"^(?:I|US|SR|NC|SC|VA|GA|TN|CA|FL|Interstate|Highway|Hwy|Route)\s*-?\s*\d", re.I)


def _tidy(part: str) -> str:
    return re.sub(r"\s+", " ", _TRAVEL.sub(" ", part)).strip(" .,-")


def split_where(where: str | None) -> tuple[str, str] | None:
    """'NC 280 at Deep Woods Dr' -> ('NC 280', 'Deep Woods Dr'); anything else None."""
    if not where or "[REDACTED]" in where:
        return None
    parts = [_tidy(p) for p in _SPLIT.split(where.strip(), maxsplit=1)]
    if len(parts) == 2:
        parts[1] = _tidy(_SPLIT.split(parts[1], maxsplit=1)[0])   # "X between A and B" -> (X, A)
    if len(parts) != 2 or not all(parts) or any(re.match(r"^\d+(?!st|nd|rd|th)\b", p, re.I) for p in parts):
        return None
    return parts[0], parts[1]


def one_street(where: str | None) -> str | None:
    """'600 block of Bannon Street' / 'West Street' -> the street name, when the place is
    one named road (not a pair, not a landmark)."""
    if not where or "[REDACTED]" in where or split_where(where):
        return None
    s = re.sub(r"^\s*\d{1,6}\s*(?:hundred\s+)?(?:block|blk)\s*(?:of\s+)?", "", where.strip(), flags=re.I)
    s = _tidy(re.sub(r"^\s*\d{1,6}\s+", "", s))
    return s if _ROADY.search(s) else None


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
          geocode=None, crosser=None, streeter=None) -> tuple[tuple | None, str | None]:
    """((lat, lon) exact, precision) for an incident — internal; publish coarse() of it."""
    from apb.context import radio_stt, roads
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
        anchor, radius = (tpt, 10000) if tpt else ((near, 20000) if near and not approx else (None, 0))
        if anchor:
            pt = (crosser or roads.cross)(pair[0], pair[1], anchor, radius)
            if pt:
                return pt, "intersection"
    street = one_street(where)
    if street:
        anchor, radius = (tpt, 10000) if tpt else ((near, 20000) if near and not approx else (None, 0))
        if anchor:
            pt = (streeter or roads.street)(street, anchor, radius)
            if pt:
                return pt, "street"
    if tpt:
        return tpt, "town"
    return None, None
