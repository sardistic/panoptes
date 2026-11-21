"""What else is going on here: every feed's items near a place and time, ranked.

Given a point (and optionally a time, an incident type and the streets a call named),
`related()` scores items from all the feeds — CAD incidents, radio incidents and lines,
hazards and warnings, news and social posts, cameras, corroborated events — by

  distance     exp(-km / (radius/2)); a warning polygon that covers the point is 0 km
  time         exp(-|minutes| / (window/2)); ongoing items without a time count 0.7
  type         x1.5 when the item is the same family (fire, medical, traffic, ...)
  street       x2 when the item's text names a street the call named (matched on the
               server — the street names themselves are never returned for radio)

and returns the best, each with a short "why". Radio items are published on their
~2 km cell only, as everywhere else.
"""
from __future__ import annotations

import math
import re

FAMILIES = {
    "violence": r"\b(?:shots?|shooting|assault|robbery|domestic|weapon|stabb\w*|violen\w*|unrest|riot)",
    "fire": r"\b(?:fire|smoke|burning|red flag|hotspot|wildfire|hazmat|explosion)",
    "medical": r"\b(?:medical|ems|overdose|cardiac|injur\w*|welfare|breathing)",
    "traffic": r"\b(?:traffic|crash|collision|vehicle|pursuit|accident|closure)",
    "weather": r"\b(?:storm|tornado|flood\w*|wind|hail|winter|snow|heat|thunder\w*)",
    "outage": r"\b(?:outage|power|internet)",
    "quake": r"\b(?:quake|earthquake|seismic)",
}

_STREET = re.compile(
    r"\b((?:[A-Z0-9][\w'.-]*\s){0,3}?[A-Z0-9][\w'.-]*)\s+(Road|Rd|Street|St|Avenue|Ave|Drive|Dr|Lane|Ln|"
    r"Court|Ct|Boulevard|Blvd|Way|Place|Pl|Parkway|Pkwy|Pike|Highway|Hwy|Trail|Trl|Circle|Cir|Terrace)\b\.?")
_ROUTE = re.compile(r"\b(?:I|US|SR|CR|NC|SC|VA|GA|TN|CA|FL|NY|NJ|PA|OH|MI|TX|AZ|Interstate|Highway|Hwy|Route|Rte)"
                    r"[\s-]*(\d{1,4})\b", re.I)


def family(text: str | None) -> str | None:
    t = str(text or "").lower().replace("_", " ")
    for fam, pat in FAMILIES.items():
        if re.search(pat, t):
            return fam
    return None


def streets(text: str | None) -> set[str]:
    """Street keys named in free text ("Oak Ave", "I-26", "NC 280") — see roads.name_key."""
    from apb.context.roads import name_key
    out = set()
    if not text:
        return out
    for m in _STREET.finditer(text):
        k = name_key(f"{m.group(1)} {m.group(2)}")
        if k:
            out.add(k)
    for m in _ROUTE.finditer(text):
        out.add("r:" + m.group(1).lower())
    return out


def km(a: tuple, b: tuple) -> float:
    lat = math.radians((a[0] + b[0]) / 2)
    return math.hypot(a[0] - b[0], (a[1] - b[1]) * math.cos(lat)) * 111.2


def _in_ring(pt: tuple, ring: list) -> bool:
    x, y, inside = pt[1], pt[0], False
    for i in range(len(ring)):
        x1, y1 = ring[i][0], ring[i][1]
        x2, y2 = ring[i - 1][0], ring[i - 1][1]
        if (y1 > y) != (y2 > y) and x < (x2 - x1) * (y - y1) / ((y2 - y1) or 1e-12) + x1:
            inside = not inside
    return inside


def covers(geometry: dict | None, pt: tuple) -> bool:
    """Does a GeoJSON Polygon / MultiPolygon cover (lat, lon)?"""
    if not geometry:
        return False
    t, c = geometry.get("type"), geometry.get("coordinates") or []
    polys = [c] if t == "Polygon" else c if t == "MultiPolygon" else []
    return any(p and _in_ring(pt, p[0]) and not any(_in_ring(pt, h) for h in p[1:]) for p in polys)


def score(origin: tuple, item: dict, *, ts: float | None, radius_km: float, window_min: float,
          fam: str | None, keys: set[str]) -> dict | None:
    """The item's relevance with its reasons, or None when it is out of range."""
    if item.get("lat") is None:
        return None
    inside = covers(item.get("geometry"), origin)
    d = 0.0 if inside else km(origin, (item["lat"], item["lon"]))
    if d > radius_km and not inside:
        return None
    why = ["covers this place" if inside else f"{d:.1f} km away" if d >= 0.1 else "same spot"]
    s = math.exp(-d / max(0.1, radius_km / 2))
    mins = None
    if ts and item.get("ts"):
        mins = (item["ts"] - ts) / 60.0
        if abs(mins) > window_min and not item.get("ongoing"):
            return None
        s *= math.exp(-abs(mins) / max(1.0, window_min / 2)) if not item.get("ongoing") else 0.8
        if abs(mins) >= 1:
            why.append(f"{abs(mins):.0f} min {'earlier' if mins < 0 else 'later'}")
        else:
            why.append("same minute")
    else:
        s *= 0.7
    ifam = family(item.get("type")) or family(item.get("title"))   # the type field first
    if fam and ifam == fam:
        s *= 1.5
        why.append(f"also {fam}")
    if keys and streets(item.get("_text") or item.get("title")) & keys:
        s *= 2.0
        why.append("same street")
    out = {k: v for k, v in item.items() if not k.startswith("_") and k != "geometry"}
    return {**out, "km": round(d, 2), "minutes": round(mins, 1) if mins is not None else None,
            "score": round(s, 3), "why": why}


def related(origin: tuple, items: list[dict], *, ts: float | None = None, radius_km: float = 3.0,
            window_min: float = 90.0, type_: str | None = None, text: str | None = None,
            exclude: set[str] | None = None, limit: int = 12, per_kind: int = 4) -> list[dict]:
    """Rank `items` (dicts with kind, id, lat, lon, ts, type, title, ...) around origin."""
    fam = family(type_) or family(text)
    keys = streets(text)
    exclude = exclude or set()
    scored = []
    for it in items:
        if f"{it.get('kind')}:{it.get('id')}" in exclude:
            continue
        r = score(origin, it, ts=ts, radius_km=radius_km, window_min=window_min, fam=fam, keys=keys)
        if r and r["score"] >= 0.05:
            scored.append(r)
    scored.sort(key=lambda r: -r["score"])
    out, per = [], {}
    for r in scored:
        if per.get(r["kind"], 0) >= per_kind:
            continue
        per[r["kind"]] = per.get(r["kind"], 0) + 1
        out.append(r)
        if len(out) >= limit:
            break
    return out


# ── item builders: each feed's rows as related items ───────────────────────────

def incident_items(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        if r.get("lat") is None:
            continue
        radio = str(r.get("metro") or "").startswith("radio_")
        out.append({"kind": "radio" if radio else "incident", "id": r.get("call_id"),
                    "lat": r["lat"], "lon": r["lon"], "ts": r.get("ts") or r.get("last_seen"),
                    "type": r.get("type"), "title": r.get("summary") or r.get("type"),
                    "sub": r.get("location") or r.get("metro"), "severity": r.get("threat_score"),
                    "_text": f"{r.get('summary') or ''} {r.get('location') or ''}", "ref": {
                        k: r.get(k) for k in ("metro", "call_id", "type", "summary", "location", "sentiment",
                                              "threat_score", "ts", "lat", "lon")}})
    return out


def thread_items(threads: list[dict]) -> list[dict]:
    from apb.context.radio_areas import clean
    out = []
    for t in threads:
        if t.get("lat") is None:
            continue
        out.append({"kind": "radio", "id": f"rthr{t['tid']}", "thread": t["tid"], "lat": t["lat"], "lon": t["lon"],
                    "ts": t.get("last_ts"), "type": t.get("type"), "title": clean(t.get("title")),
                    "sub": f"{len(t.get('calls') or [])} radio calls · {t.get('area') or ''}",
                    "severity": t.get("priority"), "_text": t.get("loc_text")})
    return out


def hazard_items(rows: list[dict]) -> list[dict]:
    out = []
    skip = {"adsb", "aisstream", "squawk", "faa_delay", "radar", "openaq", "airnow"}
    for r in rows:
        if r.get("lat") is None or r.get("source") in skip:
            continue
        title = r.get("event") or r.get("headline") or r.get("summary") or r.get("type") or r.get("source")
        out.append({"kind": "hazard", "id": f"{r.get('source')}:{r.get('id') or r.get('event_id') or round(r['lat'], 4)}",
                    "lat": r["lat"], "lon": r["lon"], "ts": r.get("ts"), "ongoing": bool(r.get("expires") or r.get("geometry")),
                    "type": r.get("type") or r.get("event"), "title": str(title)[:140],
                    "sub": f"{r.get('source')} · {r.get('location') or ''}".strip(" ·"),
                    "severity": r.get("threat_score"), "geometry": r.get("geometry"),
                    "_text": f"{r.get('summary') or ''} {r.get('headline') or ''} {r.get('location') or ''}"})
    return out


def signal_items(signals: list) -> list[dict]:
    out = []
    for s in signals:
        if s.lat is None:
            continue
        kind = s.source_kind.value
        out.append({"kind": "news" if kind == "news" else "social", "id": s.signal_id, "lat": s.lat, "lon": s.lon,
                    "ts": s.observed_at.timestamp(), "type": s.normalized_type.value, "title": s.summary[:160],
                    "sub": s.source, "url": s.url, "severity": s.severity, "_text": s.summary})
    return out


def camera_items(cams: list[dict]) -> list[dict]:
    return [{"kind": "camera", "id": c.get("uid") or c.get("id"), "lat": c["lat"], "lon": c["lon"], "ts": None,
             "ongoing": True, "title": c.get("name") or c.get("title") or "camera", "sub": c.get("source"),
             "image": c.get("image_url")} for c in cams if c.get("lat") is not None]


def fused_items(events: list[dict]) -> list[dict]:
    return [{"kind": "fused", "id": e["event_id"], "lat": e["lat"], "lon": e["lon"], "ts": e.get("latest_ts"),
             "type": next(iter(e.get("types") or {}), None),
             "title": f"Corroborated: {' + '.join(e.get('sources') or {})}",
             "sub": f"{e.get('count')} signals · score {e.get('surge_score')}", "severity": e.get("peak_severity"),
             "_text": " ".join(e.get("summaries") or [])} for e in events if e.get("lat") is not None]

