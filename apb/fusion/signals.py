"""Normalize heterogeneous source rows into EventSignal objects.

The goal is to let weak signals reinforce each other: CAD rows, radio activity,
social posts, traffic alerts, weather, and news can all become comparable event
hints before clustering.
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

from apb.common.models import EventSignal, IncidentType, SignalKind
from apb.ingest.cad import classify, parse_ts

_SOCIAL_RULES: list[tuple[str, tuple[IncidentType, float]]] = [
    (r"\b(active shooter|shots fired|gunshots?|shooting|gunfire)\b",
     (IncidentType.shots_fired, 0.9)),
    (r"\b(stabbing|stabbed|assault|fight|homicide|murder)\b",
     (IncidentType.assault, 0.75)),
    (r"\b(robbery|carjack|burglary|looting)\b", (IncidentType.robbery, 0.65)),
    (r"\b(fire|smoke|explosion|evacuat|wildfire|hazmat)\b",
     (IncidentType.fire, 0.7)),
    (r"\b(crash|collision|accident|pileup|road closed|traffic jam)\b",
     (IncidentType.traffic, 0.5)),
    (r"\b(overdose|unconscious|cpr|ambulance|mass casualty)\b",
     (IncidentType.medical, 0.65)),
    (r"\b(police chase|pursuit|manhunt|standoff)\b", (IncidentType.pursuit, 0.8)),
    (r"\b(shelter in place|lockdown|bomb threat|suspicious package)\b",
     (IncidentType.suspicious, 0.7)),
]


# Map a normalized incident row's `source` to its sensor family, so hazard / traffic
# / aircraft / authority feeds that flow through the CAD overview are categorized
# correctly for filtering instead of all collapsing to `cad`.
_SOURCE_KIND: dict[str, SignalKind] = {
    "usgs": SignalKind.weather, "nws": SignalKind.weather, "eonet": SignalKind.weather,
    "firms": SignalKind.weather,
    "511": SignalKind.traffic,
    "adsb": SignalKind.aircraft, "aisstream": SignalKind.traffic,
    "fema": SignalKind.context, "faa_tfr": SignalKind.context,
    "odin": SignalKind.context,
    "usgs_flood": SignalKind.weather, "openaq": SignalKind.weather,
    "volcano": SignalKind.weather, "hms_smoke": SignalKind.weather,
    "ndbc": SignalKind.weather, "spc": SignalKind.weather, "nhc": SignalKind.weather,
    "faa_delay": SignalKind.traffic, "nifc_fire": SignalKind.weather,
}


def _id(*parts: object) -> str:
    h = hashlib.sha1("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    return h[:20]


def _dt_from_ts(ts: float | None, fallback: object = None) -> datetime:
    if ts:
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    parsed = parse_ts(fallback)
    if parsed:
        return datetime.fromtimestamp(parsed, tz=timezone.utc)
    return datetime.now(timezone.utc)


def incident_to_signal(d: dict) -> EventSignal:
    """Convert a normalized CAD/live incident dict into an EventSignal."""
    source = str(d.get("source") or "cad")
    call_id = str(d.get("call_id") or _id(source, d.get("summary"), d.get("ts")))
    raw_type = str(d.get("summary") or d.get("type") or "")
    norm = d.get("type") or classify(raw_type)[0]
    return EventSignal(
        signal_id=f"{source}:{d.get('metro','?')}:{call_id}",
        source=source,
        source_kind=_SOURCE_KIND.get(source, SignalKind.cad),
        observed_at=_dt_from_ts(d.get("ts"), d.get("at")),
        lat=d.get("lat"),
        lon=d.get("lon"),
        metro=d.get("metro"),
        location_text=d.get("location"),
        raw_type=raw_type,
        normalized_type=IncidentType(norm if norm in IncidentType._value2member_map_ else "other"),
        summary=str(d.get("summary") or raw_type or norm),
        confidence=0.85,
        severity=float(d.get("threat_score", 0.3) or 0.3),
        dedupe_key=f"cad:{d.get('metro')}:{call_id}",
        metadata={"sentiment": d.get("sentiment"), "emerging": d.get("emerging")},
    )


def text_type(text: str) -> tuple[IncidentType, float]:
    """Classify a social/news/traffic snippet with cheap keyword rules."""
    t = text.lower()
    for pat, result in _SOCIAL_RULES:
        if re.search(pat, t):
            return result
    # Do not fall back to CAD's substring classifier for public text. It is tuned for
    # terse dispatch labels, while social text has words like "fireplace", "Starfire",
    # "photoshoot", and metaphorical "crash" that create noisy false positives.
    return IncidentType.other, 0.2


def text_signal(
    *,
    source: str,
    source_kind: SignalKind,
    text: str,
    observed_at: datetime | None = None,
    lat: float | None = None,
    lon: float | None = None,
    metro: str | None = None,
    location_text: str | None = None,
    url: str | None = None,
    confidence: float = 0.45,
    metadata: dict | None = None,
) -> EventSignal:
    """Create a signal from unstructured text, such as social or news snippets."""
    norm, severity = text_type(text)
    when = observed_at or datetime.now(timezone.utc)
    sid = _id(source, when.timestamp(), text[:160], lat, lon)
    return EventSignal(
        signal_id=f"{source}:{sid}",
        source=source,
        source_kind=source_kind,
        observed_at=when,
        lat=lat,
        lon=lon,
        metro=metro,
        location_text=location_text,
        raw_type=None,
        normalized_type=norm,
        summary=text[:280],
        confidence=confidence,
        severity=severity,
        url=url,
        dedupe_key=f"{source}:{sid}",
        metadata=metadata or {},
    )


def dict_signal(s: EventSignal) -> dict:
    """API-friendly signal payload."""
    d = s.model_dump(mode="json")
    d["type"] = d.pop("normalized_type")
    d["threat_score"] = d.pop("severity")
    d["at"] = s.observed_at.isoformat()
    d["ts"] = s.observed_at.timestamp()
    return d
