"""Source collection helpers for the fusion layer."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterable

from apb.common.models import EventSignal, SignalKind
from apb.fusion.places import resolve_place
from apb.fusion.signals import incident_to_signal, text_signal
from apb.ingest.cad import CadIngest
from apb.store import snapshots


def cad_signals(
    cad: CadIngest,
    *,
    limit_per: int = 60,
    max_age_hours: float = 24.0,
    include_live: bool = True,
) -> list[EventSignal]:
    """Fetch live CAD plus accumulated snapshot history as normalized signals."""
    merged = {(d["metro"], str(d["call_id"])): d for d in snapshots.query(max_age_hours)}
    if include_live:
        live = cad.overview(limit_per=limit_per, max_age_hours=max_age_hours)
        for d in live:
            merged[(d["metro"], str(d["call_id"]))] = d
    return [incident_to_signal(d) for d in merged.values()]


def gather_signals(
    cad: CadIngest,
    *,
    limit_per: int = 60,
    max_age_hours: float = 24.0,
    include_seed: bool = True,
    include_live: bool = True,
    kinds: set[str] | None = None,
) -> list[EventSignal]:
    """The fusion substrate: CAD live+history, seed rows, social + news buffers —
    all time-windowed to max_age_hours and optionally filtered by sensor family.
    Single source of truth for /live/signals and /live/fused."""
    from apb.fusion import news_store, social_store
    signals = cad_signals(cad, limit_per=limit_per, max_age_hours=max_age_hours,
                          include_live=include_live)
    if include_seed:
        signals.extend(seed_recent(max_age_hours))
    signals.extend(social_store.recent(max_age_hours))   # live Bluesky + social RSS
    signals.extend(news_store.recent(max_age_hours))     # live news RSS
    from apb.fusion.dedupe import dedupe_signals
    signals = dedupe_signals(signals)   # one phenomenon != multi-source corroboration
    if kinds is not None:
        signals = [s for s in signals if s.source_kind.value in kinds]
    return signals


def seed_recent(max_age_hours: float) -> list[EventSignal]:
    """Seed rows are offline test data — drop stale ones so live windows stay live."""
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_hours * 3600
    return [s for s in load_seed_signals() if s.observed_at.timestamp() >= cutoff]


def social_text_signals(rows: Iterable[dict]) -> list[EventSignal]:
    """Normalize already-collected social/news rows.

    Row shape is intentionally loose:
    {"source","text","lat","lon","metro","location","url","created_at"}.
    This lets early scrapers, manual seed files, or future stream consumers plug in
    without changing the fusion endpoint.
    """
    out: list[EventSignal] = []
    for r in rows:
        text = r.get("text") or r.get("summary") or ""
        if not text:
            continue
        created = r.get("created_at") or r.get("at")
        observed = _parse_dt(created)
        place = None
        if r.get("lat") is None or r.get("lon") is None:
            place = resolve_place(text)
        lat = r.get("lat") if r.get("lat") is not None else (place.lat if place else None)
        lon = r.get("lon") if r.get("lon") is not None else (place.lon if place else None)
        metro = r.get("metro") or (place.metro if place else None)
        location = r.get("location") or (place.name if place else None)
        out.append(text_signal(
            source=r.get("source") or "social",
            source_kind=SignalKind(r.get("source_kind") or SignalKind.social.value),
            text=text,
            observed_at=observed,
            lat=lat,
            lon=lon,
            metro=metro,
            location_text=location,
            url=r.get("url"),
            confidence=float(r.get("confidence", place.confidence if place else 0.45)),
            metadata={k: v for k, v in r.items()
                      if k not in {"text", "summary", "lat", "lon", "url"}},
        ))
    return out


def load_seed_signals(path: str = "data/social_seed.jsonl") -> list[EventSignal]:
    """Load optional local social/news seed rows for offline fusion experiments."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            rows = [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []
    return social_text_signals(rows)


def _parse_dt(v) -> datetime | None:
    if not v:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(float(v), tz=timezone.utc)
    try:
        s = str(v).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
