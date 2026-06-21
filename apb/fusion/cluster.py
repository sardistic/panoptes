"""Cross-source event clustering and surge scoring."""
from __future__ import annotations

import math
import time
from collections import defaultdict
from dataclasses import dataclass, field

from apb.common.models import EventSignal

CELL_DEG = 0.018


@dataclass
class FusedEvent:
    event_id: str
    lat: float
    lon: float
    count: int
    source_count: int
    sources: dict[str, int]
    types: dict[str, int]
    peak_severity: float
    mean_severity: float
    confidence: float
    surge_score: float
    latest_ts: float
    summaries: list[str] = field(default_factory=list)
    signal_ids: list[str] = field(default_factory=list)


def detect(
    signals: list[EventSignal],
    *,
    min_count: int = 2,
    min_sources: int = 1,
    min_score: float = 1.2,
    max_age_hours: float = 24.0,
    top: int = 50,
) -> list[FusedEvent]:
    """Cluster signals into live events.

    Score rewards recency, severity, confidence, and independent source diversity.
    This is intentionally conservative: one noisy source can make a warm dot, but
    different source kinds in the same place/time make it climb.
    """
    now = time.time()
    cutoff = now - max_age_hours * 3600
    cells: dict[tuple[int, int], list[EventSignal]] = defaultdict(list)
    for s in signals:
        if s.lat is None or s.lon is None:
            continue
        ts = s.observed_at.timestamp()
        if ts < cutoff:
            continue
        cells[(round(s.lat / CELL_DEG), round(s.lon / CELL_DEG))].append(s)

    events: list[FusedEvent] = []
    for key, pts in cells.items():
        if len(pts) < min_count:
            continue
        latest = max(p.observed_at.timestamp() for p in pts)
        sources: dict[str, int] = defaultdict(int)
        corroborating_sources: dict[str, int] = defaultdict(int)
        types: dict[str, int] = defaultdict(int)
        for p in pts:
            sources[p.source_kind.value] += 1
            types[p.normalized_type.value] += 1
            if p.source_kind.value == "cad" or (
                p.normalized_type.value != "other" and p.severity >= 0.35
            ):
                corroborating_sources[p.source_kind.value] += 1
        source_count = len(corroborating_sources)
        if source_count < min_sources:
            continue
        severities = [max(0.0, min(1.0, p.severity)) for p in pts]
        confidences = [max(0.0, min(1.0, p.confidence)) for p in pts]
        mean_sev = sum(severities) / len(severities)
        mean_conf = sum(confidences) / len(confidences)
        recency = max(0.25, 1.0 - ((now - latest) / max(1.0, max_age_hours * 3600)))
        diversity = 1.0 + math.log2(source_count)
        volume = math.sqrt(len(pts))
        score = round(volume * (0.35 + mean_sev) * (0.5 + mean_conf) * diversity * recency, 2)
        if score < min_score:
            continue
        pts_sorted = sorted(pts, key=lambda p: p.observed_at, reverse=True)
        events.append(FusedEvent(
            event_id=f"evt:{key[0]}:{key[1]}:{int(latest)}",
            lat=sum(p.lat or 0 for p in pts) / len(pts),
            lon=sum(p.lon or 0 for p in pts) / len(pts),
            count=len(pts),
            source_count=source_count,
            sources=dict(sorted(sources.items(), key=lambda x: -x[1])),
            types=dict(sorted(types.items(), key=lambda x: -x[1])),
            peak_severity=round(max(severities), 2),
            mean_severity=round(mean_sev, 2),
            confidence=round(mean_conf, 2),
            surge_score=score,
            latest_ts=latest,
            summaries=[p.summary for p in pts_sorted[:5]],
            signal_ids=[p.signal_id for p in pts_sorted[:30]],
        ))

    events.sort(key=lambda e: e.surge_score, reverse=True)
    return events[:top]
