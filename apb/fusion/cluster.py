"""Cross-source event clustering and surge scoring."""
from __future__ import annotations

import math
import time
from collections import defaultdict
from dataclasses import dataclass, field

from apb.common.grid import bucket, neighborhoods
from apb.common.models import EventSignal


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
    assessment: str = "single_source"
    score_components: dict[str, float] = field(default_factory=dict)


_FUTURE_SLACK_SEC = 900.0     # tolerate clock skew, nothing more
_COARSE_WEIGHT = 0.25         # a centroid-placed mention is a quarter of a located one
# corroborate on severity alone: these sensors observe a place, they do not describe it
_OBSERVER_KINDS = {"camera", "sensor", "radio_metadata"}
_TEXT_KINDS = {"social", "news"}


def is_coarse(p: EventSignal) -> bool:
    """True for text signals placed by name ("...in Seattle") rather than by position."""
    if p.source_kind.value not in _TEXT_KINDS:
        return False
    return bool(p.metadata.get("coarse")) or p.confidence < 0.5


def _drop_repeats(pts: list[EventSignal]) -> list[EventSignal]:
    """One headline syndicated five times is one observation, not five."""
    seen: set[tuple[str, str]] = set()
    out: list[EventSignal] = []
    for p in pts:
        if p.source_kind.value in _TEXT_KINDS:
            key = (p.source_kind.value, " ".join(p.summary.lower().split())[:120])
            if key in seen:
                continue
            seen.add(key)
        out.append(p)
    return out


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
    # Future-dated rows (scheduled 511 closures, concerts, bad feed clocks) are not
    # "happening now": left in, `now - latest` goes negative and recency explodes.
    horizon = now + _FUTURE_SLACK_SEC
    cells = bucket((s for s in signals if cutoff <= s.observed_at.timestamp() <= horizon),
                   lambda s: (s.lat, s.lon))

    events: list[FusedEvent] = []
    for key, pts in neighborhoods(cells, min_count=min_count):
        pts = _drop_repeats(pts)
        if len(pts) < min_count:
            continue
        latest = min(now, max(p.observed_at.timestamp() for p in pts))
        sources: dict[str, int] = defaultdict(int)
        corroborating_sources: dict[str, int] = defaultdict(int)
        types: dict[str, int] = defaultdict(int)
        # Types seen by a sensor that knows where it is (dispatch, cameras, feeds with
        # coordinates). A post placed on a metro centroid only corroborates when one
        # of these agrees with it — otherwise every city mention "confirms" downtown.
        precise_types = {p.normalized_type.value for p in pts if not is_coarse(p)}
        weight = 0.0
        for p in pts:
            kind = p.source_kind.value
            # radio activity and radio transcripts are one sensor: hearing the dispatch
            # that made the system busy is not a second, independent confirmation
            fam = "radio" if kind.startswith("radio") else kind
            sources[kind] += 1
            types[p.normalized_type.value] += 1
            coarse = is_coarse(p)
            weight += _COARSE_WEIGHT if coarse else 1.0
            if kind == "cad":
                corroborating_sources[fam] += 1
            elif kind in _OBSERVER_KINDS:
                if p.severity >= 0.35:
                    corroborating_sources[fam] += 1
            elif p.normalized_type.value != "other" and p.severity >= 0.35:
                if not coarse or p.normalized_type.value in precise_types:
                    corroborating_sources[fam] += 1
        source_count = len(corroborating_sources)
        if source_count < min_sources:
            continue
        severities = [max(0.0, min(1.0, p.severity)) for p in pts]
        confidences = [max(0.0, min(1.0, p.confidence)) for p in pts]
        mean_sev = sum(severities) / len(severities)
        mean_conf = sum(confidences) / len(confidences)
        recency = min(1.0, max(0.25, 1.0 - ((now - latest) / max(1.0, max_age_hours * 3600))))
        diversity = 1.0 + math.log2(max(1, source_count))
        volume = math.sqrt(max(1.0, weight))
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
            assessment=("corroborated" if source_count >= 2 else "single_source"),
            score_components={
                "volume": round(volume, 3),
                "severity": round(0.35 + mean_sev, 3),
                "confidence": round(0.5 + mean_conf, 3),
                "diversity": round(diversity, 3),
                "recency": round(recency, 3),
            },
        ))

    events.sort(key=lambda e: e.surge_score, reverse=True)
    return events[:top]
