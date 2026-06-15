"""Emerging-event detection over the live incident stream.

Per-call threat scores answer "is this one bad?"; this answers "is something
developing HERE, NOW?" — the original point of APB. We grid incidents spatially and
flag cells where multiple incidents converge with elevated severity (a spatial spike),
which is the live-stream analogue of the activity-first anomaly engine in
apb/infer/activity.py.

Pure-Python, no deps, runs on a snapshot — so it works on the live CAD overview today.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

# ~0.018 deg latitude ≈ 2 km; good neighborhood-scale cell for "converging" events.
CELL_DEG = 0.018


@dataclass
class Cluster:
    metro: str
    lat: float
    lon: float
    count: int
    peak_threat: float
    mean_threat: float
    score: float                      # ranking: volume × severity
    latest_ts: float | None = None    # newest incident in the cell (for recency sort)
    types: dict[str, int] = field(default_factory=dict)
    incident_ids: list[str] = field(default_factory=list)


def detect(incidents: list[dict], min_count: int = 3,
           threat_floor: float = 0.5, top: int = 50) -> list[Cluster]:
    """Find emerging clusters: >= min_count incidents in a ~2km cell with elevated
    severity. Returns clusters ranked by score (volume weighted by severity)."""
    cells: dict[tuple, list[dict]] = defaultdict(list)
    for d in incidents:
        if d.get("lat") is None or d.get("lon") is None:
            continue
        key = (round(d["lat"] / CELL_DEG), round(d["lon"] / CELL_DEG))
        cells[key].append(d)

    clusters: list[Cluster] = []
    for pts in cells.values():
        if len(pts) < min_count:
            continue
        threats = [p.get("threat_score", 0.0) for p in pts]
        peak, mean = max(threats), sum(threats) / len(threats)
        if peak < threat_floor and mean < 0.4:
            continue                  # busy but routine (e.g. cluster of medical aid)
        types: dict[str, int] = defaultdict(int)
        for p in pts:
            types[p.get("type", "other")] += 1
        clusters.append(Cluster(
            metro=pts[0].get("metro", "?"),
            lat=sum(p["lat"] for p in pts) / len(pts),
            lon=sum(p["lon"] for p in pts) / len(pts),
            count=len(pts), peak_threat=round(peak, 2), mean_threat=round(mean, 2),
            # score rewards both how many and how severe
            score=round(len(pts) * (0.4 + mean), 2),
            latest_ts=max((p.get("ts") or 0) for p in pts) or None,
            types=dict(sorted(types.items(), key=lambda x: -x[1])),
            incident_ids=[str(p.get("call_id")) for p in pts][:20],
        ))

    clusters.sort(key=lambda c: c.score, reverse=True)
    return clusters[:top]
