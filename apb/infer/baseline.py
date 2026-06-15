"""Temporal rate-anomaly detection over accumulated snapshot history.

Cluster detection (apb/infer/cluster.py) answers "where are incidents converging
right now"; this answers "where is the rate ABNORMALLY high vs. this area's own
normal" — the true early-warning signal, only possible once the snapshot poller has
built up history (apb/store/snapshots.py).

Method: per metro, bucket incidents into fixed time windows, treat the most recent
window as the observation and the trailing windows as the baseline (mean + std), and
flag metros whose current count exceeds baseline by z_threshold std-devs.
"""
from __future__ import annotations

import math
import time
from collections import defaultdict


def detect_rate_anomalies(incidents: list[dict], window_hours: float = 1.0,
                          z_threshold: float = 2.0, min_baseline: int = 3) -> list[dict]:
    now = time.time()
    win = window_hours * 3600

    # per metro: list of bucket indices (0 = current window, 1 = previous, ...)
    buckets: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    threat_now: dict[str, float] = defaultdict(float)
    for d in incidents:
        ts = d.get("ts") or d.get("last_seen")
        if not ts:
            continue
        idx = int((now - ts) // win)
        if idx < 0:
            continue
        m = d.get("metro", "?")
        buckets[m][idx] += 1
        if idx == 0:
            threat_now[m] = max(threat_now[m], d.get("threat_score", 0.0))

    out: list[dict] = []
    for metro, b in buckets.items():
        current = b.get(0, 0)
        baseline = [b.get(i, 0) for i in range(1, max(b.keys(), default=0) + 1)]
        if len(baseline) < min_baseline:
            continue                       # not enough history yet for this metro
        mean = sum(baseline) / len(baseline)
        var = sum((x - mean) ** 2 for x in baseline) / len(baseline)
        std = math.sqrt(var) if var > 0 else 1.0
        zscore = (current - mean) / std
        if zscore >= z_threshold and current > mean:
            out.append({
                "metro": metro, "current": current, "baseline_mean": round(mean, 2),
                "zscore": round(zscore, 2), "peak_threat": round(threat_now[metro], 2),
                "window_hours": window_hours,
            })
    out.sort(key=lambda x: x["zscore"], reverse=True)
    return out
