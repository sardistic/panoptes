"""Temporal rate-anomaly detection over accumulated snapshot history.

Cluster detection (apb/infer/cluster.py) answers "where are incidents converging
right now"; this answers "where is the rate ABNORMALLY high vs. this area's own
normal" — the true early-warning signal, only possible once the snapshot poller has
built up history (apb/store/snapshots.py).

Method: per metro, bucket incidents into fixed time windows, treat the most recent
window as the observation and the trailing windows as the baseline (mean + std), and
flag metros whose current count exceeds baseline by z_threshold std-devs.

Seasonality: incident volume swings with time of day (rush hour, bar close) and
weekday-vs-weekend. Once enough history exists, the baseline is restricted to
trailing windows from a comparable time slot (same day-part and same weekday/weekend
class); with thin history it falls back to all trailing windows so the detector
still works on day one.
"""
from __future__ import annotations

import math
import time
from collections import defaultdict
from datetime import datetime, timezone


def _slot(ts: float) -> tuple[int, bool]:
    """(day-part 0-3, is_weekend) — 6h day-parts: night/morning/afternoon/evening."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.hour // 6, dt.weekday() >= 5


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

    now_slot = _slot(now)
    out: list[dict] = []
    for metro, b in buckets.items():
        current = b.get(0, 0)
        n_windows = max(b.keys(), default=0) + 1
        # seasonal baseline: only trailing windows in the same time slot as now
        seasonal = [b.get(i, 0) for i in range(1, n_windows)
                    if _slot(now - i * win) == now_slot]
        baseline = (seasonal if len(seasonal) >= min_baseline
                    else [b.get(i, 0) for i in range(1, n_windows)])
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
