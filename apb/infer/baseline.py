"""Temporal rate-anomaly detection over accumulated snapshot history.

Cluster detection (apb/infer/cluster.py) answers "where are incidents converging
right now"; this answers "where is the rate ABNORMALLY high vs. this area's own
normal" — the true early-warning signal, only possible once the snapshot poller has
built up history (apb/store/snapshots.py).

Method: per metro, bucket incidents into fixed time windows; the most recent window
is the observation and the trailing windows are the baseline. Two tests, and a metro
is only as surprising as the LESS surprised of them:

  Poisson tail   P(X >= current | rate = baseline mean). Dispatch counts are small
                 integers; with a mean of 0.3 calls/hour, two calls is a 1-in-27
                 window, not the "3.7 sigma" a mean/std z-score reports.
  Robust z       (current - median) / (1.4826 x MAD), with the scale floored at the
                 Poisson value sqrt(median). Busy metros are over-dispersed (bursty),
                 so the Poisson test alone over-fires there; one earlier spike in the
                 baseline no longer inflates the spread the way a std-dev does.

Seasonality: volume swings with time of day and weekday vs weekend. Slots are taken
in the metro's LOCAL solar time (longitude / 15), so "evening" means evening in
Honolulu and in Boston alike. With enough history the baseline is restricted to
windows from the same day-part and weekday/weekend class; with thin history it
falls back to all trailing windows so the detector still works on day one.
"""
from __future__ import annotations

import math
import time
from collections import defaultdict
from datetime import datetime, timezone
from statistics import NormalDist

_NORMAL = NormalDist()
_Z_CAP = 12.0
_SMALL_COUNT_Z_CAP = 1.5


def _slot(ts: float, lon: float | None = None) -> tuple[int, bool]:
    """(day-part 0-3, is_weekend) in local solar time — 6h day-parts."""
    offset = round(lon / 15.0) * 3600.0 if lon is not None and -180.0 <= lon <= 180.0 else 0.0
    dt = datetime.fromtimestamp(ts + offset, tz=timezone.utc)
    return dt.hour // 6, dt.weekday() >= 5


def poisson_tail(k: int, lam: float) -> float:
    """P(X >= k) for X ~ Poisson(lam)."""
    if k <= 0:
        return 1.0
    if lam <= 0:
        return 0.0
    term = math.exp(-lam)
    cdf = term
    for i in range(1, k):
        term *= lam / i
        cdf += term
    return max(0.0, 1.0 - cdf)


def _z_from_p(p: float) -> float:
    if p >= 1.0:
        return -_Z_CAP
    if p <= 1e-300:
        return _Z_CAP
    return max(-_Z_CAP, min(_Z_CAP, _NORMAL.inv_cdf(1.0 - p)))


def surprise(current: int, baseline: list[int]) -> dict:
    """How unusual `current` is against trailing window counts."""
    n = len(baseline)
    mean = sum(baseline) / n
    ordered = sorted(baseline)
    median = ordered[n // 2] if n % 2 else (ordered[n // 2 - 1] + ordered[n // 2]) / 2
    mad = sorted(abs(x - median) for x in baseline)[n // 2]
    # a rate of exactly zero would make any single call infinitely surprising; half an
    # event over the baseline span is the most that silence can claim about the rate
    lam = max(mean, 0.5 / n)
    p = poisson_tail(current, lam)
    z_poisson = _z_from_p(p)
    scale = max(1.4826 * mad, math.sqrt(max(median, lam)), 0.5)
    z_robust = max(-_Z_CAP, min(_Z_CAP, (current - median) / scale))
    return {"mean": mean, "median": median, "p_value": p,
            "z_poisson": z_poisson, "z_robust": z_robust,
            "zscore": min(z_poisson, z_robust)}


def detect_rate_anomalies(incidents: list[dict], window_hours: float = 1.0,
                          z_threshold: float = 2.0, min_baseline: int = 3,
                          min_current: int = 3) -> list[dict]:
    now = time.time()
    win = window_hours * 3600

    # per metro: list of bucket indices (0 = current window, 1 = previous, ...)
    buckets: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    threat_now: dict[str, float] = defaultdict(float)
    lon_sum: dict[str, float] = defaultdict(float)
    lon_n: dict[str, int] = defaultdict(int)
    for d in incidents:
        ts = d.get("ts") or d.get("last_seen")
        if not ts:
            continue
        idx = int((now - ts) // win)
        if idx < 0:
            continue
        m = d.get("metro", "?")
        if str(m).startswith("radio_"):
            continue                       # radio systems are scored against their own normal
        buckets[m][idx] += 1
        if isinstance(d.get("lon"), (int, float)):
            lon_sum[m] += d["lon"]
            lon_n[m] += 1
        if idx == 0:
            threat_now[m] = max(threat_now[m], d.get("threat_score", 0.0))

    out: list[dict] = []
    for metro, b in buckets.items():
        current = b.get(0, 0)
        n_windows = max(b.keys(), default=0) + 1
        lon = lon_sum[metro] / lon_n[metro] if lon_n[metro] else None
        now_slot = _slot(now, lon)
        # seasonal baseline: only trailing windows in the same local time slot as now
        seasonal = [b.get(i, 0) for i in range(1, n_windows)
                    if _slot(now - i * win, lon) == now_slot]
        is_seasonal = len(seasonal) >= min_baseline
        baseline = seasonal if is_seasonal else [b.get(i, 0) for i in range(1, n_windows)]
        if len(baseline) < min_baseline:
            continue                       # not enough history yet for this metro
        s = surprise(current, baseline)
        if current < min_current:
            # hundreds of metros are tested every hour; one or two calls in a quiet
            # town clear p < 0.02 somewhere every time. A surge needs a few calls.
            s["zscore"] = min(s["zscore"], _SMALL_COUNT_Z_CAP)
        if s["zscore"] >= z_threshold and current > s["mean"]:
            out.append({
                "metro": metro, "current": current, "baseline_mean": round(s["mean"], 2),
                "baseline_median": s["median"], "baseline_windows": len(baseline),
                "seasonal": is_seasonal,
                "zscore": round(s["zscore"], 2), "p_value": float(f"{s['p_value']:.3g}"),
                "z_poisson": round(s["z_poisson"], 2), "z_robust": round(s["z_robust"], 2),
                "peak_threat": round(threat_now[metro], 2),
                "window_hours": window_hours,
            })
    out.sort(key=lambda x: x["zscore"], reverse=True)
    return out
