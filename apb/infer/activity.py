"""Activity-first inference: aggregate call metadata into ActivityWindows and flag
anomalies. This is APB's foundation — it produces emerging-threat signal from
metadata ALONE (no audio/transcript), so it works on encrypted systems too.

Approach:
- Bucket calls into fixed time windows per (system, talkgroup).
- Maintain a rolling baseline (EWMA mean + variance) of call_count per talkgroup.
- Flag a window anomalous when its count is a configurable number of std-devs above
  baseline. A spike in traffic — even fully encrypted — is the primary signal that
  "something is happening."
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from apb.common.models import ActivityWindow, Call


@dataclass
class _Baseline:
    """EWMA mean/variance of per-window call counts for one talkgroup."""

    alpha: float = 0.1          # smoothing; lower = longer memory
    mean: float = 0.0
    var: float = 0.0
    n: int = 0

    def zscore(self, x: float) -> float | None:
        if self.n < 5:          # not enough history to judge
            return None
        std = math.sqrt(self.var) if self.var > 0 else 1.0
        return (x - self.mean) / std

    def update(self, x: float) -> None:
        if self.n == 0:
            self.mean = x
        else:
            diff = x - self.mean
            incr = self.alpha * diff
            self.mean += incr
            self.var = (1 - self.alpha) * (self.var + diff * incr)
        self.n += 1


@dataclass
class ActivityAggregator:
    """Streaming aggregator. Feed it Calls; it emits closed ActivityWindows."""

    window_sec: int = 60
    zscore_threshold: float = 3.0
    # (system_id, talkgroup) -> bucket
    _buckets: dict[tuple[int, int], dict] = field(default_factory=lambda: defaultdict(dict))
    _baselines: dict[tuple[int, int], _Baseline] = field(default_factory=lambda: defaultdict(_Baseline))

    def _window_start(self, ts: datetime) -> datetime:
        epoch = int(ts.timestamp())
        floored = epoch - (epoch % self.window_sec)
        return datetime.fromtimestamp(floored, tz=timezone.utc)

    def add(self, call: Call) -> list[ActivityWindow]:
        """Add a call; return any windows that just closed (and are ready to score)."""
        key = (call.system_id, call.talkgroup)
        ws = self._window_start(call.start_time)
        bucket = self._buckets[key]

        closed: list[ActivityWindow] = []
        # If a new window started for this talkgroup, close the previous one.
        if bucket and bucket["window_start"] != ws:
            closed.append(self._close(key, bucket))
            self._buckets[key] = bucket = {}

        if not bucket:
            bucket.update(
                window_start=ws, metro=call.metro, system_id=call.system_id,
                talkgroup=call.talkgroup, talkgroup_label=call.talkgroup_label,
                encrypted=call.encrypted, count=0, airtime=0.0,
            )
        bucket["count"] += 1
        bucket["airtime"] += call.duration_sec
        bucket["encrypted"] = bucket["encrypted"] or call.encrypted
        return closed

    def flush(self) -> list[ActivityWindow]:
        """Close all open buckets (call at shutdown / end of batch)."""
        out = [self._close(k, b) for k, b in self._buckets.items() if b]
        self._buckets.clear()
        return out

    def _close(self, key: tuple[int, int], bucket: dict) -> ActivityWindow:
        base = self._baselines[key]
        count = bucket["count"]
        z = base.zscore(count)
        baseline_mean = base.mean if base.n >= 5 else None
        base.update(count)       # learn AFTER scoring so a spike doesn't hide itself

        return ActivityWindow(
            metro=bucket["metro"], system_id=bucket["system_id"],
            talkgroup=bucket["talkgroup"], talkgroup_label=bucket["talkgroup_label"],
            window_start=bucket["window_start"], window_sec=self.window_sec,
            call_count=count, total_airtime_sec=round(bucket["airtime"], 1),
            encrypted=bucket["encrypted"], baseline_call_count=baseline_mean,
            zscore=round(z, 2) if z is not None else None,
            is_anomalous=(z is not None and z >= self.zscore_threshold),
        )
