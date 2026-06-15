"""Offline tests for the activity-first anomaly detector — no network/GPU/DB."""
from datetime import datetime, timedelta, timezone

from apb.common.models import Call
from apb.infer.activity import ActivityAggregator


def _call(tg, ts, dur=3.0, enc=False):
    return Call(
        source="trunk-recorder", call_id=f"{tg}-{ts.timestamp()}", metro="nyc",
        system_id=1, talkgroup=tg, start_time=ts, duration_sec=dur, encrypted=enc,
    )


def test_window_aggregation_and_anomaly():
    agg = ActivityAggregator(window_sec=60, zscore_threshold=3.0)
    base = datetime(2026, 6, 14, 12, 0, 0, tzinfo=timezone.utc)
    closed = []

    # 10 quiet minutes: 2 calls/window -> builds a stable baseline of ~2.
    for w in range(10):
        wstart = base + timedelta(minutes=w)
        for i in range(2):
            closed += agg.add(_call(100, wstart + timedelta(seconds=i)))

    # Spike window: 20 calls in one minute -> should flag anomalous.
    spike_start = base + timedelta(minutes=10)
    for i in range(20):
        closed += agg.add(_call(100, spike_start + timedelta(seconds=i)))
    closed += agg.flush()

    spike = [w for w in closed if w.call_count == 20]
    assert spike, "spike window not closed"
    assert spike[0].is_anomalous is True
    assert spike[0].zscore is not None and spike[0].zscore >= 3.0


def test_encrypted_metadata_only_still_aggregates():
    agg = ActivityAggregator(window_sec=60)
    base = datetime(2026, 6, 14, 12, 0, 0, tzinfo=timezone.utc)
    for i in range(5):
        agg.add(_call(200, base + timedelta(seconds=i), enc=True))
    wins = agg.flush()
    assert len(wins) == 1
    assert wins[0].encrypted is True
    assert wins[0].call_count == 5      # signal exists with zero audio
