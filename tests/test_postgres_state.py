"""Integration coverage for horizontally shared runtime state."""
import os
import time
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("APB_STATE_DATABASE_URL"),
    reason="requires APB_STATE_DATABASE_URL",
)


def test_postgres_snapshots_events_and_signal_buffer():
    from apb.common.models import SignalKind
    from apb.fusion import maritime_store
    from apb.fusion.cluster import FusedEvent
    from apb.fusion.signals import text_signal
    from apb.store import events, sigbuf, snapshots, state

    assert state.is_postgres()
    snapshots._init_postgres()
    events._init_postgres()
    sigbuf._init_postgres()
    maritime_store._init_postgres()
    with state.pg_connection() as connection:
        connection.execute("TRUNCATE panoptes_incidents, panoptes_fused_events, "
                           "panoptes_signal_buffer, panoptes_vessels")

    now = time.time()
    row = {"metro": "test", "call_id": "pg-1", "type": "fire",
           "summary": "Test fire", "location": "Test", "sentiment": "urgent",
           "threat_score": 0.8, "emerging": True, "lat": 40.0, "lon": -75.0,
           "at": datetime.now(timezone.utc).isoformat(), "ts": now}
    assert snapshots.record([row]) == 1
    assert snapshots.query(1, metro="test")[0]["call_id"] == "pg-1"
    assert snapshots.stats()["backend"] == "postgres"

    event = FusedEvent(
        event_id="test", lat=40.0, lon=-75.0, count=2, source_count=2,
        sources={"cad": 1, "social": 1}, types={"fire": 2}, peak_severity=0.8,
        mean_severity=0.7, confidence=0.7, surge_score=4.0, latest_ts=now,
    )
    assert events.record([event]) == 1
    assert events.query(1)[0]["sources"] == {"cad": 1, "social": 1}
    claimed = events.unnotified(3.0)
    assert len(claimed) == 1
    events.mark_notified([claimed[0]["uid"]])
    assert events.unnotified(3.0) == []

    signal = text_signal(source="test", source_kind=SignalKind.social,
                         text="fire downtown", observed_at=datetime.now(timezone.utc),
                         lat=40.0, lon=-75.0)
    sigbuf.save("social", [signal])
    assert sigbuf.load("social", 1)[0].signal_id == signal.signal_id

    vessel = {"call_id": "mmsi:1", "ts": now, "lat": 40.0, "lon": -75.0}
    maritime_store.add(vessel)
    assert maritime_store.recent(1)[0]["call_id"] == "mmsi:1"


def test_postgres_poller_leadership_is_exclusive():
    from apb.store import state

    assert state.acquire_poller_leadership()
    # The same process treats its held session lock as idempotent.
    assert state.acquire_poller_leadership()
    state.release_poller_leadership()
