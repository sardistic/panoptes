"""Event registry lifecycle + quake dedupe + buffer persistence — offline."""
import time
from datetime import datetime, timezone

import pytest

from apb.common.models import SignalKind
from apb.fusion.cluster import FusedEvent
from apb.fusion.dedupe import dedupe_signal_rows, dedupe_signals
from apb.fusion.signals import text_signal


@pytest.fixture()
def store(tmp_path, monkeypatch):
    from apb.store import events, sigbuf, snapshots
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "t.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    monkeypatch.setattr(events, "_ready", False)
    monkeypatch.setattr(sigbuf, "_ready", False)
    yield events, sigbuf
    if snapshots._conn is not None:
        snapshots._conn.close()


def _evt(lat=40.0, lon=-100.0, score=3.0, count=4):
    return FusedEvent(event_id="x", lat=lat, lon=lon, count=count, source_count=2,
                      sources={"cad": 3, "social": 1}, types={"fire": 4},
                      peak_severity=0.7, mean_severity=0.5, confidence=0.6,
                      surge_score=score, latest_ts=time.time(),
                      summaries=["Structure fire"], signal_ids=[])


def test_event_keeps_identity_across_cycles(store):
    events, _ = store
    assert events.record([_evt(score=3.0)]) == 1          # new
    assert events.record([_evt(lat=40.01, score=5.0)]) == 0  # same event, moved 1km
    rows = events.query()
    assert len(rows) == 1
    assert rows[0]["peak_score"] == 5.0
    assert rows[0]["first_seen"] <= rows[0]["last_seen"]


def test_distant_event_gets_new_uid(store):
    events, _ = store
    events.record([_evt()])
    assert events.record([_evt(lat=41.0)]) == 1           # ~110 km away
    assert len(events.query()) == 2


def test_notify_marks_exactly_once(store):
    events, _ = store
    events.record([_evt(score=9.0)])
    pending = events.unnotified(min_score=6.0)
    assert len(pending) == 1
    events.mark_notified([p["uid"] for p in pending])
    assert events.unnotified(min_score=6.0) == []


def test_sigbuf_roundtrip(store):
    _, sigbuf = store
    sig = text_signal(source="bluesky", source_kind=SignalKind.social,
                      text="fire downtown", observed_at=datetime.now(timezone.utc),
                      lat=47.6, lon=-122.3)
    sigbuf.save("social", [sig])
    back = sigbuf.load("social")
    assert len(back) == 1
    assert back[0].dedupe_key == sig.dedupe_key
    assert back[0].lat == 47.6


def _quake(source, lat=35.0, lon=-118.0, ts=None):
    return {"source": source, "lat": lat, "lon": lon,
            "ts": ts if ts is not None else time.time(),
            "summary": "M5 earthquake"}


def test_quake_dedupe_prefers_usgs():
    rows = [_quake("emsc"), _quake("usgs"), _quake("gdacs")]
    kept = dedupe_signal_rows(rows)
    assert [r["source"] for r in kept] == ["usgs"]


def test_quake_dedupe_keeps_distinct_events():
    rows = [_quake("usgs"), _quake("emsc", lat=36.5),          # ~170 km apart
            _quake("emsc", ts=time.time() - 7200)]             # same spot, 2h earlier
    assert len(dedupe_signal_rows(rows)) == 3


def test_quake_dedupe_ignores_non_quakes():
    rows = [_quake("usgs"),
            {"source": "nws", "lat": 35.0, "lon": -118.0, "ts": time.time(),
             "summary": "Tornado Warning"}]
    assert len(dedupe_signal_rows(rows)) == 2
