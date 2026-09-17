"""Facts aggregator — astronomy math, the streaming collector, and failure isolation.
Offline: every upstream is stubbed."""
from datetime import datetime, timezone

from apb.context import facts


def test_sun_position_matches_known_geometry():
    # Greenwich, 2026-06-21 12:00 UTC: sun near due south (~180 deg), ~62 deg high
    p = facts.sun_position(51.48, 0.0, datetime(2026, 6, 21, 12, 0, tzinfo=timezone.utc))
    assert 60 < p["altitude_deg"] < 64 and 170 < p["azimuth_deg"] < 190 and p["phase"] == "day"
    night = facts.sun_position(51.48, 0.0, datetime(2026, 12, 21, 0, 0, tzinfo=timezone.utc))
    assert night["altitude_deg"] < -18 and night["phase"] == "night"


def test_moon_phase_cycle():
    new = facts.moon_phase(datetime(2000, 1, 6, 18, 14, tzinfo=timezone.utc))
    assert new["age_days"] < 0.1 and new["illumination"] < 0.01 and new["name"] == "new"
    full = facts.moon_phase(datetime(2000, 1, 21, 4, 40, tzinfo=timezone.utc))
    assert full["illumination"] > 0.97 and full["name"] == "full"


def test_stream_emits_fastest_first_and_isolates_failures(monkeypatch):
    facts._cache.clear()
    import time as _t

    def slow(*a):
        _t.sleep(0.3); return {"slow": 1}

    def fast(*a):
        return {"fast": 1}

    def boom(*a):
        raise RuntimeError("dead upstream")
    tasks = [("a", "slow", slow), ("b", "fast", fast), ("c", "boom", boom)]
    monkeypatch.setattr(facts, "_tasks", lambda b: (tasks, {"bounds": b, "keyed_available": {}}, ""))
    b = {"south": 1, "north": 2, "west": 3, "east": 4}
    events = list(facts.facts_stream(b, timeout=5))
    keys = [e.get("key") for e in events if "key" in e and "data" in e]
    assert keys == ["fast", "slow"]                       # arrival order, not declaration order
    assert any(e.get("error") == "RuntimeError" for e in events)
    done = events[-1]
    assert done["done"] and done["failed"] == ["c.boom: RuntimeError"]
    collected = facts.facts(b)
    assert collected["sections"] == {"a": {"slow": 1}, "b": {"fast": 1}}
    # second call replays from cache instantly
    replay = list(facts.facts_stream(b))
    assert replay[-1].get("cached") is True


def test_stream_times_out_stragglers_without_blocking(monkeypatch):
    facts._cache.clear()
    import time as _t

    def hang(*a):
        _t.sleep(3); return {"late": 1}
    monkeypatch.setattr(facts, "_tasks", lambda b: ([("x", "hang", hang)], {"bounds": b, "keyed_available": {}}, ""))
    t0 = _t.time()
    events = list(facts.facts_stream({"south": 5, "north": 6, "west": 7, "east": 8}, timeout=0.4))
    assert _t.time() - t0 < 2 and events[-1]["failed"] == ["x.hang: timeout"]


def test_digest_is_bounded():
    assert len(facts.digest({"sections": {"a": {"x": "y" * 9000}}}, limit=500)) == 500
