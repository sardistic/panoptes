"""Offline tests for source fusion — no network/API keys."""
from datetime import datetime, timezone

from apb.common.models import SignalKind
from apb.fusion.cluster import detect
from apb.fusion.places import resolve_place
from apb.fusion.signals import incident_to_signal, text_signal
from apb.fusion.sources import social_text_signals


def test_cad_incident_normalizes_to_signal():
    sig = incident_to_signal({
        "metro": "seattle", "call_id": "abc", "type": "fire",
        "summary": "Structure fire", "location": "1st Ave",
        "lat": 47.61, "lon": -122.33, "threat_score": 0.8,
        "ts": datetime(2026, 6, 14, tzinfo=timezone.utc).timestamp(),
    })
    assert sig.source_kind == SignalKind.cad
    assert sig.normalized_type.value == "fire"
    assert sig.severity == 0.8
    assert sig.lat == 47.61


def test_fusion_rewards_converging_sources():
    now = datetime.now(timezone.utc)
    cad = incident_to_signal({
        "metro": "seattle", "call_id": "1", "type": "fire",
        "summary": "Fire response", "lat": 47.610, "lon": -122.330,
        "threat_score": 0.7, "ts": now.timestamp(),
    })
    social = text_signal(
        source="bluesky", source_kind=SignalKind.social,
        text="Smoke and fire visible downtown",
        observed_at=now, lat=47.611, lon=-122.331, confidence=0.5,
    )
    events = detect([cad, social], min_count=2, min_score=0.1)
    assert len(events) == 1
    assert events[0].source_count == 2
    assert events[0].sources["cad"] == 1
    assert events[0].sources["social"] == 1
    assert events[0].types["fire"] == 2


def test_fusion_can_require_multiple_source_families():
    now = datetime.now(timezone.utc)
    cad1 = incident_to_signal({
        "metro": "seattle", "call_id": "1", "type": "fire",
        "summary": "Fire response", "lat": 47.610, "lon": -122.330,
        "threat_score": 0.7, "ts": now.timestamp(),
    })
    cad2 = incident_to_signal({
        "metro": "seattle", "call_id": "2", "type": "fire",
        "summary": "Fire response", "lat": 47.611, "lon": -122.331,
        "threat_score": 0.7, "ts": now.timestamp(),
    })
    assert detect([cad1, cad2], min_count=2, min_sources=2, min_score=0.1) == []


def test_social_text_resolves_named_metro_to_coarse_place():
    signals = social_text_signals([{
        "source": "bluesky",
        "text": "Huge fire and smoke visible in Seattle",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }])
    assert len(signals) == 1
    assert signals[0].metro == "seattle"
    assert signals[0].location_text == "Seattle"
    assert signals[0].lat is not None
    assert signals[0].confidence < 0.5


def test_place_resolver_matches_mexico_city_alias():
    place = resolve_place("reports of an explosion in CDMX")
    assert place is not None
    assert place.metro == "cdmx"
