"""Offline tests — no network, no GPU, no DB."""
from datetime import datetime, timezone

from apb.common.config import load_metros
from apb.common.models import Call, Incident, IncidentType, Sentiment


def test_load_metros():
    metros = load_metros()
    assert "nyc" in metros
    assert metros["nyc"][0].centroid == (40.7128, -74.0060)


def test_incident_defaults_redacted():
    call = Call(
        source="broadcastify", call_id="1", metro="nyc", system_id=1,
        talkgroup=100, start_time=datetime.now(timezone.utc), duration_sec=4.2,
    )
    inc = Incident(
        call_id=call.call_id, metro=call.metro,
        incident_type=IncidentType.shots_fired, summary="[REDACTED] reported",
        sentiment=Sentiment.urgent, threat_score=0.8, extracted_by="test",
    )
    assert inc.redacted is True
    assert inc.threat_score == 0.8
