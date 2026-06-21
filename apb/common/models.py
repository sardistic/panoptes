"""Domain models that flow through the pipeline."""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class Call(BaseModel):
    """A single radio transmission/call as delivered by a source.

    NOTE: APB is built activity-first. A Call may be metadata-only (encrypted or
    transcription bookmarked) — audio_* fields are optional and the system must
    function on metadata alone.
    """

    source: str                      # e.g. "broadcastify", "trunk-recorder", "openmhz"
    call_id: str                     # source-unique id
    metro: str
    system_id: int
    talkgroup: int
    talkgroup_label: str | None = None
    frequency: float | None = None
    start_time: datetime
    duration_sec: float
    encrypted: bool = False          # voice encrypted -> metadata-only, no transcript
    audio_url: str | None = None     # remote url before download
    audio_path: str | None = None    # local path after download


class ActivityWindow(BaseModel):
    """Aggregated radio ACTIVITY over a time window — the activity-first foundation.

    Built purely from call metadata (no audio/transcript needed), so it works on
    encrypted systems too. Anomalies here are the primary emerging-threat signal.
    """

    metro: str
    system_id: int
    talkgroup: int
    talkgroup_label: str | None = None
    window_start: datetime
    window_sec: int                  # e.g. 60
    call_count: int                  # transmissions in window
    total_airtime_sec: float         # sum of durations
    encrypted: bool = False
    # anomaly signal
    baseline_call_count: float | None = None   # rolling expected count
    zscore: float | None = None                # deviation from baseline
    is_anomalous: bool = False


class Transcript(BaseModel):
    call_id: str
    text: str
    language: str = "en"
    confidence: float | None = None  # mean segment logprob, normalized
    model: str


class IncidentType(str, Enum):
    traffic = "traffic"
    medical = "medical"
    fire = "fire"
    assault = "assault"
    robbery = "robbery"
    shots_fired = "shots_fired"
    domestic = "domestic"
    suspicious = "suspicious"
    pursuit = "pursuit"
    welfare = "welfare"
    quake = "quake"            # seismic event (USGS)
    weather = "weather"        # severe-weather alert / natural hazard (NWS, EONET)
    other = "other"
    noise = "noise"            # not an incident (radio check, dispatch chatter)


class Sentiment(str, Enum):
    calm = "calm"
    routine = "routine"
    elevated = "elevated"
    urgent = "urgent"
    distress = "distress"


class SignalKind(str, Enum):
    """Sensor family for a normalized event signal."""

    radio_metadata = "radio_metadata"
    radio_transcript = "radio_transcript"
    cad = "cad"
    social = "social"
    news = "news"
    traffic = "traffic"
    weather = "weather"
    aircraft = "aircraft"      # ADS-B loiter / airspace-activity signal
    context = "context"        # authority declarations (FEMA, FAA TFR)


class Incident(BaseModel):
    """Structured intelligence extracted from a transcript."""

    call_id: str
    metro: str
    incident_type: IncidentType
    summary: str
    # geo
    location_text: str | None = None     # raw place mention, pre-geocode
    lat: float | None = None
    lon: float | None = None
    units: list[str] = Field(default_factory=list)
    # signal
    sentiment: Sentiment = Sentiment.routine
    threat_score: float = 0.0            # 0..1, model-estimated severity/urgency
    is_emerging: bool = False            # flagged by anomaly/clustering layer
    # provenance
    extracted_by: str
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    redacted: bool = True


class EventSignal(BaseModel):
    """A normalized real-world event hint from any source.

    APB treats radio, CAD, social posts, news, traffic, and weather as sensors with
    different confidence levels. Fusion/clustering works over this common shape.
    """

    signal_id: str
    source: str
    source_kind: SignalKind
    observed_at: datetime
    lat: float | None = None
    lon: float | None = None
    metro: str | None = None
    location_text: str | None = None
    raw_type: str | None = None
    normalized_type: IncidentType = IncidentType.other
    summary: str
    confidence: float = 0.5
    severity: float = 0.3
    url: str | None = None
    dedupe_key: str | None = None
    metadata: dict = Field(default_factory=dict)
