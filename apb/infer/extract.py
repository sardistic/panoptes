"""Transcript -> structured Incident via Claude.

One call does the work of several stages: classify incident type, extract location
and units, score threat/urgency, judge sentiment, and redact PII. We use the LIGHT
model by default (high volume, cheap) and escalate to the HEAVY model when the light
model flags high threat or low confidence.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from anthropic import Anthropic

from apb.common.config import MetroSystem, settings
from apb.common.models import Call, Incident, IncidentType, Sentiment, Transcript

_client: Anthropic | None = None


def _anthropic() -> Anthropic:
    global _client
    if _client is None:
        _client = Anthropic(api_key=settings.anthropic_api_key)
    return _client


SYSTEM_PROMPT = """You are an analyst for public-safety radio transcripts. Transcripts \
are noisy and may contain 10-codes and jargon. Return ONE JSON object, no prose.

Rules:
- REDACT personally identifying info in every text field: replace names, street \
addresses (keep cross-streets/block-level only), phone numbers, plates, dates of \
birth, and medical/victim details with [REDACTED]. Block-level locations (e.g. \
"100 block of Main") and intersections are allowed.
- If the transmission is just dispatch chatter / radio check / unintelligible, set \
incident_type to "noise".
- threat_score: 0.0 (routine) to 1.0 (active danger to life).
- Do not invent facts not present in the transcript.

JSON schema:
{
  "incident_type": one of [traffic, medical, fire, assault, robbery, shots_fired,
                           domestic, suspicious, pursuit, welfare, other, noise],
  "summary": string (redacted, <=240 chars),
  "location_text": string|null (redacted, block/intersection level),
  "units": [string, ...],
  "sentiment": one of [calm, routine, elevated, urgent, distress],
  "threat_score": number 0..1,
  "confidence": number 0..1 (your confidence in this extraction)
}"""


def _call_model(model: str, transcript: Transcript) -> dict:
    msg = _anthropic().messages.create(
        model=model,
        max_tokens=600,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": f"Transcript:\n{transcript.text}"}],
    )
    text = "".join(b.text for b in msg.content if b.type == "text").strip()
    # be tolerant of code fences
    if text.startswith("```"):
        text = text.strip("`").split("\n", 1)[-1].rsplit("```", 1)[0]
    return json.loads(text)


def extract(
    call: Call,
    transcript: Transcript,
    system: MetroSystem | None = None,
    escalate_threshold: float = 0.6,
    geocoder=None,
) -> Incident:
    data = _call_model(settings.apb_model_light, transcript)
    model_used = settings.apb_model_light

    # Escalate ambiguous-but-dangerous cases to the heavy model.
    if data.get("threat_score", 0) >= escalate_threshold or data.get("confidence", 1) < 0.4:
        data = _call_model(settings.apb_model_heavy, transcript)
        model_used = settings.apb_model_heavy

    location_text = data.get("location_text")
    lat = lon = None
    if system is not None and geocoder is not None:
        geo = geocoder.geocode(location_text, system)
        if geo:
            lat, lon = geo.lat, geo.lon
    elif system and system.centroid:
        lat, lon = system.centroid

    return Incident(
        call_id=call.call_id,
        metro=call.metro,
        incident_type=IncidentType(data.get("incident_type", "other")),
        summary=data.get("summary", ""),
        location_text=location_text,
        lat=lat,
        lon=lon,
        units=data.get("units", []) or [],
        sentiment=Sentiment(data.get("sentiment", "routine")),
        threat_score=float(data.get("threat_score", 0.0)),
        extracted_by=model_used,
        extracted_at=datetime.now(timezone.utc),
        redacted=True,
    )
