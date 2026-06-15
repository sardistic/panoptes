"""Audio -> text via faster-whisper.

Scanner audio is noisy, narrowband, and full of jargon/10-codes. We bias the model
with an initial prompt and keep segment-level logprobs as a confidence proxy so the
inference layer can discount low-quality transcripts.
"""
from __future__ import annotations

import math

from apb.common.config import settings
from apb.common.models import Call, Transcript

# Domain prompt nudges Whisper toward dispatch vocabulary.
INITIAL_PROMPT = (
    "Police, fire, and EMS radio dispatch. Units, cross streets, signal codes, "
    "10-codes, suspect descriptions, vehicle plates."
)


class Transcriber:
    def __init__(self):
        from faster_whisper import WhisperModel  # lazy import (heavy)

        self.model = WhisperModel(
            settings.apb_whisper_model,
            device=settings.apb_whisper_device,
            compute_type=settings.apb_whisper_compute_type,
        )
        self.model_name = f"faster-whisper/{settings.apb_whisper_model}"

    def transcribe(self, call: Call) -> Transcript | None:
        if not call.audio_path:
            return None

        segments, info = self.model.transcribe(
            call.audio_path,
            language="en",
            initial_prompt=INITIAL_PROMPT,
            vad_filter=True,          # drop dead air between transmissions
            beam_size=5,
        )

        parts: list[str] = []
        logprobs: list[float] = []
        for seg in segments:
            parts.append(seg.text.strip())
            logprobs.append(seg.avg_logprob)

        text = " ".join(p for p in parts if p).strip()
        if not text:
            return None

        # map mean logprob (~ -1..0) to a 0..1 confidence
        conf = None
        if logprobs:
            mean = sum(logprobs) / len(logprobs)
            conf = max(0.0, min(1.0, math.exp(mean)))

        return Transcript(
            call_id=call.call_id,
            text=text,
            language=info.language or "en",
            confidence=conf,
            model=self.model_name,
        )
