"""Central configuration, loaded from environment / .env."""
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")

    # sources
    broadcastify_api_key: str = ""
    broadcastify_username: str = ""
    broadcastify_password: str = ""

    # inference
    anthropic_api_key: str = ""
    apb_model_heavy: str = "claude-opus-4-8"
    apb_model_light: str = "claude-haiku-4-5-20251001"

    # optional OpenAI alternative (used by the type learner if its key is set)
    openai_api_key: str = ""
    apb_model_openai: str = "gpt-4o-mini"

    # Google Programmable Search (for source discovery via dorks)
    google_api_key: str = ""
    google_cx: str = ""          # Programmable Search Engine id

    # storage
    apb_database_url: str = "postgresql+psycopg://apb:apb@localhost:5432/apb"
    apb_audio_dir: Path = Path("./data/audio")

    # whisper
    apb_whisper_model: str = "large-v3"
    apb_whisper_device: str = "cpu"
    apb_whisper_compute_type: str = "int8"


settings = Settings()


class MetroSystem(BaseModel):
    """A trunked radio system to ingest for a metro."""

    metro: str
    name: str
    broadcastify_system_id: int
    # talkgroup ids to include (empty = all decoded for the system)
    talkgroups: list[int] = []
    # rough metro centroid for fallback geocoding (lat, lon)
    centroid: tuple[float, float] | None = None
    # geocoding bounds [min_lon, min_lat, max_lon, max_lat] to constrain lookups
    bbox: tuple[float, float, float, float] | None = None
    # local trunk-recorder output dir, if ingesting from an own SDR node
    trunk_recorder_dir: str | None = None


def load_metros(path: str | Path = "config/metros.yaml") -> dict[str, list[MetroSystem]]:
    """Return {metro_key: [MetroSystem, ...]}."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    out: dict[str, list[MetroSystem]] = {}
    for metro_key, systems in (raw or {}).items():
        out[metro_key] = [MetroSystem(metro=metro_key, **s) for s in systems]
    return out
