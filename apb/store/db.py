"""Storage: Postgres + PostGIS schema and persistence helpers."""
from __future__ import annotations

import argparse

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from apb.common.config import settings
from apb.common.models import ActivityWindow, Call, Incident, Transcript

engine = create_engine(settings.apb_database_url, future=True)


class Base(DeclarativeBase):
    pass


class CallRow(Base):
    __tablename__ = "calls"
    call_id: Mapped[str] = mapped_column(String, primary_key=True)
    source: Mapped[str] = mapped_column(String)
    metro: Mapped[str] = mapped_column(String, index=True)
    system_id: Mapped[int] = mapped_column(Integer)
    talkgroup: Mapped[int] = mapped_column(Integer)
    talkgroup_label: Mapped[str | None] = mapped_column(String, nullable=True)
    start_time: Mapped["DateTime"] = mapped_column(DateTime(timezone=True), index=True)
    duration_sec: Mapped[float] = mapped_column(Float)
    audio_path: Mapped[str | None] = mapped_column(String, nullable=True)


class TranscriptRow(Base):
    __tablename__ = "transcripts"
    call_id: Mapped[str] = mapped_column(String, primary_key=True)
    text: Mapped[str] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    model: Mapped[str] = mapped_column(String)


class IncidentRow(Base):
    __tablename__ = "incidents"
    call_id: Mapped[str] = mapped_column(String, primary_key=True)
    metro: Mapped[str] = mapped_column(String, index=True)
    incident_type: Mapped[str] = mapped_column(String, index=True)
    summary: Mapped[str] = mapped_column(Text)
    location_text: Mapped[str | None] = mapped_column(String, nullable=True)
    sentiment: Mapped[str] = mapped_column(String, index=True)
    threat_score: Mapped[float] = mapped_column(Float, index=True)
    is_emerging: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    extracted_by: Mapped[str] = mapped_column(String)
    extracted_at: Mapped["DateTime"] = mapped_column(DateTime(timezone=True), index=True)
    geom: Mapped[object] = mapped_column(
        Geometry("POINT", srid=4326), nullable=True
    )


class ActivityRow(Base):
    __tablename__ = "activity_windows"
    # composite key: one row per talkgroup per window
    system_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    talkgroup: Mapped[int] = mapped_column(Integer, primary_key=True)
    window_start: Mapped["DateTime"] = mapped_column(
        DateTime(timezone=True), primary_key=True
    )
    metro: Mapped[str] = mapped_column(String, index=True)
    talkgroup_label: Mapped[str | None] = mapped_column(String, nullable=True)
    window_sec: Mapped[int] = mapped_column(Integer)
    call_count: Mapped[int] = mapped_column(Integer)
    total_airtime_sec: Mapped[float] = mapped_column(Float)
    encrypted: Mapped[bool] = mapped_column(Boolean, default=False)
    baseline_call_count: Mapped[float | None] = mapped_column(Float, nullable=True)
    zscore: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_anomalous: Mapped[bool] = mapped_column(Boolean, default=False, index=True)


def persist_activity(win: ActivityWindow) -> None:
    with Session(engine) as s:
        s.merge(ActivityRow(
            system_id=win.system_id, talkgroup=win.talkgroup,
            window_start=win.window_start, metro=win.metro,
            talkgroup_label=win.talkgroup_label, window_sec=win.window_sec,
            call_count=win.call_count, total_airtime_sec=win.total_airtime_sec,
            encrypted=win.encrypted, baseline_call_count=win.baseline_call_count,
            zscore=win.zscore, is_anomalous=win.is_anomalous,
        ))
        s.commit()


def init_db() -> None:
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        conn.commit()
    Base.metadata.create_all(engine)
    print("[db] schema initialized")


def persist(call: Call, transcript: Transcript, incident: Incident) -> None:
    with Session(engine) as s:
        s.merge(CallRow(
            call_id=call.call_id, source=call.source, metro=call.metro,
            system_id=call.system_id, talkgroup=call.talkgroup,
            talkgroup_label=call.talkgroup_label, start_time=call.start_time,
            duration_sec=call.duration_sec, audio_path=call.audio_path,
        ))
        s.merge(TranscriptRow(
            call_id=transcript.call_id, text=transcript.text,
            confidence=transcript.confidence, model=transcript.model,
        ))
        geom = None
        if incident.lat is not None and incident.lon is not None:
            geom = f"SRID=4326;POINT({incident.lon} {incident.lat})"
        s.merge(IncidentRow(
            call_id=incident.call_id, metro=incident.metro,
            incident_type=incident.incident_type.value, summary=incident.summary,
            location_text=incident.location_text, sentiment=incident.sentiment.value,
            threat_score=incident.threat_score, is_emerging=incident.is_emerging,
            extracted_by=incident.extracted_by, extracted_at=incident.extracted_at,
            geom=geom,
        ))
        s.commit()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", action="store_true", help="create extension + tables")
    args = ap.parse_args()
    if args.init:
        init_db()
