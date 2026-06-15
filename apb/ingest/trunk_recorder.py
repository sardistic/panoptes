"""Metadata-first ingest from your own trunk-recorder / SDRTrunk nodes.

This is the anti-Broadcastify-dependence path: own SDR nodes give full control, no
license terms, and control-channel metadata even on ENCRYPTED systems.

trunk-recorder writes, per call, a <name>.wav (or .m4a) plus a <name>.json sidecar
with metadata. We watch the output directory and emit Calls from the JSON. Audio is
optional — for encrypted calls there is no usable audio, but the metadata still flows.

Sidecar fields used (trunk-recorder format):
  talkgroup, talkgroup_tag, freq, start_time (epoch), call_length, encrypted, srcList
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from apb.common.config import MetroSystem
from apb.common.models import Call


def _call_from_sidecar(path: Path, system: MetroSystem) -> Call | None:
    try:
        meta = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None

    tg = int(meta.get("talkgroup", 0))
    if system.talkgroups and tg not in system.talkgroups:
        return None

    encrypted = bool(meta.get("encrypted", False))
    audio = path.with_suffix(".m4a")
    if not audio.exists():
        audio = path.with_suffix(".wav")

    return Call(
        source="trunk-recorder",
        call_id=f"{system.broadcastify_system_id}:{path.stem}",
        metro=system.metro,
        system_id=system.broadcastify_system_id,
        talkgroup=tg,
        talkgroup_label=meta.get("talkgroup_tag") or meta.get("talkgroup_description"),
        frequency=float(meta["freq"]) / 1e6 if meta.get("freq") else None,
        start_time=datetime.fromtimestamp(int(meta["start_time"]), tz=timezone.utc),
        duration_sec=float(meta.get("call_length", 0)),
        encrypted=encrypted,
        # No audio for encrypted calls; metadata still flows (activity-first).
        audio_path=str(audio) if (audio.exists() and not encrypted) else None,
    )


def watch(system: MetroSystem, output_dir: str | Path, interval: float = 2.0) -> Iterator[Call]:
    """Tail a trunk-recorder output directory, yielding Calls as sidecars appear."""
    root = Path(output_dir)
    seen: set[str] = set()
    while True:
        for sidecar in sorted(root.rglob("*.json")):
            if str(sidecar) in seen:
                continue
            seen.add(str(sidecar))
            call = _call_from_sidecar(sidecar, system)
            if call:
                yield call
        time.sleep(interval)


def scan_existing(system: MetroSystem, output_dir: str | Path) -> Iterator[Call]:
    """One-shot pass over existing sidecars (testing / backfill)."""
    for sidecar in sorted(Path(output_dir).rglob("*.json")):
        call = _call_from_sidecar(sidecar, system)
        if call:
            yield call
