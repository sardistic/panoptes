"""Broadcastify Calls ingestion.

Polls the Broadcastify Calls API for new calls on a system, yields Call objects,
and downloads the audio to local storage.

API reference: https://www.broadcastify.com/calls/  (Calls Platform / Live Calls)
This client targets the "live calls" polling endpoint shape; adjust paths/fields
to match your licensed API tier.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import httpx

from apb.common.config import MetroSystem, settings
from apb.common.models import Call

import logging

log = logging.getLogger(__name__)

LIVE_CALLS_URL = "https://api.broadcastify.com/owncalls/livecalls"


class BroadcastifyClient:
    def __init__(self, audio_dir: Path | None = None):
        self.audio_dir = Path(audio_dir or settings.apb_audio_dir)
        self.audio_dir.mkdir(parents=True, exist_ok=True)
        self._client = httpx.Client(timeout=30.0)
        self._seen: set[str] = set()

    def _params(self, system: MetroSystem, since_pos: int) -> dict:
        return {
            "key": settings.broadcastify_api_key,
            "systemId": system.broadcastify_system_id,
            "pos": since_pos,            # cursor returned by previous call
            "doInit": 0 if since_pos else 1,
        }

    def poll(self, system: MetroSystem, since_pos: int = 0) -> tuple[list[Call], int]:
        """Return (new calls, next cursor) for one poll."""
        resp = self._client.get(LIVE_CALLS_URL, params=self._params(system, since_pos))
        resp.raise_for_status()
        data = resp.json()

        calls: list[Call] = []
        for c in data.get("calls", []):
            cid = str(c["id"])
            if cid in self._seen:
                continue
            self._seen.add(cid)

            tg = int(c.get("call_tg", 0))
            if system.talkgroups and tg not in system.talkgroups:
                continue

            calls.append(
                Call(
                    source="broadcastify",
                    call_id=cid,
                    metro=system.metro,
                    system_id=system.broadcastify_system_id,
                    talkgroup=tg,
                    talkgroup_label=c.get("display") or c.get("grouping"),
                    frequency=float(c["call_freq"]) if c.get("call_freq") else None,
                    start_time=datetime.fromtimestamp(int(c["ts"]), tz=timezone.utc),
                    duration_sec=float(c.get("call_duration", 0)),
                    audio_url=c.get("enc_filepath") or c.get("filename"),
                )
            )
        return calls, int(data.get("lastPos", since_pos))

    def download_audio(self, call: Call) -> Call:
        """Download a call's audio to local storage; set call.audio_path."""
        if not call.audio_url:
            return call
        dest = self.audio_dir / call.metro / f"{call.call_id}.m4a"
        dest.parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            with self._client.stream("GET", call.audio_url) as r:
                r.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in r.iter_bytes():
                        fh.write(chunk)
        call.audio_path = str(dest)
        return call

    def stream(self, system: MetroSystem, interval: float = 5.0) -> Iterator[Call]:
        """Continuously yield downloaded Calls for a system."""
        pos = 0
        while True:
            try:
                calls, pos = self.poll(system, pos)
            except httpx.HTTPError as e:
                log.warning(f"poll error for {system.name}: {e}")
                time.sleep(interval * 3)
                continue
            for call in calls:
                yield self.download_audio(call)
            time.sleep(interval)
