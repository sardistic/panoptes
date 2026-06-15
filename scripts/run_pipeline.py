"""Single-process orchestrator for the one-metro vertical slice.

ingest -> transcribe -> infer -> store, for the systems configured for a metro.

This runs everything inline for simplicity. Before scaling to many metros at once,
split each stage into its own worker pool with a real queue (Redis/RabbitMQ) between
them, because transcription (GPU-bound) and inference (network-bound) have very
different throughput profiles.
"""
from __future__ import annotations

import argparse
import itertools

from apb.common.config import load_metros
from apb.infer.extract import extract
from apb.infer.geocode import Geocoder
from apb.ingest.broadcastify import BroadcastifyClient
from apb.store.db import persist
from apb.transcribe.whisper import Transcriber


def run(metro: str, max_calls: int | None = None) -> None:
    metros = load_metros()
    if metro not in metros:
        raise SystemExit(f"unknown metro {metro!r}; known: {', '.join(metros)}")

    client = BroadcastifyClient()
    transcriber = Transcriber()
    geocoder = Geocoder()
    processed = 0

    for system in metros[metro]:
        print(f"[run] streaming {system.name} (metro={metro})")
        stream = client.stream(system)
        if max_calls:
            stream = itertools.islice(stream, max_calls)

        for call in stream:
            transcript = transcriber.transcribe(call)
            if transcript is None:
                continue
            incident = extract(call, transcript, system=system, geocoder=geocoder)
            persist(call, transcript, incident)
            processed += 1
            print(f"[run] {call.call_id} {incident.incident_type.value} "
                  f"threat={incident.threat_score:.2f} :: {incident.summary[:80]}")
            if max_calls and processed >= max_calls:
                return


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--metro", required=True, help="metro key from config/metros.yaml")
    ap.add_argument("--max-calls", type=int, default=None, help="stop after N (testing)")
    args = ap.parse_args()
    run(args.metro, args.max_calls)
