"""Activity-first orchestrator — APB's foundation.

ingest (metadata only) -> aggregate into ActivityWindows -> anomaly-flag -> store.
No transcription. Runs on encrypted systems. This is the default day-one pipeline;
transcription/extraction is a later enrichment layer (see scripts/run_pipeline.py).

Sources, in anti-Broadcastify-dependence priority order:
  1. own trunk-recorder node (if metro has trunk_recorder_dir set)
  2. Broadcastify Calls API (fallback breadth)
"""
from __future__ import annotations

import argparse
import itertools

from apb.common.config import load_metros
from apb.infer.activity import ActivityAggregator
from apb.ingest import trunk_recorder
from apb.ingest.broadcastify import BroadcastifyClient
from apb.store.db import persist_activity


def _source(system, client: BroadcastifyClient):
    """Yield Calls from the preferred source for this system (metadata only)."""
    if system.trunk_recorder_dir:
        print(f"[activity] source=trunk-recorder dir={system.trunk_recorder_dir}")
        yield from trunk_recorder.watch(system, system.trunk_recorder_dir)
    else:
        print(f"[activity] source=broadcastify system={system.broadcastify_system_id}")
        yield from client.stream(system)


def run(metro: str, window_sec: int = 60, zthresh: float = 3.0,
        max_calls: int | None = None) -> None:
    metros = load_metros()
    if metro not in metros:
        raise SystemExit(f"unknown metro {metro!r}; known: {', '.join(metros)}")

    client = BroadcastifyClient()
    agg = ActivityAggregator(window_sec=window_sec, zscore_threshold=zthresh)
    processed = 0

    for system in metros[metro]:
        stream = _source(system, client)
        if max_calls:
            stream = itertools.islice(stream, max_calls)
        for call in stream:
            for win in agg.add(call):
                persist_activity(win)
                if win.is_anomalous:
                    print(f"[ANOMALY] {win.metro} tg={win.talkgroup} "
                          f"count={win.call_count} z={win.zscore} "
                          f"{'ENC' if win.encrypted else ''}")
            processed += 1
            if max_calls and processed >= max_calls:
                break
        for win in agg.flush():
            persist_activity(win)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--metro", required=True)
    ap.add_argument("--window-sec", type=int, default=60)
    ap.add_argument("--zthresh", type=float, default=3.0)
    ap.add_argument("--max-calls", type=int, default=None)
    args = ap.parse_args()
    run(args.metro, args.window_sec, args.zthresh, args.max_calls)
