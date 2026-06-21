"""Collect event-like Bluesky posts into APB's local fusion seed file.

This is a cheap firehose experiment, not a hard dependency for the web service.
Install `websockets` only on the machine running this collector.

Examples:
  python scripts/run_bluesky.py --seconds 120 --limit 200
  python scripts/run_bluesky.py --keep-unplaced
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apb.fusion.places import resolve_place
from apb.ingest.bluesky import BlueskyJetstream


async def run(seconds: float, limit: int, out: Path, keep_unplaced: bool) -> int:
    client = BlueskyJetstream()
    out.parent.mkdir(parents=True, exist_ok=True)
    wrote = 0

    async def _collect():
        nonlocal wrote
        async for post in client.posts():
            place = resolve_place(post.text)
            if not place and not keep_unplaced:
                continue
            row = {
                "source": "bluesky",
                "source_kind": "social",
                "text": post.text,
                "created_at": post.created_at.isoformat(),
                "url": post.url,
                "confidence": place.confidence if place else 0.25,
            }
            if place:
                row.update({
                    "lat": place.lat, "lon": place.lon, "metro": place.metro,
                    "location": place.name,
                    "place_resolution": "metro_centroid",
                })
            with out.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=True) + "\n")
            wrote += 1
            if wrote >= limit:
                break

    try:
        await asyncio.wait_for(_collect(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
    return wrote


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--out", default="data/social_seed.jsonl")
    ap.add_argument("--keep-unplaced", action="store_true",
                    help="keep keyword-matched posts even when no metro is resolved")
    args = ap.parse_args()
    wrote = asyncio.run(run(args.seconds, args.limit, Path(args.out), args.keep_unplaced))
    print(f"[bluesky] wrote {wrote} rows -> {args.out}")


if __name__ == "__main__":
    main()
