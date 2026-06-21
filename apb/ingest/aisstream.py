"""aisstream.io maritime AIS — vessel positions (free key, opt-in).

The maritime analog of the ADS-B watcher: a live picture of vessel traffic in US
coastal/port waters, including search-and-rescue activity. Streamed over a websocket
from aisstream.io, which needs a free API key. Opt-in via the AISSTREAM_KEY env var
(and `pip install websockets`), mirroring the Bluesky firehose, so the lean core never
depends on it.

This module is just the async consumer; apb.fusion.maritime_store buffers the latest
position per vessel and exposes it to the /live/maritime map layer.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

_URL = "wss://stream.aisstream.io/v0/stream"
# aisstream bounding boxes are [[lat,lon],[lat,lon]] pairs. US coastal envelopes.
_BBOXES = [
    [[24.0, -126.0], [50.0, -66.0]],     # CONUS + coastal waters
    [[17.0, -68.0], [19.0, -64.0]],      # Puerto Rico / USVI
    [[18.0, -161.0], [23.0, -154.0]],    # Hawaii
]


def api_key() -> str | None:
    return os.environ.get("AISSTREAM_KEY")


async def positions():
    """Async-yield normalized vessel-position rows from the aisstream firehose."""
    import websockets
    key = api_key()
    if not key:
        return
    async with websockets.connect(_URL, max_size=2 ** 22) as ws:
        await ws.send(json.dumps({
            "APIKey": key, "BoundingBoxes": _BBOXES,
            "FilterMessageTypes": ["PositionReport"],
        }))
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if msg.get("MessageType") != "PositionReport":
                continue
            meta = msg.get("MetaData") or {}
            lat, lon = meta.get("latitude"), meta.get("longitude")
            mmsi = meta.get("MMSI")
            if lat is None or lon is None or mmsi is None:
                continue
            name = (meta.get("ShipName") or "").strip() or f"MMSI {mmsi}"
            ts = _parse(meta.get("time_utc"))
            yield {
                "call_id": f"ais:{mmsi}", "metro": "aisstream", "type": "other",
                "summary": f"Vessel: {name}", "location": name, "source": "aisstream",
                "sentiment": "routine", "threat_score": 0.2, "emerging": False,
                "lat": float(lat), "lon": float(lon),
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            }


def _parse(s: str | None) -> float | None:
    if not s:
        return None
    try:                       # aisstream time_utc e.g. "2026-06-20 23:59:59.9 +0000 UTC"
        clean = s.split(".")[0].replace(" UTC", "").strip()
        d = datetime.strptime(clean, "%Y-%m-%d %H:%M:%S")
        return d.replace(tzinfo=timezone.utc).timestamp()
    except (ValueError, IndexError):
        return None
