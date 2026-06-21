"""Optional Bluesky Jetstream ingest.

Bluesky's Jetstream is a public JSON WebSocket over the AT Protocol firehose. This
module is intentionally optional: importing APB does not require the `websockets`
package, but running this collector can stream post text into the fusion layer.
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator

DEFAULT_JETSTREAM = (
    "wss://jetstream2.us-east.bsky.network/subscribe?"
    "wantedCollections=app.bsky.feed.post"
)


@dataclass
class BlueskyPost:
    text: str
    did: str
    created_at: datetime
    url: str | None = None


class BlueskyJetstream:
    """Tiny async client yielding public post text matching event keywords."""

    def __init__(self, uri: str = DEFAULT_JETSTREAM,
                 keywords: list[str] | None = None):
        self.uri = uri
        self.keywords = [k.lower() for k in (keywords or _DEFAULT_KEYWORDS)]

    async def posts(self) -> AsyncIterator[BlueskyPost]:
        try:
            import websockets
        except ImportError as e:
            raise RuntimeError("Install `websockets` to use BlueskyJetstream") from e

        async with websockets.connect(self.uri, ping_interval=20) as ws:
            async for raw in ws:
                post = self._parse(raw)
                if post and self._wanted(post.text):
                    yield post

    def _wanted(self, text: str) -> bool:
        t = text.lower()
        if any(re.search(p, t) for p in _NOISE_PATTERNS):
            return False
        return any(re.search(p, t) for p in self.keywords)

    def _parse(self, raw: str) -> BlueskyPost | None:
        try:
            msg = json.loads(raw)
        except ValueError:
            return None
        commit = msg.get("commit") or {}
        if commit.get("operation") != "create":
            return None
        if commit.get("collection") != "app.bsky.feed.post":
            return None
        rec = commit.get("record") or {}
        text = rec.get("text")
        if not text:
            return None
        created = _parse_dt(rec.get("createdAt"))
        rkey = commit.get("rkey")
        did = msg.get("did") or ""
        # AT URIs are stable even when handles are unresolved.
        url = f"at://{did}/app.bsky.feed.post/{rkey}" if did and rkey else None
        return BlueskyPost(text=text, did=did, created_at=created, url=url)


async def collect(limit: int = 100, seconds: float = 30.0) -> list[BlueskyPost]:
    """Collect a small bounded sample; useful for manual smoke tests."""
    client = BlueskyJetstream()
    out: list[BlueskyPost] = []

    async def _run():
        async for post in client.posts():
            out.append(post)
            if len(out) >= limit:
                break

    try:
        await asyncio.wait_for(_run(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
    return out


def _parse_dt(v) -> datetime:
    if not v:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


_DEFAULT_KEYWORDS = [
    r"\bshooting\b", r"\bshots fired\b", r"\bgunshots?\b", r"\bgunfire\b",
    r"\b(structure fire|brush fire|wildfire|house fire|apartment fire)\b",
    r"\b(explosion|evacuation|hazmat)\b", r"\b(crash|collision|pileup)\b",
    r"\bpolice chase\b", r"\bstandoff\b", r"\blockdown\b",
    r"\bshelter in place\b", r"\bactive shooter\b", r"\bmass casualty\b",
    r"#\w*fire\b",
]

_NOISE_PATTERNS = [
    r"\bfired\b", r"\bfireplace\b", r"\bstarfire\b", r"\bbackfire\b",
    r"\bphotoshoot", r"\bshooting an enzyme\b", r"\bcrashes? the market\b",
    r"\bpants on fire\b",
]
