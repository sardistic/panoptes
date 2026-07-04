"""Live social-signal buffer + optional Bluesky firehose worker.

scripts/run_bluesky.py is a BATCH collector (writes a seed file). This is the LIVE path:
a background daemon thread consumes Bluesky's Jetstream, geo-resolves each matched post
to a coarse metro, normalizes it to an EventSignal, and keeps a rolling, time-windowed
in-memory buffer that /live/fused and /live/signals merge with CAD/radio so weak social
posts can corroborate official signals in place + time.

Default ON (websockets ships with uvicorn[standard]); APB_BLUESKY_OFF / APB_SOCIAL_RSS_OFF
to disable. Degrades to a no-op if websockets is unavailable.
"""
from __future__ import annotations

import asyncio
import threading
import time

from apb.common.models import EventSignal
from apb.fusion.places import resolve_place
from apb.fusion.sources import social_text_signals

import logging

log = logging.getLogger(__name__)

_lock = threading.Lock()
_buf: list[EventSignal] = []        # rolling buffer, newest appended
_MAX = 5000
# Dedupe keys survive buffer eviction: RSS re-polls the same items every cycle, so
# keys must be remembered longer than the rolling buffer keeps the signals themselves.
_seen: set[str] = set()
_SEEN_MAX = 50_000
_started = False


_hydrated = False


def _hydrate() -> None:
    """Reload the persisted buffer once, so restarts don't blank the social layer."""
    global _hydrated
    if _hydrated:
        return
    _hydrated = True
    from apb.store import sigbuf
    add(sigbuf.load("social"), _persist=False)


def add(signals: list[EventSignal], _persist: bool = True) -> None:
    if not signals:
        return
    with _lock:
        fresh = [s for s in signals if s.dedupe_key not in _seen]
        _seen.update(s.dedupe_key for s in fresh)
        if len(_seen) > _SEEN_MAX:      # rare full reset beats unbounded growth
            _seen.clear()
            _seen.update(s.dedupe_key for s in _buf)
        _buf.extend(fresh)
        if len(_buf) > _MAX:
            del _buf[:len(_buf) - _MAX]
    if _persist and fresh:
        from apb.store import sigbuf
        sigbuf.save("social", fresh)


def recent(max_age_hours: float = 24.0) -> list[EventSignal]:
    cutoff = time.time() - max_age_hours * 3600
    with _lock:
        return [s for s in _buf if s.observed_at.timestamp() >= cutoff]


def stats() -> dict:
    with _lock:
        return {"buffered": len(_buf), "running": _started}


def _row(post, place) -> dict:
    row = {"source": "bluesky", "source_kind": "social", "text": post.text,
           "created_at": post.created_at.isoformat(), "url": post.url,
           "confidence": place.confidence if place else 0.25}
    if place:
        row.update(lat=place.lat, lon=place.lon, metro=place.metro, location=place.name)
    return row


async def _consume(keep_unplaced: bool) -> None:
    from apb.ingest.bluesky import BlueskyJetstream
    async for post in BlueskyJetstream().posts():
        place = resolve_place(post.text)
        if not place and not keep_unplaced:
            continue                      # only keep posts we can pin to a metro
        try:
            add(social_text_signals([_row(post, place)]))
        except Exception as e:            # one bad post must not kill the stream
            log.warning(f"signal error: {e}")


_rss_started = False


def _rss_poll_once(keep_unplaced: bool) -> int:
    """Pull all configured Reddit/Mastodon RSS once, place + buffer the posts."""
    from apb.ingest.social_rss import SocialRSS
    placed: list[dict] = []
    for r in SocialRSS().collect():
        place = resolve_place(r.get("text", ""))
        if not place and not keep_unplaced:
            continue
        if place:
            r = {**r, "lat": place.lat, "lon": place.lon, "metro": place.metro,
                 "location": place.name, "confidence": place.confidence}
        placed.append(r)
    before = len(_buf)
    add(social_text_signals(placed))
    return len(_buf) - before


def start_rss(interval: float = 300.0, keep_unplaced: bool = False) -> bool:
    """Launch the keyless Reddit/Mastodon RSS poller in a daemon thread. Independent of
    the Bluesky firehose (shares this buffer). No-op if already running."""
    _hydrate()
    global _rss_started
    if _rss_started:
        return False

    def _run():
        while True:
            try:
                n = _rss_poll_once(keep_unplaced)
                log.info(f"poll: +{n} signals, {stats()}")
            except Exception as e:            # one bad poll must not kill the loop
                log.warning(f"poll error: {e}")
            time.sleep(interval)

    threading.Thread(target=_run, daemon=True).start()
    _rss_started = True
    log.info("Reddit/Mastodon poller started")
    return True


def start(keep_unplaced: bool = False) -> bool:
    """Launch the Jetstream consumer in a daemon thread (auto-reconnect). No-op if
    already running or `websockets` is unavailable. Returns True if started."""
    _hydrate()
    global _started
    if _started:
        return False
    try:
        import websockets  # noqa: F401
    except ImportError:
        log.warning("websockets not installed; live firehose disabled")
        return False

    def _run():
        while True:
            try:
                asyncio.run(_consume(keep_unplaced))
            except Exception as e:
                log.warning(f"stream dropped: {e}; reconnecting in 10s")
                time.sleep(10)

    threading.Thread(target=_run, daemon=True).start()
    _started = True
    log.info("live firehose started")
    return True
