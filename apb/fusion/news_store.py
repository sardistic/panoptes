"""Live news-signal buffer + optional RSS poller (mirrors fusion.social_store).

A background daemon periodically pulls keyless news RSS (apb.ingest.news_rss),
geo-resolves each headline to a coarse metro, normalizes to an EventSignal, and keeps
a rolling time-windowed buffer that /live/fused, /live/signals, and /live/social merge
with CAD/radio — so local news can corroborate official signals in place + time.

Default ON; APB_NEWS_OFF to disable. Pure stdlib + httpx; safe for the lean image.
"""
from __future__ import annotations

import threading
import time

from apb.common.models import EventSignal
from apb.fusion.places import resolve_place
from apb.fusion.sources import social_text_signals

import logging

log = logging.getLogger(__name__)

_lock = threading.Lock()
_buf: list[EventSignal] = []
_MAX = 4000
# Dedupe keys survive buffer eviction: RSS re-polls the same items every cycle, so
# keys must be remembered longer than the rolling buffer keeps the signals themselves.
_seen: set[str] = set()
_SEEN_MAX = 50_000
_started = False


def add(signals: list[EventSignal]) -> None:
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


def recent(max_age_hours: float = 24.0) -> list[EventSignal]:
    cutoff = time.time() - max_age_hours * 3600
    with _lock:
        return [s for s in _buf if s.observed_at.timestamp() >= cutoff]


def stats() -> dict:
    with _lock:
        return {"buffered": len(_buf), "running": _started}


def _rows_to_signals(rows: list[dict], keep_unplaced: bool) -> list[EventSignal]:
    placed: list[dict] = []
    for r in rows:
        place = resolve_place(r.get("text", ""))
        if not place and not keep_unplaced:
            continue
        if place:
            r = {**r, "lat": place.lat, "lon": place.lon, "metro": place.metro,
                 "location": place.name, "confidence": place.confidence}
        placed.append(r)
    return social_text_signals(placed)


def poll_once(keep_unplaced: bool = False) -> int:
    """Pull all configured RSS once, place + buffer the items. Returns count added."""
    from apb.ingest.news_rss import NewsRSS
    rows = NewsRSS().collect()
    sigs = _rows_to_signals(rows, keep_unplaced)
    before = len(_buf)
    add(sigs)
    return len(_buf) - before


def start(interval: float = 300.0, keep_unplaced: bool = False) -> bool:
    """Launch the RSS poller in a daemon thread. No-op if already running."""
    global _started
    if _started:
        return False

    def _run():
        while True:
            try:
                n = poll_once(keep_unplaced)
                log.info(f"poll: +{n} signals, {stats()}")
            except Exception as e:            # one bad poll must not kill the loop
                log.warning(f"poll error: {e}")
            time.sleep(interval)

    threading.Thread(target=_run, daemon=True).start()
    _started = True
    log.info("RSS poller started")
    return True
