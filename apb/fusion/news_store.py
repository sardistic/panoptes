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
_stop = threading.Event()
_threads: list[threading.Thread] = []


_hydrated = False


def _hydrate() -> None:
    """Reload the persisted buffer once, so restarts don't blank the news layer."""
    global _hydrated
    if _hydrated:
        return
    _hydrated = True
    from apb.store import sigbuf
    add(sigbuf.load("news"), _persist=False)


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
        sigbuf.save("news", fresh)


def recent(max_age_hours: float = 24.0) -> list[EventSignal]:
    from apb.store import sigbuf, state
    if state.is_postgres():
        return sigbuf.load("news", max_age_hours, limit=_MAX)
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


_GNEWS_TERMS = ("police OR shooting OR fire OR explosion OR crash OR evacuation "
                "OR emergency OR hazmat")
_gnews_started = False


def gnews_poll_once(max_metros: int = 40, per_metro: int = 5) -> int:
    """Query Google News per metro ('\"Seattle\" (police OR fire ...)' when:1d) and
    buffer the placed headlines. Reuses the correlator's cached gnews client, so
    /correlate and this poller share one result cache."""
    from apb.context import gdelt
    from apb.fusion.places import places
    if gdelt._gdelt is None:
        gdelt._gdelt = gdelt.GDELT()
    rows: list[dict] = []
    seen_metros: set[str] = set()
    for place, _aliases in places()[:max_metros]:
        if _stop.is_set():
            break
        if place.metro in seen_metros:
            continue
        seen_metros.add(place.metro)
        arts = gdelt._gdelt.gnews(f'"{place.name}" ({_GNEWS_TERMS})',
                                  timespan="1d", maxrecords=per_metro)
        for a in arts:
            rows.append({"source": "gnews", "source_kind": "news",
                         "text": a.get("title") or "", "url": a.get("url"),
                         "created_at": a.get("at"),
                         "lat": place.lat, "lon": place.lon, "metro": place.metro,
                         "location": place.name, "confidence": 0.35,
                         "domain": a.get("domain")})
        if _stop.wait(1.5):
            break                           # stay polite across ~40 metros
    from apb.fusion.sources import social_text_signals
    before = len(_buf)
    add(social_text_signals(rows))
    return len(_buf) - before


def start_gnews(interval: float = 900.0) -> bool:
    """Launch the per-metro Google News poller in a daemon thread. Complements the
    generic RSS poller with metro-placed incident headlines. No-op if running."""
    _hydrate()
    global _gnews_started
    if _gnews_started:
        return False

    def _run():
        while not _stop.is_set():
            try:
                n = gnews_poll_once()
                log.info(f"gnews poll: +{n} signals, {stats()}")
            except Exception as e:            # one bad poll must not kill the loop
                log.warning(f"gnews poll error: {e}")
            _stop.wait(interval)

    _stop.clear()
    thread = threading.Thread(target=_run, daemon=True, name="apb-google-news")
    thread.start()
    _threads.append(thread)
    _gnews_started = True
    log.info("per-metro Google News poller started")
    return True


def start(interval: float = 300.0, keep_unplaced: bool = False) -> bool:
    """Launch the RSS poller in a daemon thread. No-op if already running."""
    _hydrate()
    global _started
    if _started:
        return False

    def _run():
        while not _stop.is_set():
            try:
                n = poll_once(keep_unplaced)
                log.info(f"poll: +{n} signals, {stats()}")
            except Exception as e:            # one bad poll must not kill the loop
                log.warning(f"poll error: {e}")
            _stop.wait(interval)

    _stop.clear()
    thread = threading.Thread(target=_run, daemon=True, name="apb-news-rss")
    thread.start()
    _threads.append(thread)
    _started = True
    log.info("RSS poller started")
    return True


def stop(timeout: float = 2.0) -> None:
    """Ask background pollers to stop and briefly wait for cooperative exit."""
    _stop.set()
    for thread in list(_threads):
        if thread.is_alive():
            thread.join(timeout=timeout)
