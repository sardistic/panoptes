"""Cross-source phenomenon dedupe.

The same physical event often arrives via several sensor networks — one
earthquake shows up from USGS, EMSC, and GDACS. Left alone it renders as
stacked duplicate markers and inflates cluster volume (fake corroboration:
it's one phenomenon, not three observations of converging activity).

Duplicates are detected by place + time proximity and resolved by source
priority (USGS is authoritative for quakes; EMSC fills global gaps; GDACS is
a coarse humanitarian rollup). Same-source near-duplicates are never dropped —
each network already dedupes itself.
"""
from __future__ import annotations

import math
from typing import Callable, TypeVar

T = TypeVar("T")

_QUAKE_PRIORITY = {"usgs": 3, "emsc": 2, "gdacs": 1}
_QUAKE_KM = 80.0          # location scatter between networks for one event
_QUAKE_WINDOW_SEC = 1800.0


def _is_quake(source: str | None, summary: str | None) -> bool:
    return source in ("usgs", "emsc") or (
        source == "gdacs" and "earthquake" in (summary or "").lower())


def suppress_duplicate_quakes(
    rows: list[T],
    *,
    get_source: Callable[[T], str | None],
    get_latlon: Callable[[T], tuple[float | None, float | None]],
    get_ts: Callable[[T], float | None],
    get_summary: Callable[[T], str | None],
) -> list[T]:
    """Drop lower-priority quake rows that duplicate a higher-priority one."""
    quakes: list[tuple[int, int, float, float, float]] = []
    for i, r in enumerate(rows):
        src = get_source(r)
        if not _is_quake(src, get_summary(r)):
            continue
        lat, lon = get_latlon(r)
        if lat is None or lon is None:
            continue
        quakes.append((_QUAKE_PRIORITY.get(src or "", 0), i, lat, lon, get_ts(r) or 0.0))

    quakes.sort(key=lambda q: -q[0])          # highest priority claims the event
    kept: list[tuple[int, float, float, float]] = []
    drop: set[int] = set()
    for pr, i, lat, lon, ts in quakes:
        coslat = max(0.2, math.cos(math.radians(lat)))
        dup = any(
            kpr > pr
            and abs(ts - kts) <= _QUAKE_WINDOW_SEC
            and math.hypot(lat - klat, (lon - klon) * coslat) * 111.0 <= _QUAKE_KM
            for kpr, klat, klon, kts in kept)
        if dup:
            drop.add(i)
        else:
            kept.append((pr, lat, lon, ts))
    if not drop:
        return rows
    return [r for i, r in enumerate(rows) if i not in drop]


def dedupe_signal_rows(rows: list[dict]) -> list[dict]:
    """Phenomenon dedupe for normalized incident dicts."""
    return suppress_duplicate_quakes(
        rows,
        get_source=lambda d: d.get("source"),
        get_latlon=lambda d: (d.get("lat"), d.get("lon")),
        get_ts=lambda d: d.get("ts"),
        get_summary=lambda d: d.get("summary"),
    )


def dedupe_signals(signals: list) -> list:
    """Phenomenon dedupe for EventSignal objects."""
    return suppress_duplicate_quakes(
        signals,
        get_source=lambda s: s.source,
        get_latlon=lambda s: (s.lat, s.lon),
        get_ts=lambda s: s.observed_at.timestamp(),
        get_summary=lambda s: s.summary,
    )
