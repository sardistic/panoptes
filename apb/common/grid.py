"""Shared spatial grid for the clustering layers.

Two fixes over naive `round(lat/CELL)` bucketing:

- Longitude is scaled by cos(lat) so cells stay ~2 km wide everywhere instead of
  shrinking toward the poles (0.018 deg lon is ~1.2 km in Seattle, ~2 km in Miami).
- `neighborhoods()` merges each seed cell with its 8 neighbors so one real event
  straddling a cell boundary is no longer split into fragments that each miss
  min_count. The merge is greedy non-max suppression (densest seed first, consumed
  cells can't be reused), which bounds an event to a 3x3 block (~6 km) instead of
  letting contiguous city-wide activity chain into a single mega-cluster.
"""
from __future__ import annotations

import math
from collections import defaultdict
from typing import Callable, Iterable, TypeVar

CELL_DEG = 0.018  # ~2 km of latitude; neighborhood-scale cell for "converging" events

T = TypeVar("T")
Key = tuple[int, int]


def cell_key(lat: float, lon: float) -> Key:
    """Grid cell for a point, with longitude scaled to equal ground distance."""
    coslat = max(0.2, math.cos(math.radians(lat)))
    return (round(lat / CELL_DEG), round(lon * coslat / CELL_DEG))


def bucket(items: Iterable[T], latlon: Callable[[T], tuple[float | None, float | None]],
           ) -> dict[Key, list[T]]:
    """Bucket items into grid cells, skipping those without coordinates."""
    cells: dict[Key, list[T]] = defaultdict(list)
    for it in items:
        lat, lon = latlon(it)
        if lat is None or lon is None:
            continue
        cells[cell_key(lat, lon)].append(it)
    return cells


def neighborhoods(cells: dict[Key, list[T]], min_count: int = 1,
                  ) -> list[tuple[Key, list[T]]]:
    """Greedy 3x3 merge: seed at the densest unconsumed cell, absorb its occupied
    neighbors. Returns (seed_cell, merged_points) groups meeting min_count."""
    consumed: set[Key] = set()
    out: list[tuple[Key, list[T]]] = []
    for k in sorted(cells, key=lambda c: len(cells[c]), reverse=True):
        if k in consumed:
            continue
        block = [(k[0] + dr, k[1] + dc) for dr in (-1, 0, 1) for dc in (-1, 0, 1)]
        occupied = [b for b in block if b in cells and b not in consumed]
        pts = [p for b in occupied for p in cells[b]]
        if len(pts) < min_count:
            continue
        consumed.update(occupied)
        out.append((k, pts))
    return out
