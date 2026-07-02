"""Spatial grid: latitude-scaled cells + neighbor merge (no network)."""
from apb.common.grid import CELL_DEG, bucket, cell_key, neighborhoods


def test_cells_stay_metric_across_latitudes():
    # ~1.5 km east at Seattle's latitude (cos ~0.67) should land within one cell width
    seattle_a = cell_key(47.6100, -122.3300)
    seattle_b = cell_key(47.6100, -122.3300 + 0.020)   # ~1.5 km ground distance
    assert abs(seattle_a[1] - seattle_b[1]) <= 1


def test_neighbor_merge_joins_event_split_across_cell_boundary():
    # Two points ~250 m apart that straddle a cell edge used to form two size-1
    # fragments; with neighbor merge they group.
    boundary = CELL_DEG * 10.5          # lat exactly between cells 10 and 11
    pts = [(boundary - 0.001, -100.0), (boundary + 0.001, -100.0)]
    cells = bucket(pts, lambda p: p)
    assert len(cells) == 2              # they really are in different cells
    groups = neighborhoods(cells, min_count=2)
    assert len(groups) == 1
    assert len(groups[0][1]) == 2


def test_merge_does_not_chain_across_the_whole_city():
    # A long contiguous strip of occupied cells must not collapse into one event:
    # each seed absorbs only its 3x3 block.
    pts = [(CELL_DEG * i, -100.0) for i in range(12)]
    cells = bucket(pts, lambda p: p)
    groups = neighborhoods(cells, min_count=1)
    assert len(groups) >= 3
    assert max(len(g[1]) for g in groups) <= 9


def test_min_count_still_applies_after_merge():
    cells = bucket([(10.0, -100.0)], lambda p: p)
    assert neighborhoods(cells, min_count=2) == []


def test_points_without_coords_are_skipped():
    cells = bucket([(None, -100.0), (10.0, None), (10.0, -100.0)], lambda p: p)
    assert sum(len(v) for v in cells.values()) == 1
