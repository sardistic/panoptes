"""New keyless lanes (EMSC / GDACS / SIGMET) — pure parsing logic, offline."""
from apb.ingest.emsc import _threat as emsc_threat
from apb.ingest.gdacs import _point


def test_emsc_threat_scales_with_magnitude():
    assert emsc_threat(4.0) < emsc_threat(6.0) < emsc_threat(7.5)
    assert 0.2 <= emsc_threat(2.0) and emsc_threat(9.5) <= 0.95


def test_gdacs_point_handles_point_and_polygon_geometry():
    assert _point({"type": "Point", "coordinates": [-84.5, 38.0]}) == (38.0, -84.5)
    poly = {"type": "Polygon",
            "coordinates": [[[-100.0, 30.0], [-101.0, 31.0], [-100.0, 30.0]]]}
    assert _point(poly) == (30.0, -100.0)
    assert _point({"type": "Point", "coordinates": []}) is None
    assert _point(None) is None


def test_lane_feeds_register_once():
    from apb.ingest.cad import FEEDS, load_emsc, load_gdacs, load_sigmet
    assert load_emsc() in (0, 1)      # 1 first time, 0 if already registered
    assert load_gdacs() in (0, 1)
    assert load_sigmet() in (0, 1)
    assert load_emsc() == 0           # idempotent
    for slug in ("emsc", "gdacs", "sigmet"):
        assert FEEDS[slug].hidden
