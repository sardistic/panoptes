"""New keyless lanes (EMSC / GDACS / SIGMET / CHP) — pure parsing logic, offline."""
from apb.ingest.chp import _latlon, _parse_ts
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


def test_chp_latlon_microdegrees_and_unsigned_west():
    assert _latlon("38661084:121369020") == (38.661084, -121.36902)
    assert _latlon("38661084:-121369020") == (38.661084, -121.36902)
    assert _latlon("0:0") is None            # no GPS fix
    assert _latlon("garbage") is None


def test_chp_logtime_is_pacific_local():
    ts = _parse_ts("Jul  2 2026  5:55PM")
    # 17:55 PDT == 00:55 UTC next day
    from datetime import datetime, timezone
    assert datetime.fromtimestamp(ts, tz=timezone.utc).hour == 0
    assert _parse_ts("not a time") is None


def test_chp_summary_never_includes_log_details():
    # LogDetails carries raw dispatch chatter (license plates); the parser must
    # only emit type + location fields.
    import re
    from pathlib import Path
    src = Path("apb/ingest/chp.py").read_text(encoding="utf-8")
    parse = src[src.index("def fetch"):]
    assert "IncidentDetail" not in parse
    assert "LogDetails" not in parse


def test_lane_feeds_register_once():
    from apb.ingest.cad import FEEDS, load_emsc, load_gdacs, load_sigmet
    assert load_emsc() in (0, 1)      # 1 first time, 0 if already registered
    assert load_gdacs() in (0, 1)
    assert load_sigmet() in (0, 1)
    assert load_emsc() == 0           # idempotent
    for slug in ("emsc", "gdacs", "sigmet"):
        assert FEEDS[slug].hidden
