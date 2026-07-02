"""CAD ingest: classifier, adaptive row normalization, backoff — all offline."""
import time

from apb.ingest.cad import CadFeed, CadIngest, classify, parse_ts


def test_classify_keyword_rules():
    itype, threat = classify("SHOTS FIRED / PERSON WITH GUN")
    assert itype == "shots_fired"
    assert threat >= 0.8


def test_classify_unknown_falls_back_to_other():
    assert classify("zorble frobnication") == ("other", 0.3)
    assert classify("") == ("other", 0.3)


def test_normalize_adaptive_row_detects_fields_and_drops_null_island():
    feed = CadFeed(metro="test", name="Test", url="http://x", adaptive=True)
    rows = [
        {"incident_type": "Structure Fire", "latitude": "47.61", "longitude": "-122.33",
         "datetime": "2026-07-01T12:00:00", "address": "1st Ave",
         "incident_number": "F123"},
        {"incident_type": "Junk", "latitude": "0", "longitude": "0",
         "datetime": "2026-07-01T12:00:00", "incident_number": "F124"},
    ]
    out = CadIngest()._normalize(rows, feed)
    assert len(out) == 1                       # (0,0) row dropped
    d = out[0]
    assert d["call_id"] == "F123"
    assert d["type"] == "fire"
    assert d["lat"] == 47.61
    assert d["location"] == "1st Ave"
    assert d["ts"] == parse_ts("2026-07-01T12:00:00")


def test_fetch_unknown_feed_returns_empty():
    assert CadIngest().fetch("no_such_feed") == []


def test_backoff_grows_and_expires():
    cad = CadIngest()
    cad._note_failure("m")
    n1, until1 = cad._fail["m"]
    cad._note_failure("m")
    n2, until2 = cad._fail["m"]
    assert (n1, n2) == (1, 2)
    assert until2 > until1
    assert cad._backing_off("m")
    cad._fail["m"] = (2, time.time() - 1)      # window elapsed
    assert not cad._backing_off("m")
