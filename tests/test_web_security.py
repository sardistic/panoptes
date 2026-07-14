"""Static regressions for the dependency-free map UI's trust boundary."""
from pathlib import Path


HTML = (Path(__file__).parents[1] / "web" / "index.html").read_text(encoding="utf-8")


def test_untrusted_markup_has_escaping_and_safe_links():
    assert "const esc =" in HTML
    assert "const safeLink =" in HTML
    assert "${esc(d.summary)}" in HTML
    assert 'rel="noopener noreferrer"' in HTML


def test_hazards_use_one_browser_request():
    assert "fetch(`/live/hazards/all?" in HTML
    assert "fetch(`/live/traffic?" not in HTML


def test_activity_field_and_radar_are_first_class_layers():
    assert "function drawActivityField()" in HTML
    assert "function incidentTick(d,z)" in HTML
    assert "nexrad-n0q-900913" in HTML
    assert "id=\"radarBtn\"" in HTML
    assert "L.geoJSON(d.geometry" in HTML


def test_map_animation_budget_and_emerging_fallback():
    assert "breatheCityLights" not in HTML
    assert "setInterval(()=>{ if(!document.hidden && activityPoints.length)" not in HTML
    assert "EMERGING EVENTS · NATIONAL" in HTML
    assert "officialCells=new Set" in HTML


def test_loading_indicator_names_overlapping_sources():
    assert 'id="loadPill"' in HTML
    assert "const activeLoads=new Map()" in HTML
    for label in ("INCIDENTS", "EMERGING", "FUSION", "SOCIAL", "HAZARDS", "RADAR", "SATELLITE"):
        assert f"'{label}'" in HTML


def test_live_updates_use_sse_with_slow_fallback():
    assert "new EventSource(`/live/stream?" in HTML
    assert "setInterval(load,60000)" in HTML
    assert "setInterval(load,15000)" not in HTML
