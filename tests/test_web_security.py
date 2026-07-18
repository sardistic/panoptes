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
    for label in ("INCIDENTS", "EMERGING", "FUSION", "SOCIAL", "HAZARDS", "WARNINGS",
                  "ENVIRONMENT", "RADAR", "SATELLITE"):
        assert f"'{label}'" in HTML


def test_progressive_loading_keeps_context_off_incident_critical_path():
    assert "await loadEmerging(true)" in HTML
    assert "drawEmergingPreview()" in HTML
    assert "void refreshSecondary(forceContext,previewed)" in HTML
    assert "if(data===providedData && providedData!==null) return" in HTML
    assert "max_age_hours=${w}&limit=300" in HTML


def test_signal_overlays_avoid_generic_dot_and_ring_markers():
    assert "function fusedBeacon(e,z)" in HTML
    assert "function socialSignal(s,z,corro)" in HTML
    assert "function hazardBeacon(d,z,st)" in HTML
    assert "color:'#d7dde5'" not in HTML
    assert "className:'soc'" not in HTML
    assert "className:'haz'" not in HTML
    assert "L.circleMarker" not in HTML
    assert "L.circle(" not in HTML


def test_single_canvas_alert_field_uses_city_light_baseline():
    assert "const IncidentField=L.Layer.extend" in HTML
    incident_field = HTML.split("const IncidentField=L.Layer.extend", 1)[1].split(
        "const ENV_COLORS", 1)[0]
    assert "createRadialGradient" not in incident_field
    assert "createLinearGradient" in incident_field
    assert "incidentField.setData" in HTML
    assert "L.heatLayer" not in HTML
    assert "leaflet.heat" not in HTML
    assert "FIELD_COLORS" in HTML
    assert "rateSignalByMetro" in HTML
    assert "lookback_hours=72&z=0" in HTML
    assert "setGoes(true)" in HTML
    assert 'Light = activity above local rate' in HTML


def test_environment_field_warning_layer_and_load_stopwatch():
    assert "const EnvironmentField=L.Layer.extend" in HTML
    assert "environmentScores" in HTML
    assert "'/live/environment'" in HTML
    assert 'id="envReadout"' in HTML
    assert 'id="warningsBtn"' in HTML
    assert "source=nws" in HTML
    assert "performance.now()-r.started" in HTML
    assert "LOADED · ${completedLoad.label}" in HTML
    assert "cursor:crosshair!important" in HTML


def test_live_updates_use_sse_with_slow_fallback():
    assert "new EventSource(`/live/stream?" in HTML
    assert "setInterval(load,60000)" in HTML
    assert "setInterval(load,15000)" not in HTML
