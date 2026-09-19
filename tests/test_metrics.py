"""Baselines, notable flags, noise index, camera observation parsing."""
from apb.context import facts, explain
from apb.store import metrics


def test_baseline_needs_history_then_ranks(tmp_path, monkeypatch):
    from apb.store import snapshots
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "m.sqlite", raising=False)
    monkeypatch.setattr(snapshots, "_conn", None)
    monkeypatch.setattr(metrics, "_ready", False)
    cell = metrics.cell_for(40.755, -73.98)
    assert cell == "40.75,-74.00"
    assert metrics.baseline(cell, {"aqi": 80}) == {}                 # no history yet
    import time
    for i, v in enumerate([40, 42, 45, 50, 55, 60]):                  # six past samples, spaced out
        metrics.record(cell, {"aqi": v})
        # push them into the past so the 10-minute de-dup does not collapse them
        with metrics._lock:
            metrics._conn().execute("UPDATE box_metrics SET ts = ts - ? WHERE cell = ? AND ts > ?", (3600 * (i + 1), cell, time.time() - 30)).connection.commit()
    b = metrics.baseline(cell, {"aqi": 80})
    assert b["aqi"]["samples"] == 6 and b["aqi"]["percentile"] == 100 and b["aqi"]["median"] == 50


def test_notable_flags_thresholds_and_history():
    sections = {"air": {"us_aqi": 130, "aqi_band": "unhealthy for sensitive groups"}, "atmosphere": {"kp_now": 6, "lifted_index": -5},
                "sky": {"now": {"wind_gusts_10m": 70, "visibility": 800}}, "health": {"drought_monitor": {"worst_class": "D3"}},
                "water": {"river_discharge_m3s": 40, "river_discharge_longterm_mean_m3s": 10}}
    base = {"noise_complaints_7d": {"samples": 12, "percentile": 97, "median": 100, "vs_median": 400}}
    flags = facts.notable(sections, base)
    assert any("AQI 130" in f for f in flags) and any("Kp 6" in f for f in flags) and any("D3" in f for f in flags)
    assert any("97th percentile" in f for f in flags) and any("3x" in f for f in flags)
    assert facts.notable({}, {}) == []


def test_noise_index_from_held_data():
    ni = facts.noise_index({"sky": {"count": 6}, "activity": {"osm": {"major_roads": 12}},
                            "safety": {"noise_complaints_311": {"total_7d": 300}},
                            "terrain": {"land_cover_nlcd_2021_pct": {"developed, high intensity": 60}}})
    assert ni["band"] in ("loud", "very loud") and set(ni["components"]) == {"aircraft", "major_roads", "complaints_311", "urban_intensity"}
    assert facts.noise_index({}) is None


def test_tracked_values_walk_dotted_paths():
    v = facts.tracked_values({"air": {"us_aqi": 41, "pm2_5": 8.3}, "sky": {"count": 3, "now": {"wind_gusts_10m": 20}}})
    assert v == {"aqi": 41.0, "pm2_5": 8.3, "aircraft_overhead": 3.0, "wind_gusts": 20.0}


def test_explainer_strips_and_parses_camera_obs(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "k"); monkeypatch.setenv("GEMINI_MODEL", "m")
    monkeypatch.setattr(explain, "weather_at", lambda lat, lon: {})
    monkeypatch.setattr(explain, "sky_images", lambda b: [])
    class _R:
        status_code = 200
        def json(self): return {"candidates": [{"content": {"parts": [{"text":
            'Cameras look quiet.\nOBS: [{"i":1,"vehicles":3,"pedestrians":0,"road_wet":false,"visibility":"good","notable":""}]'}]}}]}
    monkeypatch.setattr(explain._client, "post", lambda *a, **k: _R())
    out = explain.explain({"bounds": {}, "view": {"lat": 1, "lon": 2}, "focus": "cameras"}, [("cam A", b"\xff\xd8", "image/jpeg")])
    assert out["text"] == "Cameras look quiet." and out["camera_obs"][0]["vehicles"] == 3
