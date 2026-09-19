"""Background sampler: cell rotation, lite facts, camera reads, notable persistence."""
import json

import pytest

from apb.context import sampler
from apb.store import metrics as mstore


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    from apb.store import snapshots
    from apb.store import looks
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "apb.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    monkeypatch.setattr(mstore, "_ready", False)
    monkeypatch.setattr(looks, "_ready", False, raising=False)
    sampler._state.update(cursor=0, cam_cursor=0, errors=0)
    yield
    if snapshots._conn is not None:
        snapshots._conn.close()


def test_cell_bounds_snap_to_grid():
    b = sampler._cell_bounds(35.78, -78.64)
    assert b == {"south": 35.75, "north": 35.8, "west": -78.65, "east": -78.6}


def test_facts_pass_rotates_and_uses_lite(monkeypatch):
    from apb.context import facts
    calls = []
    monkeypatch.setattr(facts, "facts", lambda bounds, timeout, lite: calls.append((bounds, lite)))
    monkeypatch.setattr(sampler, "FACTS_BATCH", 2)
    monkeypatch.setattr(sampler, "METROS", [(37.81, -122.3)])
    centers = [(47.6, -122.3), (39.1, -77.2)]
    assert sampler.facts_pass(centers) == 2
    assert sampler.facts_pass(centers) == 2
    assert all(lite for _, lite in calls)
    souths = [c[0]["south"] for c in calls]
    assert souths == [47.6, 39.1, 37.8, 47.6]          # CAD centers first, then metros, then wraps


def test_cams_pass_records_observations(monkeypatch):
    from apb.context import explain

    class Reg:
        def query(self, limit, online_only):
            return [{"id": "x:1", "lat": 35.78, "lon": -78.64, "image_url": "http://a"},
                    {"id": "x:2", "lat": 35.78, "lon": -78.64, "image_url": "http://b"}]

        def snapshot(self, cam_id):
            return b"\xff\xd8jpeg", "image/jpeg"

    monkeypatch.setattr(explain, "api_key", lambda: "k")
    seen = {}
    def gen(parts, max_tokens):
        seen["images"] = sum(1 for p in parts if "inline_data" in p)
        return 'OBS: [{"i":1,"vehicles":4,"pedestrians":0,"road_wet":true,"visibility":"good","notable":""},' \
               '{"i":2,"vehicles":0,"pedestrians":2,"road_wet":false,"visibility":"reduced","notable":"fog"}]'
    monkeypatch.setattr(explain, "generate", gen)
    assert sampler.cams_pass(Reg()) == 2
    assert seen["images"] == 2
    agg = mstore.camera_obs(mstore.cell_for(35.78, -78.64))
    assert agg["frames_read_24h"] == 2 and agg["mean_vehicles_per_frame"] == 2.0 and agg["notable"] == ["fog"]


def test_notable_cells_roundtrip():
    mstore.record_notable("35.75,-78.65", 35.775, -78.625, ["PM2.5 unhealthy (61 µg/m³)"])
    mstore.record_notable("35.80,-78.65", 35.825, -78.625, [])
    rows = mstore.notable_cells()
    assert [r["cell"] for r in rows] == ["35.75,-78.65"]
    assert rows[0]["flags"] == ["PM2.5 unhealthy (61 µg/m³)"]
    mstore.record_notable("35.75,-78.65", 35.775, -78.625, [])
    assert mstore.notable_cells() == []


def test_start_respects_off_switch(monkeypatch):
    monkeypatch.setenv("APB_SAMPLER_OFF", "1")
    monkeypatch.setattr(sampler, "_thread", None)
    sampler.start([], None)
    assert sampler.status()["running"] is False


def test_notable_history_and_streak():
    cell = "35.75,-78.65"
    import time
    for i, flags in enumerate([["drought D2"], [], ["drought D2"], ["AQI 120 (usg)"]]):
        mstore.record_notable(cell, 35.775, -78.625, flags)
        with mstore._lock:                                   # space the passes past the 10-min de-dup
            mstore._conn().execute("UPDATE cell_notable_log SET ts = ts - ? WHERE cell = ? AND ts > ?",
                                   (3600 * (4 - i), cell, time.time() - 30)).connection.commit()
    h = mstore.notable_history(cell)
    assert h["passes"] == 4 and h["flagged"] == 3 and h["flagged_recent"] == 3
    assert h["recurring"][0] == ("drought D2", 2)
    rows = mstore.notable_cells(max_age_hours=24)
    assert rows[0]["streak"] == 3 and rows[0]["passes"] == 4


def test_camera_obs_series_bins():
    cell = "35.75,-78.65"
    mstore.record_camera_obs(cell, [{"camera_id": "x:1", "vehicles": 4, "pedestrians": 1}, {"camera_id": "x:2", "vehicles": 6}])
    s = mstore.camera_obs_series(cell, hours=24, bins=12)
    assert len(s) == 12 and s[-1]["frames"] == 2 and s[-1]["vehicles"] == 5.0 and s[-1]["pedestrians"] == 1.0
    assert s[0]["vehicles"] is None


def test_watchlist_includes_metros():
    cells = sampler._cells_to_watch([(47.6, -122.3)])
    assert len(cells) >= 50 and (47.6, -122.3) in cells


def test_cams_pass_tolerates_backticks_and_zero_index(monkeypatch):
    from apb.context import explain

    class Reg:
        _loading = set()
        def query(self, limit, online_only):
            return [{"id": "y:1", "lat": 40.0, "lon": -100.0, "image_url": "http://a"},
                    {"id": "y:2", "lat": 40.0, "lon": -100.0, "image_url": "http://b"}]
        def snapshot(self, cam_id):
            return b"\xff\xd8", "image/jpeg"

    monkeypatch.setattr(explain, "api_key", lambda: "k")
    monkeypatch.setattr(explain, "generate", lambda parts, max_tokens:
        '`OBS: `[{"i": 0, "vehicles": 3, "pedestrians": 0, "road_wet": false, "visibility": "good", "notable": ""}, '
        '{"i": 1, "vehicles": 5, "pedestrians": 1, "road_wet": false, "visibility": "good", "notable": "x"}]')
    assert sampler.cams_pass(Reg()) == 2
    assert mstore.camera_obs(mstore.cell_for(40.0, -100.0))["mean_vehicles_per_frame"] == 4.0
