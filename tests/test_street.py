"""Street-level imagery — grid sampling, provider gating, token proxy, cost guard."""
import httpx

from apb.context import street


def test_grid_spreads_points_across_the_box():
    pts = street._grid({"south": 40.0, "north": 40.2, "west": -74.0, "east": -73.7})
    assert len(pts) == 6 and min(p[0] for p in pts) > 40.0 and max(p[1] for p in pts) < -73.7


def test_providers_gate_on_keys(monkeypatch):
    monkeypatch.delenv("MAPILLARY_TOKEN", raising=False)
    monkeypatch.delenv("GOOGLE_MAPS_KEY", raising=False)
    assert street.mapillary({"south": 1, "north": 2, "west": 3, "east": 4}) == []
    assert street.streetview({"south": 1, "north": 2, "west": 3, "east": 4}) == []
    assert street.providers() == {"mapillary": False, "kartaview": True, "streetview": False}


def test_mapillary_picks_newest_per_cell_and_streetview_bills_only_covered_points(monkeypatch):
    monkeypatch.setenv("MAPILLARY_TOKEN", "t")
    monkeypatch.setenv("GOOGLE_MAPS_KEY", "g")
    calls = []

    class _R:
        def __init__(self, d): self._d = d
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return self._d
    def get(url, params=None, **kw):
        calls.append((url, params))
        if "mapillary" in url:
            return _R({"data": [
                {"id": "a", "thumb_1024_url": "https://m/a.jpg", "captured_at": 1_000, "geometry": {"coordinates": [-73.95, 40.05]}},
                {"id": "b", "thumb_1024_url": "https://m/b.jpg", "captured_at": 2_000, "geometry": {"coordinates": [-73.95, 40.05]}},
                {"id": "c", "thumb_1024_url": "https://m/c.jpg", "captured_at": 500, "geometry": {"coordinates": [-73.75, 40.15]}, "is_pano": True}]})
        if "streetview/metadata" in url:
            ok = params["location"].startswith("40.05")
            return _R({"status": "OK", "location": {"lat": 40.05, "lng": -73.9}, "date": "2024-05", "pano_id": "xyz123"} if ok else {"status": "ZERO_RESULTS"})
        raise AssertionError(url)
    monkeypatch.setattr(street._client, "get", get)
    b = {"south": 40.0, "north": 40.2, "west": -74.0, "east": -73.7}
    fr = street.mapillary(b)
    assert len(fr) == 6 and all(f["captured"] == "1970-01-01" for f in fr)   # one query per grid cell, newest wins
    assert sum(1 for u, _ in calls if "mapillary" in u) == 6
    sv = street.streetview(b)
    assert len(sv) == 3 and all(f["provider"] == "streetview" for f in sv)
    assert not any("maps/api/streetview?" in u for u, _ in calls)      # images are fetched lazily via tokens
    assert street.image("0" * 18) is None                             # unknown token: nothing fetched


def test_frames_only_spends_streetview_when_paid(monkeypatch):
    street._cache.clear()
    monkeypatch.setattr(street, "mapillary", lambda b, limit=6: [street._frame("mapillary", "https://m/x.jpg", 1, 2)])
    monkeypatch.setattr(street, "kartaview", lambda b, limit=4: [])
    spent = []
    monkeypatch.setattr(street, "streetview", lambda b, limit=6: spent.append(1) or [street._frame("streetview", "https://g/x", 1, 2)])
    b = {"south": 1, "north": 2, "west": 3, "east": 4}
    assert [f["provider"] for f in street.frames(b)] == ["mapillary"] and not spent
    assert [f["provider"] for f in street.frames(b, paid=True)] == ["streetview", "mapillary"] and spent


def test_streetview_budget_is_a_hard_ceiling(monkeypatch, tmp_path):
    from apb.store import spend, snapshots
    monkeypatch.setattr(spend, "_ready", False)
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "s.sqlite", raising=False)
    spend.reset("streetview")
    monkeypatch.setattr(street, "SV_MONTHLY_CAP", 3)
    monkeypatch.setattr(street, "SV_DAILY_CAP", 2)
    assert spend.allow("streetview", 1, 3, 2) and spend.allow("streetview", 1, 3, 2)
    assert not spend.allow("streetview", 1, 3, 2)                     # daily cap hit: nothing reserved
    assert spend.used("streetview") == {"month": 2, "day": 2}
    b = street.budget()
    assert b["exhausted"] and b["month_usd"] == round(2 * street.SV_UNIT_USD, 2)
    monkeypatch.setenv("GOOGLE_MAPS_KEY", "g")
    assert street.streetview({"south": 1, "north": 2, "west": 3, "east": 4}) == []   # paused, no metadata calls either
    # an exhausted budget also blocks the billable fetch of an already-issued token
    tok = street._token("https://maps.googleapis.com/maps/api/streetview?size=1&key=g")
    assert street.image(tok) is None
