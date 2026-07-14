"""API surface smoke tests — offline: background lanes disabled, no live fetches."""
import importlib
import os

import pytest

_FLAGS = ("APB_POLLER_OFF", "APB_BLUESKY_OFF", "APB_NEWS_OFF",
          "APB_GNEWS_OFF", "APB_SOCIAL_RSS_OFF", "APB_ADSB_OFF")


@pytest.fixture(scope="module")
def client():
    for f in _FLAGS:
        os.environ[f] = "1"
    from fastapi.testclient import TestClient
    main = importlib.import_module("apb.api.main")
    with TestClient(main.app) as c:
        yield c


def test_import_is_side_effect_free():
    # Registration happens in lifespan, not at import: pre-import module state
    # only has the curated flagship feeds.
    import apb.ingest.cad  # noqa: F401  (already imported; assert lifespan grew it)


def test_health_and_metros(client):
    assert client.get("/health").json() == {"status": "ok"}
    metros = client.get("/live/metros").json()
    assert len(metros) > 100                      # catalogs registered via lifespan
    assert {"metro", "name", "state", "center"} <= set(metros[0])


def test_live_lane_endpoints_shape(client):
    # Keyed lanes without keys must be empty lists, not errors.
    for path in ("/live/airnow", "/live/unrest", "/live/airquality", "/live/fire"):
        r = client.get(path)
        assert r.status_code == 200
        assert r.json() == []


def test_fused_offline_returns_scored_clusters(client):
    r = client.get("/live/fused", params={"include_live": "false"})
    assert r.status_code == 200
    for evt in r.json():
        assert evt["surge_score"] >= 1.2
        assert evt["count"] >= 2


def test_live_endpoints_get_cache_control(client):
    r = client.get("/live/metros")
    assert r.headers.get("cache-control") == "public, max-age=15"
    assert "cache-control" not in client.get("/health").headers


def test_security_headers_and_readiness(client):
    r = client.get("/health")
    assert r.headers["x-content-type-options"] == "nosniff"
    assert r.headers["x-frame-options"] == "DENY"
    assert "default-src 'self'" in r.headers["content-security-policy"]
    ready = client.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"


@pytest.mark.parametrize(("path", "params"), [
    ("/live/overview", {"limit_per": 100_000}),
    ("/live/social", {"max_age_hours": -1}),
    ("/feeds", {"lat": 91, "lon": 0}),
    ("/correlate", {"lat": 0, "lon": 0, "timespan": "forever"}),
])
def test_public_query_bounds(client, path, params):
    assert client.get(path, params=params).status_code == 422


def test_hazard_aggregate_is_offline_safe(client, monkeypatch):
    from apb.api import main
    monkeypatch.setattr(main._cad, "fetch", lambda _slug, limit=400: [])
    r = client.get("/live/hazards/all", params={"max_age_hours": 24})
    assert r.status_code == 200
    assert r.json() == []
    assert r.headers["etag"]
    cached = client.get("/live/hazards/all", params={"max_age_hours": 24},
                        headers={"If-None-Match": r.headers["etag"]})
    assert cached.status_code == 304


def test_sse_snapshot_selects_national_or_metro(monkeypatch):
    from apb.api import main
    calls = []
    def query(max_age_hours, metro=None, limit=8000):
        calls.append((max_age_hours, metro, limit))
        return [{"call_id": metro or "national"}]
    monkeypatch.setattr(main.snapshots, "query", query)
    assert main._stream_snapshot("__all__", 1)["incidents"][0]["call_id"] == "national"
    local = main._stream_snapshot("seattle", 1)
    assert local["metro"] == "seattle"
    assert local["incidents"][0]["call_id"] == "seattle"
    assert calls == [(1, None, 8000), (1, "seattle", 400)]


def test_display_routes_never_fetch_upstream(client, monkeypatch):
    from apb.api import main
    def forbidden(*_args, **_kwargs):
        raise AssertionError("interactive display route attempted an upstream fetch")
    monkeypatch.setattr(main._cad, "fetch", forbidden)
    monkeypatch.setattr(main._cad, "overview", forbidden)
    assert client.get("/live/incidents", params={"metro": "seattle",
                                                   "max_age_hours": 24}).status_code == 200
    assert client.get("/live/overview", params={"max_age_hours": 24}).status_code == 200
    assert client.get("/live/emerging", params={"max_age_hours": 24}).status_code == 200
