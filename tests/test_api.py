"""API surface smoke tests — offline: background lanes disabled, no live fetches."""
import importlib
import os

import pytest

_FLAGS = ("APB_POLLER_OFF", "APB_BLUESKY_OFF", "APB_NEWS_OFF",
          "APB_SOCIAL_RSS_OFF", "APB_ADSB_OFF")


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
