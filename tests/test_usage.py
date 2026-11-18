"""Model usage meter: token split, recording through the Gemini helpers, cost summary."""
import time

import pytest


@pytest.fixture()
def db(tmp_path, monkeypatch):
    from apb.context import tasking
    from apb.store import alertlog, events, feedhealth, metrics, radio, snapshots, spend, usage
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "u.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    for mod in (usage, spend, radio, events, feedhealth, alertlog, metrics, tasking):
        monkeypatch.setattr(mod, "_ready", False)
    monkeypatch.setattr(events, "_hist_ready", False)
    monkeypatch.delenv("APB_STATE_DATABASE_URL", raising=False)
    for k in ("APB_PRICE_IN_TEXT", "APB_PRICE_IN_AUDIO", "APB_PRICE_OUT"):
        monkeypatch.delenv(k, raising=False)
    yield usage
    if snapshots._conn is not None:
        snapshots._conn.close()


def test_split_by_modality():
    from apb.store.usage import split
    u = {"promptTokenCount": 3000, "candidatesTokenCount": 120, "thoughtsTokenCount": 30,
         "promptTokensDetails": [{"modality": "TEXT", "tokenCount": 400},
                                 {"modality": "IMAGE", "tokenCount": 2580},
                                 {"modality": "AUDIO", "tokenCount": 20}]}
    assert split(u) == {"in_text": 400, "in_image": 2580, "in_audio": 20, "out": 150}
    assert split({"promptTokenCount": 77, "candidatesTokenCount": 5})["in_text"] == 77


def test_summary_prices_each_feature(db, monkeypatch):
    usage = db
    now = time.time()
    usage.record("camera_sampler", "gemini-x-flash", {"promptTokenCount": 1_000_000,
                 "candidatesTokenCount": 100_000}, now)
    usage.record("radio_stt", "gemini-x-flash", {"promptTokensDetails": [
        {"modality": "AUDIO", "tokenCount": 500_000}, {"modality": "TEXT", "tokenCount": 0}],
        "candidatesTokenCount": 0}, now)
    usage.record("radio_stt", "gemini-x-flash", {"promptTokensDetails": [
        {"modality": "AUDIO", "tokenCount": 500_000}], "candidatesTokenCount": 0}, now - 86400)
    s = usage.summary(7)
    assert s["by_feature"]["camera_sampler"]["usd"] == pytest.approx(0.30 + 0.25)
    assert s["by_feature"]["radio_stt"]["usd"] == pytest.approx(1.00)
    assert s["by_feature"]["radio_stt"]["calls"] == 2
    assert s["today_usd"] == pytest.approx(1.05) and s["avg_day_usd"] == pytest.approx(0.5)
    assert list(s["by_feature"]) == ["radio_stt", "camera_sampler"]          # costliest first
    monkeypatch.setenv("APB_PRICE_OUT", "0")
    assert usage.summary(7)["by_feature"]["camera_sampler"]["usd"] == pytest.approx(0.30)


def test_meter_never_breaks_the_caller(db, monkeypatch):
    usage = db
    monkeypatch.setattr(usage, "_conn", lambda: (_ for _ in ()).throw(RuntimeError("disk full")))
    usage.record("x", "m", {"promptTokenCount": 1})                         # no exception


def test_generate_records_feature_and_model(db, monkeypatch):
    from apb.context import explain

    class R:
        status_code = 200
        text = ""

        def json(self):
            return {"candidates": [{"content": {"parts": [{"text": "ok"}]}}],
                    "usageMetadata": {"promptTokenCount": 1000, "candidatesTokenCount": 10}}
    monkeypatch.setattr(explain, "api_key", lambda: "k")
    monkeypatch.setattr(explain, "healthy_models", lambda key: ["gemini-test-flash"])
    monkeypatch.setattr(explain._client, "post", lambda *a, **k: R())
    assert explain.generate([{"text": "hi"}], feature="sitrep") == "ok"
    text, _ = explain.generate_usage([{"text": "hi"}], feature="radio_stt")
    s = db.summary(1)
    assert s["by_feature"]["sitrep"]["calls"] == 1 and s["by_feature"]["sitrep"]["models"] == ["gemini-test-flash"]
    assert s["by_feature"]["radio_stt"]["in_text"] == 1000


def test_costs_endpoints(db):
    import importlib
    import os
    for f in ("APB_POLLER_OFF", "APB_BLUESKY_OFF", "APB_NEWS_OFF", "APB_GNEWS_OFF", "APB_SOCIAL_RSS_OFF",
              "APB_ADSB_OFF", "APB_CAMERAS_OFF", "APB_SAMPLER_OFF", "APB_RDIO_OFF"):
        os.environ[f] = "1"
    from fastapi.testclient import TestClient
    db.record("camera_tasking", "m", {"promptTokenCount": 2000, "candidatesTokenCount": 100})
    main = importlib.import_module("apb.api.main")
    with TestClient(main.app) as c:
        costs = c.get("/status/costs").json()
        assert costs["gemini"]["by_feature"]["camera_tasking"]["calls"] == 1
        assert {"youtube_units", "radio_audio_sec"} <= set(costs["quotas"])
        assert "camera_tasking" in c.get("/status").json()["costs"]["top_features_7d"]
