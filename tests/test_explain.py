"""Scene explainer — prompt assembly, model choice and the /explain guard rails.
Offline: Gemini and Open-Meteo are stubbed."""
import httpx
import pytest

from apb.context import explain


def test_pick_model_prefers_highest_stable_flash(monkeypatch):
    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    explain._model_cache.update(names=None, at=0.0)
    names = ["gemini-2.5-flash", "gemini-2.5-pro", "gemini-3.1-flash-preview-09-01",
             "gemini-3.1-flash", "gemini-3.1-flash-lite", "gemini-3.1-flash-image",
             "gemini-3.2-flash-preview-11-01"]

    class _R:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"models": [{"name": "models/" + n,
                                            "supportedGenerationMethods": ["generateContent"]}
                                           for n in names]}
    monkeypatch.setattr(explain._client, "get", lambda *a, **k: _R())
    assert explain.pick_model("k") == "gemini-3.2-flash-preview-11-01"   # newest wins, preview or not
    names.remove("gemini-3.2-flash-preview-11-01")
    explain._model_cache.update(names=None, at=0.0)
    assert explain.pick_model("k") == "gemini-3.1-flash"                  # stable beats preview at equal version
    monkeypatch.setenv("GEMINI_MODEL", "gemini-9-flash")
    assert explain.pick_model("k") == "gemini-9-flash"


def test_pick_model_falls_back_without_caching_failure(monkeypatch):
    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    explain._model_cache.update(names=None, at=0.0)
    def boom(*a, **k): raise httpx.ConnectError("down")
    monkeypatch.setattr(explain._client, "get", boom)
    assert explain.pick_model("k") == explain._FALLBACK_MODEL
    assert explain._model_cache["names"] is None


def test_prompt_carries_focus_layers_weather_and_camera_order():
    ctx = {"bounds": {"south": 40.7, "north": 40.8, "west": -74.0, "east": -73.9},
           "view": {"lat": 40.75, "lon": -73.95, "zoom": 12}, "layers": ["spikes", "cameras"],
           "incidents": [{"type": "fire", "threat": 0.8}], "focus": "cameras",
           "social": [{"text": "smoke on 5th"}], "counts": {"incidents_in_box": 1}}
    text = explain.build_prompt(ctx, {"conditions": "fog", "temperature_2m": 12}, ["A (nyctmc)", "B (ny511)"])
    assert "For EACH attached still" in text
    assert "Layers on: spikes, cameras" in text and '"conditions": "fog"' in text
    assert "[1] A (nyctmc); [2] B (ny511)" in text and "smoke on 5th" in text
    quiet = explain.build_prompt({"bounds": {}, "view": {}}, {}, [])
    assert "No camera stills" in quiet and "what am I looking at" in quiet


def test_explain_requires_key_and_surfaces_upstream_errors(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        explain.explain({"bounds": {}, "view": {}}, [])
    monkeypatch.setenv("GEMINI_API_KEY", "k")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-test")
    monkeypatch.setattr(explain, "weather_at", lambda lat, lon: {"conditions": "clear"})

    class _Bad:
        status_code = 429
        def json(self): return {"error": {"message": "quota"}}
    monkeypatch.setattr(explain._client, "post", lambda *a, **k: _Bad())
    with pytest.raises(RuntimeError, match="429"):
        explain.explain({"bounds": {}, "view": {"lat": 1, "lon": 2}}, [])

    sent = {}
    class _Ok:
        status_code = 200
        def json(self): return {"candidates": [{"content": {"parts": [{"text": " hi "}]}}]}
    def post(url, params=None, json=None, **k):
        sent.update(url=url, body=json); return _Ok()
    monkeypatch.setattr(explain._client, "post", post)
    out = explain.explain({"bounds": {}, "view": {"lat": 1, "lon": 2}, "focus": "weather"},
                          [("cam A", b"\xff\xd8", "image/jpeg")])
    assert out["text"] == "hi" and out["focus"] == "weather" and out["cameras"] == ["cam A"]
    assert sent["url"].endswith("/models/gemini-test:generateContent")
    parts = sent["body"]["contents"][0]["parts"]
    assert parts[1]["inline_data"]["mime_type"] == "image/jpeg"


def test_explain_steps_down_when_newest_model_is_busy(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "k")
    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    monkeypatch.setattr(explain, "candidate_models", lambda key: ["gemini-9-flash", "gemini-8-flash"])
    monkeypatch.setattr(explain, "weather_at", lambda lat, lon: {})
    tried = []
    class _R:
        def __init__(self, code, text=""): self.status_code = code; self._t = text
        def json(self):
            return ({"candidates": [{"content": {"parts": [{"text": self._t}]}}]} if self.status_code == 200
                    else {"error": {"message": "high demand"}})
    def post(url, **k):
        tried.append(url.split("/models/")[1].split(":")[0])
        return _R(503) if "9-flash" in url else _R(200, "ok")
    monkeypatch.setattr(explain._client, "post", post)
    out = explain.explain({"bounds": {}, "view": {"lat": 1, "lon": 2}}, [])
    assert out["model"] == "gemini-8-flash" and tried == ["gemini-9-flash", "gemini-8-flash"]
