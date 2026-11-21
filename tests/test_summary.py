import json
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
from fastapi.testclient import TestClient


def test_view_all_uses_stored_windows_without_models_or_live_fetches(tmp_path, monkeypatch):
    from apb.api import main
    from apb.context import brief, explain
    from apb.fusion.signals import text_signal
    from apb.common.models import SignalKind
    from apb.store import events, feedhealth, metrics, radio, sigbuf, snapshots, spend, usage

    monkeypatch.delenv("APB_STATE_DATABASE_URL", raising=False)
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "summary.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    for store in (events, feedhealth, metrics, radio, sigbuf, spend, usage):
        monkeypatch.setattr(store, "_ready", False)
    monkeypatch.setattr(main, "_resp_cache", {})
    monkeypatch.setattr(main, "_place_name", lambda lat, lon: "Seattle")
    monkeypatch.setattr(main, "CAD_FEEDS", {"hz_nws": SimpleNamespace(name="NWS Alerts")})

    def unavailable(*args, **kwargs):
        raise AssertionError("The brief must use the database only")

    monkeypatch.setattr(explain, "generate", unavailable)
    monkeypatch.setattr(explain._client, "post", unavailable)
    monkeypatch.setattr(main._cad, "overview", unavailable)

    now = 1_800_000_000
    at = [now - 6 * 86400]
    monkeypatch.setattr(snapshots.time, "time", lambda: at[0])
    incident = {"metro": "city_cad", "call_id": "001", "type": "fire", "emerging": True,
                "lat": 47, "lon": -122, "summary": "local CAD private details", "ts": at[0]}
    snapshots.record([incident])
    radio.add_transcript("abc", 6, at[0], 1, 2, "done", kind="dispatch", incident_type="fire")
    radio.record("abc", {1: (14, {})}, 600)

    at[0] = now - 50
    snapshots.record([incident, {**incident, "metro": "hz_nws", "type": "weather",
                                 "summary": "High wind warning"}])
    radio.add_recent("abc", [(7, now - 180, 1, 2)])
    radio.add_recent("abc", [(7, now - 180, 1, 2)])
    radio.add_transcript("abc", 7, now - 180, 1, 2, "done", kind="dispatch", incident_type="medical",
                         text="private scanner details", lat=47, lon=-122)
    bird = ("iNaturalist sightings", "observation-1", "Bald Eagle")
    metrics.record_fact_updates("47.00,-122.00", [("Temperature", 1, "8 °C")], [bird])
    metrics.record_fact_updates("47.00,-122.00", [("Temperature", 1, "9 °C")], [bird])
    metrics.record_fact_updates("47.05,-122.00", [], [bird])
    metrics.record_notable("47.00,-122.00", 47, -122, ["gusts 60 km/h"])
    sigbuf.save("news", [text_signal(source="test", source_kind=SignalKind.news,
                                     text="Headline for test", lat=47, lon=-122,
                                     observed_at=datetime.fromtimestamp(now - 180, timezone.utc))])
    with snapshots.db_lock:
        c = events._conn()
        c.execute("""INSERT INTO fused_events (uid, lat, lon, first_seen, last_seen, peak_score,
                                               source_count, types, sources)
                     VALUES (?,?,?,?,?,?,?,?,?)""",
                  ("fev:47:-122:1800000000", 47, -122, at[0], at[0], 8, 2,
                   json.dumps({"fire": 2}), json.dumps({"cad": 1, "radio_transcript": 1})))
        c.commit()

    at[0] = now
    client = TestClient(main.app)

    def summary(period):
        response = client.get("/live/summary", params={"period": period})
        assert response.status_code == 200
        assert "private" not in response.text
        data = response.json()
        assert data["until"] == now
        assert "Seattle" in data["read"]
        return {s["title"]: {t["label"]: t["count"] for t in s["totals"]} for s in data["sections"]}

    minute = summary("minute")
    assert minute["Incidents & emerging events"] == {
        "incident reports": 0, "new fused events": 1, "marked emerging": 0,
    }
    assert minute["Radio"]["call records received"] == 1
    assert minute["Radio"]["calls classified"] == 1
    assert minute["Weather & storms"] == {"feed reports": 1, "sampled condition updates": 1}
    assert minute["Ecology & environment"]["sightings"] == 1
    assert minute["Notable facts"]["new flags"] == 1
    assert minute["Other feeds"]["news items"] == 1
    assert summary("day")["Radio"]["calls classified"] == 1
    week = summary("week")
    assert week["Radio"]["calls tallied"] == 14
    assert week["Radio"]["calls classified"] == 2
    assert week["Incidents & emerging events"]["incident reports"] == 1
    assert client.get("/live/summary?period=year").status_code == 422

    monkeypatch.setattr(brief, "_cache", {})
    monkeypatch.setattr(brief, "DAILY_CALLS", 1)
    monkeypatch.setattr(explain, "api_key", lambda: "test-key")
    calls = []

    def fake_model(url, **kwargs):
        body = kwargs["json"]
        calls.append(body)
        assert brief.MODEL in url
        assert body["generationConfig"]["thinkingConfig"] == {"thinkingLevel": "MINIMAL"}
        packet = json.loads(body["contents"][0]["parts"][1]["text"])
        assert "private" not in str(packet)
        same_event = next(e for e in packet["evidence"] if e.get("event"))
        assert same_event["detail"] == "1 cad, 1 radio transcript"
        point = {"text": "Two source kinds reported a fire near Seattle.", "sources": [same_event["id"], 999]}
        return httpx.Response(200, json={
            "candidates": [{"content": {"parts": [{"text": json.dumps({"points": [point]})}]}}],
            "usageMetadata": {"promptTokenCount": 500, "candidatesTokenCount": 80},
        })

    monkeypatch.setattr(explain._client, "post", fake_model)
    result = client.post("/live/summary/brief?period=minute")
    assert result.status_code == 200
    data = result.json()
    assert data["state"] == "ready" and len(data["points"]) == 1
    assert data["points"][0]["sources"] == [data["evidence"][0]["id"]]
    assert client.post("/live/summary/brief?period=minute").json() == data
    assert client.post("/live/summary/brief?period=day").status_code == 429
    assert len(calls) == 1 and spend.used(brief.FEATURE)["day"] == 1
    assert usage.summary(1)["by_feature"][brief.FEATURE]["calls"] == 1
    at[0] += 120
    assert client.post("/live/summary/brief?period=minute").json()["state"] == "empty"
    assert len(calls) == 1
    snapshots._conn.close()
