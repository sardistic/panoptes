import json
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient


def test_view_all_uses_stored_windows_without_models_or_live_fetches(tmp_path, monkeypatch):
    from apb.api import main
    from apb.context import explain
    from apb.fusion.signals import text_signal
    from apb.common.models import SignalKind
    from apb.store import events, metrics, radio, sigbuf, snapshots

    monkeypatch.delenv("APB_STATE_DATABASE_URL", raising=False)
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "summary.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    for store in (events, metrics, radio, sigbuf):
        monkeypatch.setattr(store, "_ready", False)
    monkeypatch.setattr(main, "_resp_cache", {})
    monkeypatch.setattr(main, "_place_name", lambda lat, lon: "Seattle")
    monkeypatch.setattr(main, "CAD_FEEDS", {"hz_nws": SimpleNamespace(name="NWS Alerts")})

    def unavailable(*args, **kwargs):
        raise AssertionError("The brief must use the database only")

    monkeypatch.setattr(explain, "generate", unavailable)
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
                                               source_count, types)
                     VALUES (?,?,?,?,?,?,?,?)""",
                  ("fev:47:-122:1800000000", 47, -122, at[0], at[0], 8, 2, json.dumps({"fire": 2})))
        c.commit()

    at[0] = now
    client = TestClient(main.app)

    def summary(period):
        response = client.get("/live/summary", params={"period": period})
        assert response.status_code == 200
        assert "private" not in response.text
        data = response.json()
        assert data["until"] == now
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
    snapshots._conn.close()
