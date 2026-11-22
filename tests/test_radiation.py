"""Radiation lane: parsing, per-sensor baselines, flags, the budgeted digest and the API."""
from __future__ import annotations

import json
import time

import pytest

H = 3600


@pytest.fixture()
def rad(tmp_path, monkeypatch):
    from apb.context import radiation
    from apb.ingest import radnet
    from apb.store import radiation as store
    from apb.store import snapshots, spend
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "t.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    for mod in (store, spend):
        monkeypatch.setattr(mod, "_ready", False)
    monkeypatch.delenv("APB_STATE_DATABASE_URL", raising=False)
    monkeypatch.setattr(radiation, "_state", {"sensors": [], "at": 0.0, "baseline_at": 0.0, "baselines": {}})
    monkeypatch.setattr(radiation, "_place", lambda lat, lon: "Testville")
    monkeypatch.setattr(radnet, "_stations", {"AL:TESTTOWN": {"state": "AL", "city": "TESTTOWN",
                                                             "name": "Testtown, AL", "lat": 33.5, "lon": -86.8}})
    yield radiation
    if snapshots._conn is not None:
        snapshots._conn.close()


def _seed(device, now, base, recent, hours=72, lat=40.0, lon=-75.0):
    """`hours` of hourly readings at `base`, then the last two hours at `recent`."""
    from apb.store import radiation as store
    rows = []
    for h in range(hours, 1, -1):
        rows.append({"device": device, "lat": lat, "lon": lon, "ts": now - h * H, "cpm": None, "tube": "7318u", "usvh": base})
    for h in (1, 0):
        rows.append({"device": device, "lat": lat, "lon": lon, "ts": now - h * H - 60, "cpm": None,
                     "tube": "7318u", "usvh": recent})
    store.add_readings(rows)


def test_safecast_normalize_converts_tubes_and_drops_junk():
    from apb.ingest import safecast
    now = 1_800_000_000.0
    iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 600))
    raw = [{"device_urn": "geigiecast:1", "loc_lat": 42.5, "loc_lon": -70.9, "when_captured": iso, "lnd_7318u": 33},
           {"device_urn": "pointcast:2", "loc_lat": 37.4, "loc_lon": 141.0, "when_captured": iso, "lnd_7128ec": 108},
           {"device_urn": "note:dev:3", "loc_lat": 49.2, "loc_lon": -123.1, "when_captured": iso},       # air only
           {"device_urn": "old:4", "loc_lat": 1, "loc_lon": 1, "lnd_7318u": 40,
            "when_captured": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 9 * H))},
           {"device_urn": "zero:5", "loc_lat": 0, "loc_lon": 0, "when_captured": iso, "lnd_7318u": 30}]
    rows = {r["device"]: r for r in safecast.normalize(raw, now)}
    assert set(rows) == {"geigiecast:1", "pointcast:2"}
    assert rows["geigiecast:1"]["usvh"] == round(33 / 334, 4) and rows["pointcast:2"]["usvh"] == 1.0


def test_radnet_parse_reads_hourly_dose_in_utc(rad):
    from apb.ingest import radnet
    st = radnet.stations()["AL:TESTTOWN"]
    text = ("LOCATION_NAME,SAMPLE COLLECTION TIME,DOSE EQUIVALENT RATE (nSv/h),GAMMA COUNT RATE R02 (CPM),STATUS,\n"
            "AL: TESTTOWN,11/22/2026 01:46:00,51.00,1800,APPROVED,\n"
            "AL: TESTTOWN,11/22/2026 02:46:00,53.00,1810,REJECTED,\n"
            "AL: TESTTOWN,11/22/2026 03:46:00,55.00,1820,APPROVED,\n")
    rows = radnet.parse(text, "AL:TESTTOWN", st, since=0)
    assert [r["usvh"] for r in rows] == [0.051, 0.055]
    assert time.strftime("%H:%M", time.gmtime(rows[-1]["ts"])) == "03:46" and rows[0]["device"] == "radnet:AL:TESTTOWN"
    assert radnet.parse(text, "AL:TESTTOWN", st, since=rows[0]["ts"])[0]["usvh"] == 0.055     # only newer


def test_each_sensor_is_judged_against_its_own_history(rad):
    now = time.time()
    _seed("quiet", now, 0.10, 0.10)
    _seed("spike", now, 0.10, 0.30, lat=40.1)                  # 3x its own normal, sustained
    _seed("near", now, 0.10, 0.13, lat=40.15)                 # 1.3x, within 30 km: corroborates
    _seed("chernobyl", now, 2.6, 2.6, lat=51.4, lon=30.0)    # high but its normal
    _seed("blip", now, 0.10, 0.10, lat=10, lon=10)
    from apb.store import radiation as store
    store.add_readings([{"device": "blip", "lat": 10, "lon": 10, "ts": now - 30, "cpm": None, "tube": "7318u",
                         "usvh": 0.9}])                       # one reading only: not sustained
    _seed("newbie", now, 6.0, 6.0, hours=3, lat=-20, lon=20)  # no baseline, extreme
    s = {x["id"]: x for x in rad.assess(now)}
    assert s["quiet"]["status"] == "normal" and s["quiet"]["ratio"] == 1.0
    assert s["spike"]["status"] == "elevated" and s["spike"]["corroborated"] and s["spike"]["ratio"] >= 2.5
    assert s["chernobyl"]["status"] == "normal"
    assert s["blip"]["status"] == "normal"
    assert s["newbie"]["status"] == "high" and s["newbie"]["baseline"] is None
    rad._state["sensors"] = list(s.values())
    rows = {r["call_id"]: r for r in rad.hazard_rows()}
    assert set(rows) == {"safecast:spike", "safecast:newbie"}
    assert rows["safecast:spike"]["threat_score"] == 0.8 and "nearby sensor agrees" in rows["safecast:spike"]["summary"]
    assert rows["safecast:spike"]["lat"] == 40.1                # ~1 km rounding


def test_digest_runs_only_on_change_and_within_budget(rad, monkeypatch):
    now = time.time()
    _seed("a", now, 0.10, 0.10)
    rad._state["sensors"] = rad.assess(now)
    calls = []

    def gen(parts, max_tokens):
        calls.append(parts[0]["text"])
        return json.dumps({"headline": f"note {len(calls)}", "summary": "All normal.", "changes": "none",
                           "level": "normal"}), {}
    assert rad.summarise(now, gen=gen)["headline"] == "note 1"
    assert '"change_vs_24h"' in calls[0] and "(none yet)" in calls[0]
    assert rad.summarise(now + 600, gen=gen) is None                       # nothing changed
    _seed("b", now, 0.10, 0.40, lat=41)
    rad._state["baseline_at"] = 0
    rad._state["sensors"] = rad.assess(now)
    assert rad.summarise(now + 1200, gen=gen) is None                      # changed, but too soon
    assert rad.summarise(now + 3 * H, gen=gen)["headline"] == "note 2"     # changed and past the gap
    assert "note 1" in calls[-1]                                           # sees its own history
    assert rad.summarise(now + 3 * H + 60, gen=gen) is None
    assert rad.summarise(now + 28 * H, gen=gen)["headline"] == "note 3"    # daily heartbeat
    monkeypatch.setenv("APB_RAD_SUMMARY_DAILY", "0")
    assert rad.summarise(now + 60 * H, gen=gen, force=True) is None and len(calls) == 3
    monkeypatch.setenv("APB_RAD_SUMMARY_DAILY", "8")

    def boom(parts, max_tokens):
        raise RuntimeError("gemini 400")
    from apb.store import spend
    before = spend.used("radiation_summary_calls")["day"]
    with pytest.raises(RuntimeError):
        rad.summarise(now + 90 * H, gen=boom, force=True)
    assert spend.used("radiation_summary_calls")["day"] == before           # refunded


def test_radnet_rotation_backfills_a_few_stations_per_cycle(rad, monkeypatch):
    from apb.ingest import radnet
    now = time.time()
    monkeypatch.setattr(radnet, "_stations", {f"AL:T{i}": {"state": "AL", "city": f"T{i}", "name": f"T{i}, AL",
                                                          "lat": 33 + i, "lon": -86.0} for i in range(30)})
    asked = []

    def fetch(key, year, since):
        asked.append((key, since))
        st = radnet.stations()[key]
        return [{"device": radnet.device_id(key), "lat": st["lat"], "lon": st["lon"], "ts": t, "cpm": None,
                 "tube": "radnet", "usvh": 0.05, "name": st["name"]}
                for t in range(int(now - 72 * H), int(now), H) if t > since]
    n = rad.radnet_rotation(now, fetch)
    per = len(asked)
    assert 1 <= per < 30 and n == per * 72 and all(s <= now - 29 * 86400 for _, s in asked)   # backfill
    rad.radnet_rotation(now + 900, fetch)
    assert len({k for k, _ in asked}) == 2 * per                         # the next cycle takes others
    s = {x["id"]: x for x in rad.assess(now)}
    assert s["radnet:AL:T0"]["status"] == "normal" and s["radnet:AL:T0"]["name"] == "T0, AL"
    assert radnet.device_id("NY:NYC (EML)") == "radnet:NY:NYC_EML" and radnet.device_id("TX:FT. WORTH") == "radnet:TX:FT._WORTH"


def test_api_serves_sensors_summary_history_and_v1(rad, monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    from apb.api import main
    now = time.time()
    _seed("geigiecast:9", now, 0.10, 0.30)
    _seed("geigiecast:10", now, 0.10, 0.10, lat=45)
    rad._state["sensors"] = rad.assess(now)
    rad.summarise(now, gen=lambda p, max_tokens: (json.dumps({"headline": "One sensor up", "summary": "s",
                                                              "changes": "c", "level": "watch"}), {}))
    c = TestClient(main.app)
    rows = c.get("/live/radiation").json()
    assert {r["id"] for r in rows} == {"geigiecast:9", "geigiecast:10"}
    assert [r["id"] for r in c.get("/live/radiation?flagged_only=true").json()] == ["geigiecast:9"]
    summ = c.get("/live/radiation/summary").json()
    assert summ["latest"]["headline"] == "One sensor up" and summ["latest"]["stats"]["elevated_count"] == 1
    hist = c.get("/live/radiation/sensor/geigiecast:9?days=2").json()
    assert len(hist) >= 40 and hist[-1]["mean"] == 0.3
    assert c.get("/live/radiation/sensor/bad id!").status_code in (400, 404)
    from apb.api import platform
    ds = platform.registry()["datasets"]
    assert ds["radiation"]["status"] == "enabled" and ds["radiation_summary"]["status"] == "enabled"
