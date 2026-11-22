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


# ---- drive surveys -------------------------------------------------------------------

def _log(lat0, lon0, cpm, n=30, start="2026-11-20T10:00:00Z", kind="BN"):
    """A bGeigie log driving east from (lat0, lon0), one reading per ~8 m."""
    from datetime import datetime, timedelta
    t0 = datetime.fromisoformat(start.replace("Z", "+00:00"))
    lines = ["# header", f"$BNRDD,0001,{start},10,1,10,V,0000.0000,N,00000.0000,E,0,V,0,0*00"]   # no fix
    for i in range(n):
        lat, lon = lat0, lon0 + i * 0.0001
        d, m = int(abs(lat)), (abs(lat) - int(abs(lat))) * 60
        e, k = int(abs(lon)), (abs(lon) - int(abs(lon))) * 60
        ts = (t0 + timedelta(seconds=5 * i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        lines.append(f"${kind}RDD,0001,{ts},{cpm},3,100,A,{d:02d}{m:07.4f},{'N' if lat >= 0 else 'S'},"
                     f"{e:03d}{k:07.4f},{'E' if lon >= 0 else 'W'},12.0,A,9,90*3F")
    return "\n".join(lines)


def test_bgeigie_parse_reads_every_rdd_kind_and_needs_a_fix():
    from apb.ingest import bgeigie
    pts = bgeigie.parse(_log(40.5, -74.25, 33.4, n=3) + "\n" + _log(49.5, 16.5, 40, n=2, kind="CZ"))
    assert len(pts) == 5 and {p["kind"] for p in pts} == {"BN", "CZ"}
    assert abs(pts[0]["lat"] - 40.5) < 1e-4 and abs(pts[0]["lon"] + 74.25) < 1e-4
    assert abs(pts[0]["usvh"] - 0.1) < 1e-6
    assert bgeigie.region(40.5, -74.25) == "north_america" and bgeigie.region(49.5, 16.5) == "europe"


def test_survey_ingest_folds_cells_and_flags_raised_roads(rad):
    from apb.context import radsurvey
    from apb.store import radiation as store

    def imp(i, created="2026-11-20T12:00:00Z"):
        return {"id": i, "created_at": created, "status": "done", "approved": True, "rejected": False,
                "source": {"url": f"https://example.org/{i}.log"}, "cities": "Testville", "credits": "volunteer"}
    for i in (1, 2, 3):                                      # three earlier drives: normal road
        radsurvey.ingest(imp(i), _log(40.5, -74.25, 33.4, n=60))
    row = radsurvey.ingest(imp(4), _log(40.5, -74.25, 100.2, n=60))      # 3x on the same road
    assert row["state"] == "ingested" and row["region"] == "north_america" and row["n"] == 60
    assert row["flagged_cells"] >= 5
    quiet = radsurvey.ingest(imp(5), _log(40.5, -74.25, 34, n=60))
    assert quiet["flagged_cells"] == 0
    assert radsurvey.ingest(imp(6), "garbage")["state"] == "empty"
    cells = store.survey_cells(-74.3, 40.4, -74.2, 40.6, 1)
    assert cells and max(c[4] for c in cells) >= 5            # several drives' readings per cell
    t = radsurvey.tile(8, 75, 96)                             # the tile over (40.5, -74.25)
    assert t and all(len(r) == 6 for r in t)
    st = radsurvey.digest_stats(time.time())
    assert st["drives_7d"] >= 4 and st["flagged_drives_7d"][0]["raised_cells"] >= 5


def test_backfill_keeps_north_america_and_skips_the_rest(rad, monkeypatch):
    from apb.context import radsurvey
    from apb.ingest import bgeigie
    from apb.store import radiation as store
    logs = {1: _log(40.5, -74.25, 33), 2: _log(49.5, 16.5, 40), 3: _log(35.6, 139.7, 30)}
    imps = [{"id": 1, "status": "done", "approved": True, "source": {"url": "u1"}, "cities": "Newark"},
            {"id": 2, "status": "done", "approved": True, "source": {"url": "u2"}, "cities": "Brno"},
            {"id": 3, "status": "done", "approved": True, "source": {"url": "u3"}, "cities": "東京都"},
            {"id": 4, "status": "submitted", "approved": False, "source": {"url": "u4"}, "cities": ""}]
    pages = {1: imps, 2: []}
    monkeypatch.setattr(bgeigie, "page", lambda n, newest_first=False: pages.get(n, []))
    probed, fetched = [], []
    monkeypatch.setattr(bgeigie, "probe", lambda url: probed.append(url) or bgeigie.parse(logs[int(url[1:])])[0:1]
                        and (bgeigie.parse(logs[int(url[1:])])[0]["lat"], bgeigie.parse(logs[int(url[1:])])[0]["lon"]))
    monkeypatch.setattr(bgeigie, "fetch", lambda url: fetched.append(url) or logs[int(url[1:])])
    assert radsurvey.backfill_step(pages=1) == 1
    assert sorted(probed) == ["u1", "u2"] and fetched == ["u1"]        # Japanese city text: no request
    st = store.survey_counts()
    assert st["ingested_by_region"] == {"north_america": 1} and st["logs_by_state"]["skipped"] == 2
    assert radsurvey.backfill_step(pages=1) == 0 and store.survey_meta("backfill_done")
    assert store.survey_known([4]) == set()                            # not approved: retried later


def test_survey_endpoints(rad):
    from fastapi.testclient import TestClient
    from apb.api import main
    from apb.context import radsurvey
    radsurvey.ingest({"id": 9, "created_at": "2026-11-20T12:00:00Z", "status": "done", "approved": True,
                      "source": {"url": "x"}, "cities": "Newark"}, _log(40.5, -74.25, 33.4, n=40))
    c = TestClient(main.app)
    assert c.get("/live/radiation/survey/8/75/96").json()
    assert c.get("/live/radiation/survey/8/999/96").status_code == 400
    box = c.get("/live/radiation/survey?bbox=-74.3,40.4,-74.2,40.6&z=12").json()
    assert box and abs(box[0][2] - 0.1) < 0.001
    assert c.get("/live/radiation/survey?bbox=1,2").status_code == 400
    drives = c.get("/live/radiation/survey-drives?days=36500&region=north_america").json()
    assert drives[0]["id"] == 9 and drives[0]["cities"] == "Newark"
