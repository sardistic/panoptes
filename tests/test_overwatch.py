"""Overwatch layer — feed health, event history, alert rules, scorekeeping, baseline
statistics, camera tasking, derived fusion signals, Radar lane. All offline."""
import time
from datetime import datetime, timedelta, timezone

import pytest

from apb.common.models import SignalKind
from apb.fusion.cluster import FusedEvent, detect
from apb.fusion.signals import incident_to_signal, text_signal


@pytest.fixture()
def db(tmp_path, monkeypatch):
    from apb.context import tasking
    from apb.store import alertlog, events, feedhealth, metrics, snapshots, spend
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "t.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    for mod in (events, feedhealth, alertlog, metrics, tasking, spend):
        monkeypatch.setattr(mod, "_ready", False)
    monkeypatch.setattr(events, "_hist_ready", False)
    tasking._queue.clear()
    monkeypatch.delenv("APB_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("APB_STATE_DATABASE_URL", raising=False)
    monkeypatch.setenv("APB_ALERT_RULES", str(tmp_path / "no-rules.json"))
    yield tmp_path
    if snapshots._conn is not None:
        snapshots._conn.close()


def _cad(i, kind="fire", lat=47.610, lon=-122.330, ts=None, threat=0.7):
    return incident_to_signal({"metro": "seattle", "call_id": str(i), "type": kind,
                               "summary": f"{kind} response", "lat": lat, "lon": lon,
                               "threat_score": threat, "ts": ts or time.time()})


def _evt(lat=40.0, lon=-100.0, score=3.0, count=4, sources=2):
    return FusedEvent(event_id="x", lat=lat, lon=lon, count=count, source_count=sources,
                      sources={"cad": 3, "social": 1}, types={"fire": 4},
                      peak_severity=0.7, mean_severity=0.5, confidence=0.6,
                      surge_score=score, latest_ts=time.time(),
                      summaries=["Structure fire"], signal_ids=[])


# ── fusion scoring ───────────────────────────────────────────────────────────

def test_future_dated_signals_cannot_inflate_the_score():
    # prod: a far-future timestamp drove recency to ~29,000 and the score to ~500,000
    far = time.time() + 20 * 365 * 86400
    events = detect([_cad(1), _cad(2), _cad(3, ts=far)], min_count=2, min_score=0.1)
    assert len(events) == 1
    assert events[0].count == 2                       # the scheduled row is not "now"
    assert events[0].score_components["recency"] <= 1.0
    assert events[0].surge_score < 20


def test_centroid_mention_needs_a_located_signal_of_the_same_kind():
    now = datetime.now(timezone.utc)

    def post(text):
        return text_signal(source="bluesky", source_kind=SignalKind.social, text=text,
                           observed_at=now, lat=47.610, lon=-122.330, confidence=0.35,
                           metadata={"coarse": True})
    # a "shooting" mention placed on the Seattle centroid next to fire calls: no vote
    lone = detect([_cad(1), _cad(2), post("shooting reported in Seattle")],
                  min_count=2, min_score=0.1)
    assert lone[0].source_count == 1 and lone[0].assessment == "single_source"
    # the same mention agreeing with what dispatch located there: corroboration
    agree = detect([_cad(1), _cad(2), post("huge fire and smoke in Seattle")],
                   min_count=2, min_score=0.1)
    assert agree[0].source_count == 2 and agree[0].assessment == "corroborated"
    assert agree[0].score_components["volume"] == pytest.approx((2 + 0.25) ** 0.5, abs=0.01)


def test_syndicated_headline_counts_once():
    now = datetime.now(timezone.utc)
    news = [text_signal(source=f"rss{i}", source_kind=SignalKind.news,
                        text="Explosion and fire at downtown warehouse", observed_at=now,
                        lat=47.611, lon=-122.331, confidence=0.6) for i in range(5)]
    events = detect([_cad(1)] + news, min_count=2, min_score=0.1)
    assert events[0].sources == {"cad": 1, "news": 1}


# ── event registry: identity + history ───────────────────────────────────────

def test_event_matches_nearest_row_and_duplicates_age_out(db):
    from apb.store import events
    events.record([_evt(lat=40.000), _evt(lat=40.030)])       # two clusters ~3.3 km apart
    assert len(events.query()) == 2
    a, b = sorted(events.query(), key=lambda r: r["lat"])
    events.record([_evt(lat=40.029, score=7.0)])              # nearest row is `b`
    rows = {r["uid"]: r for r in events.query()}
    assert rows[b["uid"]]["latest_score"] == 7.0
    assert rows[a["uid"]]["latest_score"] == 3.0              # untouched, will age out


def test_event_history_gives_a_trend(db):
    from apb.store import events
    for score in (3.0, 3.1, 3.0, 5.2):
        events.record([_evt(score=score)])
    row = events.query()[0]
    assert row["points"] == 4 and row["trend"] == "rising" and row["growing"]
    assert row["spark"] == [3.0, 3.1, 3.0, 5.2]
    events.record([_evt(score=2.0)])
    assert events.query()[0]["trend"] == "falling"
    detail = events.get(row["uid"])
    assert [p["score"] for p in detail["history"]] == [3.0, 3.1, 3.0, 5.2, 2.0]
    assert events.get("fev:0:0:1") is None


# ── feed health ──────────────────────────────────────────────────────────────

def _obs(slug="seattle", kind="socrata", rows=10, at=0.0, fresh=None, fails=0):
    return {"slug": slug, "kind": kind, "name": slug, "fetched_at": at, "rows": rows,
            "freshest": fresh, "fails": fails}


def test_feed_going_silent_is_flagged_then_recovers(db):
    from apb.store import feedhealth
    t0 = 1_000_000.0
    for i in range(10):                                        # learn: ~10 rows per fetch
        assert feedhealth.observe([_obs(at=t0 + i * 900, rows=10)], now=t0 + i * 900) == []
    changes = []
    for i in range(10, 16):                                    # then nothing, for 90 min
        changes += feedhealth.observe([_obs(at=t0 + i * 900, rows=0)], now=t0 + i * 900)
    assert [(c["from"], c["to"]) for c in changes] == [("ok", "silent")]
    summary = feedhealth.summary()
    assert summary["by_state"] == {"silent": 1} and summary["problems"][0]["slug"] == "seattle"
    assert summary["problems"][0]["typical_rows"] == 10.0      # baseline frozen during the outage
    back = feedhealth.observe([_obs(at=t0 + 16 * 900, rows=9)], now=t0 + 16 * 900)
    assert [(c["from"], c["to"]) for c in back] == [("silent", "ok")]


def test_bursty_lane_may_be_empty_but_failures_always_count(db):
    from apb.store import feedhealth
    t0 = 1_000_000.0
    for i in range(10):
        feedhealth.observe([_obs("nhc", "nhc", rows=4, at=t0 + i * 900)], now=t0 + i * 900)
    for i in range(10, 30):                                    # season over: no cyclones
        assert feedhealth.observe([_obs("nhc", "nhc", rows=0, at=t0 + i * 900)],
                                  now=t0 + i * 900) == []
    # no new successful fetch, the ingest layer reports 3 consecutive failures
    out = feedhealth.observe([_obs("nhc", "nhc", rows=0, at=t0 + 29 * 900, fails=3)],
                             now=t0 + 30 * 900)
    assert out[0]["to"] == "failing"


def test_feed_serving_old_rows_is_stale(db):
    from apb.store import feedhealth
    t0 = 1_000_000.0
    for i in range(10):                                        # normally ~5 min behind
        now = t0 + i * 900
        feedhealth.observe([_obs(at=now, fresh=now - 300)], now=now)
    frozen = t0 + 9 * 900 - 300
    out = []
    for i in range(10, 26):                                    # rows keep coming, clock stopped
        now = t0 + i * 900
        out += feedhealth.observe([_obs(at=now, fresh=frozen)], now=now)
    assert [c["to"] for c in out] == ["stale"]


# ── baseline statistics ──────────────────────────────────────────────────────

def test_small_counts_are_not_called_surges():
    from apb.infer.baseline import detect_rate_anomalies, surprise
    # mean 0.3/h and two calls now: mean/std said 3.7 sigma; a Poisson tail says ~1.8
    s = surprise(2, [0, 0, 1, 0, 0, 1, 0, 0, 1, 0])
    assert s["p_value"] == pytest.approx(0.037, abs=0.002)
    assert 1.5 < s["zscore"] < 2.0
    now = time.time()
    rows = [{"metro": "m", "ts": now - 60, "lon": -122.3} for _ in range(2)]
    rows += [{"metro": "m", "ts": now - h * 3600 - 60, "lon": -122.3} for h in (3, 6, 9)]
    rows += [{"metro": "m", "ts": now - 11 * 3600, "lon": -122.3}]
    assert detect_rate_anomalies(rows, z_threshold=2.0) == []


def test_real_spike_is_flagged_and_overdispersion_tempers_it():
    from apb.infer.baseline import detect_rate_anomalies, surprise
    now = time.time()
    rows = [{"metro": "m", "ts": now - 60, "lon": -75.0, "threat_score": 0.8} for _ in range(30)]
    for h in range(1, 12):
        rows += [{"metro": "m", "ts": now - h * 3600 - 60, "lon": -75.0} for _ in range(4)]
    out = detect_rate_anomalies(rows, z_threshold=2.0)
    assert out and out[0]["metro"] == "m" and out[0]["zscore"] >= 5 and out[0]["peak_threat"] == 0.8
    # a bursty history (one earlier spike) makes the same count less surprising
    calm, bursty = surprise(12, [4] * 10), surprise(12, [4, 4, 14, 4, 2, 9, 4, 1, 11, 4])
    assert bursty["zscore"] < calm["zscore"]


def test_day_part_is_local_solar_time():
    from apb.infer.baseline import _slot
    noon_utc = datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc).timestamp()   # a Wednesday
    assert _slot(noon_utc) == (2, False)
    assert _slot(noon_utc, lon=-157.8) == (0, False)           # 02:00 in Honolulu
    assert _slot(noon_utc + 2.5 * 86400, lon=139.7)[1] is True  # already Saturday in Tokyo


# ── alert rules, escalation, scorekeeping ────────────────────────────────────

def test_rule_gates():
    from apb.alerts.rules import default_rule, matches, parse
    home, gun = parse([
        {"name": "home", "center": [35.78, -78.64], "radius_km": 40, "min_score": 4},
        {"name": "gunfire", "types": ["shots_fired"], "bbox": [-125, 32, -114, 49], "min_sources": 2},
    ])
    e = {"lat": 35.9, "lon": -78.9, "source_count": 1, "types": {"fire": 3}, "age_min": 10}
    assert matches(home, e) and not matches(gun, e)
    assert not matches(home, {**e, "lat": 40.7, "lon": -74.0})
    la = {"lat": 34.05, "lon": -118.24, "source_count": 2, "types": {"shots_fired": 2, "other": 5}, "age_min": 30}
    assert matches(gun, la) and not matches(gun, {**la, "source_count": 1})
    standing = {**la, "age_min": 40 * 60, "trend": "steady"}
    assert not matches(gun, standing) and matches(gun, {**standing, "trend": "rising"})
    assert default_rule().min_sources == 2
    assert parse([{"name": "bad", "bbox": [1, 2]}, "junk"]) == []


def test_alert_fires_once_then_escalates(db, monkeypatch):
    from apb.alerts import notify
    from apb.store import alertlog, events
    sent = []
    monkeypatch.setenv("APB_WEBHOOK_URL", "https://example.test/hook")
    monkeypatch.setattr(notify, "_post", lambda client, url, text, event=None: sent.append(text) or True)
    events.record([_evt(score=3.0)])
    assert notify.send_pending() == 0                          # under the threshold
    events.record([_evt(score=7.0)])
    assert notify.send_pending() == 1 and "surge" in sent[-1] and "#v=40.0,-100.0,12" in sent[-1]
    assert notify.send_pending() == 0                          # exactly once
    events.record([_evt(score=8.0)])
    assert notify.send_pending() == 0                          # grew, but under 1.5x
    events.record([_evt(score=11.0)])
    monkeypatch.setattr(notify, "_MIN_UPDATE_GAP_SEC", 0.0)
    assert notify.send_pending() == 1 and "update" in sent[-1] and "was 7.0" in sent[-1]
    uid = events.query()[0]["uid"]
    assert [a["kind"] for a in alertlog.for_event(uid)] == ["new", "update"]
    assert alertlog.outcome(uid)["status"] == "pending"


def test_single_source_events_do_not_alert_by_default(db):
    from apb.alerts import notify
    from apb.store import alertlog, events
    events.record([_evt(score=9.0, sources=1)])
    notify.send_pending()
    assert alertlog.recent() == []


def test_no_webhook_is_a_dry_run_that_still_gets_scored(db):
    from apb.alerts import notify
    from apb.store import alertlog, events
    events.record([_evt(lat=47.61, lon=-122.33, score=9.0)])
    assert notify.send_pending() == 0
    log = alertlog.recent()
    assert len(log) == 1 and log[0]["sent"] == 0 and log[0]["kind"] == "new"
    uid = log[0]["uid"]
    news = text_signal(source="rss", source_kind=SignalKind.news, text="Crews battle large fire downtown",
                       observed_at=datetime.now(timezone.utc) + timedelta(minutes=12),
                       lat=47.62, lon=-122.35, url="https://news.test/a")
    far = text_signal(source="rss", source_kind=SignalKind.news, text="Fire across the state",
                      observed_at=datetime.now(timezone.utc), lat=46.0, lon=-119.0)
    assert alertlog.score_pending([far, news], now=time.time() + 900) == 1
    o = alertlog.outcome(uid)
    assert o["status"] == "confirmed" and 11 < o["lead_min"] < 13 and o["news_url"] == "https://news.test/a"
    card = alertlog.scorecard()
    assert card["confirmed"] == 1 and card["first_share"] == 1.0 and card["messages_sent"] == 0


def test_alert_with_no_news_closes_unconfirmed(db):
    from apb.alerts import notify
    from apb.store import alertlog, events
    events.record([_evt(score=9.0)])
    notify.send_pending()
    assert alertlog.score_pending([], now=time.time() + 3600) == 0       # still open
    assert alertlog.score_pending([], now=time.time() + 25 * 3600) == 1
    assert alertlog.scorecard()["unconfirmed"] == 1


def test_digest_once_per_day(db, monkeypatch):
    from apb.alerts import notify
    from apb.store import events
    events.record([_evt(score=9.0)])
    sent = []
    monkeypatch.setenv("APB_WEBHOOK_URL", "https://example.test/hook")
    monkeypatch.setenv("APB_DIGEST_HOUR_UTC", "13")
    monkeypatch.setattr(notify, "_post", lambda client, url, text, event=None: sent.append(text) or True)
    day = datetime(2026, 11, 14, tzinfo=timezone.utc).timestamp()
    assert not notify.send_digest_if_due(day + 12 * 3600)
    assert notify.send_digest_if_due(day + 13 * 3600 + 60)
    assert not notify.send_digest_if_due(day + 20 * 3600)
    assert notify.send_digest_if_due(day + 86400 + 14 * 3600)
    assert len(sent) == 2 and "1 events" in sent[0]


# ── camera tasking ───────────────────────────────────────────────────────────

class _Reg:
    _loading = set()

    def __init__(self):
        self.asked = None

    def query(self, bbox=None, limit=2000, source=None, online_only=True, extras=True):
        self.asked = {"bbox": bbox, "extras": extras}
        return [{"id": "x:far", "name": "I-5 at far", "lat": 47.640, "lon": -122.330, "image_url": "u"},
                {"id": "x:near", "name": "4th & Pike", "lat": 47.611, "lon": -122.331, "image_url": "u"},
                {"id": "x:hls", "name": "stream only", "lat": 47.610, "lon": -122.330, "image_url": None}]

    def snapshot(self, cam_id):
        return b"\xff\xd8jpeg", "image/jpeg"


def test_parse_reply_tolerates_backticks_and_zero_based():
    from apb.context.tasking import parse_reply
    obs, verdict = parse_reply('OBS: `[{"i":0,"evidence":"supports"},{"i":1,"evidence":"none"}]`\n'
                               'VERDICT: Engines on scene.', 2)
    assert [k for k, _ in obs] == [0, 1] and verdict == "Engines on scene."
    assert parse_reply("no idea", 2) == ([], "")


def test_tasked_check_reads_nearest_cameras_and_becomes_a_signal(db, monkeypatch):
    from apb.context import explain, tasking
    from apb.fusion.sources import derived_signals
    from apb.store import events
    from apb.store import metrics as mstore
    events.record([_evt(lat=47.610, lon=-122.330, score=8.0)])
    row = events.query()[0]
    monkeypatch.setattr(explain, "api_key", lambda: "k")
    seen = {}

    def gen(parts, max_tokens):
        seen["prompt"] = parts[0]["text"]
        seen["images"] = sum(1 for p in parts if "inline_data" in p)
        return ('OBS: [{"i":1,"vehicles":6,"pedestrians":3,"road_wet":false,"visibility":"good",'
                '"evidence":"supports","notable":"fire engines, smoke"},'
                '{"i":2,"vehicles":9,"pedestrians":0,"road_wet":false,"visibility":"good",'
                '"evidence":"none","notable":""}]\nVERDICT: yes - Engines and smoke at 4th & Pike.')
    monkeypatch.setattr(explain, "generate", gen)
    reg = _Reg()
    assert tasking.enqueue([row]) == 1
    assert tasking.enqueue([row]) == 0                         # already queued
    assert tasking.run(reg) == 1
    assert reg.asked["extras"] is False                        # never the metered lanes
    assert seen["images"] == 2 and "1. 4th & Pike" in seen["prompt"] and "fire" in seen["prompt"]
    check = tasking.checks_for(row["uid"])[0]
    assert (check["cameras"], check["supports"], check["overall"]) == (2, 1, "yes")
    assert check["verdict"] == "Engines and smoke at 4th & Pike."
    assert check["frames"][0]["camera_id"] == "x:near"
    assert mstore.camera_obs(mstore.cell_for(47.611, -122.331))["frames_read_24h"] == 2
    assert tasking.enqueue([row]) == 0                         # checked: wait for the recheck window
    sigs = [s for s in derived_signals() if s.source_kind == SignalKind.camera]
    assert len(sigs) == 1 and sigs[0].normalized_type.value == "fire"
    # one dispatch call + a camera that saw it = two independent source kinds
    fused = detect([_cad(1)] + sigs, min_count=2, min_score=0.1)
    assert fused[0].source_count == 2 and fused[0].sources == {"cad": 1, "camera": 1}


def test_one_lenient_frame_does_not_confirm_a_scene_the_model_calls_normal(db, monkeypatch):
    # seen live: "vehicle stopped on shoulder" marked supports while the verdict said normal
    from apb.context import explain, tasking
    from apb.fusion.sources import derived_signals
    monkeypatch.setattr(explain, "api_key", lambda: "k")
    monkeypatch.setattr(explain, "generate", lambda parts, max_tokens: (
        'OBS: [{"i":1,"evidence":"supports","notable":"vehicle stopped on shoulder"},'
        '{"i":2,"evidence":"none"}]\nVERDICT: no - normal late-night traffic.'))
    e = {"uid": "fev:47.61:-122.33:1", "lat": 47.61, "lon": -122.33, "latest_score": 9.0,
         "last_seen": time.time(), "age_min": 5, "types": {"suspicious": 3}}
    tasking.check_event(_Reg(), e)
    check = tasking.checks_for(e["uid"])[0]
    assert (check["supports"], check["overall"]) == (1, "no")
    assert [s for s in derived_signals() if s.source_kind == SignalKind.camera] == []


def test_tasking_skips_standing_clusters_and_respects_budget(db, monkeypatch):
    from apb.context import explain, tasking
    monkeypatch.setattr(explain, "api_key", lambda: "k")
    monkeypatch.setattr(explain, "generate", lambda parts, max_tokens: "OBS: []\nVERDICT: nothing.")
    now = time.time()
    base = {"lat": 47.61, "lon": -122.33, "latest_score": 9.0, "last_seen": now, "types": {"fire": 2}}
    assert tasking.enqueue([{**base, "uid": "old", "age_min": 900, "trend": "steady"}]) == 0
    assert tasking.enqueue([{**base, "uid": "weak", "age_min": 5, "latest_score": 1.0}]) == 0
    monkeypatch.setenv("APB_TASK_DAILY", "1")
    assert tasking.enqueue([{**base, "uid": "a", "age_min": 5}, {**base, "uid": "b", "age_min": 5}]) == 2
    assert tasking.run(_Reg()) == 1                            # second check is over the day's budget
    assert tasking.status()["queued"] == 0


def test_notable_cell_is_context_unless_it_is_fresh_activity(db):
    from apb.fusion.sources import derived_signals
    from apb.store import metrics as mstore
    mstore.record_notable("47.60,-122.35", 47.61, -122.33, ["temperature anomaly +12 °F"])
    mstore.record_notable("40.70,-74.05", 40.71, -74.0, ["attention 4.1x median (Wikipedia views)"])
    sev = {s.lat: s.severity for s in derived_signals() if s.source_kind == SignalKind.sensor}
    assert sev == {47.61: 0.25, 40.71: 0.45}
    # weather leaving its range does not "corroborate" a medical call next door
    ctx = [s for s in derived_signals() if s.lat == 47.61]
    fused = detect([_cad(1, kind="medical")] + ctx, min_count=2, min_score=0.1)
    assert fused[0].source_count == 1


# ── Radar lane ───────────────────────────────────────────────────────────────

def test_radar_rows(monkeypatch):
    from apb.ingest import radar
    monkeypatch.setenv("CLOUDFLARE_RADAR_TOKEN", "t")
    ing = radar.RadarIngest()
    payloads = {
        "entities/locations": {"locations": [
            {"alpha2": "IQ", "name": "Iraq", "latitude": "33.2", "longitude": "43.7"},
            {"alpha2": "UA", "name": "Ukraine", "latitude": "48.4", "longitude": "31.2"}]},
        "annotations/outages": {"annotations": [
            {"id": "1", "eventType": "OUTAGE", "startDate": "2026-11-13T10:00:00Z", "endDate": None,
             "locations": ["IQ"], "scope": None, "description": "Exam shutdown.",
             "outage": {"outageCause": "GOVERNMENT_DIRECTED", "outageType": "NATIONWIDE"}},
            {"id": "2", "eventType": "OUTAGE", "startDate": "2026-11-12T10:00:00Z", "endDate": None,
             "locations": [], "description": "Global platform outage.",
             "outage": {"outageCause": "TECHNICAL_PROBLEM", "outageType": "PLATFORM"}}]},
        "traffic_anomalies": {"trafficAnomalies": [
            {"uuid": "u1", "startDate": "2026-11-13T09:00:00Z", "endDate": "2026-11-13T11:00:00Z",
             "locationDetails": None,
             "asnDetails": {"asn": "64500", "name": "ExampleNet", "location": {"code": "UA", "name": "Ukraine"}}}]},
    }
    monkeypatch.setattr(ing, "_get", lambda path, **params: payloads[path])
    rows = {r["call_id"]: r for r in ing.fetch()}
    assert set(rows) == {"radar:outage:1:IQ", "radar:anomaly:u1"}     # the placeless one is dropped
    iq = rows["radar:outage:1:IQ"]
    assert (iq["lat"], iq["lon"], iq["ongoing"], iq["threat_score"]) == (33.2, 43.7, True, 0.75)
    assert "government directed" in iq["summary"] and iq["source"] == "radar"
    assert rows["radar:anomaly:u1"]["ongoing"] is False
    monkeypatch.delenv("CLOUDFLARE_RADAR_TOKEN")
    monkeypatch.delenv("CLOUDFLARE_API_TOKEN", raising=False)
    assert radar.RadarIngest().fetch() == []


# ── Shodan discovery sweep ───────────────────────────────────────────────────

def test_shodan_sweep_counts_without_credits_and_reports_search_refusal(monkeypatch):
    from apb.discover import shodan_sweep as ss

    def fake_get(client, path, key, **params):
        if path == "/api-info":
            return {"plan": "oss", "query_credits": 0}
        if path == "/shodan/host/count":
            assert "country:US" in params["query"]
            return {"total": 42, "facets": {"org": [{"value": "City of X", "count": 3}]}}
        raise PermissionError("Requires membership or higher to access")
    monkeypatch.setattr(ss, "_get", fake_get)
    monkeypatch.setattr(ss.time, "sleep", lambda s: None)
    out = ss.sweep("k", ["rdio"], search=True, country="US", client=object())
    q = out["queries"]["rdio"]
    assert out["plan"]["query_credits"] == 0 and q["total"] == 42
    assert q["facets"]["org"] == [("City of X", 3)] and "membership" in q["search_error"]
    row = ss.slim({"ip_str": "203.0.113.5", "port": 3000, "hostnames": ["scanner.example.org"],
                   "http": {"title": "Rdio Scanner"}, "location": {"city": "Raleigh", "region_code": "NC"}})
    assert (row["ip"], row["port"], row["title"], row["region"]) == ("203.0.113.5", 3000, "Rdio Scanner", "NC")
