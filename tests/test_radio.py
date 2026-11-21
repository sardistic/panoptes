"""Smart radio: call capture, talkgroup importance, transcription on a budget, audio
relay, fusion of transcribed dispatches, and the instance-import parser. Offline."""
import json
import time

import pytest

from apb.common.models import SignalKind


@pytest.fixture()
def db(tmp_path, monkeypatch):
    from apb.context import radio_stt
    from apb.ingest import rdio
    from apb.context import tasking
    from apb.store import alertlog, events, feedhealth, metrics, radio, snapshots, spend
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "t.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    for mod in (radio, spend, events, feedhealth, alertlog, metrics, tasking):
        monkeypatch.setattr(mod, "_ready", False)      # every store recreates its tables here
    monkeypatch.setattr(events, "_hist_ready", False)
    rdio._places["at"] = 0.0
    radio_stt._weights["at"] = 0.0
    monkeypatch.delenv("APB_STATE_DATABASE_URL", raising=False)
    monkeypatch.setenv("APB_RDIO_PROXY", "socks5h://proxy.test:1080")
    rdio._cfg.clear()
    rdio._cal_hits.clear()
    import httpx as _hx
    from apb.context import roads as _roads

    def _offline(*a, **k):
        raise _hx.ConnectError("no network in tests")
    monkeypatch.setattr(_roads, "_fetch", _offline)
    monkeypatch.setattr(radio_stt, "_census_cache", {})
    radio_stt._mime_label.clear()
    inst = {"host": "scan.example.org", "port": None, "scheme": "wss", "place": "Essex, MD",
            "loc": (39.31, -76.47), "labels": {1: "Baltimore County"}}
    inst["key"] = rdio.key_of(inst)
    monkeypatch.setattr(rdio, "_instances", [inst])
    rdio._cfg[inst["key"]] = (time.time(), rdio._systems_from_cfg({"systems": [{
        "id": 1, "label": "Baltimore County", "talkgroups": [
            {"id": 7, "label": "BCoFD Disp", "name": "County Fire Dispatch", "tag": "Fire Dispatch", "group": "Fire"},
            {"id": 8, "label": "PW 2", "name": "Public Works", "tag": "Public Works", "group": "Services"},
            {"id": 9, "label": "FG 3", "name": "Fireground 3", "tag": "Fire-Tac", "group": "Fire"}]}]}))
    yield inst
    if snapshots._conn is not None:
        snapshots._conn.close()


# a 64 kbit/s MPEG-1 layer III frame header, padded: 8000 bytes ~ one second
MP3 = b"\xff\xfb\x50\x00" + b"\x00" * 7996


def test_importance_ranks_dispatch_over_everything():
    from apb.context.radio_stt import importance
    assert importance({"tag": "Fire Dispatch"}) == (1.0, "fire")
    assert importance({"tag": "EMS Dispatch", "label": "Medic Disp"})[0] == 1.0
    assert importance({"tag": "Law Dispatch"}) == (0.85, "law")
    assert importance({"tag": "Fire-Tac"})[0] == 0.55
    assert importance({"tag": "Public Works"})[0] == 0.05
    assert importance({"tag": "Untagged", "label": "Colonie Records"})[0] == 0.2
    assert importance({"tag": "Interop", "label": "Mutual Aid 1"})[0] == 0.05


def test_block_addresses_and_mp3_length():
    from apb.context.radio_stt import block_address, est_seconds
    assert block_address("1200 block of N Elm St") == "1200 N Elm St"
    assert block_address("100 blk Main Street.") == "100 Main Street"
    assert block_address("5th Ave and Elm St") is None          # intersections: not geocodable
    assert block_address("[REDACTED] Elm St") is None
    assert block_address("the mall") is None
    assert est_seconds(MP3, "audio/mpeg") == 1.0
    assert est_seconds(b"x" * 10000, "audio/mp4") == 2.0         # unknown container: 40 kbit/s


def test_capture_and_pick_only_what_matters(db, monkeypatch):
    from apb.context import radio_stt
    from apb.store import radio
    now = time.time()
    radio.add_recent(db["key"], [(100, now - 30, 1, 7), (101, now - 20, 1, 8),
                                 (102, now - 10, 1, 9), (103, now - 900, 1, 7)])
    picked = radio_stt.pick(now)
    assert [c["id"] for c in picked] == [100]                    # fire dispatch, still fresh
    # the system surging lifts its fireground channel over the bar; public works never
    radio.record(db["key"], {1: (0, {})}, 600, now=now)
    monkeypatch.setattr(radio, "latest", lambda max_age: [{"key": db["key"], "system": 1, "z": 4.2}])
    assert [c["id"] for c in radio_stt.pick(now)] == [100, 102]


def _gen_ok(parts, max_tokens):
    clips = sum(1 for p in parts if "inline_data" in p)
    assert parts[0]["text"].startswith("You transcribe public-safety radio")
    assert "talkgroup \"BCoFD Disp\" (Fire Dispatch, Fire)" in parts[1]["text"]
    rows = [{"i": 1, "transcript": "Engine 12, Medic 4, structure fire, 1200 block of Oak Ave, "
                                  "caller [REDACTED] reports smoke",
             "kind": "dispatch", "incident_type": "fire", "location_text": "1200 block of Oak Ave",
             "municipality": "Essex", "units": ["Engine 12", "Medic 4"], "priority": 0.7,
             "summary": "Structure fire, smoke showing"}]
    rows += [{"i": n, "kind": "noise", "incident_type": "noise", "transcript": "",
              "priority": 0} for n in range(2, clips + 1)]
    usage = {"promptTokensDetails": [{"modality": "TEXT", "tokenCount": 300},
                                     {"modality": "AUDIO", "tokenCount": 32 * clips}]}
    return "```json\n" + json.dumps(rows) + "\n```", usage


def test_run_once_transcribes_geocodes_and_maps_a_dispatch(db, monkeypatch):
    from apb.context import radio_stt
    from apb.fusion.sources import cad_signals
    from apb.store import radio, snapshots
    now = time.time()
    radio.add_recent(db["key"], [(500, now - 40, 1, 7), (501, now - 35, 1, 7)])
    fetched = []

    def fetch(inst, ids):
        fetched.append(list(ids))
        return {i: (MP3 * 3, "audio/mpeg") for i in ids}          # 3 s each (estimated)
    located = []

    def locate(loc, muni, area, near, approx, state=None):
        located.append((loc, muni, area))
        return (39.30, -76.49)
    n = radio_stt.run_once(now, fetch=fetch, gen=_gen_ok, locate=locate)
    assert n == 2 and fetched == [[501, 500]]          # newest first
    assert located == [("1200 block of Oak Ave", "Essex", "Essex, MD")]
    calls = radio.recent_calls(db["key"], 1)
    done = {c["id"]: c for c in calls}
    assert done[501]["kind"] == "dispatch" and "[REDACTED]" in done[501]["text"]   # clip 1 = newest
    assert done[501]["lat"] == 39.30 and done[500]["kind"] == "noise"
    st = radio_stt.status()
    assert st["audio_tokens"] == 64 and st["incidents"] == 1
    assert st["used_sec"]["day"] == 2                            # trued up to what Gemini heard
    rows = snapshots.query(1.0)
    assert len(rows) == 1 and rows[0]["metro"] == f"radio_{db['key']}" and rows[0]["type"] == "fire"
    sig = [s for s in cad_signals(None, include_live=False) if s.source == "radio_stt"]
    assert sig and sig[0].source_kind == SignalKind.radio_transcript
    assert radio_stt.pick(now) == []                             # attempted calls are not retried


def test_budget_stops_transcription(db, monkeypatch):
    from apb.context import radio_stt
    from apb.store import radio
    monkeypatch.setenv("APB_RADIO_STT_DAILY_SEC", "4")
    now = time.time()
    radio.add_recent(db["key"], [(600, now - 10, 1, 7)])
    called = []
    n = radio_stt.run_once(now, fetch=lambda inst, ids: {i: (MP3 * 5, "audio/mpeg") for i in ids},
                           gen=lambda parts, max_tokens: called.append(1) or ("[]", {}),
                           locate=lambda *a: None)
    assert n == 0 and called == [] and radio_stt._stats["budget_stops"] >= 1


def test_unsupported_mime_switches_label(db):
    from apb.context import explain, radio_stt
    from apb.store import radio
    now = time.time()
    radio.add_recent(db["key"], [(700, now - 10, 1, 7)])

    def gen(parts, max_tokens):
        raise explain.GenerateError(400, "Unsupported MIME type: audio/aac")
    radio_stt.run_once(now, fetch=lambda inst, ids: {i: (b"\x00\x00\x00\x18ftypM4A " + b"\x00" * 20000, "audio/mp4") for i in ids},
                       gen=gen, locate=lambda *a: None)
    assert radio_stt.gemini_mime("audio/mp4") == "audio/mp4"     # next label tried next time
    assert radio_stt.pick(now)                                   # the call is still eligible


def test_radio_counts_once_in_fusion():
    from datetime import datetime, timezone
    from apb.common.models import EventSignal, IncidentType
    from apb.fusion.cluster import detect
    now = datetime.now(timezone.utc)

    def sig(kind, sid):
        return EventSignal(signal_id=sid, source=kind, source_kind=kind, observed_at=now,
                           lat=39.3, lon=-76.49, normalized_type=IncidentType.fire,
                           summary="fire", confidence=0.7, severity=0.6)
    ev = detect([sig(SignalKind.radio_metadata, "a"), sig(SignalKind.radio_transcript, "b")],
                min_count=2, min_score=0.1)
    assert ev[0].source_count == 1                               # one sensor, not two


def test_cal_allowance_protects_home_servers():
    from apb.ingest import rdio
    rdio._cal_hits.clear()
    t = 1_000.0
    assert rdio.cal_allowed("k", rdio.CAL_PER_MIN, now=t)
    assert not rdio.cal_allowed("k", 1, now=t + 30)
    assert rdio.cal_allowed("k", 1, now=t + 61)


def test_audio_relay_endpoint(db, monkeypatch):
    import importlib
    import os
    for f in ("APB_POLLER_OFF", "APB_BLUESKY_OFF", "APB_NEWS_OFF", "APB_GNEWS_OFF",
              "APB_SOCIAL_RSS_OFF", "APB_ADSB_OFF", "APB_CAMERAS_OFF", "APB_SAMPLER_OFF",
              "APB_RDIO_OFF"):
        os.environ[f] = "1"
    from fastapi.testclient import TestClient
    from apb.ingest import rdio
    from apb.store import radio
    main = importlib.import_module("apb.api.main")
    main._audio_cache.clear()
    main._audio_cache_bytes = 0
    radio.add_recent(db["key"], [(900, time.time() - 5, 1, 7)])
    asked = []
    monkeypatch.setattr(rdio, "fetch_calls", lambda inst, ids: asked.append(ids) or {900: (MP3, "audio/mpeg")})
    with TestClient(main.app) as c:
        assert c.get(f"/live/radio/audio/{db['key']}/999").status_code == 404   # never listed
        assert c.get("/live/radio/audio/notakey/900").status_code == 400
        r = c.get(f"/live/radio/audio/{db['key']}/900")
        assert r.status_code == 200 and r.headers["content-type"] == "audio/mpeg" and r.content == MP3
        r = c.get(f"/live/radio/audio/{db['key']}/900", headers={"Range": "bytes=0-99"})
        assert r.status_code == 206 and len(r.content) == 100
        assert r.headers["content-range"] == f"bytes 0-99/{len(MP3)}"
        assert asked == [[900]]                                  # second and third from cache
        calls = c.get(f"/live/radio/{db['key']}:1/calls").json()
        assert calls[0]["id"] == 900 and calls[0]["tag"] == "Fire Dispatch" and calls[0]["weight"] == 1.0
        assert calls[0]["audio"] == f"/live/radio/audio/{db['key']}/900"
        assert "source" not in calls[0]
        assert c.get("/live/radio/transcripts").json() == []
        assert c.get("/live/radio/nothex:1/calls").status_code == 400


def test_import_parser_reads_censys_and_shodan_pages():
    from apb.discover.rdio_import import parse
    page = ("68.170.103.74\nHost\nMorristown, Tennessee (US)\nServices (2)\n80 / HTTP\n3000 / HTTP\n\n"
            "Matched Fields\nhost.services.endpoints.http.html_title\nRdio Scanner\n3000 / HTTP\n"
            "87.44.17.151\n:\n443\nWeb Property\n"
            "Rdio Scanner\n2026-11-14T06:37:03\n98.17.102.142\nh142.102.17.98.dynamic.ip.windstream.net\n"
            "source.scanchq.com\nWindstream Communications LLC\n")
    got = {e["host"]: e for e in parse(page)}
    assert got["68.170.103.74"]["ports"] == [3000] and got["68.170.103.74"]["place"] == "Morristown, TN"
    assert got["87.44.17.151"]["ports"] == [443]
    assert got["source.scanchq.com"]["ports"] == [3000, 443, 80]   # usual ports, likeliest first
    assert not any("windstream" in h for h in got)               # ISP reverse names are not sites


def test_probe_merge_replaces_reprobed_and_keeps_the_rest():
    from apb.discover.rdio_probe import merge
    old = {"instances": [{"host": "a.org", "port": None, "state": "open"},
                         {"host": "1.2.3.4", "port": 3000, "state": "unreachable"}]}
    out = merge(old, [{"host": "1.2.3.4", "port": 3000, "state": "open"},
                      {"host": "b.org", "port": 443, "state": "access_code"}])
    assert out["states"] == {"open": 2, "access_code": 1}


def test_redaction_backstop_turns_house_numbers_into_blocks():
    from apb.context.radio_stt import blockify
    assert blockify("505 Mill Road, 2-story dwelling") == "500 block of Mill Road, 2-story dwelling"
    assert blockify("respond 1234 N Elm St for a fall") == "respond 1200 block of N Elm St for a fall"
    assert blockify("the 1200 block of Oak Ave") == "the 1200 block of Oak Ave"   # untouched
    assert blockify("Engine 39, Medic 4, 2 patients") == "Engine 39, Medic 4, 2 patients"
    assert blockify("time out 21:20") == "time out 21:20"


def test_geocode_trusts_the_model_state_over_a_guessed_place(monkeypatch):
    from apb.context import radio_stt
    asked = []

    class R:
        def raise_for_status(self):
            pass

        def json(self):
            return {"result": {"addressMatches": [{"coordinates": {"x": -74.79, "y": 39.98}}]}}
    monkeypatch.setattr(radio_stt._http, "get", lambda url, params: asked.append(params) or R())
    # system placed (wrongly) in Burlington NC; the model says NJ -> trusted, no distance test
    pt = radio_stt.geocode("500 block of Mill Road", "Lumberton", None, (36.09, -79.44), True, "nj")
    assert pt == (39.98, -74.79) and asked[0]["address"] == "500 Mill Road, Lumberton, NJ"
    # a known listener town wins over the model, and the distance test applies
    assert radio_stt.geocode("500 block of Mill Road", None, "Essex, MD", (39.31, -76.47), False, "NJ") is None
    assert radio_stt.geocode("100 Grey Crescent", None, None, (43.86, -79.43), True, "ON") is None


def test_scrub_existing_blockifies_stored_transcripts(db):
    from apb.context import radio_stt
    from apb.store import radio
    radio.add_transcript(db["key"], 1, time.time(), 1, 7, "done", text="respond 505 Mill Road",
                         location_text="505 Mill Road", summary="smoke at 505 Mill Road")
    assert radio_stt.scrub_existing() == 1
    row = radio.transcripts(3600)[0]
    assert row["location_text"] == "500 block of Mill Road" and "505" not in row["text"]
    assert radio_stt.scrub_existing() == 0


# ── placement by evidence; nothing transcribed is published ──────────────────

def test_profile_places_a_system_and_names_its_dispatch_talkgroups(db, monkeypatch):
    from apb.context import radio_stt
    from apb.ingest import rdio
    from apb.store import radio
    now = time.time()
    radio.add_recent(db["key"], [(1, now - 60, 1, 8), (2, now - 50, 1, 9), (3, now - 40, 1, 77)])
    monkeypatch.setattr(radio_stt, "state_at", lambda lat, lon: "OH")
    seen = {}

    def gen(parts, max_tokens):
        seen["prompt"] = parts[0]["text"]
        return json.dumps({"area": "Franklin County, OH", "state": "OH", "lat": 39.97, "lon": -83.0,
                           "statewide": False, "confidence": 0.8,
                           "dispatch_talkgroups": [77, 9999]}), {}          # 9999 is not active
    out = radio_stt.profile_one(db["key"], 1, gen)
    assert out["ok"] and out["dispatch"] == [77]
    assert "Listener's town: Essex, MD" in seen["prompt"] and "77\t77" in seen["prompt"]
    rdio._places["at"] = 0.0
    pl = rdio.place_info(db, "Baltimore County", 1)
    assert pl["by"] == "profile" and pl["loc"] == (39.97, -83.0) and not pl["approx"]
    radio_stt._weights["at"] = 0.0
    w, _ = radio_stt.weight_for(db["key"], 1, 77, rdio.talkgroup(db["key"], 1, 77))
    assert w == 0.9                                                    # untagged, but dispatch


def test_profile_point_in_the_wrong_state_is_rejected(db, monkeypatch):
    from apb.context import radio_stt
    from apb.store import radio
    radio.add_recent(db["key"], [(1, time.time(), 1, 7)])
    monkeypatch.setattr(radio_stt, "state_at", lambda lat, lon: "PA")
    out = radio_stt.profile_one(db["key"], 1, lambda parts, max_tokens: (json.dumps(
        {"area": "Franklin County, OH", "state": "OH", "lat": 40.6, "lon": -80.2, "confidence": 0.9}), {}))
    assert not out["ok"] and out["confidence"] <= 0.3
    assert radio.profiles()[(db["key"], 1)]["lat"] is None


def test_own_dispatches_beat_the_profile_and_demote_chatty_talkgroups(db):
    from apb.context import radio_stt
    from apb.ingest import rdio
    from apb.store import radio
    now = time.time()
    for i, (la, lo) in enumerate([(39.40, -76.60), (39.42, -76.55), (39.38, -76.58)]):
        radio.add_transcript(db["key"], 10 + i, now, 1, 7, "done", kind="dispatch", lat=la, lon=lo)
    for i in range(9):
        radio.add_transcript(db["key"], 100 + i, now, 1, 7, "done", kind="unit")
    rdio._places["at"] = 0.0
    pl = rdio.place_info(db, "Baltimore County", 1)
    assert pl["by"] == "dispatches" and pl["loc"] == (39.40, -76.58)
    radio_stt._weights["at"] = 0.0                                     # 3 of 12 = 25 %: neither
    assert radio_stt.weight_for(db["key"], 1, 7, {"tag": "Fire Dispatch"})[0] == 1.0
    for i in range(30):
        radio.add_transcript(db["key"], 200 + i, now, 1, 7, "done", kind="unit")
    radio_stt._weights["at"] = 0.0
    assert radio_stt.weight_for(db["key"], 1, 7, {"tag": "Fire Dispatch"})[0] == 0.5


def test_published_incident_is_a_coarse_area_without_words():
    from apb.context.radio_stt import _incident, blockify, coarse
    c = {"key": "abcdef0123", "id": 5, "ts": time.time()}
    res = {"incident_type": "fire", "priority": 0.7, "transcript": "Engine 5, 500 block of Oak",
           "summary": "Structure fire", "location_text": "500 block of Oak Ave", "municipality": "Essex"}
    inc = _incident(c, res, (39.3071, -76.4823), "MD")
    assert (inc["lat"], inc["lon"]) == coarse((39.3071, -76.4823)) == (39.31, -76.49)
    assert inc["summary"] == "fire dispatch heard on radio" and inc["location"] == "Essex, MD"
    assert "Oak" not in json.dumps(inc) and "Engine" not in json.dumps(inc)
    assert blockify("300 block of South Broad Street, apartment 1203") == "300 block of South Broad Street"


def test_api_never_serves_transcript_words(db, monkeypatch):
    import importlib
    import os
    for f in ("APB_POLLER_OFF", "APB_BLUESKY_OFF", "APB_NEWS_OFF", "APB_GNEWS_OFF",
              "APB_SOCIAL_RSS_OFF", "APB_ADSB_OFF", "APB_CAMERAS_OFF", "APB_SAMPLER_OFF", "APB_RDIO_OFF"):
        os.environ[f] = "1"
    from fastapi.testclient import TestClient
    from apb.store import radio
    now = time.time()
    radio.add_recent(db["key"], [(42, now - 30, 1, 7)])
    radio.add_transcript(db["key"], 42, now - 30, 1, 7, "done", kind="dispatch", incident_type="fire",
                         text="Engine 5 respond 500 block of Oak Ave, caller [REDACTED]",
                         summary="Fire on Oak Ave", location_text="500 block of Oak Ave",
                         units=["Engine 5"], priority=0.7, lat=39.3071, lon=-76.4823)
    main = importlib.import_module("apb.api.main")
    with TestClient(main.app) as c:
        bodies = [c.get("/live/radio/transcripts").text,
                  c.get(f"/live/radio/{db['key']}:1/calls").text,
                  c.get("/live/board").text]
    for body in (bodies[0], bodies[2]):                       # classification only
        assert "Oak" not in body and "Engine 5" not in body and "REDACTED" not in body
    assert "500" not in bodies[1] and "Engine 5" not in bodies[1] and "REDACTED" not in bodies[1]
    assert json.loads(bodies[1])[0]["line"] == "Engine respond Oak Ave, caller"
    t = json.loads(bodies[0])[0]
    assert t["type"] == "fire" and (t["lat"], t["lon"]) == (39.31, -76.49)
    assert json.loads(bodies[1])[0]["located"] is True


def test_scrub_coarsens_radio_incidents_already_published(db):
    from apb.context import radio_stt
    from apb.store import snapshots
    snapshots.record([{"metro": "radio_abcdef0123", "call_id": "rdio1", "type": "fire",
                       "summary": "Fire at 500 block of Oak Ave", "location": "500 block of Oak Ave",
                       "lat": 39.3071, "lon": -76.4823, "ts": time.time()}])
    radio_stt.scrub_existing()
    row = snapshots.query(1.0)[0]
    assert (row["lat"], row["lon"]) == (39.31, -76.49) and "Oak" not in row["summary"] + row["location"]


def test_dead_proxy_pauses_the_lane_without_blaming_scanners(db, monkeypatch):
    from apb.ingest import rdio
    from apb.store import radio
    rdio._proxy.update(ok=None, at=0.0, down_since=None, downs=0)
    up = {"v": True}
    monkeypatch.setattr(rdio, "_probe_proxy", None)
    t0 = time.time() - 120
    assert rdio.proxy_ok(now=t0, probe=lambda: up["v"]) is True
    radio.set_state(db["key"], state="failing", fails=5, next_at=t0 + 6 * 3600)
    up["v"] = False
    assert rdio.proxy_ok(now=t0 + 100, probe=lambda: up["v"]) is False
    assert rdio.status()["proxy_ok"] is False
    assert rdio.fetch_calls(db, [1]) == {}                       # no audio while the egress is down
    up["v"] = True
    assert rdio.proxy_ok(now=t0 + 200, probe=lambda: up["v"]) is True
    st = radio.state(db["key"])                                  # recovery clears the backoff
    assert (st["state"], st["fails"], st["next_at"]) == ("open", 0, 0.0)
    assert rdio._proxy["downs"] == 1


def test_proxy_probe_accepts_the_socks5h_url_the_lane_uses(monkeypatch):
    # production went 35 min "proxy down" because python-socks rejects socks5h://
    import asyncio
    import python_socks.async_.asyncio as ps
    from apb.ingest import rdio
    seen = {}

    class P:
        async def connect(self, host, port, timeout=None):
            seen["dest"] = (host, port)

            class S:
                def close(self):
                    pass
            return S()

    def from_url(url, rdns=False):
        assert url.startswith("socks5://") and rdns     # raises ValueError for socks5h in the real library
        seen["url"] = url
        return P()
    monkeypatch.setattr(ps.Proxy, "from_url", staticmethod(from_url))
    assert asyncio.run(rdio._probe_proxy("socks5h://warp-egress:1080")) is True
    assert seen == {"url": "socks5://warp-egress:1080", "dest": rdio.PROXY_PROBE}


def test_focus_hears_every_talkgroup_of_a_system(db, monkeypatch):
    from apb.context import radio_stt
    from apb.store import radio
    now = time.time()
    radio.add_recent(db["key"], [(600, now - 30, 1, 9)])           # fireground: 0.55, below the bar
    assert radio_stt.pick(now) == []
    monkeypatch.setenv("APB_RADIO_FOCUS", f"{db['key']}:1")
    radio_stt._weights["at"] = 0.0
    assert [c["id"] for c in radio_stt.pick(now)] == [600]
    radio.add_recent(db["key"], [(601, now - 20, 1, 8)])           # public works stays out
    assert [c["id"] for c in radio_stt.pick(now)] == [600]


def _add_done(db, n, ts, text, loc=None, lat=None, lon=None, kind="dispatch"):
    from apb.store import radio
    radio.add_transcript(db["key"], n, ts, 1, 7, "done", weight=1.0, tg_label="BCoFD Disp",
                         tag="Fire Dispatch", kind=kind, incident_type="fire", text=text,
                         summary=text[:60], location_text=loc, units=["Engine 12"], priority=0.5,
                         seconds=5.0, lat=lat, lon=lon)


def test_area_summary_is_cleaned_placed_and_published_without_addresses(db, monkeypatch):
    from fastapi.testclient import TestClient
    from apb.api.main import app
    from apb.context import radio_areas
    now = time.time()
    _add_done(db, 1, now - 300, "Engine 12 respond 1200 block of Oak Ave for smoke", "1200 block of Oak Ave",
              39.3012, -76.4911)
    _add_done(db, 2, now - 200, "Medic 4 respond Oak Ave", "Oak Ave", 39.3030, -76.4890)
    _add_done(db, 3, now - 100, "radio check", kind="noise")
    _add_done(db, 4, now - 50, "Engine 3 returning to quarters")
    seen = {}

    def gen(parts, max_tokens):
        seen["text"] = parts[0]["text"]
        return json.dumps([
            {"area": "Essex", "town": "Essex", "state": "MD", "priority": 0.6, "calls": [1, 2],
             "activity": "Engine 12 and Medic 4 on a smoke call at 1234 Oak Avenue"},
            {"area": "unknown", "town": None, "activity": "Units returning to quarters", "calls": [3]}]), {}
    assert [k for k, _ in radio_areas.due(now)] == [(db["key"], 1)]
    assert radio_areas.run_once(now, gen=gen) == 1
    assert "noise/" not in seen["text"] and "1. [" in seen["text"]          # noise left out
    out = radio_areas.public()
    assert len(out) == 1 and out[0]["id"] == f"{db['key']}:1"
    a = out[0]["areas"][0]
    assert a["area"] == "Essex" and "1234" not in a["activity"] and "Engine 12" not in a["activity"]
    assert "1200" not in a["activity"] and "Oak Avenue" in a["activity"]   # a road, no number
    assert (a["lat"], a["lon"]) == (39.31, -76.49)              # the ~2 km cell, not the call
    assert out[0]["areas"][1]["lat"] is None                     # unknown stays off the map
    assert radio_areas.due(now) == []                            # nothing new since
    body = TestClient(app).get("/live/radio/areas").json()
    assert body[0]["areas"][0]["activity"] == a["activity"]
    assert "units" not in json.dumps(body)


def test_area_summaries_respect_the_daily_cap(db, monkeypatch):
    from apb.context import radio_areas
    monkeypatch.setenv("APB_RADIO_AREAS_DAILY", "0")
    now = time.time()
    for n in range(3):
        _add_done(db, 10 + n, now - 60 * n, "Engine respond Main St")
    assert radio_areas.run_once(now, gen=lambda p, max_tokens: ("[]", {})) == 0
    assert radio_areas.status()["budget_stops"] >= 1


def test_area_lines_never_carry_conditions_block_numbers_or_streets():
    from apb.context.radio_areas import area_label, clean
    assert clean("EMS dispatched to the 100 block of Old Bridge Road for a hemorrhage.") == \
        "EMS dispatched to Old Bridge Road for a medical call."
    assert "seizure" not in clean("Code 3 to a seizure call on Beatrice Street")
    assert "panic" not in clean("responding to a panic attack call on Poplar Avenue")
    assert clean("Medic 12 and Mama 1 at 1234 Oak Avenue") == "Medic and Mama at Oak Avenue"
    assert area_label({"area": "Le Chateau Cove", "town": "Memphis"}) == "Memphis"
    assert area_label({"area": "Country Haven Drive and Country Meadow Lane"}) == "unknown"
    assert area_label({"area": "Henderson County"}) == "Henderson County"
    assert area_label({"area": "I-26 near Fletcher"}) == "I-26 near Fletcher"
    assert area_label({"area": "Falls Church"}) == "Falls Church"


def test_calls_are_linked_into_threads_placed_and_sent_to_fusion(db, monkeypatch):
    from apb.context import radio_areas, radio_place
    from apb.store import radio, snapshots
    now = time.time()
    _add_done(db, 21, now - 600, "Troop G respond NC 280 at Deep Woods, vehicle struck a deer")
    _add_done(db, 22, now - 500, "Trooper 4 on scene NC 280 Deep Woods", kind="unit")
    _add_done(db, 23, now - 400, "10-4 headed to the station", kind="unit")
    placed = []

    def crosser(a, b, anchor, radius):
        placed.append((a, b, radius))
        return (35.4321, -82.5432)
    monkeypatch.setattr(radio_place, "town_point", lambda town, near, max_km=250.0: (35.40, -82.50))

    def gen1(parts, max_tokens):
        assert "Open incidents:\n(none)" in parts[0]["text"]
        return json.dumps({"incidents": [{"thread": None, "title": "Vehicle vs deer on NC 280",
                                          "type": "traffic", "town": "Fletcher", "state": "NC",
                                          "where": "NC 280 at Deep Woods", "priority": 0.4,
                                          "activity": "Trooper 4 responded to a deer strike.",
                                          "status": "active", "calls": [1, 2]}],
                           "areas": []}), {}
    real = radio_place.place
    monkeypatch.setattr(radio_place, "place", lambda *a, **k: real(*a, **{**k, "crosser": crosser,
                                                                          "geocode": lambda *x: None}))
    assert radio_areas.run_once(now, gen=gen1) == 1
    th = radio_areas.public_threads()
    assert len(th) == 1 and [c["id"] for c in th[0]["calls"]] == [21, 22]
    t = th[0]
    assert t["precision"] == "intersection" and (t["lat"], t["lon"]) == (35.43, -82.55)
    assert placed == [("NC 280", "Deep Woods", 10000)]
    assert "Trooper 4" not in json.dumps(th) and "Deep Woods" not in t["title"] + t["area"]
    assert "loc_text" not in t and "where" not in t
    rows = snapshots.query(1.0)
    assert [r["call_id"] for r in rows] == [f"rthr{t['id']}"] and rows[0]["type"] == "traffic"

    # a later call continues the open incident by its label instead of starting another
    _add_done(db, 24, now - 60, "Troop G, deer removed, NC 280 clear", kind="unit")
    radio.add_recent(db["key"], [(n, now - 600 + 100 * k, 1, 7) for k, n in enumerate((21, 22, 23))]
                     + [(24, now - 60, 1, 7)])

    def gen2(parts, max_tokens):
        assert "T1 (since" in parts[0]["text"] and "Vehicle vs deer" in parts[0]["text"]
        return json.dumps({"incidents": [{"thread": "T1", "title": "Vehicle vs deer on NC 280",
                                          "type": "traffic", "town": "Fletcher", "where": "NC 280 at Deep Woods",
                                          "activity": "Deer removed, road clear.", "status": "cleared",
                                          "calls": [4]}], "areas": []}), {}
    assert radio_areas.run_once(now + 1, gen=gen2) == 1
    th = radio_areas.public_threads()
    assert len(th) == 1 and [c["id"] for c in th[0]["calls"]] == [21, 22, 24]
    assert th[0]["status"] == "cleared" and len(snapshots.query(1.0)) == 1
    from apb.api.main import app
    from fastapi.testclient import TestClient
    calls = TestClient(app).get(f"/live/radio/{db['key']}:1/calls").json()
    assert {c["id"]: c.get("thread") for c in calls}[24] == th[0]["id"]


def test_split_and_road_keys():
    from apb.context.radio_place import split_where
    from apb.context.roads import name_key, tiger_key
    assert split_where("NC 280 at Deep Woods Dr") == ("NC 280", "Deep Woods Dr")
    assert split_where("Main St and 5th Ave") == ("Main St", "5th Ave")
    assert split_where("1200 block of Oak Ave") is None
    assert split_where("the mall") is None
    assert name_key("NC 280") == name_key("NC-280") == "r:280"
    assert name_key("I-26") == name_key("Interstate 26") == "r:26"
    assert name_key("Deep Woods Dr") == name_key("Deep Woods Drive") == "n:deep woods"
    assert name_key("North Main St.") == "n:main"
    assert name_key("5th Ave") == "n:5th"
    assert name_key("Dr") is None
    assert tiger_key({"BASENAME": "280", "RTTYP": "S"}) == "r:280"
    assert tiger_key({"BASENAME": "Deep Woods", "RTTYP": "M"}) == "n:deep woods"


def _tiger(roads_by_layer):
    """A fake TIGERweb: {layer: [(BASENAME, RTTYP, [[(lon, lat), ...], ...]), ...]}."""
    asked = []

    def fetch(layer, params):
        asked.append((layer, params["where"]))
        return {"features": [{"attributes": {"BASENAME": n, "RTTYP": t}, "geometry": {"paths": paths}}
                             for n, t, paths in roads_by_layer.get(layer, [])]}
    return fetch, asked


# NC 280 runs north-south through (-82.543, 35.432); Deep Woods Dr runs east from it (a T)
_NC280 = ("280", "S", [[(-82.541, 35.421), (-82.543, 35.432), (-82.546, 35.445)]])
_DEEPWOODS = ("Deep Woods", "M", [[(-82.543, 35.432), (-82.55, 35.433)]])
_ELM = ("Elm", "M", [[(-82.546, 35.42), (-82.547, 35.44)]])


def test_crossing_found_from_tiger_road_lines_and_cached(db):
    from apb.context import roads
    fetch, asked = _tiger({2: [], 6: [_NC280], 8: [_DEEPWOODS]})
    pt = roads.cross("NC 280", "Deep Woods Dr", (35.40, -82.50), 10000, fetch=fetch)
    assert pt is not None and abs(pt[0] - 35.432) < 1e-4 and abs(pt[1] + 82.543) < 1e-4
    assert len(asked) == 3 and "BASENAME = '280'" in asked[0][1] and "UPPER(BASENAME) = 'DEEP WOODS'" in asked[0][1]
    assert roads.cross("Deep Woods", "Highway 280", (35.40, -82.50), 10000, fetch=fetch) == pt
    assert len(asked) == 3                                           # second answer came from the cache
    # two roads that cross mid-segment (an X, not a T)
    fetch2, _ = _tiger({8: [_DEEPWOODS, _ELM]})
    x = roads.cross("Elm St", "Deep Woods", (35.40, -82.50), 10000, fetch=fetch2)
    assert x is not None and abs(x[0] - 35.4325) < 0.002
    # roads that do not meet, and a crossing outside the radius
    fetch3, _ = _tiger({8: [_DEEPWOODS, ("Oak", "M", [[(-82.6, 35.5), (-82.61, 35.51)]])]})
    assert roads.cross("Oak Ave", "Deep Woods", (35.40, -82.50), 10000, fetch=fetch3) is None
    assert roads.cross("NC 280", "Deep Woods", (36.40, -82.50), 10000, fetch=fetch) is None


def test_tiger_failures_are_not_cached(db):
    import httpx
    from apb.context import roads

    def down(layer, params):
        raise httpx.ConnectError("down")
    assert roads.cross("NC 280", "Deep Woods", (35.40, -82.50), 10000, fetch=down) is None
    fetch, asked = _tiger({6: [_NC280], 8: [_DEEPWOODS]})
    assert roads.cross("NC 280", "Deep Woods", (35.40, -82.50), 10000, fetch=fetch) is not None
    assert asked


def test_unplaced_incident_is_re_placed_when_its_roads_resolve(db, monkeypatch):
    from apb.context import radio_areas, radio_place, roads
    from apb.store import radio, snapshots
    now = time.time()
    monkeypatch.setattr(radio_place, "town_point", lambda town, near, max_km=250.0: (35.40, -82.50))
    radio.save_thread({"tid": "abc123def456", "key": db["key"], "system": 1, "first_ts": now - 300,
                       "last_ts": now - 200, "type": "traffic", "title": "Crash on NC 280",
                       "activity": "Two vehicles.", "area": "Fletcher", "state": "NC", "priority": 0.5,
                       "status": "active", "loc_text": "NC 280 at Deep Woods", "lat": 35.40, "lon": -82.50,
                       "precision": "town", "calls": [31], "published": 0})
    fetch, _ = _tiger({6: [_NC280], 8: [_DEEPWOODS]})
    real = roads.cross
    monkeypatch.setattr(roads, "cross", lambda *a, **k: None)          # TIGERweb unreachable
    assert radio_areas.replace_pending(now) == 0
    monkeypatch.setattr(roads, "cross", lambda *a, **k: real(*a, **{**k, "fetch": fetch}))
    assert radio_areas.replace_pending(now) == 1
    t = radio_areas.public_threads()[0]
    from apb.context.radio_stt import coarse
    assert t["precision"] == "intersection" and (t["lat"], t["lon"]) == coarse((35.432, -82.543))
    assert [r["call_id"] for r in snapshots.query(1.0)] == ["rthrabc123def456"]
    assert radio_areas.replace_pending(now) == 0                     # done once


def test_live_feed_reports_new_calls_and_cleaned_lines_after_the_cursor(db, monkeypatch):
    from fastapi.testclient import TestClient
    from apb.api.main import app
    from apb.store import radio
    c = TestClient(app)
    now = time.time()
    first = c.get("/live/radio/feed").json()
    assert first["calls"] == [] and first["lines"] == []
    radio.add_recent(db["key"], [(51, now - 30, 1, 7), (52, now - 20, 1, 8)])
    _add_done(db, 51, now - 30, "Engine 12 respond 1200 block of Oak Ave for a seizure", lat=39.3012, lon=-76.4911)
    _add_done(db, 53, now - 10, "radio check", kind="noise")
    radio.add_transcript(db["key"], 53, now - 10, 1, 7, "done", kind="noise", incident_type="noise", text="radio check")
    d = c.get(f"/live/radio/feed?since={first['now']}").json()
    assert [(x["sys"], x["id"], x["talkgroup"]) for x in d["calls"]] == [
        (f"{db['key']}:1", 51, "BCoFD Disp"), (f"{db['key']}:1", 52, "PW 2")]
    assert d["calls"][0]["weight"] > d["calls"][1]["weight"]
    assert [x["id"] for x in d["lines"]] == [51]                     # noise is not shown
    line = d["lines"][0]
    assert "1200" not in line["line"] and "Engine 12" not in line["line"] and "Oak Ave" in line["line"]
    from apb.context.radio_stt import coarse
    assert (line["lat"], line["lon"]) == coarse((39.3012, -76.4911)) != (39.3012, -76.4911)
    again = c.get(f"/live/radio/feed?since={d['now']}").json()
    assert again["calls"] == [] and again["lines"] == []


def test_focus_instances_are_polled_every_minute(db, monkeypatch):
    from apb.ingest import rdio
    monkeypatch.setenv("APB_RADIO_FOCUS", f"{db['key']}:1")
    assert rdio.every_for(db["key"]) == min(rdio.EVERY, rdio.FOCUS_EVERY)
    assert rdio.every_for("0123456789") == rdio.EVERY


def test_more_spoken_places_resolve_to_roads():
    from apb.context.radio_place import one_street, split_where
    from apb.context.radio_stt import address_variants
    assert split_where("Westbound US 50 east of Highway 99") == ("US 50", "Highway 99")
    assert split_where("I-26 Westbound and I-40 ramp") == ("I-26", "I-40")
    assert split_where("southbound Interstate 5 north of Laguna Boulevard") == ("Interstate 5", "Laguna Boulevard")
    assert split_where("Main St between 5th Ave and 6th Ave") == ("Main St", "5th Ave")
    assert one_street("600 block of Bannon Street") == "Bannon Street"
    assert one_street("West Street") == "West Street"
    assert one_street("Briarwood") is None and one_street("NC 280 at Deep Woods") is None
    assert address_variants("600 block of Bannon St", "600 Bannon St") == [
        "600 Bannon St", "601 Bannon St", "650 Bannon St", "651 Bannon St"]
    assert address_variants("0 block of Ray St", "0 Ray St")[0] == "1 Ray St"
    assert address_variants("123 Main St", "123 Main St") == ["123 Main St"]


def test_a_short_street_places_an_incident_a_long_one_does_not(db):
    from apb.context import radio_place, roads
    short = ("Bannon", "M", [[(-121.495, 38.590), (-121.488, 38.591)]])          # ~600 m
    long_ = ("Central Florida", "M", [[(-121.60, 38.50), (-121.40, 38.65)]])      # ~24 km
    fetch, _ = _tiger({8: [short, long_]})
    assert roads.street("Bannon Street", (38.58, -121.49), 10000, fetch=fetch) is not None
    assert roads.street("Central Florida Parkway", (38.58, -121.49), 10000, fetch=fetch) is None
    pt, prec = radio_place.place("600 block of Bannon Street", "Sacramento", "CA", [], None, None, False,
                                 geocode=lambda *a: None,
                                 streeter=lambda n, an, r: roads.street(n, an, r, fetch=fetch))
    assert prec == "street" and abs(pt[0] - 38.5905) < 0.001



_YP = """<directory>
<entry><server_name>FDNY Manhattan</server_name><listen_url>http://s.example/fdnym</listen_url>
 <server_type>audio/mpeg</server_type><bitrate>16</bitrate><genre>Scanner</genre></entry>
<entry><server_name>FDNY Manhattan</server_name><listen_url>http://t.example/fdnym</listen_url>
 <server_type>audio/mpeg</server_type><bitrate>16</bitrate><genre>Scanner</genre></entry>
<entry><server_name>Maricopa County - Police &amp; DPS</server_name><listen_url>https://s.example/mcso</listen_url>
 <server_type>audio/mpeg</server_type><bitrate>16</bitrate><genre>Scanner</genre></entry>
<entry><server_name>146.88 WB8CRS Repeater</server_name><listen_url>http://s.example/ham</listen_url>
 <server_type>audio/mpeg</server_type><bitrate>16</bitrate><genre>Scanner</genre></entry>
<entry><server_name>Fire Hits FM</server_name><listen_url>http://s.example/hits</listen_url>
 <server_type>audio/mpeg</server_type><bitrate>128</bitrate><genre>Pop Hits</genre></entry>
<entry><server_name>She On Fire</server_name><listen_url>http://s.example/she</listen_url>
 <server_type>audio/mpeg</server_type><bitrate>128</bitrate><genre>various</genre></entry>
<entry><server_name>Cops and Robbers: Police Procedural OTR</server_name><listen_url>http://s.example/otr</listen_url>
 <server_type>audio/mpeg</server_type><bitrate>64</bitrate><genre>Old Time Radio OTR</genre></entry>
<entry><server_name>38 LSB From Phoenix (CB RADIO)</server_name><listen_url>http://s.example/cb</listen_url>
 <server_type>audio/mpeg</server_type><bitrate>16</bitrate><genre>Live Scanner Arizona Emergency CB Radio</genre></entry>
<entry><server_name>Charlotte Fire</server_name><listen_url>http://s.example/clt.ogg</listen_url>
 <server_type>application/ogg</server_type><bitrate>16</bitrate><genre>Scanner</genre></entry>
</directory>"""


def test_stream_directory_keeps_public_safety_mp3_streams_once():
    from apb.ingest import streams
    got = streams.parse_yp(_YP)
    assert [s["name"] for s in got] == ["FDNY Manhattan", "Maricopa County - Police & DPS"]
    assert got[0]["tag"] == "Fire Dispatch" and got[1]["tag"] == "Law Dispatch"
    assert got[0]["key"] == streams.key_of("http://s.example/fdnym") and len(got[0]["key"]) == 10


def test_stream_calls_are_cut_stored_served_and_counted(db, monkeypatch, tmp_path):
    import array
    import math
    import random
    from fastapi.testclient import TestClient
    from apb.api.main import app
    from apb.ingest import rdio, streams
    from apb.store import radio
    monkeypatch.setenv("APB_STREAMS_LIST", str(tmp_path / "streams.json"))
    assert streams.discover(fetch=lambda: _YP) == 2
    monkeypatch.setenv("APB_STREAMS_MAX", "1")
    s = streams.choose()[0]
    assert rdio.instance(s["key"])["kind"] == "stream" and rdio._label(rdio.instance(s["key"]), 1) == "FDNY Manhattan"
    assert rdio.talkgroup(s["key"], 1, 0)["tag"] == "Fire Dispatch"
    # the cutter finds one transmission in noise
    rnd = random.Random(1)
    cut = streams.Cutter()
    pcm = array.array("h", [int(rnd.gauss(0, 60)) for _ in range(16000)]
                      + [int(3000 * math.sin(i * .3)) for i in range(24000)]
                      + [int(rnd.gauss(0, 60)) for _ in range(24000)])
    calls = cut.feed(pcm)
    assert len(calls) == 1 and 1.0 < calls[0][0] < 2.0 and 4.5 < calls[0][1] < 5.5
    now = time.time()
    cid = streams.save_call(s, now - 30, b"\xff\xfbMP3DATA", now=now)
    assert radio.has_call(s["key"], cid)
    assert rdio.fetch_calls(rdio.instance(s["key"]), [cid]) == {cid: (b"\xff\xfbMP3DATA", "audio/mpeg")}
    r = TestClient(app).get(f"/live/radio/audio/{s['key']}/{cid}")
    assert r.status_code == 200 and r.content == b"\xff\xfbMP3DATA"
    streams._live[s["key"]]["since"] = now - 700
    streams.record_counts(now)
    act = [a for a in rdio.activity(3600) if a["id"] == f"{s['key']}:1"]
    assert act and act[0]["calls"] == 1 and act[0]["system"] == "FDNY Manhattan"
    assert streams.prune(now + 7 * 3600) == 1 and streams.fetch(s["key"], [cid]) == {}
    streams._streams.clear()


def test_model_prices_are_dated_per_model_and_batch_is_half():
    from apb.store import usage
    assert usage.prices("gemini-3.8-flash", "2026-11-20") == {"in_text": 0.75, "in_audio": 0.75, "out": 3.75}
    assert usage.prices("gemini-3.8-flash", "2027-01-01")["out"] == 7.50
    assert usage.prices("models/gemini-3.5-flash-lite", "2026-11-20")["in_audio"] == 0.30
    assert usage.prices("something-else")["in_audio"] == 1.00
    row = {"day": "2026-11-20", "model": "gemini-3.8-flash", "feature": "radio_stt",
           "in_text": 0, "in_image": 0, "in_audio": 1_000_000, "out": 1_000_000}
    assert usage._usd(row) == 4.50
    assert usage._usd({**row, "feature": "radio_stt_batch"}) == 2.25


def test_backlog_goes_to_the_batch_api_and_comes_back(db, monkeypatch):
    from apb.context import radio_stt
    from apb.store import radio, spend
    now = time.time()
    # one call the live pass left behind (20 min old), one too fresh for the backlog
    radio.add_recent(db["key"], [(600, now - 1200, 1, 7), (601, now - 60, 1, 7)])
    posted = []

    def post(path, body):
        posted.append((path, body))
        return {"name": "batches/abc"}
    n = radio_stt.submit_backlog(now, fetch=lambda inst, ids: {i: (MP3 * 3, "audio/mpeg") for i in ids}, post=post)
    assert n == 1 and posted[0][0] == f"models/{radio_stt.STT_MODEL}:batchGenerateContent"
    req = posted[0][1]["batch"]["input_config"]["requests"]["requests"]
    assert len(req) == 1 and req[0]["metadata"] == {"key": "r0"}
    assert "summary" not in req[0]["request"]["contents"][0]["parts"][0]["text"]
    assert [c["id"] for c in radio.candidates(now - 7200)] == [601]          # 600 is queued
    assert spend.used("radio_stt_batch_sec")["day"] >= 3
    # still running: nothing changes
    assert radio_stt.poll_backlog(now, get=lambda name: {"metadata": {"state": "BATCH_STATE_RUNNING"}}) == 0
    reply = json.dumps([{"i": 1, "transcript": "Engine 12 respond 1200 block of Oak Ave", "kind": "dispatch",
                         "incident_type": "fire", "location_text": "1200 block of Oak Ave", "priority": 0.5}])
    done = {"done": True, "metadata": {"state": "BATCH_STATE_SUCCEEDED", "output": {"inlinedResponses": {
        "inlinedResponses": [{"metadata": {"key": "r0"}, "response": {
            "candidates": [{"content": {"parts": [{"text": reply}]}}],
            "usageMetadata": {"promptTokenCount": 200, "candidatesTokenCount": 40,
                              "promptTokensDetails": [{"modality": "AUDIO", "tokenCount": 96}]}}}]}}}}
    assert radio_stt.poll_backlog(now + 300, get=lambda name: done, locate=lambda *a: None) == 1
    t = radio.transcripts_for(db["key"], [600])[600]
    assert t["status"] == "done" and t["kind"] == "dispatch" and t["summary"].startswith("Engine 12")
    assert radio.jobs() == []


def test_a_failed_or_stale_batch_job_marks_its_calls_failed(db):
    from apb.context import radio_stt
    from apb.store import radio
    now = time.time()
    radio.add_recent(db["key"], [(700, now - 1500, 1, 7)])
    radio_stt.submit_backlog(now, fetch=lambda inst, ids: {i: (MP3 * 3, "audio/mpeg") for i in ids},
                             post=lambda path, body: {"name": "batches/x"})
    radio_stt.poll_backlog(now, get=lambda name: {"metadata": {"state": "BATCH_STATE_EXPIRED"}})
    assert radio.transcripts_for(db["key"], [700])[700]["status"] == "failed" and radio.jobs() == []


def test_related_ranks_by_place_time_type_and_street():
    from apb.fusion import related
    origin, ts = (35.43, -82.54), 1_800_000_000.0
    items = [
        {"kind": "incident", "id": "a", "lat": 35.432, "lon": -82.541, "ts": ts - 180, "type": "traffic",
         "title": "Crash", "_text": "two-vehicle crash NC 280 at Deep Woods Dr"},
        {"kind": "incident", "id": "b", "lat": 35.44, "lon": -82.52, "ts": ts - 1800, "type": "medical", "title": "Medical"},
        {"kind": "incident", "id": "far", "lat": 36.0, "lon": -82.0, "ts": ts, "type": "traffic", "title": "Far"},
        {"kind": "hazard", "id": "w", "lat": 36.5, "lon": -83.5, "ts": ts - 7200, "ongoing": True, "type": "Flood Warning",
         "title": "Flood Warning", "geometry": {"type": "Polygon", "coordinates": [[[-83, 35], [-82, 35], [-82, 36], [-83, 36], [-83, 35]]]}},
    ]
    out = related.related(origin, items, ts=ts, type_="traffic", text="NC 280 at Deep Woods")
    assert [r["id"] for r in out][:1] == ["a"] and "far" not in {r["id"] for r in out}
    a = out[0]
    assert "same street" in a["why"] and "also traffic" in a["why"] and "3 min earlier" in a["why"]
    assert "_text" not in a and "geometry" not in a
    w = next(r for r in out if r["id"] == "w")
    assert w["km"] == 0.0 and w["why"][0] == "covers this place"
    assert related.streets("Respond 1200 block of Oak Ave and I-26") == {"n:oak", "r:26"}


def test_hazards_vote_in_fusion_by_kind():
    from apb.fusion.sources import hazard_signals
    rows = [{"source": "firms", "lat": 35.0, "lon": -82.0, "ts": 1_800_000_000, "type": "fire", "threat_score": 0.6},
            {"source": "nws", "lat": 35.0, "lon": -82.0, "ts": 1_800_000_000, "event": "Red Flag Warning", "threat_score": 0.7},
            {"source": "nws", "lat": 35.0, "lon": -82.0, "ts": 1_800_000_000, "event": "Winter Storm Warning", "threat_score": 0.8},
            {"source": "511", "lat": 35.0, "lon": -82.0, "ts": 1_800_000_000, "type": "traffic", "summary": "Crash", "threat_score": 0.5},
            {"source": "adsb", "lat": 35.0, "lon": -82.0, "ts": 1_800_000_000}]
    got = {(s.source, s.normalized_type.value): s for s in hazard_signals(rows)}
    assert set(got) == {("firms", "fire"), ("nws", "fire"), ("nws", "weather"), ("511", "traffic")}
    assert got[("nws", "weather")].severity <= 0.3                 # context, not corroboration
    assert got[("nws", "fire")].severity == 0.7 and got[("511", "traffic")].source_kind.value == "traffic"


def test_related_endpoint_for_a_radio_thread_never_returns_its_street(db, monkeypatch):
    from fastapi.testclient import TestClient
    from apb.api import main
    from apb.store import radio, snapshots
    now = time.time()
    radio.save_thread({"tid": "0123456789ab", "key": db["key"], "system": 1, "first_ts": now - 600, "last_ts": now - 300,
                       "type": "traffic", "title": "Crash on the highway", "activity": "", "area": "Fletcher",
                       "state": "NC", "priority": 0.5, "status": "active", "loc_text": "NC 280 at Deep Woods",
                       "lat": 35.43, "lon": -82.55, "precision": "intersection", "calls": [1], "published": 1})
    snapshots.record([{"metro": "chp", "call_id": "x1", "type": "traffic", "summary": "Collision NC 280 at Deep Woods Dr",
                       "location": "NC 280", "sentiment": "elevated", "threat_score": 0.5, "lat": 35.432, "lon": -82.548,
                       "at": "2026-11-20T00:00:00+00:00", "ts": now - 420}])
    monkeypatch.setattr(main, "_hazards_all_rows", lambda h=24.0: [])
    monkeypatch.setattr(main, "live_fused", lambda **k: [])
    r = TestClient(main.app).get("/live/related?thread=0123456789ab")
    assert r.status_code == 200
    body = r.json()
    assert body and body[0]["id"] == "x1" and "same street" in body[0]["why"]
    assert "Deep Woods" not in json.dumps([x for x in body if x["kind"] == "radio"])
    assert all(x.get("id") != "rthr0123456789ab" for x in body)
