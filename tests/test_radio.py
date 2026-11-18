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
    from apb.store import radio, snapshots, spend
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "t.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    for mod in (radio, spend):
        monkeypatch.setattr(mod, "_ready", False)
    monkeypatch.delenv("APB_STATE_DATABASE_URL", raising=False)
    monkeypatch.setenv("APB_RDIO_PROXY", "socks5h://proxy.test:1080")
    rdio._cfg.clear()
    rdio._cal_hits.clear()
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
    assert est_seconds(b"x" * 4000, "audio/mp4") == 2.0          # unknown container: 16 kbit/s


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
        return {i: (MP3 * 3, "audio/mpeg") for i in ids}          # 3 s each
    located = []

    def locate(loc, muni, area, near, approx):
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
    assert st["audio_tokens"] == 64 and st["incidents"] == 1 and st["used_sec"]["day"] == 6
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
    radio_stt.run_once(now, fetch=lambda inst, ids: {i: (b"\x00\x00\x00\x18ftypM4A " + b"\x00" * 6000, "audio/mp4") for i in ids},
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
