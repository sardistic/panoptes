"""Public live-camera registry — parsers on captured shapes, query sampling, proxy
guard rails. Offline: the registry's fetch helpers are stubbed per test."""
import json

import httpx
import pytest

from apb.ingest import cameras
from apb.ingest.cameras import CameraRegistry, SOURCES, STREAM_HOSTS, _row


class _Stub(CameraRegistry):
    def __init__(self, payloads: dict):
        super().__init__()
        self._payloads = payloads

    def get_json(self, url):
        for k, v in self._payloads.items():
            if k in url:
                return v
        raise httpx.HTTPError(f"no stub for {url}")

    def get_text(self, url):
        return self.get_json(url)

    def __getattribute__(self, name):       # parsers that call the raw client get the same stubs
        if name == "_client":
            stub = self
            class _C:
                def get(self, url, **kw):
                    class _R:
                        text = json.dumps(stub.get_json(url))
                        def raise_for_status(self): pass
                    return _R()
            return _C()
        return super().__getattribute__(name)


def test_row_rejects_unplaceable_or_blind_cameras():
    assert _row("x", 1, "a", 0, 0, image_url="u") is None            # null island
    assert _row("x", 1, "a", 91, 0, image_url="u") is None           # out of range
    assert _row("x", 1, "a", "nan", 1, image_url="u") is None
    assert _row("x", 1, "a", 40.7, -74.0) is None                    # no still, no stream
    r = _row("x", "id-1", " Name ", "40.71234567", -74.0, image_url="u", direction="N")
    assert r["id"] == "x:id-1" and r["name"] == "Name" and r["lat"] == 40.71235


def test_caltrans_shape_flattens_districts_and_flags_out_of_service():
    rec = lambda i, svc: {"cctv": {"index": str(i), "inService": svc,
        "location": {"locationName": f"TV{i}", "latitude": "37.8", "longitude": "-122.2",
                     "route": "I-580", "direction": "West"},
        "imageData": {"streamingVideoURL": "https://wzmedia.dot.ca.gov/x.m3u8",
                      "static": {"currentImageURL": f"https://cwwp2.dot.ca.gov/{i}.jpg"}}}}
    reg = _Stub({"cctvStatusD04": {"data": [rec(1, "true"), rec(2, "false")]}})
    rows = cameras._caltrans(reg, SOURCES["caltrans"])
    assert [r["id"] for r in rows] == ["caltrans:d4-1", "caltrans:d4-2"]
    assert rows[0]["online"] and not rows[1]["online"]
    assert rows[0]["stream_url"].startswith("https://wzmedia.dot.ca.gov")


def test_carmanah_v2_emits_one_row_per_enabled_view_and_needs_key_when_keyed(monkeypatch):
    payload = [{"Id": 7, "Roadway": "QEW", "Direction": "Unknown", "Latitude": 42.9,
                "Longitude": -78.9, "Location": "QEW at Thompson",
                "Views": [{"Id": 1, "Url": "https://511on.ca/map/Cctv/1", "Status": "Enabled",
                           "Description": "Toronto Bound"},
                          {"Id": 2, "Url": "https://511on.ca/map/Cctv/2", "Status": "Disabled"}]}]
    reg = _Stub({"/api/v2/get/cameras": payload})
    rows = cameras._carmanah_v2(reg, SOURCES["on511"])
    assert len(rows) == 1 and rows[0]["id"] == "on511:7.1"
    assert rows[0]["name"] == "QEW at Thompson · Toronto Bound"
    monkeypatch.delenv("T511_GA_KEY", raising=False)
    assert cameras._carmanah_v2(reg, SOURCES["ga511"]) == []
    assert "ga511" not in cameras.available() and "on511" in cameras.available()


def test_chart_md_derives_hls_from_streamer_host_only():
    data = {"data": [
        {"id": "abc", "cctvIp": "strmr5.sha.maryland.gov", "lat": 39.2, "lon": -77.3,
         "name": "I-270", "opStatus": "OK", "routePrefix": "IS", "routeNumber": 270},
        {"id": "evil", "cctvIp": "attacker.example", "lat": 39.2, "lon": -77.3, "name": "x",
         "opStatus": "OK"},
    ]}
    rows = cameras._chart_md(_Stub({"getCameraMapDataJSON": data}), SOURCES["chart_md"])
    assert len(rows) == 1
    assert rows[0]["stream_url"] == "https://strmr5.sha.maryland.gov/rtplive/abc/playlist.m3u8"
    assert rows[0]["roadway"] == "IS 270" and rows[0]["image_url"] is None


def test_nzta_xml_and_tfl_property_bag():
    xml = ('<response><camera><id>714</id><name>SH1 Tinwald</name><latitude>-43.9</latitude>'
           '<longitude>171.7</longitude><imageUrl>/camera/714.jpg</imageUrl><offline>false</offline>'
           '<underMaintenance>false</underMaintenance><highway>SH1</highway></camera></response>')
    rows = cameras._nzta(_Stub({"trafficnz": xml}), SOURCES["nzta"])
    assert rows[0]["image_url"] == "https://trafficnz.info/camera/714.jpg"
    tfl = [{"id": "JamCams_00002.00865", "commonName": "A406", "lat": 51.6, "lon": -0.01,
            "additionalProperties": [{"key": "available", "value": "true"},
                                     {"key": "imageUrl", "value": "https://s3-eu-west-1.amazonaws.com/j/x.jpg"},
                                     {"key": "view", "value": "West"}]}]
    rows = cameras._tfl(_Stub({"JamCam": tfl}), SOURCES["tfl"])
    assert rows[0]["id"] == "tfl:00002.00865" and rows[0]["direction"] == "West"


def test_query_filters_bbox_online_and_samples_deterministically():
    reg = CameraRegistry()
    reg._rows = {"a": [_row("a", i, f"c{i}", 40 + i * .01, -74, image_url="u",
                            online=i % 5 != 0) for i in range(50)]}
    reg._at = {k: 1e12 for k in SOURCES}          # never refetch during the test
    reg._rebuild_index()
    assert len(reg.query()) == 40                  # offline dropped
    assert len(reg.query(online_only=False)) == 50
    inside = reg.query(bbox=(-75, 40.0, -73, 40.105))
    assert {r["id"] for r in inside} == {f"a:{i}" for i in range(11) if i % 5}
    s1 = reg.query(limit=7); s2 = reg.query(limit=7)
    assert len(s1) == 7 and [r["id"] for r in s1] == [r["id"] for r in s2]
    assert reg.get("a:1")["name"] == "c1" and reg.get("nope") is None


def test_snapshot_only_resolves_registered_stills(monkeypatch):
    reg = CameraRegistry()
    reg._rows = {"a": [_row("a", 1, "still", 40, -74, image_url="https://cam.example/1.jpg"),
                       _row("a", 2, "hls", 40, -74, stream_url="https://cam.example/2.m3u8")]}
    reg._at = {k: 1e12 for k in SOURCES}
    reg._rebuild_index()
    assert reg.snapshot("a:2") is None             # stream-only camera has no still
    assert reg.snapshot("https://evil.example/x") is None
    calls = []

    class _R:
        content = b"\xff\xd8jpeg"; headers = {"content-type": "image/jpeg; charset=binary"}
        def raise_for_status(self): pass

    monkeypatch.setattr(reg._client, "get", lambda url, **kw: calls.append(url) or _R())
    assert reg.snapshot("a:1") == (b"\xff\xd8jpeg", "image/jpeg")
    assert reg.snapshot("a:1")[1] == "image/jpeg" and calls == ["https://cam.example/1.jpg"]

    class _Html(_R):
        headers = {"content-type": "text/html"}
    reg._snap.clear()
    monkeypatch.setattr(reg._client, "get", lambda url, **kw: _Html())
    with pytest.raises(httpx.HTTPError):
        reg.snapshot("a:1")


def test_stream_hosts_are_https_origins_for_csp():
    for h in STREAM_HOSTS:
        assert h.startswith("https://") and "/" not in h[8:]


def test_second_sweep_parsers_on_captured_shapes():
    tm = {"reportTables": [{"displayName": "I-90", "cells": [
        {"externalId": "IL-IDOTD4-3019", "agency": "IDOT", "location": "US-51 at LaSalle",
         "latitude": 41.58, "longitude": -89.06, "url": None,
         "imageDirections": {"N": {"url": "https://cctv.travelmidwest.com/snapshots/a_N.jpg"},
                             "S": {"url": "https://cctv.travelmidwest.com/snapshots/a_S.jpg"}}},
        {"externalId": "WI-WisDOT-117", "agency": "WisDOT", "location": "I-39 at County V",
         "latitude": 43.25, "longitude": -89.37, "url": "https://511wi.gov/map/Cctv/1053",
         "singleView": True, "imageDirections": None, "videoUrl": None}]}]}
    rows = cameras._travelmidwest(_Stub({"cameraReport.json": tm}), SOURCES["travelmidwest"])
    assert [r["id"] for r in rows] == ["travelmidwest:IL-IDOTD4-3019.N", "travelmidwest:IL-IDOTD4-3019.S",
                                       "travelmidwest:WI-WisDOT-117"]
    assert rows[0]["state"] == "IL" and rows[2]["state"] == "WI" and rows[0]["direction"] == "N"

    js = 'var x = {"features":[{"attributes":{"cameraId":277,"filename":"A.jpg","latitude":46.1,'          '"longitude":-123.8,"route":"US101 ","title":"US101 at Astoria"}}]};'
    rows = cameras._tripcheck(_Stub({"cctvinventory": js}), SOURCES["tripcheck"])
    assert rows[0]["image_url"] == "https://tripcheck.com/RoadCams/cams/A.jpg" and rows[0]["roadway"] == "US101"

    algo = [{"id": 1, "location": {"latitude": 30.5, "longitude": -88.2, "displayRouteDesignator": "I-10",
             "displayCrossStreet": "McDonald Rd", "direction": "East"},
             "playbackUrls": {"hls": "https://cdn3.wowza.com/x/playlist.m3u8"},
             "snapshotImageUrl": "https://api.algotraffic.com/v4.0/Cameras/1/Snapshot"}]
    rows = cameras._algo(_Stub({"Cameras": algo}), SOURCES["algo"])
    assert rows[0]["name"] == "I-10 @ McDonald Rd" and rows[0]["stream_url"].startswith("https://cdn3.wowza.com")

    fi = {"features": [{"geometry": {"coordinates": [24.0, 60.05]}, "properties": {
        "name": "kt51_Inkoo", "collectionStatus": "GATHERING",
        "presets": [{"id": "C0150301", "inCollection": True}, {"id": "C0150302", "inCollection": False}]}}]}
    rows = cameras._digitraffic(_Stub({"weathercam": fi}), SOURCES["digitraffic"])
    assert len(rows) == 1 and rows[0]["kind"] == "weather" and rows[0]["lat"] == 60.05

    hk = "<image-list><image><key>H1</key><description>Praya Rd</description><latitude>22.2</latitude>"          "<longitude>114.1</longitude><district>Southern</district><url>https://tdcctv.data.one.gov.hk/H1.JPG</url></image></image-list>"
    assert cameras._hongkong(_Stub({"Traffic_Camera_Locations": hk}), SOURCES["hk"])[0]["id"] == "hk:H1"


def test_arcgis_reader_pages_until_transfer_limit_clears():
    pages = [{"features": [{"attributes": {"n": i}, "geometry": {"x": -120, "y": 40}} for i in range(50)],
              "exceededTransferLimit": True},
             {"features": [{"attributes": {"n": 50}, "geometry": {"x": -120, "y": 40}}]}]
    calls = []
    class _Reg(CameraRegistry):
        def get_json(self, url):
            calls.append(url); return pages[len(calls) - 1]
    rows = cameras._arcgis_points(_Reg(), SOURCES["alertca"], "https://x/FeatureServer/0",
                                  build=lambda a, g: _row("alertca", a["n"], "c", g["y"], g["x"], image_url="u"))
    assert len(rows) == 51 and "resultOffset=50" in calls[1]


def test_snapshot_sniffs_octet_stream_images(monkeypatch):
    reg = CameraRegistry()
    reg._rows = {"a": [_row("a", 1, "s", 1.3, 103.8, image_url="https://images.data.gov.sg/x.jpg")]}
    reg._at = {k: 1e12 for k in SOURCES}; reg._rebuild_index()
    class _R:
        content = bytes.fromhex("ffd8ff") + b"rest"; headers = {"content-type": "application/octet-stream"}
        def raise_for_status(self): pass
    monkeypatch.setattr(reg._client, "get", lambda *a, **k: _R())
    assert reg.snapshot("a:1")[1] == "image/jpeg"


def test_iteris_graphql_lane_maps_camera_views_and_skips_other_features():
    payload = {"data": {"mapFeaturesQuery": {"mapFeatures": [
        {"__typename": "Camera", "uri": "camera/59618189", "title": "IA 163 @ MM 4.3", "active": True,
         "features": [{"geometry": {"type": "Point", "coordinates": [-93.517, 41.599]}}],
         "views": [{"url": "https://atms.iowadot.gov/a.jpg"}, {"url": "https://s.iowadot.gov/x/playlist.m3u8"}]},
        {"__typename": "Event", "uri": "event/1", "title": "roadwork",
         "features": [{"geometry": {"coordinates": [-93.5, 41.6]}}]}]}}}
    class _Reg(CameraRegistry):
        def __init__(self): super().__init__(); self.sent = None
    reg = _Reg()
    class _R:
        def raise_for_status(self): pass
        def json(self): return payload
    def post(url, json=None, headers=None, **k):
        reg.sent = (url, json); return _R()
    reg._client.post = post
    src = SOURCES["it_ia"]
    rows = cameras._iteris(reg, src)
    assert [r["id"] for r in rows] == ["it_ia:59618189.0", "it_ia:59618189.1"]
    assert rows[0]["image_url"].endswith("a.jpg") and rows[1]["stream_url"].endswith("playlist.m3u8")
    assert reg.sent[0] == "https://511ia.org/api/graphql"
    assert reg.sent[1]["variables"]["input"]["layerSlugs"] == ["normalCameras"]
    assert "... on Plow" in reg.sent[1]["query"]           # trimmed queries get "Server error"


def test_cars_colorado_skips_broken_and_private_views():
    data = {"features": [
        {"geometry": {"coordinates": [-105.0, 40.58]}, "properties": {"id": 1, "name": "I-25", "public": True, "route": "I-25",
         "views": [{"name": "I-25 SB", "url": "https://p.cotrip.org/rtplive/x/playlist.m3u8", "videoPreviewUrl": "https://c.carsprogram.org/x.png", "broken": False},
                   {"name": "I-25 NB", "url": "https://p.cotrip.org/rtplive/y/playlist.m3u8", "videoPreviewUrl": "https://c.carsprogram.org/y.png", "broken": True}]}},
        {"geometry": {"coordinates": [-105.1, 40.5]}, "properties": {"id": 2, "name": "private", "public": False,
         "views": [{"url": "https://p/z.m3u8", "videoPreviewUrl": "https://c/z.png"}]}}]}
    rows = cameras._cars_co(_Stub({"map-features": data}), SOURCES["cotrip"])
    assert len(rows) == 1 and rows[0]["id"] == "cotrip:1.0" and rows[0]["stream_url"].endswith("x/playlist.m3u8")


def test_atis_lane_handles_hls_sites_multi_still_sites_and_data_wrapper():
    doc = {"data": [
        {"geometry": {"coordinates": [-77.3, 38.8]}, "properties": {"id": "1", "description": "Univ Dr", "route": "",
         "https_url": "https://m.vdotcameras.com/rtplive/x/playlist.m3u8", "problem_stream": False}},
        {"geometry": {"coordinates": [-77.4, 38.9]}, "properties": {"id": "2", "description": "broken",
         "https_url": "https://m.vdotcameras.com/rtplive/y/playlist.m3u8", "problem_stream": True}},
        {"geometry": {"coordinates": [-100.0, 44.9]}, "properties": {"id": "7", "name": "Watertown North", "route": "I-29",
         "cameras": [{"id": "a", "name": "Looking South", "image": "https://sd.cdn.iteris-atis.com/c/1/latest.jpg"},
                     {"id": "b", "name": "Looking North", "image": "https://sd.cdn.iteris-atis.com/c/4/latest.jpg"}]}}]}
    rows = cameras._atis(_Stub({"": doc}), SOURCES["at_va"])
    ids = [r["id"] for r in rows]
    assert ids == ["at_va:1", "at_va:7.a", "at_va:7.b"]           # broken stream skipped, stills expanded
    assert rows[0]["stream_url"].endswith("x/playlist.m3u8") and rows[1]["image_url"].endswith("1/latest.jpg")
