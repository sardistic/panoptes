"""Public live-camera registry — parsers on captured shapes, query sampling, proxy
guard rails. Offline: the registry's fetch helpers are stubbed per test."""
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
