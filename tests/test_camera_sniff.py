"""Camera sniffer — shape detection on captured payloads, and the runtime's replay
of a discovered spec (nested camera lists, HLS vs stills, POST bodies)."""
import json

from apb.discover.camera_sniff import _walk_arrays, score_list
from apb.ingest import cameras
from apb.ingest.cameras import CameraRegistry, CameraSource


def _cams(n, img="https://x.gov/cams/{i}.jpg"):
    return [{"id": i, "name": f"cam {i}", "lat": 38 + i * .01, "lon": -90 - i * .01, "url": img.format(i=i)} for i in range(n)]


def test_score_list_finds_camera_shape_and_rejects_icons_and_small_lists():
    sc = score_list(_cams(30))
    assert sc and sc["lat"] == "lat" and sc["lon"] == "lon" and sc["image"] == "url" and sc["count"] == 30
    assert score_list(_cams(5)) is None                                        # too few to be a fleet
    assert score_list(_cams(30, img="https://x.gov/images/tg_marker.svg")) is None   # map icons, not cameras
    nested = [{"site": {"latitude": 38.1, "longitude": -90.1}, "views": [{"snapshot": "https://x/1.jpg"}]} for _ in range(20)]
    sc = score_list(nested)
    assert sc and sc["lat"] == "site.latitude" and sc["image"] == "views[0].snapshot"


def test_walk_arrays_finds_lists_anywhere():
    doc = {"data": {"query": {"features": _cams(16)}}, "meta": [1, 2]}
    paths = [p for p, rows in _walk_arrays(doc) if score_list(rows)]
    assert paths == ["$.data.query.features"]


class _Reg(CameraRegistry):
    def __init__(self, payload, want_post=False):
        super().__init__()
        self.payload, self.want_post, self.calls = payload, want_post, []

    class _R:
        def __init__(self, text): self.text = text
        def raise_for_status(self): pass

    def _fake(self, url, **kw):
        self.calls.append((url, kw))
        return self._R(json.dumps(self.payload))


def test_discovered_spec_replays_nested_lists_and_hls(monkeypatch):
    payload = {"poles": [{"name": f"pole {i}", "mapCameras": [
        {"latitude": 35 + i * .01, "longitude": -97, "urlVar": f"/api/media.amp?camera={i}"},
        {"latitude": 35 + i * .01, "longitude": -97, "urlVar": f"https://s.gov/rtplive/c{i}/playlist.m3u8"}]}
        for i in range(3)]}
    spec = {"endpoint": "https://oktraffic.org/api/CameraPoles", "items_path": "$.poles", "method": "GET",
            "referer": "https://oktraffic.org/", "state": "OK", "enabled": True,
            "fields": {"lat": "mapCameras[0].latitude", "lon": "mapCameras[0].longitude",
                       "image": "mapCameras[0].urlVar", "name": "name", "id": "name"}}
    reg = _Reg(payload)
    monkeypatch.setattr(reg._client, "get", reg._fake)
    src = CameraSource("dx_ok", "OK", cameras._discovered, "OK"); src.spec = spec
    rows = cameras._discovered(reg, src)
    assert len(rows) == 6                                        # both views of every pole
    assert rows[0]["image_url"] == "https://oktraffic.org/api/media.amp?camera=0"   # relative URL resolved
    assert rows[1]["stream_url"].endswith("playlist.m3u8") and rows[1]["image_url"] is None
    assert reg.calls[0][1]["headers"]["Referer"] == "https://oktraffic.org/"


def test_discovered_spec_replays_post_bodies(monkeypatch):
    payload = {"data": {"cams": _cams(20)}}
    spec = {"endpoint": "https://x.gov/graphql", "items_path": "$.data.cams", "method": "POST",
            "post_data": '{"query":"{cams{id}}"}', "referer": "https://x.gov/", "enabled": True,
            "fields": {"lat": "lat", "lon": "lon", "image": "url", "name": "name", "id": "id"}}
    reg = _Reg(payload)
    monkeypatch.setattr(reg._client, "post", lambda url, **kw: reg._fake(url, **kw))
    src = CameraSource("dx_x", "X", cameras._discovered, "XX"); src.spec = spec
    rows = cameras._discovered(reg, src)
    assert len(rows) == 20 and reg.calls[0][1]["content"] == '{"query":"{cams{id}}"}'


def test_load_discoveries_skips_disabled(tmp_path, monkeypatch):
    f = tmp_path / "d.json"
    f.write_text(json.dumps({"a": {"enabled": True, "endpoint": "u", "items_path": "$", "fields": {}},
                             "b": {"enabled": False, "endpoint": "u", "items_path": "$", "fields": {}}}))
    monkeypatch.setattr(cameras, "_DISCOVERIES", f)
    got = cameras._load_discoveries()
    assert list(got) == ["dx_a"] and got["dx_a"].spec["endpoint"] == "u"
