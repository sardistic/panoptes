"""Public live-camera registry — DOT / traffic cameras first, one parser per vendor.

Cameras are not incidents: they are slow-changing points (name, lat/lon, snapshot URL,
optional HLS stream) that the map draws as a layer and refreshes on click. So this
module is a REGISTRY of sources (mirrors the traffic511 pattern), each fetched on its
own long TTL and flattened to one normalized dict:

    {id, source, kind, name, lat, lon, state, roadway, direction, image_url,
     stream_url, online, page_url}

`kind` is "traffic" (default), "wildfire" or "weather" so the UI can style/filter.

`id` is "<source>:<native id>" and is the only thing the browser sends back — the
snapshot proxy in apb.api.main resolves it through this registry, so the server never
fetches arbitrary URLs.

Verified keyless on 2026-09-17 (row counts at probe time): Caltrans 12 districts
(3.6k), NYC DOT TMC (979), 511NY (2.9k, 1.7k with HLS), DelDOT (361, HLS only),
Maryland CHART (552, HLS only), Seattle SDOT+WSDOT city set (653), Ontario 511 (944
cameras / 1.7k views), TfL JamCams (890), NZTA (~300). Second sweep (catalog
search + endpoint probing, same day): ODOT TripCheck (1.1k), ALGO Alabama (646,
stills + HLS), TravelMidwest gateway (IL/IN/WI/KY + Tollway + Lake County, 1.7k
sites, multi-direction stills), Austin (1k), Baton Rouge (118), ALERTCalifornia
wildfire cams (1.3k), Calgary (216), Ottawa (428), Vancouver (218 x 4 views),
Finland Digitraffic weather cams (810 stations), Singapore, Hong Kong (1k), TfNSW
(147 via ArcGIS mirror). The Carmanah 511 platform's
`/api/v2/get/cameras?key=` is keyed on every other state; those entries light up
when the matching `T511_*_KEY` is set (same key as the events lane).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin
from typing import Callable
from xml.etree import ElementTree as ET

import httpx

log = logging.getLogger(__name__)

# Browser-shaped UA with our identity: some DOT image hosts (TfNSW) 403 bare bots.
_UA = {"User-Agent": "Mozilla/5.0 (compatible; apb/0.1; +https://panoptes.run)"}
_LIST_TTL = 45 * 60.0          # camera inventories change rarely
_FAIL_TTL = 5 * 60.0           # retry a failed source after this long
_SNAPSHOT_TTL = 6.0            # most DOT stills refresh every 5-60s
_SNAPSHOT_MAX_BYTES = 2_000_000


@dataclass
class CameraSource:
    key: str
    name: str
    parse: Callable[["CameraRegistry", "CameraSource"], list[dict]]
    state: str | None = None
    env_key: str | None = None      # keyed sources stay dark until this env var is set
    host: str | None = None         # Carmanah v2 host, e.g. "511ga.org"
    spec: dict | None = None        # discovered-feed spec (apb.discover.camera_sniff)


def _f(v) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x == x else None          # NaN guard; range checked in _row


def _row(source: str, native_id, name, lat, lon, *, state=None, roadway=None,
         direction=None, image_url=None, stream_url=None, online=True,
         page_url=None, kind: str = "traffic", embed_url=None) -> dict | None:
    lat, lon = _f(lat), _f(lon)
    if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    if not (lat or lon) or not (image_url or stream_url or embed_url):
        return None
    return {
        "id": f"{source}:{native_id}", "source": source, "kind": kind,
        "name": str(name or "").strip()[:140], "lat": round(lat, 5), "lon": round(lon, 5),
        "state": state, "roadway": (roadway or None) and str(roadway)[:80],
        "direction": (direction or None) and str(direction)[:40],
        "image_url": image_url or None, "stream_url": stream_url or None,
        "online": bool(online), "page_url": page_url or None,
        **({"embed_url": embed_url} if embed_url else {}),
    }


# ---------------------------------------------------------------------------
# parsers — each returns normalized rows for one source

def _caltrans(reg: "CameraRegistry", src: CameraSource) -> list[dict]:
    out: list[dict] = []
    for d in range(1, 13):
        url = f"https://cwwp2.dot.ca.gov/data/d{d}/cctv/cctvStatusD{d:02d}.json"
        try:
            data = reg.get_json(url).get("data", [])
        except (httpx.HTTPError, ValueError, AttributeError) as e:
            log.warning(f"[cameras] caltrans D{d}: {e}")
            continue
        for item in data:
            c = item.get("cctv") or {}
            loc, img = c.get("location") or {}, (c.get("imageData") or {})
            still = (img.get("static") or {}).get("currentImageURL")
            stream = img.get("streamingVideoURL") or None
            r = _row("caltrans", f"d{d}-{c.get('index')}", loc.get("locationName"),
                     loc.get("latitude"), loc.get("longitude"), state="CA",
                     roadway=loc.get("route"), direction=loc.get("direction"),
                     image_url=still, stream_url=stream,
                     online=str(c.get("inService")).lower() == "true")
            if r:
                out.append(r)
    return out


def _nyctmc(reg, src) -> list[dict]:
    out = []
    for c in reg.get_json("https://webcams.nyctmc.org/api/cameras"):
        r = _row("nyctmc", c.get("id"), c.get("name"), c.get("latitude"), c.get("longitude"),
                 state="NY", roadway=c.get("area"), image_url=c.get("imageUrl"),
                 online=str(c.get("isOnline")).lower() == "true")
        if r:
            out.append(r)
    return out


def _ny511(reg, src) -> list[dict]:
    """511NY v1 getcameras (keyless). `Url` is the still (/map/Cctv/{id}); most
    cameras also expose an HLS playlist on *.nysdot.skyvdn.com."""
    out = []
    for c in reg.get_json("https://511ny.org/api/getcameras?format=json"):
        if c.get("Blocked"):
            continue
        r = _row("ny511", c.get("ID"), c.get("Name"), c.get("Latitude"), c.get("Longitude"),
                 state="NY", roadway=c.get("RoadwayName"), direction=c.get("DirectionOfTravel"),
                 image_url=c.get("Url"), stream_url=c.get("VideoUrl"),
                 online=not c.get("Disabled"))
        if r:
            out.append(r)
    return out


def _carmanah_v2(reg, src) -> list[dict]:
    """Carmanah 511 `/api/v2/get/cameras` (verified keyless on 511on.ca; keyed
    elsewhere). One row per *view* so multi-angle sites are all reachable."""
    url = f"https://{src.host}/api/v2/get/cameras"
    if src.env_key:
        key = os.environ.get(src.env_key, "").strip()
        if not key:
            return []
        url += f"?key={key}"
    out = []
    for c in reg.get_json(url):
        views = c.get("Views") or []
        for v in views:
            if str(v.get("Status", "Enabled")).lower() == "disabled":
                continue
            label = c.get("Location") or c.get("Roadway") or ""
            if len(views) > 1 and v.get("Description"):
                label = f"{label} · {v['Description']}"
            r = _row(src.key, f"{c.get('Id')}.{v.get('Id')}", label,
                     c.get("Latitude"), c.get("Longitude"), state=src.state,
                     roadway=c.get("Roadway"), direction=c.get("Direction"),
                     image_url=v.get("Url"), stream_url=v.get("VideoUrl"))
            if r:
                out.append(r)
    return out


def _deldot(reg, src) -> list[dict]:
    out = []
    data = reg.get_json("https://tmc.deldot.gov/json/videocamera.json")
    for c in data.get("videoCameras", []):
        urls = c.get("urls") or {}
        r = _row("deldot", c.get("id"), c.get("title"), c.get("lat"), c.get("lon"),
                 state="DE", roadway=c.get("county"),
                 stream_url=urls.get("m3u8s") or None,
                 online=bool(c.get("enabled")) and c.get("status") == "Active")
        if r:
            out.append(r)
    return out


def _chart_md(reg, src) -> list[dict]:
    """Maryland CHART: HLS on strmr*.sha.maryland.gov (derived exactly as the
    public GetVideo page does)."""
    out = []
    data = reg.get_json("https://chartexp1.sha.maryland.gov/CHARTExportClientService/"
                        "getCameraMapDataJSON.do")
    for c in data.get("data", []):
        ip, cid = c.get("cctvIp"), c.get("id")
        if not ip or not cid or not str(ip).endswith(".sha.maryland.gov"):
            continue
        road = " ".join(str(x) for x in (c.get("routePrefix"), c.get("routeNumber")) if x)
        r = _row("chart_md", cid, c.get("name") or c.get("description"), c.get("lat"),
                 c.get("lon"), state="MD", roadway=road or None,
                 stream_url=f"https://{ip}/rtplive/{cid}/playlist.m3u8",
                 online=c.get("opStatus") == "OK", page_url=c.get("publicVideoURL"))
        if r:
            out.append(r)
    return out


def _seattle(reg, src) -> list[dict]:
    """Seattle Travelers map: SDOT stills plus the WSDOT cameras inside the city."""
    base = {"sdot": "https://www.seattle.gov/trafficcams/images/",
            "wsdot": "https://images.wsdot.wa.gov/nw/"}
    out = []
    data = reg.get_json("https://web.seattle.gov/Travelers/api/Map/Data?zoomId=13&type=2")
    for f in data.get("Features", []):
        pt = f.get("PointCoordinate") or [None, None]
        for c in f.get("Cameras") or []:
            b = base.get(str(c.get("Type", "")).lower())
            if not b or not c.get("ImageUrl"):
                continue
            r = _row("seattle", c.get("Id"), c.get("Description"), pt[0], pt[1], state="WA",
                     image_url=b + c["ImageUrl"])
            if r:
                out.append(r)
    return out


def _tfl(reg, src) -> list[dict]:
    out = []
    for c in reg.get_json("https://api.tfl.gov.uk/Place/Type/JamCam"):
        props = {p.get("key"): p.get("value") for p in c.get("additionalProperties") or []}
        r = _row("tfl", str(c.get("id", "")).replace("JamCams_", ""), c.get("commonName"),
                 c.get("lat"), c.get("lon"), state="UK", direction=props.get("view"),
                 image_url=props.get("imageUrl"), stream_url=props.get("videoUrl"),
                 online=str(props.get("available")).lower() == "true")
        if r:
            out.append(r)
    return out


def _nzta(reg, src) -> list[dict]:
    root = ET.fromstring(reg.get_text("https://trafficnz.info/service/traffic/rest/4/cameras/all"))
    out = []
    for c in root.findall("camera"):
        img = c.findtext("imageUrl")
        r = _row("nzta", c.findtext("id"), c.findtext("name") or c.findtext("description"),
                 c.findtext("latitude"), c.findtext("longitude"), state="NZ",
                 roadway=c.findtext("highway"), direction=c.findtext("direction"),
                 image_url=("https://trafficnz.info" + img) if img else None,
                 online=c.findtext("offline") != "true" and c.findtext("underMaintenance") != "true")
        if r:
            out.append(r)
    return out


def _tripcheck(reg, src) -> list[dict]:
    """ODOT TripCheck: the public map's inventory JS (ArcGIS-shaped JSON in a script
    file); stills live at tripcheck.com/RoadCams/cams/{filename}."""
    txt = reg.get_text("https://tripcheck.com/Scripts/map/data/cctvinventory.js")
    data = json.loads(txt[txt.index("{"):txt.rindex("}") + 1])
    out = []
    for f in data.get("features", []):
        a = f.get("attributes") or {}
        if not a.get("filename"):
            continue
        r = _row("tripcheck", a.get("cameraId"), a.get("title"), a.get("latitude"),
                 a.get("longitude"), state="OR", roadway=(a.get("route") or "").strip(),
                 image_url="https://tripcheck.com/RoadCams/cams/" + a["filename"])
        if r:
            out.append(r)
    return out


def _algo(reg, src) -> list[dict]:
    """ALGO Traffic (Alabama DOT): keyless v4 API, every camera has a still + HLS."""
    out = []
    for c in reg.get_json("https://api.algotraffic.com/v4.0/Cameras"):
        loc = c.get("location") or {}
        name = " @ ".join(x for x in (loc.get("displayRouteDesignator"),
                                      loc.get("displayCrossStreet")) if x)
        r = _row("algo", c.get("id"), name or loc.get("city"), loc.get("latitude"),
                 loc.get("longitude"), state="AL", roadway=loc.get("routeDesignator"),
                 direction=loc.get("direction"), image_url=c.get("snapshotImageUrl"),
                 stream_url=(c.get("playbackUrls") or {}).get("hls"),
                 page_url=c.get("permLink"))
        if r:
            out.append(r)
    return out


def _travelmidwest(reg, src) -> list[dict]:
    """Gary-Chicago-Milwaukee gateway: IDOT, Illinois Tollway, Lake County, InDOT,
    WisDOT (via the keyless 511wi /map/Cctv still endpoint) and a few KYTC cameras.
    Multi-direction sites emit one row per direction."""
    data = reg.get_json("https://www.travelmidwest.com/lmiga/cameraReport.json?path=GATEWAY")
    out = []
    for table in data.get("reportTables", []):
        for c in table.get("cells", []):
            agency = str(c.get("agency", "")).replace("&nbsp;", " ")
            base = dict(state={"IDOT": "IL", "Illinois Tollway": "IL", "Lake County": "IL",
                               "InDOT": "IN", "WisDOT": "WI", "KYTC": "KY"}.get(agency),
                        roadway=table.get("displayName"))
            views = c.get("imageDirections") or {}
            if views:
                for d, v in views.items():
                    r = _row("travelmidwest", f"{c.get('externalId')}.{d}",
                             f"{c.get('location')} · {d}", c.get("latitude"), c.get("longitude"),
                             direction=d, image_url=v.get("url"), **base)
                    if r:
                        out.append(r)
            else:
                r = _row("travelmidwest", c.get("externalId"), c.get("location"),
                         c.get("latitude"), c.get("longitude"), direction=c.get("direction"),
                         image_url=c.get("url"), stream_url=c.get("videoUrl"), **base)
                if r:
                    out.append(r)
    return out


def _socrata_points(reg, src, url, *, id_key, name_key, img, geom_key, state, kind="traffic",
                    status=None):
    """Shared Socrata reader: GeoJSON point column + an image-URL column/lambda."""
    out = []
    for c in reg.get_json(url + ("&" if "?" in url else "?") + "$limit=5000"):
        g = c.get(geom_key) or {}
        coords = g.get("coordinates") or [None, None]
        image = img(c) if callable(img) else c.get(img)
        online = status(c) if status else True
        r = _row(src.key, c.get(id_key), c.get(name_key), coords[1], coords[0], state=state,
                 image_url=image, online=online, kind=kind)
        if r:
            out.append(r)
    return out


def _austin(reg, src) -> list[dict]:
    return _socrata_points(reg, src, "https://data.austintexas.gov/resource/b4k4-adkb.json",
                           id_key="camera_id", name_key="location_name",
                           img="screenshot_address", geom_key="location", state="TX",
                           status=lambda c: c.get("camera_status") == "TURNED_ON")


def _brla(reg, src) -> list[dict]:
    """Baton Rouge: the 511la still endpoint (/map/Cctv/{id}) is keyless even though
    the statewide list API is not."""
    return _socrata_points(reg, src, "https://data.brla.gov/resource/6z6u-ts44.json",
                           id_key="id", name_key="id", img="image_view",
                           geom_key="the_geom", state="LA")


def _calgary(reg, src) -> list[dict]:
    return _socrata_points(reg, src, "https://data.calgary.ca/resource/k7p9-kppz.json",
                           id_key="camera_location", name_key="camera_location",
                           img=lambda c: (c.get("camera_url") or {}).get("url"),
                           geom_key="point", state="AB")


def _arcgis_points(reg, src, layer_url, *, build, where="1=1"):
    """Shared ArcGIS FeatureServer reader (WGS84, paged)."""
    out, offset = [], 0
    while True:
        data = reg.get_json(f"{layer_url}/query?where={where}&outFields=*&outSR=4326&f=json"
                            f"&resultRecordCount=2000&resultOffset={offset}")
        feats = data.get("features", [])
        for f in feats:
            r = build(f.get("attributes") or {}, f.get("geometry") or {})
            if isinstance(r, dict) and "rows" in r and "id" not in r:
                out.extend(r["rows"])
            elif r:
                out.append(r)
        if not feats or not data.get("exceededTransferLimit"):   # server page size varies (50..2000)
            return out
        offset += len(feats)


def _alertca(reg, src) -> list[dict]:
    """ALERTCalifornia (UC San Diego) wildfire cameras — public latest-frame stills."""
    def build(a, g):
        return _row("alertca", a.get("siteId") or a.get("OBJECTID"), a.get("cameraName"),
                    g.get("y"), g.get("x"), state="CA", roadway=(a.get("county") or "").title(),
                    image_url=a.get("imageURL"), page_url=a.get("cameraURL"),
                    online=a.get("isOnline") == "online" and a.get("isActive") == "active",
                    kind="wildfire")
    return _arcgis_points(reg, src, "https://services8.arcgis.com/X84q166Srnyl4JMV/arcgis/"
                          "rest/services/ALERTCalifornia_Camera_Feed/FeatureServer/0", build=build)


def _tfnsw(reg, src) -> list[dict]:
    """Transport for NSW live cameras via a public ArcGIS mirror of the (keyed) API;
    the image URLs on transport.nsw.gov.au are keyless."""
    def build(a, g):
        return _row("tfnsw", a.get("id"), a.get("properti02"), a.get("F_latitude"),
                    a.get("F_longitude"), state="NSW", roadway=a.get("properti01"),
                    direction=a.get("properties"), image_url=a.get("properti00"))
    return _arcgis_points(reg, src, "https://services7.arcgis.com/uFAr0LUPy14bDaLg/arcgis/"
                          "rest/services/LiveTraffic_Cameras_TfNSW/FeatureServer/0", build=build)


def _ottawa(reg, src) -> list[dict]:
    out = []
    for c in reg.get_json("https://traffic.ottawa.ca/beta/camera_list"):
        r = _row("ottawa", c.get("number"), c.get("description"), c.get("latitude"),
                 c.get("longitude"), state="ON", roadway=c.get("type"),
                 image_url=f"https://traffic.ottawa.ca/beta/camera?c={c.get('number')}")
        if r:
            out.append(r)
    return out


def _vancouver(reg, src) -> list[dict]:
    """City of Vancouver: open-data list of camera pages; each page embeds 1-4 stills
    (one per direction) under trafficcams.vancouver.ca/cameraimages/."""
    base = ("https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/"
            "web-cam-url-links/records?limit=100&offset=")
    data = reg.get_json(base + "0")
    recs = list(data.get("results", []))
    for off in range(100, int(data.get("total_count", 0)), 100):
        recs += reg.get_json(base + str(off)).get("results", [])
    out = []

    def page(rec):
        try:
            html = reg.get_text(rec["url"])
        except httpx.HTTPError:
            return []
        pt = rec.get("geo_point_2d") or {}
        rows = []
        for i, img in enumerate(re.findall(r'src="(cameraimages/[^"]+\.jpg)"', html)):
            rows.append(_row("vancouver", f"{rec.get('mapid')}.{i}", rec.get("name"),
                             pt.get("lat"), pt.get("lon"), state="BC",
                             image_url="https://trafficcams.vancouver.ca/" + img,
                             page_url=rec["url"]))
        return rows
    with ThreadPoolExecutor(6) as ex:
        for rows in ex.map(page, [r for r in recs if r.get("url")]):
            out.extend(r for r in rows if r)
    return out


def _digitraffic(reg, src) -> list[dict]:
    """Fintraffic weather cameras (Finland): one row per preset (viewing direction)."""
    out = []
    for f in reg.get_json("https://tie.digitraffic.fi/api/weathercam/v1/stations").get("features", []):
        p = f.get("properties") or {}
        coords = (f.get("geometry") or {}).get("coordinates") or [None, None]
        for preset in p.get("presets") or []:
            if not preset.get("inCollection"):
                continue
            r = _row("digitraffic", preset.get("id"), p.get("name", "").replace("_", " "),
                     coords[1], coords[0], state="FI",
                     image_url=f"https://weathercam.digitraffic.fi/{preset.get('id')}.jpg",
                     online=p.get("collectionStatus") == "GATHERING", kind="weather")
            if r:
                out.append(r)
    return out


def _singapore(reg, src) -> list[dict]:
    out = []
    for item in reg.get_json("https://api.data.gov.sg/v1/transport/traffic-images").get("items", []):
        for c in item.get("cameras", []):
            loc = c.get("location") or {}
            r = _row("sg", c.get("camera_id"), f"LTA camera {c.get('camera_id')}",
                     loc.get("latitude"), loc.get("longitude"), state="SG", image_url=c.get("image"))
            if r:
                out.append(r)
    return out


def _hongkong(reg, src) -> list[dict]:
    root = ET.fromstring(reg.get_text("https://static.data.gov.hk/td/traffic-snapshot-images/"
                                      "code/Traffic_Camera_Locations_En.xml"))
    out = []
    for c in root.findall("image"):
        r = _row("hk", c.findtext("key"), c.findtext("description"), c.findtext("latitude"),
                 c.findtext("longitude"), state="HK", roadway=c.findtext("district"),
                 image_url=c.findtext("url"))
        if r:
            out.append(r)
    return out


def _toronto(reg, src) -> list[dict]:
    """City of Toronto RESCU cameras (open data GeoJSON; still per site)."""
    out = []
    data = reg.get_json("https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/a3309088-5fd4-4d34-8297-"
                        "77c8301840ac/resource/4a568300-c7f8-496d-b150-dff6f5dc6d4f/download/traffic-camera-list-4326.geojson")
    for f in data.get("features", []):
        p, c = f.get("properties") or {}, (f.get("geometry") or {}).get("coordinates") or [None, None]
        if c and isinstance(c[0], list):          # MultiPoint
            c = c[0]
        if len(c) < 2:
            continue
        r = _row("toronto", p.get("REC_ID"), f"{p.get('MAINROAD')} @ {p.get('CROSSROAD')}", c[1], c[0],
                 state="ON", roadway=p.get("MAINROAD"), image_url=p.get("IMAGEURL"))
        if r:
            out.append(r)
    return out


def _buoycams(reg, src) -> list[dict]:
    """NOAA NDBC BuoyCAMs: ocean-buoy panoramas (kind=marine), hourly stills."""
    out = []
    for c in reg.get_json("https://www.ndbc.noaa.gov/buoycams.php"):
        r = _row("buoycam", c.get("id"), c.get("name"), c.get("lat"), c.get("lng"), state="sea",
                 image_url=f"https://www.ndbc.noaa.gov/buoycam.php?station={c.get('id')}", kind="marine")
        if r:
            out.append(r)
    return out


def _wsdot(reg, src) -> list[dict]:
    """WSDOT statewide cameras — free AccessCode (WSDOT_ACCESS_CODE). Shape per WSDOT
    Traveler API docs; unverified until a code is set."""
    key = os.environ.get("WSDOT_ACCESS_CODE", "").strip()
    if not key:
        return []
    out = []
    for c in reg.get_json("https://wsdot.wa.gov/Traffic/api/HighwayCameras/HighwayCamerasREST.svc/"
                          f"GetCamerasAsJson?AccessCode={key}"):
        loc = c.get("CameraLocation") or {}
        r = _row("wsdot", c.get("CameraID"), c.get("Title"), loc.get("Latitude"), loc.get("Longitude"),
                 state="WA", roadway=loc.get("RoadName"), image_url=c.get("ImageURL"),
                 online=bool(c.get("IsActive", True)))
        if r:
            out.append(r)
    return out


def _ohgo(reg, src) -> list[dict]:
    """OHGO (Ohio DOT) cameras — free key (OHGO_API_KEY, `Authorization: APIKEY <key>`).
    Shape per publicapi.ohgo.com docs; unverified until a key is set."""
    key = os.environ.get("OHGO_API_KEY", "").strip()
    if not key:
        return []
    out = []
    r0 = reg._client.get("https://publicapi.ohgo.com/api/v1/cameras?page-all=true",
                         headers={"Authorization": f"APIKEY {key}"})
    r0.raise_for_status()
    for c in r0.json().get("results", []):
        for i, v in enumerate(c.get("cameraViews") or []):
            r = _row("ohgo", f"{c.get('id')}.{i}", f"{c.get('location')} · {v.get('direction') or ''}".strip(" ·"),
                     c.get("latitude"), c.get("longitude"), state="OH", direction=v.get("direction"),
                     image_url=v.get("largeUrl") or v.get("smallUrl"))
            if r:
                out.append(r)
    return out


# ── Carmanah / TravelIQ 511 sites: the website's own camera list (keyless) ──────
# Their public API (`/api/v2/get/cameras`) wants a key, but every site's "Cameras
# List" page loads the same data through a DataTables route, 100 rows a page, with
# WKT coordinates, roadway, direction and one `/map/Cctv/{id}` still per view.
# Found on drivenc.gov; the route is identical on every site the vendor hosts.
TRAVELIQ: dict[str, tuple[str, str | None]] = {
    "nc": ("www.drivenc.gov", "NC"), "ga": ("511ga.org", "GA"), "pa": ("www.511pa.com", "PA"),
    "fl": ("fl511.com", "FL"), "la": ("511la.org", "LA"), "id": ("511.idaho.gov", "ID"),
    "ne6": ("newengland511.org", None), "az": ("az511.gov", "AZ"), "ct": ("ctroads.org", "CT"),
    "ut": ("www.udottraffic.utah.gov", "UT"), "nv": ("www.nvroads.com", "NV"), "wi": ("511wi.gov", "WI"),
    "ak": ("511.alaska.gov", "AK"), "ab": ("511.alberta.ca", "AB"), "sk": ("hotline.gov.sk.ca", "SK"),
    "ns": ("511.novascotia.ca", "NS"), "nb": ("511.gnb.ca", "NB"), "nl": ("511nl.ca", "NL"),
    "yk": ("511yukon.ca", "YT"), "pe": ("511.gov.pe.ca", "PE"), "mb": ("www.manitoba511.ca", "MB"),
}
_WKT_POINT = re.compile(r"POINT\s*\(\s*(-?[\d.]+)\s+(-?[\d.]+)\s*\)")


def _traveliq(reg, src) -> list[dict]:
    host, state = TRAVELIQ[src.key.split("_", 1)[1]]
    hdr = {"Referer": f"https://{host}/cctv", "X-Requested-With": "XMLHttpRequest",
           "Accept": "application/json, text/javascript, */*; q=0.01",
           "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

    def page(start: int) -> dict:
        for attempt in range(3):                           # the big sites 500 under parallel load
            r = reg._client.post(f"https://{host}/List/GetData/Cameras", headers=hdr,
                                 content=f"draw=1&start={start}&length=100&search%5Bvalue%5D=&search%5Bregex%5D=false")
            if r.status_code < 500:
                r.raise_for_status()
                return r.json()
            time.sleep(1.5 * (attempt + 1))
        if start == 0:
            r.raise_for_status()                          # nothing at all: let the lane report it
        log.info("[cameras] %s page %d kept failing (%d); skipping it", host, start, r.status_code)
        return {}
    first = page(0)
    total = int(first.get("recordsTotal") or 0)
    rows = list(first.get("data") or [])
    for start in range(100, min(total, 8000), 100):        # sequential on purpose (see page)
        rows.extend(page(start).get("data") or [])
    out = []
    for c in rows:
        wkt = ((c.get("latLng") or {}).get("geography") or {}).get("wellKnownText") or ""
        m = _WKT_POINT.search(wkt)
        if not m:
            continue
        lon, lat = float(m.group(1)), float(m.group(2))
        base = " ".join(x for x in (c.get("roadway"), c.get("location")) if x) or c.get("location")
        views = [v for v in (c.get("images") or []) if v.get("imageUrl") and not v.get("blocked")]
        for v in views:
            label = base + (f" · {v['description']}" if len(views) > 1 and v.get("description") else "")
            r_ = _row(src.key, v.get("id") or f"{c.get('id')}.{v.get('sortOrder')}", label, lat, lon,
                      state=state or c.get("state"), roadway=c.get("roadway"), direction=c.get("direction"),
                      image_url=urljoin(f"https://{host}/", v["imageUrl"]),
                      online=not v.get("disabled"), page_url=f"https://{host}/cctv")
            if r_:
                out.append(r_)
    return out


# ── "one-network" (Iteris/Castle Rock) 511 platform: GraphQL MapFeatures ──────
# Found by the sniffer on 511ia.org: the public map asks /api/graphql for a bbox
# of features per layer slug; the camera layer is keyless and returns every camera
# un-clustered when asked at zoom 14 for the whole state. The query must carry the
# UI's full fragment set (a trimmed query gets "Server error").
_ITERIS_QUERY = ("query MapFeatures($input: MapFeaturesArgs!, $plowType: String) { mapFeaturesQuery(input: $input) "
                 "{ mapFeatures { bbox title tooltip uri features { id geometry properties type } ... on Cluster "
                 "{ maxZoom } ... on Sign { signDisplayType } ... on Event { priority } __typename ... on Camera "
                 "{ active views(limit: 5) { uri ... on CameraView { url } category } } ... on Plow { views(limit: 5, "
                 "plowType: $plowType) { uri ... on PlowCameraView { url } category } } } error { message type } } }")
# key -> (host, state, (north, south, east, west))
ITERIS: dict[str, tuple[str, str, tuple[float, float, float, float]]] = {
    "ia": ("511ia.org", "IA", (43.6, 40.3, -90.1, -96.7)),
    "in": ("511in.org", "IN", (41.8, 37.7, -84.7, -88.1)),
    "mn": ("511mn.org", "MN", (49.4, 43.4, -89.4, -97.3)),
    "ne": ("511.nebraska.gov", "NE", (43.1, 39.9, -95.2, -104.1)),
    "ma": ("mass511.com", "MA", (42.9, 41.2, -69.9, -73.6)),
    "ks": ("www.kandrive.gov", "KS", (40.1, 36.9, -94.5, -102.1)),
    # probed 2026-09-17 and NOT on this platform: CO (CARS "511x"), SD, ND, KY, VA, SC,
    # WV, CT, NC, WA, AR, HI — see SOURCES.md for their status
}


def _iteris(reg, src) -> list[dict]:
    host, state, (n, s, e, w) = ITERIS[src.key.split("_", 1)[1]]
    body = {"query": _ITERIS_QUERY, "plowType": None,
            "variables": {"input": {"north": n, "south": s, "east": e, "west": w, "zoom": 14,
                                    "layerSlugs": ["normalCameras"], "nonClusterableUris": ["dashboard"]},
                          "plowType": "plowCameras"}}
    r = reg._client.post(f"https://{host}/api/graphql", json=body, headers={
        "Referer": f"https://{host}/", "Origin": f"https://{host}", "Accept": "*/*"})
    r.raise_for_status()
    feats = ((r.json().get("data") or {}).get("mapFeaturesQuery") or {}).get("mapFeatures") or []
    out = []
    for c in feats:
        if c.get("__typename") != "Camera":
            continue
        pt = ((c.get("features") or [{}])[0].get("geometry") or {}).get("coordinates") or [None, None]
        views = [v for v in (c.get("views") or []) if v.get("url")]
        for i, v in enumerate(views):
            url = v["url"]
            r_ = _row(src.key, f"{c.get('uri', '').split('/')[-1]}.{i}",
                      c.get("title") + (f" · view {i + 1}" if len(views) > 1 else ""), pt[1], pt[0],
                      state=state, image_url=None if ".m3u8" in url else url,
                      stream_url=url if ".m3u8" in url else None, online=bool(c.get("active", True)),
                      page_url=f"https://{host}/")
            if r_:
                out.append(r_)
    return out


def _cars_co(reg, src) -> list[dict]:
    """Colorado COtrip: Castle Rock CARS "511x" API (keyless GeoJSON) — every camera
    view carries an HLS playlist and a snapshot PNG."""
    data = reg.get_json("https://api-511x-co.carsprogram.org/cameras/map-features")
    out = []
    for f in data.get("features", []):
        p, c = f.get("properties") or {}, (f.get("geometry") or {}).get("coordinates") or [None, None]
        if not p.get("public", True):
            continue
        for i, v in enumerate(p.get("views") or []):
            if v.get("broken"):
                continue
            r = _row("cotrip", f"{p.get('id')}.{i}", v.get("name") or p.get("name"), c[1], c[0], state="CO",
                     roadway=p.get("route"), image_url=v.get("videoPreviewUrl"),
                     stream_url=v.get("url") if ".m3u8" in str(v.get("url", "")) else None,
                     page_url="https://www.cotrip.org/")
            if r:
                out.append(r)
    return out


# ── Iteris ATIS platform: GeoJSON camera icons (found by the sniffer on 511sc.org) ──
# CDN shape: features whose properties carry HLS urls (`https_url`/`ios_url`) or a
# `cameras[]` list of stills (`image`). VDOT serves the same shape on its own host.
ATIS: dict[str, tuple[str, str]] = {
    "sc": ("https://sc.cdn.iteris-atis.com/geojson/icons/metadata/icons.cameras.geojson", "SC"),
    "mt": ("https://mt.cdn.iteris-atis.com/geojson/icons/metadata/icons.cameras.geojson", "MT"),
    "sd": ("https://sd.cdn.iteris-atis.com/geojson/icons/metadata/icons.cameras.geojson", "SD"),
    "va": ("https://511.vdot.virginia.gov/services/map/array/cameras", "VA"),
}


def _atis(reg, src) -> list[dict]:
    url, state = ATIS[src.key.split("_", 1)[1]]
    r = reg._client.get(url, headers={"Referer": f"https://{url.split('/')[2]}/", "Accept": "application/json"})
    r.raise_for_status()
    doc = json.loads(r.text.lstrip())
    feats = (doc.get("features") or doc.get("data") or []) if isinstance(doc, dict) else doc
    out = []
    for f in feats:
        p, g = f.get("properties") or {}, f.get("geometry") or {}
        c = g.get("coordinates") or [None, None]
        if c and isinstance(c[0], list):
            c = c[0]
        base = p.get("description") or p.get("name") or p.get("route") or ""
        views = p.get("cameras") or []
        if views:                                   # multi-still sites (MT, SD)
            for v in views:
                r_ = _row(src.key, f"{p.get('id')}.{v.get('id')}", f"{base} · {v.get('name') or v.get('description') or ''}".strip(" ·"),
                          c[1], c[0], state=state, roadway=p.get("route"), direction=v.get("direction"),
                          image_url=v.get("image"))
                if r_:
                    out.append(r_)
            continue
        hls = p.get("https_url") or p.get("ios_url")
        if hls and not p.get("problem_stream"):
            r_ = _row(src.key, p.get("id") or p.get("guid"), base, c[1], c[0], state=state,
                      roadway=p.get("route"), direction=p.get("direction"), stream_url=hls)
            if r_:
                out.append(r_)
    return out


def _nddot(reg, src) -> list[dict]:
    """NDDOT: the travel map's ArcGIS 'Cameras-Still' layer — up to six stills per site
    on dot.nd.gov (found via the sniffer's dump of gis.dot.nd.gov requests)."""
    def build(a, g):
        rows = []
        for i in ("", "2", "3", "4", "5", "6"):
            url, desc = a.get(f"FullPath{i}") or a.get(f"Link{i}"), a.get(f"Description{i}")
            if url and str(url).lower().endswith((".jpg", ".jpeg", ".png")):
                rows.append(_row("nddot", f"{a.get('OBJECTID')}.{i or '1'}", desc or a.get("Description"),
                                 g.get("y"), g.get("x"), state="ND", image_url=url,
                                 online=str(a.get("Active", "Y")).upper() != "N"))
        return [r for r in rows if r]
    return _arcgis_points(reg, src, "https://gis.dot.nd.gov/arcgis/rest/services/external/rcrs_dynamic/"
                          "MapServer/5", build=lambda a, g: {"rows": build(a, g)})


_MI_IMG = re.compile(r'id="(\d+)Img"[^>]*src="([^"]+)"')


def _midrive(reg, src) -> list[dict]:
    """MDOT Mi Drive: `camera/AllForMap` has ids + coordinates; `camera/list` carries
    each camera's thumbnail inside an HTML snippet (micamerasimages.net)."""
    hdr = {"Referer": "https://mdotjboss.state.mi.us/MiDrive/map", "X-Requested-With": "XMLHttpRequest"}
    r = reg._client.get("https://mdotjboss.state.mi.us/MiDrive/camera/AllForMap/", headers=hdr)
    r.raise_for_status()
    sites = r.json()
    r2 = reg._client.get("https://mdotjboss.state.mi.us/MiDrive/camera/list", headers=hdr)
    r2.raise_for_status()
    lst = r2.json()
    thumbs = {}
    for row in (lst if isinstance(lst, list) else lst.get("data") or []):
        m = _MI_IMG.search(str(row.get("image", "")))
        if m:
            thumbs[m.group(1)] = m.group(2)
    out = []
    for s in sites if isinstance(sites, list) else sites.get("data") or []:
        img = thumbs.get(str(s.get("id")))
        if not img:
            continue
        r_ = _row("midrive", s.get("id"), s.get("title"), s.get("latitude"), s.get("longitude"), state="MI",
                  image_url=img.replace("/thumbs/", "/").replace(".flv.jpg", ".jpg"),
                  page_url="https://mdotjboss.state.mi.us/MiDrive/map")
        if r_:
            out.append(r_)
    return out


def _ak_rwis(reg, src) -> list[dict]:
    """Alaska DOT&PF road-weather (RWIS) cameras: every site with its latest still per
    view, on Azure blob storage (served as octet-stream; the proxy sniffs JPEG)."""
    coords = {s_["siteId"]: (s_.get("latitude"), s_.get("longitude"))
              for s_ in reg.get_json("https://roadweather.alaska.gov/api/sites") if "siteId" in s_}
    out = []
    for site in reg.get_json("https://roadweather.alaska.gov/api/cameras"):
        lat, lon = coords.get(site.get("siteId"), (None, None))
        for v in (site.get("latestImages") or {}).values():
            img = (v.get("latestImage") or {}).get("downloadUrl")
            if not img:
                continue
            r = _row("ak_rwis", f"{site.get('siteId')}.{v.get('viewNumber')}",
                     f"{site.get('description')} · {v.get('viewName') or ''}".strip(" ·"),
                     lat, lon, state="AK", image_url=img, kind="weather")
            if r:
                out.append(r)
    return out


def _tw_tdx(reg, src) -> list[dict]:
    """Taiwan MOTC TDX open data: freeway + provincial-highway CCTV (keyless within the
    anonymous quota). `VideoStreamURL` is an MJPEG stream; the same host serves a
    single JPEG at `/abs2mjpg/jpg?camera=N`."""
    out = []
    for road in ("Freeway", "Highway"):
        try:
            data = reg.get_json(f"https://tdx.transportdata.tw/api/basic/v2/Road/Traffic/CCTV/{road}?$format=JSON")
        except (httpx.HTTPError, ValueError) as e:
            log.info("[cameras] tdx %s: %s", road, e)
            continue
        for c in data.get("CCTVs", []):
            url = c.get("VideoStreamURL") or ""
            still = url.replace("/bmjpg?", "/jpg?") if "/bmjpg?" in url else (url if url.lower().endswith((".jpg", ".jpeg")) else None)
            name = f"{c.get('RoadName', '')} {c.get('LocationMile', '')} {c.get('RoadDirection', '')}".strip()
            sec = c.get("RoadSection") or {}
            if sec.get("Start"):
                name += f" · {sec.get('Start')}→{sec.get('End', '')}"
            r = _row("tw_tdx", c.get("CCTVID"), name, c.get("PositionLat"), c.get("PositionLon"), state="TW",
                     roadway=c.get("RoadName"), direction=c.get("RoadDirection"), image_url=still)
            if r:
                out.append(r)
    return out


# ── discovered feeds: specs written by apb.discover.camera_sniff ──────────────
_DISCOVERIES = Path(__file__).resolve().parents[2] / "data" / "camera_discoveries.json"
_PATH_PART = re.compile(r"([^\[]+)(?:\[(\d+)\])?$")


def _dget(d, dotted):
    cur = d
    for part in dotted.split("."):
        m = _PATH_PART.match(part)
        if not m or not isinstance(cur, dict):
            return None
        cur = cur.get(m.group(1))
        if m.group(2) is not None and isinstance(cur, list):
            cur = cur[int(m.group(2))] if cur else None
    return cur


def _discovered(reg, src) -> list[dict]:
    """Replay a sniffed endpoint (GET or POST with its body, page Referer) and map
    records through the discovered field paths. A `X[0].` prefix shared by the
    lat/lon/image paths means X is a per-site list of cameras: expand it."""
    spec = src.spec
    hdr = {"Referer": spec.get("referer") or "", "Accept": "application/json, text/plain, */*",
           **{k: v for k, v in (spec.get("headers") or {}).items() if k.lower() != "content-type"}}
    if spec.get("cookies"):
        hdr["Cookie"] = spec["cookies"]
    if spec.get("static_items"):                 # session-bound list, public media: browser snapshot
        return _map_discovered(spec, src, spec["static_items"])
    if spec.get("method", "GET").upper() == "POST":
        r = reg._client.post(spec["endpoint"], content=spec.get("post_data") or "", headers={
            **hdr, "Content-Type": "application/json"})
    else:
        r = reg._client.get(spec["endpoint"], headers=hdr)
    r.raise_for_status()
    text = r.text
    try:
        doc = json.loads(text)
    except ValueError:
        m = re.search(r"[\[{]", text)
        doc = json.loads(text[m.start():text.rindex("}") + 1]) if m else []
    items = doc
    for part in spec["items_path"].split(".")[1:]:
        m = _PATH_PART.match(part)
        items = items.get(m.group(1)) if isinstance(items, dict) else None
        if m and m.group(2) is not None and isinstance(items, list):
            items = items[int(m.group(2))]
    if not isinstance(items, list):
        return []
    return _map_discovered(spec, src, items)


def _map_discovered(spec: dict, src, items: list) -> list[dict]:
    f = spec["fields"]
    prefix = None
    m = re.match(r"^(.+?)\[0\]\.", f["lat"])
    if m and f["lon"].startswith(m.group(0)) and f["image"].startswith(m.group(0)):
        prefix = m.group(1)
    out = []
    seen: set = set()
    for i, rec in enumerate(items):
        subs = [(rec, "")] if not prefix else [(x, "") for x in (_dget(rec, prefix) or []) if isinstance(x, dict)]
        for j, (node, _) in enumerate(subs):
            strip = lambda p: p[len(prefix) + 4:] if prefix and p.startswith(prefix + "[0].") else p
            img = _dget(node, strip(f["image"]))
            stream = _dget(node, strip(f["stream"])) if f.get("stream") else None
            if not img and not stream:
                continue
            img = urljoin(spec["endpoint"], str(img)) if img else None
            if img and (".m3u8" in img or "/rtplive/" in img):
                stream, img = stream or img, None
            name = _dget(node, strip(f["name"])) if f.get("name") else None
            nid = _dget(node, strip(f["id"])) if f.get("id") else None
            nid = str(nid) if nid not in (None, "", 0, "0") else f"{i}.{j}"
            if nid in seen:                              # ids like cameraId=0 repeat: keep rows unique
                nid = f"{nid}.{i}.{j}"
            seen.add(nid)
            row = _row(src.key, nid, name or spec.get("label") or src.key,
                       _dget(node, strip(f["lat"])), _dget(node, strip(f["lon"])), state=spec.get("state"),
                       image_url=img, stream_url=stream, page_url=spec.get("referer"),
                       kind=spec.get("kind") or "traffic")
            if row:
                out.append(row)
    return out


def _load_discoveries() -> dict[str, "CameraSource"]:
    try:
        specs = json.loads(_DISCOVERIES.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    out = {}
    for key, spec in specs.items():
        if not spec.get("enabled"):
            continue
        cs = CameraSource(f"dx_{key}", f"{spec.get('label') or key.upper()} (discovered)", _discovered,
                          spec.get("state") or key.upper())
        cs.spec = spec
        out[cs.key] = cs
    return out


SOURCES: dict[str, CameraSource] = {
    "caltrans": CameraSource("caltrans", "Caltrans CCTV (12 districts)", _caltrans, "CA"),
    "nyctmc": CameraSource("nyctmc", "NYC DOT traffic cameras", _nyctmc, "NY"),
    "ny511": CameraSource("ny511", "511NY cameras", _ny511, "NY"),
    "deldot": CameraSource("deldot", "DelDOT cameras", _deldot, "DE"),
    "chart_md": CameraSource("chart_md", "Maryland CHART cameras", _chart_md, "MD"),
    "seattle": CameraSource("seattle", "Seattle SDOT/WSDOT cameras", _seattle, "WA"),
    "on511": CameraSource("on511", "Ontario 511 cameras", _carmanah_v2, "ON", host="511on.ca"),
    "tfl": CameraSource("tfl", "TfL JamCams (London)", _tfl, "UK"),
    "nzta": CameraSource("nzta", "NZTA cameras (New Zealand)", _nzta, "NZ"),
    "tripcheck": CameraSource("tripcheck", "ODOT TripCheck cameras", _tripcheck, "OR"),
    "algo": CameraSource("algo", "ALGO Traffic cameras (Alabama)", _algo, "AL"),
    "travelmidwest": CameraSource("travelmidwest", "TravelMidwest gateway cameras (IL/IN/WI/KY)",
                                  _travelmidwest, None),
    "austin": CameraSource("austin", "Austin Transportation cameras", _austin, "TX"),
    "brla": CameraSource("brla", "Baton Rouge traffic cameras", _brla, "LA"),
    "alertca": CameraSource("alertca", "ALERTCalifornia wildfire cameras", _alertca, "CA"),
    "calgary": CameraSource("calgary", "City of Calgary cameras", _calgary, "AB"),
    "ottawa": CameraSource("ottawa", "City of Ottawa / MTO cameras", _ottawa, "ON"),
    "vancouver": CameraSource("vancouver", "City of Vancouver cameras", _vancouver, "BC"),
    "digitraffic": CameraSource("digitraffic", "Fintraffic weather cameras (Finland)", _digitraffic, "FI"),
    "sg": CameraSource("sg", "Singapore LTA traffic images", _singapore, "SG"),
    "hk": CameraSource("hk", "Hong Kong TD traffic snapshots", _hongkong, "HK"),
    "tfnsw": CameraSource("tfnsw", "Transport for NSW cameras", _tfnsw, "NSW"),
    "toronto": CameraSource("toronto", "City of Toronto RESCU cameras", _toronto, "ON"),
    "buoycam": CameraSource("buoycam", "NOAA NDBC BuoyCAMs", _buoycams, None),
    "wsdot": CameraSource("wsdot", "WSDOT cameras", _wsdot, "WA", env_key="WSDOT_ACCESS_CODE"),
    "ohgo": CameraSource("ohgo", "OHGO Ohio cameras", _ohgo, "OH", env_key="OHGO_API_KEY"),
    # Carmanah v2 hosts that answered "Invalid Key" (2026-09-17): same free key as
    # the 511 events lane where one exists. Shape unverified until a key is set.
    "ga511": CameraSource("ga511", "511 Georgia cameras", _carmanah_v2, "GA",
                          env_key="T511_GA_KEY", host="511ga.org"),
    "pa511": CameraSource("pa511", "511 Pennsylvania cameras", _carmanah_v2, "PA",
                          env_key="T511_PA_KEY", host="www.511pa.com"),
    "fl511": CameraSource("fl511", "FL511 cameras", _carmanah_v2, "FL",
                          env_key="T511_FL_KEY", host="fl511.com"),
    "la511": CameraSource("la511", "511 Louisiana cameras", _carmanah_v2, "LA",
                          env_key="T511_LA_KEY", host="511la.org"),
    "id511": CameraSource("id511", "511 Idaho cameras", _carmanah_v2, "ID",
                          env_key="T511_ID_KEY", host="511.idaho.gov"),
    "ne511": CameraSource("ne511", "New England 511 cameras", _carmanah_v2, None,
                          env_key="T511_NE_KEY", host="newengland511.org"),
    "ut511": CameraSource("ut511", "UDOT cameras", _carmanah_v2, "UT",
                          env_key="T511_UT_KEY", host="www.udottraffic.utah.gov"),
    "nv511": CameraSource("nv511", "NV Roads cameras", _carmanah_v2, "NV",
                          env_key="T511_NV_KEY", host="www.nvroads.com"),
    "wi511": CameraSource("wi511", "511 Wisconsin cameras", _carmanah_v2, "WI",
                          env_key="T511_WI_KEY", host="511wi.gov"),
    "az511": CameraSource("az511", "AZ511 cameras", _carmanah_v2, "AZ",
                          env_key="T511_AZ_KEY", host="az511.gov"),
    "ct511": CameraSource("ct511", "CT Travel Smart cameras", _carmanah_v2, "CT",
                          env_key="T511_CT_KEY", host="cttravelsmart.org"),
    "ak511": CameraSource("ak511", "511 Alaska cameras", _carmanah_v2, "AK",
                          env_key="T511_AK_KEY", host="511.alaska.gov"),
    "ab511": CameraSource("ab511", "511 Alberta cameras", _carmanah_v2, "AB",
                          env_key="T511_AB_KEY", host="511.alberta.ca"),
}
SOURCES.update({f"tq_{k}": CameraSource(f"tq_{k}", f"{v[1] or 'New England'} 511 cameras (TravelIQ site list)", _traveliq, v[1])
                for k, v in TRAVELIQ.items()})
SOURCES.update({f"at_{k}": CameraSource(f"at_{k}", f"{v[1]} 511 cameras (Iteris ATIS)", _atis, v[1])
                for k, v in ATIS.items()})
SOURCES["ak_rwis"] = CameraSource("ak_rwis", "Alaska RWIS road-weather cameras", _ak_rwis, "AK")
SOURCES["tw_tdx"] = CameraSource("tw_tdx", "Taiwan freeway/highway CCTV (TDX)", _tw_tdx, "TW")
SOURCES["nddot"] = CameraSource("nddot", "NDDOT travel cameras", _nddot, "ND")
SOURCES["midrive"] = CameraSource("midrive", "MDOT Mi Drive cameras", _midrive, "MI")
SOURCES["cotrip"] = CameraSource("cotrip", "COtrip Colorado cameras", _cars_co, "CO")
SOURCES.update({f"it_{k}": CameraSource(f"it_{k}", f"{v[1]} 511 cameras (one-network)", _iteris, v[1])
                for k, v in ITERIS.items()})
SOURCES.update(_load_discoveries())      # sniffed feeds (data/camera_discoveries.json)

# Hosts the browser may pull HLS playlists/segments from directly (hls.js fetch +
# <video>). The API's Content-Security-Policy is built from this list so a new
# streaming source cannot silently be blocked in production.
STREAM_HOSTS: tuple[str, ...] = (
    "https://*.nysdot.skyvdn.com",      # 511NY
    "https://wzmedia.dot.ca.gov",       # Caltrans
    "https://video.deldot.gov",         # DelDOT
    "https://*.sha.maryland.gov",       # Maryland CHART
    "https://s3-eu-west-1.amazonaws.com",   # TfL mp4 clips
    "https://*.wowza.com",              # ALGO Alabama HLS CDN
    "https://*.modot.mo.gov",           # MoDOT (discovered) HLS
    "https://*.cotrip.org",             # COtrip HLS
    "https://*.skyvdn.com",             # Iteris ATIS streamers (SC, NY)
    "https://*.vdotcameras.com",        # VDOT HLS
    "https://nj-511.wink.co",           # 511NJ HLS (no CORS header: native-HLS browsers only)
    "https://stream.oktraffic.org",     # OKtraffic HLS
)


def _sniff_image(b: bytes) -> str | None:
    if b[:3] == bytes.fromhex("ffd8ff"):
        return "image/jpeg"
    if b[:8] == bytes.fromhex("89504e470d0a1a0a"):
        return "image/png"
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return "image/webp"
    return None


def available() -> dict[str, CameraSource]:
    return {k: s for k, s in SOURCES.items()
            if not s.env_key or os.environ.get(s.env_key, "").strip()}


class CameraRegistry:
    """Lazy, per-source cached inventory + a tiny snapshot cache for the proxy.

    Inventory fetches run in a background thread on first demand (a full Caltrans
    sweep is ~15 MB); requests never block on upstream and serve whatever is loaded.
    """

    def __init__(self):
        self._client = httpx.Client(timeout=30.0, headers=_UA, follow_redirects=True)
        self._rows: dict[str, list[dict]] = {}          # source -> rows
        self._at: dict[str, float] = {}                 # source -> fetched at
        self._err: dict[str, str] = {}
        self._index: dict[str, dict] = {}               # id -> row
        self._lock = threading.Lock()
        self._loading: set[str] = set()
        self._snap: dict[str, tuple[float, bytes, str]] = {}
        self._snap_lock = threading.Lock()
        self._windy_cache: dict[tuple, tuple[float, list[dict]]] = {}

    # -- upstream helpers used by parsers
    def get_json(self, url: str):
        return self._client.get(url).json()

    def get_text(self, url: str) -> str:
        return self._client.get(url).text

    # -- inventory
    def _refresh(self, key: str) -> None:
        src = SOURCES[key]
        try:
            rows = src.parse(self, src)
            with self._lock:
                self._rows[key] = rows
                self._err.pop(key, None)
                self._rebuild_index()
            log.info("[cameras] %s: %d cameras", key, len(rows))
        except Exception as e:                      # one bad vendor must not stall the rest
            log.warning("[cameras] %s failed: %s", key, e)
            with self._lock:
                self._err[key] = f"{type(e).__name__}: {e}"[:200]
        finally:
            with self._lock:
                self._at[key] = time.time()
                self._loading.discard(key)

    def _rebuild_index(self) -> None:
        self._index = {r["id"]: r for rows in self._rows.values() for r in rows}

    def ensure_loaded(self, block: bool = False) -> None:
        """Kick stale/unloaded sources; with block=True fetch inline (tests, CLI)."""
        now = time.time()
        for key in available():
            with self._lock:
                at = self._at.get(key)
                ttl = _FAIL_TTL if key in self._err else _LIST_TTL
                if key in self._loading or (at and now - at < ttl):
                    continue
                self._loading.add(key)
            if block:
                self._refresh(key)
            else:
                threading.Thread(target=self._refresh, args=(key,), daemon=True,
                                 name=f"cameras-{key}").start()

    def windy(self, bbox: tuple[float, float, float, float]) -> list[dict]:
        """Windy Webcams (WINDY_WEBCAMS_KEY): the world's largest public webcam index,
        looked up per box (v3 `nearby`) and cached 10 min. Adds beaches, mountains,
        cities, harbours — anything not run by a DOT."""
        key = os.environ.get("WINDY_WEBCAMS_KEY", "").strip()
        if not key:
            return []
        w, s, e, n = bbox
        lat, lon = (s + n) / 2, (w + e) / 2
        radius = max(3, min(250, int(max(n - s, (e - w) * 0.7) * 111 / 2 + 2)))
        ck = (round(lat, 2), round(lon, 2), radius)
        now = time.time()
        with self._snap_lock:
            hit = self._windy_cache.get(ck)
            if hit and now - hit[0] < 600:
                return hit[1]
        rows: list[dict] = []
        try:
            r = self._client.get("https://api.windy.com/webcams/api/v3/webcams",
                                 params={"nearby": f"{lat},{lon},{radius}", "limit": 50,
                                         "include": "location,images,player,urls"},
                                 headers={"x-windy-api-key": key})
            r.raise_for_status()
            for c in r.json().get("webcams", []):
                loc, imgs = c.get("location") or {}, ((c.get("images") or {}).get("current") or {})
                row = _row("windy", c.get("webcamId"), c.get("title"), loc.get("latitude"), loc.get("longitude"),
                           state=loc.get("country"), roadway=loc.get("city"), image_url=imgs.get("preview"),
                           page_url=((c.get("urls") or {}).get("detail")), online=c.get("status") == "active",
                           kind="webcam")
                if row:
                    rows.append(row)
        except (httpx.HTTPError, ValueError) as ex:
            log.info("[cameras] windy lookup failed: %s", ex)
        with self._snap_lock:
            self._windy_cache[ck] = (now, rows)
            for r_ in rows:
                self._index[r_["id"]] = r_          # so the snapshot proxy can resolve them
        return rows

    def youtube_live(self, bbox: tuple[float, float, float, float]) -> list[dict]:
        """Local broadcast cams (YOUTUBE_API_KEY): live streams geotagged inside the
        view — TV-station tower cams, weather cams, harbour/beach cams. One Search
        call (100 quota units) per rounded view, cached an hour; played via the
        privacy-enhanced embed in the popup."""
        key = os.environ.get("YOUTUBE_API_KEY", "").strip()
        if not key:
            return []
        w, s, e, n = bbox
        lat, lon = (s + n) / 2, (w + e) / 2
        radius_km = max(5, min(500, int(max(n - s, (e - w) * 0.7) * 111 / 2 + 3)))
        ck = ("yt", round(lat, 1), round(lon, 1), radius_km)
        now = time.time()
        with self._snap_lock:
            hit = self._windy_cache.get(ck)
            if hit and now - hit[0] < 3600:
                return hit[1]
        rows: list[dict] = []
        try:
            r = self._client.get("https://www.googleapis.com/youtube/v3/search", params={
                "part": "snippet", "eventType": "live", "type": "video", "maxResults": 50,
                "q": "live cam|tower cam|weather cam|traffic cam|harbor cam|beach cam|webcam",
                "location": f"{lat:.4f},{lon:.4f}", "locationRadius": f"{radius_km}km", "key": key})
            r.raise_for_status()
            ids = [it["id"]["videoId"] for it in r.json().get("items", []) if it.get("id", {}).get("videoId")]
            if ids:                                          # a second call for exact coordinates
                v = self._client.get("https://www.googleapis.com/youtube/v3/videos", params={
                    "part": "snippet,recordingDetails", "id": ",".join(ids), "key": key})
                v.raise_for_status()
                for it in v.json().get("items", []):
                    loc = (it.get("recordingDetails") or {}).get("location") or {}
                    sn = it.get("snippet") or {}
                    thumb = ((sn.get("thumbnails") or {}).get("medium") or {}).get("url")
                    row = _row("youtube", it["id"], sn.get("title"), loc.get("latitude"), loc.get("longitude"),
                               state=None, roadway=sn.get("channelTitle"), image_url=thumb,
                               embed_url=f"https://www.youtube-nocookie.com/embed/{it['id']}?autoplay=1&mute=1",
                               page_url=f"https://www.youtube.com/watch?v={it['id']}", kind="broadcast")
                    if row:
                        rows.append(row)
        except (httpx.HTTPError, ValueError, KeyError) as ex:
            log.info("[cameras] youtube lookup failed: %s", ex)
        with self._snap_lock:
            self._windy_cache[ck] = (now, rows)
            for r_ in rows:
                self._index[r_["id"]] = r_
        return rows

    def query(self, bbox: tuple[float, float, float, float] | None = None,
              limit: int = 2000, source: str | None = None,
              online_only: bool = True) -> list[dict]:
        """Rows inside bbox=(w,s,e,n), evenly sampled by id hash when over `limit` so a
        zoomed-out view shows the spread of coverage rather than one vendor's block."""
        self.ensure_loaded()
        with self._lock:
            rows = [r for k, rs in self._rows.items() if not source or k == source
                    for r in rs]
        if bbox and (not source or source == "windy") and (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) < 6:
            rows = rows + self.windy(bbox)
        if bbox and (not source or source == "youtube") and (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) < 25:
            rows = rows + self.youtube_live(bbox)
        if online_only:
            rows = [r for r in rows if r["online"]]
        if bbox:
            w, s, e, n = bbox
            rows = [r for r in rows if s <= r["lat"] <= n and w <= r["lon"] <= e]
        if len(rows) > limit:
            rows.sort(key=lambda r: hashlib.blake2b(r["id"].encode(), digest_size=4).digest())
            rows = rows[:limit]
        return rows

    def get(self, cam_id: str) -> dict | None:
        with self._lock:
            return self._index.get(cam_id)

    def stats(self) -> dict:
        now = time.time()
        with self._lock:
            return {
                "sources": {k: {"rows": len(self._rows.get(k, [])),
                                "age_s": round(now - self._at[k]) if k in self._at else None,
                                "error": self._err.get(k)}
                            for k in available()},
                "total": sum(len(v) for v in self._rows.values()),
                "snapshot_cache": len(self._snap),
            }

    # -- snapshot proxy
    def snapshot(self, cam_id: str) -> tuple[bytes, str] | None:
        """Fetch (cached ~6s) the still for a registered camera. None if the camera
        is unknown or has no still; raises httpx.HTTPError on upstream failure."""
        cam = self.get(cam_id)
        if not cam or not cam.get("image_url"):
            return None
        now = time.time()
        with self._snap_lock:
            hit = self._snap.get(cam_id)
            if hit and now - hit[0] < _SNAPSHOT_TTL:
                return hit[1], hit[2]
        r = self._client.get(cam["image_url"], timeout=15.0)
        r.raise_for_status()
        ctype = r.headers.get("content-type", "").split(";")[0].strip().lower()
        if not ctype.startswith("image/"):          # some CDNs (data.gov.sg) send octet-stream
            ctype = _sniff_image(r.content) or ctype
        if not ctype.startswith("image/") or len(r.content) > _SNAPSHOT_MAX_BYTES:
            raise httpx.HTTPError(f"unexpected snapshot payload ({ctype}, {len(r.content)}B)")
        with self._snap_lock:
            self._snap[cam_id] = (now, r.content, ctype)
            if len(self._snap) > 400:            # bound memory: drop the oldest half
                for k in sorted(self._snap, key=lambda k: self._snap[k][0])[:200]:
                    self._snap.pop(k, None)
        return r.content, ctype
