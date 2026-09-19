"""Every public metric we can get for a map rectangle — the "facts" behind a box.

One call, many keyless sources, fetched in parallel with per-source timeouts so a
slow or dead upstream costs nothing but its own section. Sections:

  place       reverse geocode, Wikipedia landmarks, NWS office/zone/radar, Census
              geography (tract / county / place)                       [US parts US-only]
  population  WorldPop 2020 headcount inside the box; ACS tract+county figures
              (population, median income, income bands, age, home value, poverty)
              when CENSUS_API_KEY is set
  economy     BLS county unemployment (latest month); EPA TRI facilities in county
  terrain     elevation min/mean/max over a 3x3 grid; FEMA flood zone at center
  water       nearest NOAA tide station + latest water level; USGS river gauges in
              the box; Open-Meteo river discharge and marine (waves, SST)
  sky         cloud layers, visibility, CAPE, UV, day/night, wind; sunrise/sunset/
              daylight; sun altitude/azimuth and moon phase (computed); aircraft
              overhead (ADS-B); NASA POWER solar irradiance (7-day mean)
  air         Open-Meteo/CAMS air quality (AQI, PM2.5/10, O3, NO2, SO2, CO, dust, AOD)
  nature      iNaturalist observations last 30 days (top species, by class); GBIF
              occurrences this year; OSM trees / woodland
  activity    OSM points of interest by kind (shops, food, schools, health, safety,
              fuel), major roads; eBird notable sightings when EBIRD_API_KEY is set
  atmosphere  pressure-level winds/temps (850/700/500/250 hPa), boundary layer,
              freezing level, lifted index/CIN, snow depth, soil; nearest NWS station's
              latest observation; METARs in the box, PIREPs nearby, SIGMET/AIRMET
              overlaps; NOAA space weather (Kp, R/S/G scales, aurora probability)
  safety      city open-data crime (NYC/Chicago/LA/SF/Seattle) and 311 noise
              complaints (NYC) in the box, last 7 days by type
  health      US Drought Monitor (county), CDC wastewater SARS-CoV-2 trend (county)
  connectivity IODA internet-outage detections (county, 14 d)
  land        NLCD 2021 land cover mix sampled on the grid           [inside terrain]

Results are cached 10 minutes per rounded box. Nothing here is street-grade
truth: it is context, labelled with its source, for the explainer and the UI.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone

import httpx

log = logging.getLogger(__name__)

_UA = {"User-Agent": "Mozilla/5.0 (compatible; apb/0.1; +https://panoptes.run)"}
_client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)
_cache: dict[tuple, tuple[float, dict]] = {}
_lock = threading.Lock()
_TTL = 600.0
OVERPASS = os.environ.get("APB_OVERPASS_URL", "https://overpass.kumi.systems/api/interpreter")


def _j(url: str, **kw):
    r = _client.get(url, **kw)
    r.raise_for_status()
    return r.json()


def _num(v, nd=1):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return round(f, nd) if math.isfinite(f) else None


# ── astronomy (no library: NOAA solar equations, simple synodic moon) ─────────
def sun_position(lat: float, lon: float, when: datetime | None = None) -> dict:
    t = when or datetime.now(timezone.utc)
    jd = t.timestamp() / 86400.0 + 2440587.5
    d = jd - 2451545.0
    g = math.radians((357.529 + 0.98560028 * d) % 360)
    q = (280.459 + 0.98564736 * d) % 360
    lam = math.radians(q + 1.915 * math.sin(g) + 0.020 * math.sin(2 * g))
    eps = math.radians(23.439 - 0.00000036 * d)
    ra = math.degrees(math.atan2(math.cos(eps) * math.sin(lam), math.cos(lam))) % 360
    dec = math.asin(math.sin(eps) * math.sin(lam))
    gmst = (18.697374558 + 24.06570982441908 * d) % 24
    ha = math.radians((gmst * 15 + lon - ra + 540) % 360 - 180)
    phi = math.radians(lat)
    alt = math.asin(math.sin(phi) * math.sin(dec) + math.cos(phi) * math.cos(dec) * math.cos(ha))
    az = math.degrees(math.atan2(-math.sin(ha), math.tan(dec) * math.cos(phi) - math.sin(phi) * math.cos(ha))) % 360
    a = math.degrees(alt)
    phase = "day" if a > 0 else "civil twilight" if a > -6 else "nautical twilight" if a > -12 \
        else "astronomical twilight" if a > -18 else "night"
    return {"altitude_deg": round(a, 1), "azimuth_deg": round(az, 1), "phase": phase}


def moon_phase(when: datetime | None = None) -> dict:
    t = when or datetime.now(timezone.utc)
    days = (t.timestamp() - 947182440) / 86400.0          # new moon 2000-01-06 18:14 UTC
    age = days % 29.530588853
    frac = (1 - math.cos(2 * math.pi * age / 29.530588853)) / 2
    names = ["new", "waxing crescent", "first quarter", "waxing gibbous", "full",
             "waning gibbous", "last quarter", "waning crescent"]
    return {"age_days": round(age, 1), "illumination": round(frac, 2),
            "name": names[int((age / 29.530588853) * 8 + 0.5) % 8]}


# ── sources (each returns a dict or raises; the collector isolates failures) ──
def _place(lat, lon, span_km):
    out: dict = {}
    try:
        n = _j(f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=jsonv2&zoom=12")
        out["name"] = n.get("display_name")
        out["address"] = {k: v for k, v in (n.get("address") or {}).items()
                          if k in ("city", "town", "village", "county", "state", "country", "postcode")}
    except Exception as e:
        log.info("facts nominatim: %s", e)
    try:
        radius = int(min(10000, max(1000, span_km * 600)))
        w = _j(f"https://en.wikipedia.org/w/api.php?action=query&list=geosearch&gscoord={lat}|{lon}"
               f"&gsradius={radius}&gslimit=10&format=json")
        out["landmarks"] = [{"title": x["title"], "km": round(x["dist"] / 1000, 1)}
                            for x in w.get("query", {}).get("geosearch", [])]
    except Exception as e:
        log.info("facts wiki: %s", e)
    return out


def _us_geo(lat, lon):
    g = _j(f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={lon}&y={lat}"
           f"&benchmark=Public_AR_Current&vintage=Current_Current&format=json")
    geo = g.get("result", {}).get("geographies", {})
    out = {}
    for key, label in (("Census Tracts", "tract"), ("Counties", "county"), ("Incorporated Places", "place"),
                       ("Urban Areas", "urban_area"), ("119th Congressional Districts", "congressional_district")):
        rows = geo.get(key) or []
        if rows:
            out[label] = {"name": rows[0].get("NAME"), "geoid": rows[0].get("GEOID"),
                          "state": rows[0].get("STATE"), "county": rows[0].get("COUNTY"),
                          "tract": rows[0].get("TRACT")}
    return out


def _nws(lat, lon):
    p = _j(f"https://api.weather.gov/points/{lat},{lon}").get("properties", {})
    rel = (p.get("relativeLocation") or {}).get("properties") or {}
    return {"office": p.get("cwa"), "radar": p.get("radarStation"),
            "forecast_zone": (p.get("forecastZone") or "").rsplit("/", 1)[-1],
            "fire_zone": (p.get("fireWeatherZone") or "").rsplit("/", 1)[-1],
            "nearest": f"{rel.get('city')}, {rel.get('state')}" if rel.get("city") else None,
            "timezone": p.get("timeZone")}


def _worldpop(s, w, n, e):
    gj = json.dumps({"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {},
          "geometry": {"type": "Polygon", "coordinates": [[[w, s], [e, s], [e, n], [w, n], [w, s]]]}}]})
    tid = None
    for attempt in range(3):                       # the service drops connections under load
        try:
            # fresh connection each time: the service resets pooled keep-alive sockets
            r0 = httpx.get("https://api.worldpop.org/v1/services/stats", headers=_UA, timeout=20.0,
                           params={"dataset": "wpgppop", "year": 2020, "geojson": gj})
            r0.raise_for_status()
            tid = r0.json().get("taskid")
            break
        except httpx.HTTPError:
            if attempt == 2:
                raise
            time.sleep(0.8)
    for _ in range(7):
        time.sleep(1.4)
        r = httpx.get(f"https://api.worldpop.org/v1/tasks/{tid}", headers=_UA, timeout=15.0).json()
        if r.get("status") == "finished":
            if r.get("error"):
                raise RuntimeError(r.get("error_message"))
            pop = (r.get("data") or {}).get("total_population")
            return {"worldpop_2020_in_box": int(pop) if pop is not None else None}
    raise TimeoutError("worldpop task")


_ACS = {"B01003_001E": "population", "B19013_001E": "median_household_income_usd",
        "B01002_001E": "median_age", "B25077_001E": "median_home_value_usd",
        "B17001_002E": "below_poverty", "B23025_005E": "unemployed", "B23025_003E": "labor_force",
        "B15003_022E": "bachelors", "B15003_001E": "adults_25plus", "B25003_003E": "renter_households",
        "B25003_001E": "households"}
_BANDS = {"under_25k": ["002", "003", "004", "005"], "25k_50k": ["006", "007", "008", "009", "010"],
          "50k_100k": ["011", "012", "013"], "100k_200k": ["014", "015", "016"], "over_200k": ["017"]}


def _acs(geo, key):
    def pull(level):
        if level == "tract":
            t = geo["tract"]; where = f"for=tract:{t['tract']}&in=state:{t['state']}%20county:{t['county']}"
        else:
            c = geo["county"]; where = f"for=county:{c['county']}&in=state:{c['state']}"
        fields = ",".join(list(_ACS) + [f"B19001_{b}E" for bs in _BANDS.values() for b in bs] + ["B19001_001E"])
        rows = _j(f"https://api.census.gov/data/2023/acs/acs5?get=NAME,{fields}&{where}&key={key}")
        hdr, row = rows[0], rows[1]
        d = dict(zip(hdr, row))
        out = {"name": d.get("NAME")}
        for code, label in _ACS.items():
            out[label] = _num(d.get(code), 0)
        total = _num(d.get("B19001_001E"), 0) or 0
        if total:
            out["household_income_bands_pct"] = {
                band: round(100 * sum(_num(d.get(f"B19001_{b}E"), 0) or 0 for b in bs) / total, 1)
                for band, bs in _BANDS.items()}
        if out.get("labor_force"):
            out["unemployment_rate_pct"] = round(100 * (out.get("unemployed") or 0) / out["labor_force"], 1)
        if out.get("adults_25plus"):
            out["bachelors_or_higher_pct"] = round(100 * (out.get("bachelors") or 0) / out["adults_25plus"], 1)
        if out.get("households"):
            out["renter_pct"] = round(100 * (out.get("renter_households") or 0) / out["households"], 1)
        if out.get("population") and out.get("below_poverty") is not None:
            out["poverty_rate_pct"] = round(100 * out["below_poverty"] / out["population"], 1)
        for k in ("unemployed", "labor_force", "bachelors", "adults_25plus", "renter_households",
                  "households", "below_poverty"):
            out.pop(k, None)
        return out
    return {lvl: pull(lvl) for lvl in ("tract", "county") if lvl in geo}


def _bls(geo):
    c = geo["county"]
    sid = f"LAUCN{c['state']}{c['county']}0000000003"
    d = _j(f"https://api.bls.gov/publicAPI/v1/timeseries/data/{sid}")["Results"]["series"][0]["data"][0]
    return {"county_unemployment_rate_pct": _num(d["value"]), "period": f"{d['periodName']} {d['year']}",
            "source": "BLS LAUS"}


def _tri(geo, state_abbr):
    c = geo["county"]["name"].upper().replace(" COUNTY", "").replace(" PARISH", "")
    r = _client.get(f"https://data.epa.gov/efservice/tri_facility/state_abbr/{state_abbr}/county_name/{c}/JSON/rows/0:500")
    r.raise_for_status()
    try:
        rows = r.json()
        return {"epa_tri_facilities_in_county": len(rows)}
    except ValueError:
        return {"epa_tri_facilities_in_county": r.text.count("<tri_facility>")}


def _elevation(s, w, n, e):
    lats = [s + (n - s) * f for f in (0.1, 0.5, 0.9)]
    lons = [w + (e - w) * f for f in (0.1, 0.5, 0.9)]
    pts = [(la, lo) for la in lats for lo in lons]
    d = _j("https://api.open-meteo.com/v1/elevation", params={
        "latitude": ",".join(f"{p[0]:.4f}" for p in pts), "longitude": ",".join(f"{p[1]:.4f}" for p in pts)})
    el = [x for x in d.get("elevation", []) if x is not None]
    return {"elevation_m": {"min": round(min(el)), "mean": round(sum(el) / len(el)), "max": round(max(el))}} if el else {}


def _fema(lat, lon):
    f = _j("https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query", params={
        "geometry": f"{lon},{lat}", "geometryType": "esriGeometryPoint", "inSR": 4326,
        "outFields": "FLD_ZONE,ZONE_SUBTY", "returnGeometry": "false", "f": "json"}).get("features", [])
    zones = {"A": "1% annual-chance flood (no BFE)", "AE": "1% annual-chance flood", "AH": "shallow flooding",
             "AO": "sheet flow flooding", "VE": "coastal high-hazard (wave action)", "V": "coastal high-hazard",
             "X": "minimal / moderate flood hazard", "D": "undetermined"}
    if not f:
        return {"fema_flood_zone_at_center": "not mapped"}
    z = f[0]["attributes"].get("FLD_ZONE")
    return {"fema_flood_zone_at_center": z, "meaning": zones.get(z, ""),
            "subtype": f[0]["attributes"].get("ZONE_SUBTY")}


_tide_stations: dict = {"at": 0.0, "rows": []}


def _tides(lat, lon):
    now = time.time()
    if now - _tide_stations["at"] > 6 * 3600:
        rows = _j("https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=waterlevels").get("stations", [])
        _tide_stations.update(at=now, rows=[{"id": r["id"], "name": r["name"], "lat": r["lat"], "lng": r["lng"]} for r in rows])
    coslat = max(0.2, math.cos(math.radians(lat)))
    best = min(_tide_stations["rows"], key=lambda st: math.hypot(st["lat"] - lat, (st["lng"] - lon) * coslat), default=None)
    if not best:
        return {}
    km = math.hypot(best["lat"] - lat, (best["lng"] - lon) * coslat) * 111
    if km > 40:
        return {}
    d = _j("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter", params={
        "product": "water_level", "station": best["id"], "date": "latest", "datum": "MLLW",
        "units": "english", "time_zone": "lst_ldt", "format": "json"}).get("data", [])
    return {"nearest_tide_station": best["name"], "km": round(km, 1),
            "water_level_ft_mllw": _num(d[0]["v"], 2) if d else None, "at_local": d[0]["t"] if d else None}


def _usgs_water(s, w, n, e):
    ts = _j("https://waterservices.usgs.gov/nwis/iv/", params={
        "format": "json", "bBox": f"{w:.3f},{s:.3f},{e:.3f},{n:.3f}", "parameterCd": "00065,00060",
        "siteStatus": "active"})["value"]["timeSeries"]
    gauges: dict = {}
    for t in ts[:20]:
        name = t["sourceInfo"]["siteName"]
        var = "stage_ft" if t["variable"]["variableCode"][0]["value"] == "00065" else "discharge_cfs"
        vals = t["values"][0]["value"]
        if vals:
            gauges.setdefault(name, {})[var] = _num(vals[-1]["value"], 2)
    return {"usgs_gauges_in_box": len(gauges), "gauges": [{"site": k, **v} for k, v in list(gauges.items())[:6]]}


def _sky(lat, lon):
    d = _j("https://api.open-meteo.com/v1/forecast", params={
        "latitude": lat, "longitude": lon, "timezone": "auto",
        "current": "temperature_2m,apparent_temperature,relative_humidity_2m,cloud_cover,cloud_cover_low,"
                   "cloud_cover_mid,cloud_cover_high,visibility,cape,uv_index,is_day,wind_speed_10m,"
                   "wind_gusts_10m,wind_direction_10m,precipitation,weather_code,pressure_msl",
        "daily": "sunrise,sunset,daylight_duration,uv_index_max,precipitation_probability_max,"
                 "temperature_2m_max,temperature_2m_min", "forecast_days": 1})
    c, dy = d.get("current", {}), d.get("daily", {})
    first = lambda k: (dy.get(k) or [None])[0]
    return {"now": c, "sunrise": first("sunrise"), "sunset": first("sunset"),
            "daylight_hours": round((first("daylight_duration") or 0) / 3600, 1),
            "uv_index_max": first("uv_index_max"), "precip_probability_max_pct": first("precipitation_probability_max"),
            "temp_max_c": first("temperature_2m_max"), "temp_min_c": first("temperature_2m_min"),
            "timezone": d.get("timezone"), "sun": sun_position(lat, lon), "moon": moon_phase()}


def _air(lat, lon):
    c = _j("https://air-quality-api.open-meteo.com/v1/air-quality", params={
        "latitude": lat, "longitude": lon, "timezone": "auto",
        "current": "us_aqi,pm2_5,pm10,ozone,nitrogen_dioxide,sulphur_dioxide,carbon_monoxide,dust,"
                   "aerosol_optical_depth,uv_index"}).get("current", {})
    aqi = c.get("us_aqi")
    band = (None if aqi is None else "good" if aqi <= 50 else "moderate" if aqi <= 100 else
            "unhealthy for sensitive groups" if aqi <= 150 else "unhealthy" if aqi <= 200 else "very unhealthy/hazardous")
    return {**{k: v for k, v in c.items() if k not in ("time", "interval")}, "aqi_band": band}


def _marine(lat, lon):
    c = _j("https://marine-api.open-meteo.com/v1/marine", params={
        "latitude": lat, "longitude": lon, "current": "wave_height,wave_period,sea_surface_temperature"}).get("current", {})
    c = {k: v for k, v in c.items() if k not in ("time", "interval") and v is not None}
    return {"marine": c} if c else {}


def _flood(lat, lon):
    d = _j("https://flood-api.open-meteo.com/v1/flood", params={
        "latitude": lat, "longitude": lon, "daily": "river_discharge,river_discharge_mean", "forecast_days": 1}).get("daily", {})
    q, m = (d.get("river_discharge") or [None])[0], (d.get("river_discharge_mean") or [None])[0]
    return {"river_discharge_m3s": _num(q), "river_discharge_longterm_mean_m3s": _num(m)} if q is not None else {}


def _aircraft(lat, lon, radius_nm):
    d = _j(f"https://api.adsb.lol/v2/lat/{lat}/lon/{lon}/dist/{int(radius_nm)}")
    ac = d.get("ac") or []
    return {"aircraft_within_nm": int(radius_nm), "count": len(ac),
            "sample": [{"flight": (a.get("flight") or "").strip() or a.get("r"), "type": a.get("t"),
                        "alt_ft": a.get("alt_baro"), "gs_kt": _num(a.get("gs"), 0)} for a in ac[:6]]}


def _solar(lat, lon):
    end = datetime.now(timezone.utc)
    start = end.timestamp() - 9 * 86400
    d = _j("https://power.larc.nasa.gov/api/temporal/daily/point", params={
        "parameters": "ALLSKY_SFC_SW_DWN", "community": "RE", "longitude": lon, "latitude": lat,
        "start": datetime.fromtimestamp(start, timezone.utc).strftime("%Y%m%d"), "end": end.strftime("%Y%m%d"),
        "format": "JSON"})["properties"]["parameter"]["ALLSKY_SFC_SW_DWN"]
    vals = [v for v in d.values() if v is not None and v > -900]
    return {"solar_irradiance_kwh_m2_day_recent_mean": round(sum(vals) / len(vals), 2), "days": len(vals)} if vals else {}


def _inat(s, w, n, e):
    since = datetime.fromtimestamp(time.time() - 30 * 86400, timezone.utc).strftime("%Y-%m-%d")
    d = _j("https://api.inaturalist.org/v1/observations", params={
        "nelat": n, "nelng": e, "swlat": s, "swlng": w, "d1": since, "per_page": 60, "order_by": "observed_on"})
    by_class: dict = {}
    species: dict = {}
    for o in d.get("results", []):
        t = o.get("taxon") or {}
        by_class[t.get("iconic_taxon_name") or "other"] = by_class.get(t.get("iconic_taxon_name") or "other", 0) + 1
        name = t.get("preferred_common_name") or t.get("name")
        if name:
            species[name] = species.get(name, 0) + 1
    return {"inaturalist_obs_30d": d.get("total_results"), "by_class_sampled": by_class,
            "top_species": [k for k, _ in sorted(species.items(), key=lambda kv: -kv[1])[:8]]}


def _gbif(s, w, n, e):
    y = datetime.now(timezone.utc).year
    d = _j("https://api.gbif.org/v1/occurrence/search", params={
        "decimalLatitude": f"{s},{n}", "decimalLongitude": f"{w},{e}", "year": y, "limit": 0})
    return {"gbif_occurrences_this_year": d.get("count")}


def _overpass(s, w, n, e):
    box = f"({s},{w},{n},{e})"
    groups = {"shops": "node[shop]", "food_drink": "node[amenity~'restaurant|cafe|bar|pub|fast_food']",
              "schools": "node[amenity~'school|college|university']", "health": "node[amenity~'hospital|clinic|doctors|pharmacy']",
              "public_safety": "node[amenity~'police|fire_station']", "fuel_charging": "node[amenity~'fuel|charging_station']",
              "trees_mapped": "node[natural=tree]", "major_roads": "way[highway~'motorway|trunk|primary']"}
    out = {}
    q = "[out:json][timeout:12];" + "".join(f"{sel}{box}->.{k};" for k, sel in groups.items())
    # one count per group: Overpass emits a `count` element per `out count`
    q += "".join(f".{k} out count;" for k in groups)
    r = _client.post(OVERPASS, data={"data": q}, headers={"Accept": "application/json"}, timeout=16.0)
    r.raise_for_status()
    els = [x for x in r.json().get("elements", []) if x.get("type") == "count"]
    for k, el in zip(groups, els):
        out[k] = int(el.get("tags", {}).get("total", 0))
    return {"osm": out}


def _ebird(lat, lon, key):
    d = _j(f"https://api.ebird.org/v2/data/obs/geo/recent/notable?lat={lat}&lng={lon}&dist=25&back=7",
           headers={"X-eBirdApiToken": key})
    return {"ebird_notable_7d": [{"species": x.get("comName"), "n": x.get("howMany"), "where": x.get("locName")}
                                 for x in d[:8]], "ebird_notable_count": len(d)}


# ── atmosphere by level, aviation, space weather ─────────────────────────────
def _levels(lat, lon):
    """Open-Meteo pressure-level fields: the vertical structure over the box."""
    d = _j("https://api.open-meteo.com/v1/forecast", params={
        "latitude": lat, "longitude": lon, "timezone": "UTC", "forecast_hours": 1,
        "hourly": ("temperature_850hPa,wind_speed_850hPa,wind_direction_850hPa,temperature_700hPa,"
                   "wind_speed_500hPa,wind_direction_500hPa,geopotential_height_500hPa,wind_speed_250hPa,"
                   "boundary_layer_height,freezing_level_height,lifted_index,convective_inhibition,"
                   "shortwave_radiation,snow_depth,soil_temperature_0cm,soil_moisture_0_to_1cm")})
    h = d.get("hourly") or {}
    first = lambda k: (h.get(k) or [None])[0]
    return {"levels": {
        "850hPa": {"temp_c": first("temperature_850hPa"), "wind_kmh": first("wind_speed_850hPa"), "wind_dir": first("wind_direction_850hPa")},
        "700hPa": {"temp_c": first("temperature_700hPa")},
        "500hPa": {"wind_kmh": first("wind_speed_500hPa"), "wind_dir": first("wind_direction_500hPa"), "height_m": first("geopotential_height_500hPa")},
        "250hPa_jet_kmh": first("wind_speed_250hPa")},
        "boundary_layer_m": first("boundary_layer_height"), "freezing_level_m": first("freezing_level_height"),
        "lifted_index": first("lifted_index"), "cin": first("convective_inhibition"),
        "shortwave_w_m2": first("shortwave_radiation"), "snow_depth_m": first("snow_depth"),
        "soil_temp_0cm_c": first("soil_temperature_0cm"), "soil_moisture_0_1cm": first("soil_moisture_0_to_1cm")}


def _nws_obs(lat, lon):
    """Nearest NWS/ASOS station's latest observation — measured, not modelled."""
    st = _j(f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}/stations",
            headers={"Accept": "application/geo+json"}).get("features") or []
    if not st:
        return {}
    sid, name = st[0]["properties"]["stationIdentifier"], st[0]["properties"]["name"]
    o = _j(f"https://api.weather.gov/stations/{sid}/observations/latest",
           headers={"Accept": "application/geo+json"}).get("properties") or {}
    v = lambda k: (o.get(k) or {}).get("value")
    return {"station": f"{sid} {name}", "observed": o.get("timestamp"), "sky": o.get("textDescription"),
            "temp_c": _num(v("temperature")), "dewpoint_c": _num(v("dewpoint")), "rh_pct": _num(v("relativeHumidity"), 0),
            "wind_kmh": _num(v("windSpeed")), "wind_dir": _num(v("windDirection"), 0), "gust_kmh": _num(v("windGust")),
            "pressure_hpa": _num((v("barometricPressure") or 0) / 100) if v("barometricPressure") else None,
            "visibility_km": _num((v("visibility") or 0) / 1000) if v("visibility") else None,
            "cloud_layers": [f"{c.get('amount')} {int(((c.get('base') or {}).get('value') or 0))}m" for c in (o.get("cloudLayers") or [])][:4]}


def _aviation(s, w, n, e):
    """METARs inside the box (flight category), PIREPs within a degree (turbulence /
    icing reports from aircraft) and SIGMET/AIRMET polygons that overlap the box."""
    out: dict = {}
    rm = _client.get(f"https://aviationweather.gov/api/data/metar?bbox={s},{w},{n},{e}&format=json")
    m = rm.json() if rm.status_code == 200 and rm.text.strip() else []      # 204 = no stations in box
    if isinstance(m, list) and m:
        out["metars"] = [{"station": x.get("icaoId"), "category": x.get("fltCat"), "raw": x.get("rawOb", "")[:90]} for x in m[:6]]
    try:
        p = _j(f"https://aviationweather.gov/api/data/pirep?bbox={s - 1},{w - 1},{n + 1},{e + 1}&format=json&age=3")
    except (httpx.HTTPError, ValueError):
        p = []
    if isinstance(p, list) and p:
        out["pireps_3h"] = len(p)
        out["pirep_samples"] = [{"ac": x.get("acType"), "fl": x.get("fltLvl"), "turbulence": (x.get("tbInt1") or "") + " " + (x.get("tbType1") or ""),
                                 "icing": x.get("icgInt1") or ""} for x in p[:4]]
    try:
        sig = _j("https://aviationweather.gov/api/data/airsigmet?format=json")
    except (httpx.HTTPError, ValueError):
        sig = []
    hits = []
    for x in sig if isinstance(sig, list) else []:
        cs = x.get("coords") or []
        if not cs:
            continue
        lats, lons = [c["lat"] for c in cs], [c["lon"] for c in cs]
        if min(lats) <= n and max(lats) >= s and min(lons) <= e and max(lons) >= w:
            hits.append({"type": x.get("airSigmetType"), "hazard": x.get("hazard"), "severity": x.get("severity"),
                         "alt_hi_ft": x.get("altitudeHi1")})
    if hits:
        out["sigmets_airmets_over_box"] = hits[:5]
    return out


_aurora_cache: dict = {"at": 0.0, "grid": None}


def _space(lat, lon):
    kp = _j("https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json")
    sc = _j("https://services.swpc.noaa.gov/products/noaa-scales.json").get("0") or {}
    out = {"kp_now": (kp[-1] or {}).get("Kp") if kp else None,
           "noaa_scales": {k: f"{k}{(sc.get(k) or {}).get('Scale')} {(sc.get(k) or {}).get('Text')}" for k in ("R", "S", "G") if sc.get(k)}}
    now = time.time()
    if now - _aurora_cache["at"] > 1800:
        try:
            _aurora_cache.update(at=now, grid=_j("https://services.swpc.noaa.gov/json/ovation_aurora_latest.json").get("coordinates"))
        except (httpx.HTTPError, ValueError):
            pass
    g = _aurora_cache["grid"]
    if g:
        glon, glat = int(round(lon)) % 360, int(round(lat))
        prob = next((c[2] for c in g if c[0] == glon and c[1] == glat), None)
        out["aurora_probability_pct"] = prob
    return out


# ── safety: city open-data crime + 311 noise, last 7 days in the box ─────────
# (dataset, location clause builder, date column, type column, city bbox s,w,n,e)
_CRIME = {
    "New York City": ("data.cityofnewyork.us", "5uac-w243", lambda s, w, n, e: f"within_box(lat_lon,{n},{w},{s},{e})", "cmplnt_fr_dt", "ofns_desc", (40.49, -74.27, 40.92, -73.68)),
    "Chicago": ("data.cityofchicago.org", "ijzp-q8t2", lambda s, w, n, e: f"within_box(location,{n},{w},{s},{e})", "date", "primary_type", (41.64, -87.94, 42.02, -87.52)),
    "Los Angeles": ("data.lacity.org", "2nrs-mtv8", lambda s, w, n, e: f"lat between {s} and {n} AND lon between {w} and {e}", "date_occ", "crm_cd_desc", (33.70, -118.67, 34.34, -118.15)),
    "San Francisco": ("data.sfgov.org", "wg3w-h783", lambda s, w, n, e: f"latitude between {s} and {n} AND longitude between {w} and {e}", "incident_datetime", "incident_category", (37.70, -122.52, 37.84, -122.35)),
    "Seattle": ("data.seattle.gov", "tazs-3rd5", lambda s, w, n, e: f"latitude between {s} and {n} AND longitude between {w} and {e}", "offense_date", "offense_category", (47.49, -122.44, 47.74, -122.23)),
}
_NOISE = {
    "New York City": ("data.cityofnewyork.us", "erm2-nwe9", lambda s, w, n, e: f"within_box(location,{n},{w},{s},{e}) AND complaint_type like 'Noise%'", "created_date", "descriptor", (40.49, -74.27, 40.92, -73.68)),
}


def _socrata_counts(spec, s, w, n, e, days=7):
    """Counts by type over the `days` before the feed's freshest record (city feeds
    can lag weeks), so the window is always populated and its dates are stated."""
    host, ds, where, datecol, typecol, (cs, cw, cn, ce) = spec
    if n < cs or s > cn or e < cw or w > ce:
        return None
    latest = (_j(f"https://{host}/resource/{ds}.json", params={"$select": f"max({datecol}) as latest"}, timeout=30.0) or [{}])[0].get("latest")
    end = datetime.fromisoformat(latest[:19]) if latest else datetime.now(timezone.utc).replace(tzinfo=None)
    since = (end.timestamp() - days * 86400)
    since_s = datetime.utcfromtimestamp(since).strftime("%Y-%m-%dT00:00:00")
    q = f"{where(s, w, n, e)} AND {datecol} > '{since_s}'"
    rows = _j(f"https://{host}/resource/{ds}.json", params={
        "$select": f"{typecol} as type, count(*) as n", "$where": q, "$group": typecol, "$order": "n DESC", "$limit": 8}, timeout=40.0)
    total = sum(int(r["n"]) for r in rows)
    return {f"total_{days}d": total, "through": (latest or "")[:10], "by_type": {r["type"]: int(r["n"]) for r in rows}}


def _crime(s, w, n, e):
    for city, spec in _CRIME.items():
        r = _socrata_counts(spec, s, w, n, e)
        if r is not None:
            return {"crime": {"city_feed": city, **r}}
    return {}


def _noise(s, w, n, e):
    for city, spec in _NOISE.items():
        r = _socrata_counts(spec, s, w, n, e)
        if r is not None:
            return {"noise_complaints_311": {"city_feed": city, **r}}
    return {}


# ── health, land, connectivity ───────────────────────────────────────────────
def _drought(geo):
    c = geo["county"]
    end = datetime.now(timezone.utc)
    start = datetime.fromtimestamp(end.timestamp() - 30 * 86400, timezone.utc)
    rows = _j("https://usdmdataservices.unl.edu/api/CountyStatistics/GetDroughtSeverityStatisticsByAreaPercent", params={
        "aoi": c["geoid"], "startdate": start.strftime("%m/%d/%Y"), "enddate": end.strftime("%m/%d/%Y"), "statisticsType": 1},
        headers={"Accept": "application/json"})            # the service answers CSV unless asked for JSON
    if not rows:
        return {}
    r = rows[-1]
    worst = next((k.upper() for k in ("d4", "d3", "d2", "d1", "d0") if float(r.get(k) or 0) > 0), "none")
    return {"drought_monitor": {"week": (r.get("mapDate") or "")[:10], "worst_class": worst,
                                "pct_in_drought_d1_plus": round(sum(float(r.get(k) or 0) for k in ("d1", "d2", "d3", "d4")), 1)}}


def _cdc_ww(geo):
    name = (geo["county"]["name"] or "").replace(" County", "").replace(" Parish", "")
    rows = _j("https://data.cdc.gov/resource/2ew6-ywp6.json", params={
        "$where": f"county_fips='{geo['county']['geoid']}'", "$order": "date_end DESC", "$limit": 3})
    if not rows:
        return {}
    r = rows[0]
    return {"wastewater_sarscov2": {"site": r.get("wwtp_jurisdiction"), "through": r.get("date_end"),
                                    "change_15d_pct": _num(r.get("ptc_15d"), 0), "percentile": _num(r.get("percentile"), 0),
                                    "population_served": r.get("population_served")}}


_NLCD = {11: "open water", 12: "perennial ice/snow", 21: "developed, open space", 22: "developed, low intensity",
         23: "developed, medium intensity", 24: "developed, high intensity", 31: "barren land", 41: "deciduous forest",
         42: "evergreen forest", 43: "mixed forest", 52: "shrub/scrub", 71: "grassland", 81: "pasture/hay",
         82: "cultivated crops", 90: "woody wetlands", 95: "emergent herbaceous wetlands"}


def _nlcd(s, w, n, e):
    """NLCD land cover sampled on the 3x3 grid (WMS GetFeatureInfo; class = palette index)."""
    counts: dict = {}
    for la, lo in [(s + (n - s) * f, w + (e - w) * g_) for f in (.2, .5, .8) for g_ in (.2, .5, .8)]:
        try:
            r = _j("https://www.mrlc.gov/geoserver/mrlc_display/NLCD_2021_Land_Cover_L48/wms", params={
                "SERVICE": "WMS", "VERSION": "1.1.1", "REQUEST": "GetFeatureInfo", "LAYERS": "NLCD_2021_Land_Cover_L48",
                "QUERY_LAYERS": "NLCD_2021_Land_Cover_L48", "SRS": "EPSG:4326", "BBOX": f"{lo - .002},{la - .002},{lo + .002},{la + .002}",
                "WIDTH": 21, "HEIGHT": 21, "X": 10, "Y": 10, "INFO_FORMAT": "application/json"}, timeout=8.0)
            idx = ((r.get("features") or [{}])[0].get("properties") or {}).get("PALETTE_INDEX")
            if idx is not None:
                k = _NLCD.get(int(idx), f"class {idx}")
                counts[k] = counts.get(k, 0) + 1
        except (httpx.HTTPError, ValueError):
            continue
    if not counts:
        return {}
    total = sum(counts.values())
    return {"land_cover_nlcd_2021_pct": {k: round(100 * v / total) for k, v in sorted(counts.items(), key=lambda kv: -kv[1])}}


def _ioda(geo):
    now = int(time.time())
    d = _j("https://api.ioda.inetintel.cc.gatech.edu/v2/outages/summary", params={
        "entityType": "county", "entityCode": geo["county"]["geoid"], "from": now - 14 * 86400, "until": now})
    rows = d.get("data") or []
    return {"internet_outages_14d": len(rows), "latest": [{"score": r.get("score"), "level": r.get("level")} for r in rows[:3]]} if rows else {"internet_outages_14d": 0}


_FIPS = {"01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT", "10": "DE",
         "11": "DC", "12": "FL", "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN", "19": "IA",
         "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
         "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH", "34": "NJ", "35": "NM",
         "36": "NY", "37": "NC", "38": "ND", "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
         "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
         "54": "WV", "55": "WI", "56": "WY", "72": "PR"}


# ── collector ────────────────────────────────────────────────────────────────
def _tasks(bounds: dict):
    s, n = float(bounds["south"]), float(bounds["north"])
    w, e = float(bounds["west"]), float(bounds["east"])
    lat, lon = (s + n) / 2, (w + e) / 2
    span_km = max(n - s, (e - w) * max(0.2, math.cos(math.radians(lat)))) * 111
    us = -170 <= lon <= -50 and 17 <= lat <= 72
    census_key = os.environ.get("CENSUS_API_KEY", "").strip()
    ebird_key = os.environ.get("EBIRD_API_KEY", "").strip()
    tasks = [
        ("place", "geocode", _place, lat, lon, span_km),
        ("sky", "conditions", _sky, lat, lon),
        ("air", "quality", _air, lat, lon),
        ("terrain", "elevation", _elevation, s, w, n, e),
        ("sky", "aircraft", _aircraft, lat, lon, max(5, min(40, span_km))),
        ("water", "marine", _marine, lat, lon),
        ("water", "flood", _flood, lat, lon),
        ("nature", "gbif", _gbif, s, w, n, e),
        ("nature", "inaturalist", _inat, s, w, n, e),
        ("sky", "solar", _solar, lat, lon),
        ("activity", "overpass", _overpass, s, w, n, e),
        ("population", "worldpop", _worldpop, s, w, n, e),
    ]
    tasks += [("atmosphere", "levels", _levels, lat, lon),
              ("atmosphere", "space_weather", _space, lat, lon)]
    if us:
        tasks += [("place", "nws", _nws, lat, lon), ("terrain", "fema", _fema, lat, lon),
                  ("water", "tides", _tides, lat, lon), ("water", "usgs", _usgs_water, s, w, n, e),
                  ("place", "us_geo", _us_geo, lat, lon),
                  ("atmosphere", "station_obs", _nws_obs, lat, lon),
                  ("atmosphere", "aviation", _aviation, s, w, n, e),
                  ("safety", "crime", _crime, s, w, n, e), ("safety", "noise", _noise, s, w, n, e),
                  ("terrain", "land_cover", _nlcd, s, w, n, e)]
    if ebird_key:
        tasks.append(("activity", "ebird", _ebird, lat, lon, ebird_key))
    meta = {"bounds": {"south": s, "north": n, "west": w, "east": e},
            "center": {"lat": round(lat, 4), "lon": round(lon, 4)}, "span_km": round(span_km, 1),
            "keyed_available": {"CENSUS_API_KEY": bool(census_key), "EBIRD_API_KEY": bool(ebird_key)}}
    return tasks, meta, census_key


def facts_stream(bounds: dict, timeout: float = 15.0):
    """Yield events as each source answers — fastest first. Events:
    {"meta":...} once, then {"section","key","data"} per source, then {"done":true,
    "failed":[...]} . US Census-dependent metrics (BLS, TRI, ACS) start once the
    geocoder returns. A finished result is cached whole for 10 minutes."""
    key = tuple(round(float(bounds[k]), 3) for k in ("south", "north", "west", "east"))
    now = time.time()
    with _lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < _TTL:                 # replay a cached result instantly
        yield {"meta": {k: v for k, v in hit[1].items() if k not in ("sections", "sources_failed")}}
        for section, data in hit[1]["sections"].items():
            yield {"section": section, "key": "cached", "data": data}
        yield {"done": True, "failed": hit[1].get("sources_failed", []), "cached": True}
        return
    tasks, meta, census_key = _tasks(bounds)
    yield {"meta": meta}
    result = {**meta, "sections": {}, "sources_failed": []}
    ex = ThreadPoolExecutor(max_workers=22)
    pending = {ex.submit(t[2], *t[3:]): t for t in tasks}
    deadline = now + timeout

    def run_us_dependents(geo):
        if "county" in geo:
            pending[ex.submit(_bls, geo)] = ("economy", "bls")
            pending[ex.submit(_drought, geo)] = ("health", "drought")
            pending[ex.submit(_cdc_ww, geo)] = ("health", "wastewater")
            pending[ex.submit(_ioda, geo)] = ("connectivity", "ioda")
            if geo["county"].get("state") in _FIPS:
                pending[ex.submit(_tri, geo, _FIPS[geo["county"]["state"]])] = ("economy", "tri")
            if census_key and "tract" in geo:
                pending[ex.submit(_acs, geo, census_key)] = ("population", "acs")

    while pending and time.time() < deadline:
        done, _ = wait(list(pending), timeout=max(0.05, deadline - time.time()), return_when="FIRST_COMPLETED")
        for f in done:
            section, k = pending.pop(f)[:2]
            try:
                data = f.result()
            except Exception as e_:
                result["sources_failed"].append(f"{section}.{k}: {type(e_).__name__}")
                yield {"section": section, "key": k, "error": type(e_).__name__}
                continue
            if k == "us_geo":
                run_us_dependents(data)
                data = {"census_geography": {kk: v.get("name") for kk, v in data.items()}}
            if data:
                result["sections"].setdefault(section, {}).update(data)
                yield {"section": section, "key": k, "data": data}
    for f, t in pending.items():
        result["sources_failed"].append(f"{t[0]}.{t[1]}: timeout")
    ex.shutdown(wait=False)
    result["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with _lock:
        _cache[key] = (now, result)
        if len(_cache) > 200:
            for kk in sorted(_cache, key=lambda kk: _cache[kk][0])[:100]:
                _cache.pop(kk, None)
    yield {"done": True, "failed": result["sources_failed"]}


def facts(bounds: dict, timeout: float = 15.0) -> dict:
    """Collected form of facts_stream (what the explainer and the JSON endpoint use)."""
    out: dict = {"sections": {}, "sources_failed": []}
    for ev in facts_stream(bounds, timeout):
        if "meta" in ev:
            out.update(ev["meta"])
        elif "done" in ev:
            out["sources_failed"] = ev.get("failed", [])
        elif "data" in ev:
            out["sections"].setdefault(ev["section"], {}).update(ev["data"])
    return out


def digest(f: dict, limit: int = 3500) -> str:
    """Compact JSON for the model prompt (sections only)."""
    return json.dumps(f.get("sections", {}), ensure_ascii=False, default=str)[:limit]
