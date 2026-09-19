"""More keyless facts lanes — infrastructure, radio, compliance, climate normals, attention.

Every function here follows the facts.py contract: takes what `_tasks` hands it, returns
a dict (empty means "nothing here"), raises on failure so the stream marks the source.
All sources were verified keyless on 2026-09-19; see SOURCES.md for the table.

  HIFLD mirror (services5/HDRa0B57OVrv2E1q)  critical-infrastructure and FCC tower
                                              layers as ArcGIS feature services
  EPA ECHO                                    regulated facilities, violators, penalties
  BLS QCEW                                    county establishments / jobs / wages, YoY
  CDC PLACES                                  county health outcomes (model estimates)
  NOAA RCC ACIS                               station observations vs 30-yr normals
  NASA POWER                                  monthly climatology (solar / temp / precip)
  USGS design maps                            seismic design PGA + category
  NPS NRHP                                    historic places in the box
  NRCS AWDB                                   SNOTEL snowpack (mountain West)
  FEMA OpenFEMA                               county disaster-declaration history
  NWS alerts                                  active alerts at the box centre
  Wikimedia pageviews                         attention on the place's article
"""
from __future__ import annotations

import csv
import io
import json
import logging
import statistics
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import httpx

log = logging.getLogger(__name__)
_UA = {"User-Agent": "Panoptes/1.0 (https://panoptes.run; ops@panoptes.run)",
       "Api-User-Agent": "Panoptes/1.0 (https://panoptes.run; ops@panoptes.run)"}
_client = httpx.Client(timeout=12.0, headers=_UA, follow_redirects=True)
_HIFLD = "https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services"
_POWER = "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/Power_Plants_in_the_US/FeatureServer/0/query"
_HOSP = "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/Medicare_Hospitals/FeatureServer/0/query"
_NRHP = "https://mapservices.nps.gov/arcgis/rest/services/cultural_resources/nrhp_locations/MapServer/0/query"


def _env(s, w, n, e):
    return json.dumps({"xmin": w, "ymin": s, "xmax": e, "ymax": n, "spatialReference": {"wkid": 4326}})


def _arc(url, s, w, n, e, count_only=True, fields="*", limit=8, where="1=1"):
    p = {"where": where, "geometry": _env(s, w, n, e), "geometryType": "esriGeometryEnvelope", "inSR": 4326,
         "spatialRel": "esriSpatialRelIntersects", "f": "json"}
    if count_only:
        p["returnCountOnly"] = "true"
    else:
        p.update({"outFields": fields, "resultRecordCount": limit, "returnGeometry": "false"})
    r = _client.get(url, params=p)
    r.raise_for_status()
    j = r.json()
    if "error" in j:
        raise RuntimeError(j["error"].get("message", "arcgis error"))
    return j.get("count", 0) if count_only else [f.get("attributes", {}) for f in j.get("features", [])]


# ── infrastructure: what is physically in the box ──
_INFRA = [("fire_stations", "Fire_Stations"), ("law_enforcement", "Local_Law_Enforcement_Locations"),
          ("electric_substations", "Electric_Substations"), ("urgent_care", "Urgent_Care_Facilities"),
          ("colleges", "Colleges_and_Universities"), ("public_schools", "Public_Schools"),
          ("child_care_centers", "Child_Care_Centers"), ("nursing_homes", "Nursing_Homes"),
          ("mobile_home_parks", "Mobile_Home_Parks"), ("prisons", "Prison_Boundaries"),
          ("gas_pipelines", "Natural_Gas_Pipelines")]


def infrastructure(s, w, n, e):
    out: dict = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_arc, f"{_HIFLD}/{svc}/FeatureServer/0/query", s, w, n, e): key for key, svc in _INFRA}
        futs[ex.submit(_arc, _HOSP, s, w, n, e)] = "hospitals_medicare"
        futs[ex.submit(_arc, _POWER, s, w, n, e, False, "Plant_Name,PrimSource,Total_MW", 6)] = "power_plants"
        for f, key in futs.items():
            try:
                v = f.result()
            except Exception as ex_:
                log.info("facts infra %s: %s", key, ex_)
                continue
            if key == "power_plants":
                if v:
                    out["power_plants"] = [{"name": p.get("Plant_Name"), "source": p.get("PrimSource"), "mw": p.get("Total_MW")} for p in v]
                    out["power_capacity_mw"] = round(sum(float(p.get("Total_MW") or 0) for p in v), 1)
            elif v:
                out[key] = v
    if out:
        out["infrastructure_source"] = "HIFLD / EIA / CMS"
    return out


# ── radio: licensed transmitters (FCC via HIFLD) ──
def radio(s, w, n, e):
    out: dict = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        fm = ex.submit(_arc, f"{_HIFLD}/FM_Transmission_Towers/FeatureServer/0/query", s, w, n, e, False, "CALLSIGN,FREQUENCY,LICENSEE,CLASS", 10)
        am = ex.submit(_arc, f"{_HIFLD}/AM_Transmission_Towers/FeatureServer/0/query", s, w, n, e, False, "CALLSIGN,FREQUENCY,LICENSEE", 6)
        lm = ex.submit(_arc, f"{_HIFLD}/Land_Mobile_Commercial_Transmission_Towers/FeatureServer/0/query", s, w, n, e, False, "LICENSEE,CALLSIGN,STRUCTYPE", 60)
        counts = {k: ex.submit(_arc, f"{_HIFLD}/{svc}/FeatureServer/0/query", s, w, n, e) for k, svc in
                  (("cellular_towers", "Cellular_Towers"), ("microwave_towers", "Microwave_Service_Towers"),
                   ("paging_towers", "Paging_Transmission_Towers"), ("tv_transmitters", "TV_Digital_Station_Transmitters"))}
        try:
            rows = fm.result()
            if rows:
                out["fm_stations"] = [f"{r.get('CALLSIGN')} {r.get('FREQUENCY')}" + (f" ({r['LICENSEE'][:40]})" if r.get("LICENSEE") else "") for r in rows]
        except Exception as ex_:
            log.info("facts radio fm: %s", ex_)
        try:
            rows = am.result()
            if rows:
                out["am_stations"] = [f"{r.get('CALLSIGN')} {r.get('FREQUENCY')}" for r in rows]
        except Exception as ex_:
            log.info("facts radio am: %s", ex_)
        try:
            rows = lm.result()
            if rows:
                lic: dict[str, int] = {}
                for r in rows:
                    k = (r.get("LICENSEE") or "unknown").strip()[:50]
                    lic[k] = lic.get(k, 0) + 1
                out["land_mobile_sites"] = len(rows)
                out["land_mobile_licensees"] = [f"{k} ×{v}" for k, v in sorted(lic.items(), key=lambda kv: -kv[1])[:8]]
        except Exception as ex_:
            log.info("facts radio lm: %s", ex_)
        for k, f in counts.items():
            try:
                v = f.result()
                if v:
                    out[k] = v
            except Exception as ex_:
                log.info("facts radio %s: %s", k, ex_)
    if out:
        out["radio_source"] = "FCC licensing via HIFLD"
    return out


# ── EPA ECHO: compliance in the box ──
def echo(s, w, n, e):
    r = _client.get("https://echodata.epa.gov/echo/echo_rest_services.get_facilities",
                    params={"output": "JSON", "p_c1lat": s, "p_c1lon": w, "p_c2lat": n, "p_c2lon": e})
    r.raise_for_status()
    res = (r.json() or {}).get("Results") or {}
    if res.get("Message") != "Success":
        raise RuntimeError(res.get("Message") or "echo")
    def n_(k):
        try:
            return int(res.get(k) or 0)
        except ValueError:
            return 0
    return {"regulated_facilities": n_("QueryRows"), "significant_violators": n_("SVRows"),
            "current_violations": n_("CVRows"), "formal_actions_5y": n_("FEARows"), "inspections_5y": n_("INSPRows"),
            "penalties_5y": res.get("TotalPenalties"), "clean_air_act": n_("CAARows"), "clean_water_act": n_("CWARows"),
            "hazardous_waste_rcra": n_("RCRRows"), "toxics_release_inventory": n_("TRIRows"), "compliance_source": "EPA ECHO (box)"}


# ── BLS QCEW: county employment / wages ──
def qcew(geo):
    c = geo["county"]
    fips = f"{c['state']}{c['county']}"
    today = date.today()
    last = None
    for back in range(2, 6):                                 # latest published quarter (~5-month lag)
        y, q = (today.year, (today.month - 1) // 3 + 1 - back)
        while q < 1:
            y, q = y - 1, q + 4
        r = _client.get(f"https://data.bls.gov/cew/data/api/{y}/{q}/area/{fips}.csv")
        if r.status_code == 200 and r.text.startswith('"area_fips"'):
            last = r.text
            break
    if not last:
        return {}
    rows = [x for x in csv.DictReader(io.StringIO(last)) if x["own_code"] == "0" and x["agglvl_code"] == "70"]
    if not rows:
        return {}
    x = rows[0]
    f = lambda k: float(x[k]) if x.get(k) not in (None, "") else None
    return {"establishments": f("qtrly_estabs"), "employment": f("month3_emplvl"), "avg_weekly_wage_usd": f("avg_wkly_wage"),
            "employment_yoy_pct": f("oty_month3_emplvl_pct_chg"), "wage_yoy_pct": f("oty_avg_wkly_wage_pct_chg"),
            "establishments_yoy_pct": f("oty_qtrly_estabs_pct_chg"), "period": f"{x['year']} Q{x['qtr']}", "qcew_source": "BLS QCEW (county)"}


# ── CDC PLACES: county health outcomes ──
_PLACES = {"OBESITY": "obesity_pct", "DEPRESSION": "depression_pct", "MHLTH": "poor_mental_health_14d_pct", "BPHIGH": "high_blood_pressure_pct",
           "SLEEP": "short_sleep_pct", "CASTHMA": "asthma_pct", "BINGE": "binge_drinking_pct", "CSMOKING": "smoking_pct", "LPA": "no_leisure_activity_pct"}


def cdc_places(geo, state_abbr):
    name = geo["county"]["name"]
    for suffix in (" County", " Parish", " Borough", " Municipio", " city"):
        name = name.replace(suffix, "")
    r = _client.get("https://data.cdc.gov/resource/swc5-untb.json",
                    params={"stateabbr": state_abbr, "locationname": name, "$select": "measureid,data_value,year",
                            "$where": "measureid in(" + ",".join(f"'{k}'" for k in _PLACES) + ")", "$limit": 20})
    r.raise_for_status()
    out = {}
    year = None
    for row in r.json():
        k = _PLACES.get(row.get("measureid"))
        if k and row.get("data_value") not in (None, ""):
            out[k] = float(row["data_value"])
            year = row.get("year") or year
    if out:
        out["year"] = year
        out["places_source"] = "CDC PLACES (county model estimates, adults)"
    return out


# ── ACIS: recent observations vs 30-year normals (US) ──
def acis(s, w, n, e):
    pad = 0.15
    r = _client.post("https://data.rcc-acis.org/StnMeta", json={"bbox": f"{w - pad},{s - pad},{e + pad},{n + pad}",
                                                                "elems": "maxt,mint,pcpn", "meta": "name,sids,ll,valid_daterange"})
    r.raise_for_status()
    stns = r.json().get("meta") or []
    recent = (date.today() - timedelta(days=10)).isoformat()
    dr_ = lambda x, i: ((x.get("valid_daterange") or [[], [], []]) + [[], [], []])[i]
    stns = [x for x in stns if dr_(x, 0) and dr_(x, 0)[1] >= recent]      # temperature reporters only (CoCoRaHS is rain-only)
    if not stns:
        return {}
    lat, lon = (s + n) / 2, (w + e) / 2
    stns.sort(key=lambda x: (x["ll"][1] - lat) ** 2 + (x["ll"][0] - lon) ** 2)
    st = stns[0]
    ed = date.today() - timedelta(days=1)
    r = _client.post("https://data.rcc-acis.org/StnData", json={
        "sid": st["sids"][0].split(" ")[0], "sdate": (ed - timedelta(days=6)).isoformat(), "edate": ed.isoformat(),
        "elems": [{"name": "maxt"}, {"name": "mint"}, {"name": "pcpn"}, {"name": "maxt", "normal": "1"}, {"name": "mint", "normal": "1"},
                  {"name": "pcpn", "normal": "1"}]})
    r.raise_for_status()
    data = r.json().get("data") or []
    f = lambda v: float(v) if v not in ("M", "T", None, "") else (0.0 if v == "T" else None)
    hi = [f(d[1]) for d in data if f(d[1]) is not None]
    lo = [f(d[2]) for d in data if f(d[2]) is not None]
    pc = [f(d[3]) for d in data if f(d[3]) is not None]
    nhi = [f(d[4]) for d in data if f(d[4]) is not None]
    nlo = [f(d[5]) for d in data if f(d[5]) is not None]
    npc = [f(d[6]) for d in data if f(d[6]) is not None]
    if not hi or not nhi:
        return {}
    out = {"station": st.get("name"), "days": len(data), "mean_high_f": round(statistics.mean(hi), 1),
           "normal_high_f": round(statistics.mean(nhi), 1), "high_anomaly_f": round(statistics.mean(hi) - statistics.mean(nhi), 1)}
    if lo and nlo:
        out.update({"mean_low_f": round(statistics.mean(lo), 1), "normal_low_f": round(statistics.mean(nlo), 1),
                    "low_anomaly_f": round(statistics.mean(lo) - statistics.mean(nlo), 1)})
    if pc and npc:
        out.update({"precip_7d_in": round(sum(pc), 2), "normal_precip_7d_in": round(sum(npc), 2)})
    out["acis_source"] = "NOAA RCC ACIS (last 7 days vs 1991-2020 normals)"
    return out


# ── NASA POWER: monthly climatology at the point (global) ──
def power_climatology(lat, lon):
    r = _client.get("https://power.larc.nasa.gov/api/temporal/climatology/point",
                    params={"parameters": "T2M,T2M_MAX,T2M_MIN,PRECTOTCORR,ALLSKY_SFC_SW_DWN,WS10M", "community": "RE",
                            "latitude": lat, "longitude": lon, "format": "JSON"})
    r.raise_for_status()
    p = r.json()["properties"]["parameter"]
    m = date.today().strftime("%b").upper()
    g = lambda k: (p.get(k) or {}).get(m)
    return {"monthly_normals": {"month": m, "mean_temp_c": g("T2M"), "extreme_high_c": g("T2M_MAX"), "extreme_low_c": g("T2M_MIN"),
                                "precip_mm_per_day": g("PRECTOTCORR"), "solar_kwh_m2_day": g("ALLSKY_SFC_SW_DWN"), "wind_10m_ms": g("WS10M")},
            "annual_solar_kwh_m2_day": (p.get("ALLSKY_SFC_SW_DWN") or {}).get("ANN"), "normals_source": "NASA POWER 2001-2020 climatology"}


# ── USGS seismic design values ──
def seismic(lat, lon):
    r = _client.get("https://earthquake.usgs.gov/ws/designmaps/asce7-16.json",
                    params={"latitude": lat, "longitude": lon, "riskCategory": "II", "siteClass": "D", "title": "panoptes"})
    r.raise_for_status()
    d = (r.json().get("response") or {}).get("data") or {}
    if not d:
        return {}
    return {"pga_g": d.get("pga"), "ss_g": d.get("ss"), "s1_g": d.get("s1"), "seismic_design_category": d.get("sdc"),
            "seismic_source": "USGS ASCE 7-16 design maps (site class D)"}


# ── NPS National Register of Historic Places ──
def historic(s, w, n, e):
    rows = _arc(_NRHP, s, w, n, e, False, "RESNAME,ResType,CertDate", 200)
    if not rows:
        return {}
    return {"historic_places_listed": len(rows), "examples": [r.get("RESNAME") for r in rows[:6] if r.get("RESNAME")],
            "historic_source": "NPS National Register"}


# ── NRCS AWDB: SNOTEL snowpack ──
_awdb_cache: dict = {}


def _awdb_load():
    import time as _t
    try:
        # the station endpoint ignores every filter and returns all ~4.4k sites (~8 s): fetched
        # once in the background and kept for 6 h, so a facts pass never waits on it
        r = _client.get("https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations",
                        params={"networkCds": "SNTL", "activeOnly": "true"}, timeout=45.0)
        r.raise_for_status()
        _awdb_cache.update(at=_t.time(), rows=[x for x in r.json() if x.get("networkCode") == "SNTL"])
    except Exception as e:
        log.info("awdb stations: %s", e)
    finally:
        _awdb_cache.pop("loading", None)


def snotel(s, w, n, e):
    import threading
    import time as _t
    if "rows" not in _awdb_cache or _t.time() - _awdb_cache.get("at", 0) > 6 * 3600:
        if not _awdb_cache.get("loading"):
            _awdb_cache["loading"] = True
            threading.Thread(target=_awdb_load, daemon=True, name="awdb-warm").start()
        if "rows" not in _awdb_cache:
            return {}
    pad = 0.25
    near = [x for x in _awdb_cache["rows"] if s - pad <= x["latitude"] <= n + pad and w - pad <= x["longitude"] <= e + pad]
    if not near:
        return {}
    lat, lon = (s + n) / 2, (w + e) / 2
    near.sort(key=lambda x: (x["latitude"] - lat) ** 2 + (x["longitude"] - lon) ** 2)
    st = near[0]
    ed = date.today()
    r = _client.get("https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data",
                    params={"stationTriplets": st["stationTriplet"], "elements": "WTEQ,SNWD,TOBS", "duration": "DAILY",
                            "beginDate": (ed - timedelta(days=3)).isoformat(), "endDate": ed.isoformat()})
    r.raise_for_status()
    out = {"station": st.get("name"), "elevation_ft": st.get("elevation"), "snotel_sites_nearby": len(near)}
    for blk in r.json() or []:
        for el in blk.get("data") or []:
            vals = [v for v in (el.get("values") or []) if v.get("value") is not None]
            if vals:
                k = {"WTEQ": "snow_water_equivalent_in", "SNWD": "snow_depth_in", "TOBS": "temp_f"}.get((el.get("stationElement") or {}).get("elementCode"))
                if k:
                    out[k] = vals[-1]["value"]
                    out["as_of"] = vals[-1].get("date")
    out["snotel_source"] = "NRCS SNOTEL"
    return out


# ── FEMA: disaster-declaration history for the county ──
def fema_declarations(geo, state_abbr):
    c = geo["county"]
    r = _client.get("https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",
                    params={"$filter": f"state eq '{state_abbr}' and fipsCountyCode eq '{c['county']}'",
                            "$orderby": "declarationDate desc", "$top": 60,
                            "$select": "declarationTitle,declarationDate,incidentType,disasterNumber"})
    r.raise_for_status()
    rows = r.json().get("DisasterDeclarationsSummaries") or []
    seen, decl = set(), []
    for x in rows:
        if x["disasterNumber"] in seen:
            continue
        seen.add(x["disasterNumber"])
        decl.append(x)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=3652)).isoformat()
    ten = [x for x in decl if x["declarationDate"] >= cutoff]
    types: dict[str, int] = {}
    for x in ten:
        types[x["incidentType"]] = types.get(x["incidentType"], 0) + 1
    return {"disaster_declarations_10y": len(ten), "disasters_by_type_10y": types,
            "latest_declarations": [f"{x['declarationDate'][:10]} {x['declarationTitle'].title()}" for x in decl[:3]], "fema_source": "OpenFEMA (county)"}


# ── NWS: active alerts at the centre ──
def nws_alerts(lat, lon):
    r = _client.get("https://api.weather.gov/alerts/active", params={"point": f"{lat},{lon}"}, headers={"Accept": "application/geo+json"})
    r.raise_for_status()
    feats = r.json().get("features") or []
    if not feats:
        return {"active_alerts": 0}
    return {"active_alerts": len(feats),
            "alerts": [{"event": f["properties"].get("event"), "severity": f["properties"].get("severity"),
                        "headline": (f["properties"].get("headline") or "")[:140], "ends": f["properties"].get("ends")} for f in feats[:6]]}


# ── Wikimedia pageviews: attention on the place ──
def attention(lat, lon, place):
    addr = (place or {}).get("address") or {}
    city = addr.get("city") or addr.get("town") or addr.get("village")
    state = addr.get("state")
    if not city:
        return {}
    titles = [f"{city}, {state}" if state else city, city]
    end = datetime.now(timezone.utc) - timedelta(days=1)
    start = end - timedelta(days=30)
    best = None
    for t in dict.fromkeys(titles):                  # "City, State" is often a redirect stub; keep the busier article
        tt = t.replace(" ", "_")
        r = _client.get(f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/{tt}/daily/"
                        f"{start:%Y%m%d}00/{end:%Y%m%d}00")
        if r.status_code != 200:
            continue
        items = r.json().get("items") or []
        if len(items) < 7:
            continue
        views = [i["views"] for i in items]
        med = statistics.median(views)
        if best is None or med > best[1]:
            best = (t, med, views, items)
    if not best:
        return {}
    t, med, views, items = best
    latest = views[-1]
    return {"wikipedia_article": t, "wikipedia_views_yesterday": latest, "wikipedia_median_daily_30d": med,
            "attention_ratio": round(latest / med, 2) if med else None, "wikipedia_peak_30d": max(views),
            "wikipedia_peak_day": items[views.index(max(views))]["timestamp"][:8], "attention_source": "Wikimedia pageviews"}
