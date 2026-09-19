"""New keyless lanes: parsing against captured response shapes (no network)."""
import json

import httpx

from apb.context import facts_more as m


class _R:
    def __init__(self, payload, status=200, text=None):
        self.status_code, self._p = status, payload
        self.text = text if text is not None else json.dumps(payload)

    def json(self):
        return self._p

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("x", request=None, response=None)


def _route(monkeypatch, table):
    def get(url, params=None, **kw):
        for frag, payload in table.items():
            if frag in url:
                return payload(url, params) if callable(payload) else _R(payload)
        raise AssertionError(url)
    monkeypatch.setattr(m._client, "get", get)
    monkeypatch.setattr(m._client, "post", lambda url, json=None, **kw: get(url, json))


def test_infrastructure_counts_and_power(monkeypatch):
    def arc(url, params):
        if "Power_Plants" in url:
            return _R({"features": [{"attributes": {"Plant_Name": "A", "PrimSource": "solar", "Total_MW": 4.5}}]})
        return _R({"count": 3 if "Fire_Stations" in url else 0})
    _route(monkeypatch, {"arcgis": arc})
    out = m.infrastructure(35, -79, 36, -78)
    assert out["fire_stations"] == 3 and "law_enforcement" not in out
    assert out["power_capacity_mw"] == 4.5 and out["power_plants"][0]["source"] == "solar"


def test_radio_groups_licensees(monkeypatch):
    def arc(url, params):
        if "FM_Transmission" in url:
            return _R({"features": [{"attributes": {"CALLSIGN": "WKNC-FM", "FREQUENCY": "88.1 MHz", "LICENSEE": "NCSU"}}]})
        if "Land_Mobile" in url:
            return _R({"features": [{"attributes": {"LICENSEE": "NEXTEL"}}, {"attributes": {"LICENSEE": "NEXTEL"}}, {"attributes": {"LICENSEE": "RR"}}]})
        if "AM_Transmission" in url:
            return _R({"features": []})
        return _R({"count": 5})
    _route(monkeypatch, {"arcgis": arc})
    out = m.radio(35, -79, 36, -78)
    assert out["fm_stations"] == ["WKNC-FM 88.1 MHz (NCSU)"] and "am_stations" not in out
    assert out["land_mobile_sites"] == 3 and out["land_mobile_licensees"][0] == "NEXTEL ×2"
    assert out["microwave_towers"] == 5


def test_echo_counts(monkeypatch):
    _route(monkeypatch, {"echodata": {"Results": {"Message": "Success", "QueryRows": "12", "SVRows": "1", "CVRows": "3", "FEARows": "4",
                                                   "INSPRows": "2", "TotalPenalties": "$5", "CAARows": "1", "CWARows": "2", "RCRRows": "3", "TRIRows": "0"}}})
    out = m.echo(35, -79, 36, -78)
    assert out["regulated_facilities"] == 12 and out["significant_violators"] == 1 and out["penalties_5y"] == "$5"


def test_qcew_parses_latest_quarter(monkeypatch):
    csv = ('"area_fips","own_code","industry_code","agglvl_code","size_code","year","qtr","disclosure_code","qtrly_estabs","month1_emplvl",'
           '"month2_emplvl","month3_emplvl","total_qtrly_wages","taxable_qtrly_wages","qtrly_contributions","avg_wkly_wage","lq_disclosure_code",'
           '"oty_qtrly_estabs_pct_chg","oty_month3_emplvl_pct_chg","oty_avg_wkly_wage_pct_chg"\n'
           '"37183","0","10","70","0","2025","1","","47605","1","1","655755","1","1","1","1635","","-5.2","1.4","6.7"\n')
    calls = []

    def get(url, params=None, **kw):
        calls.append(url)
        return _R(None, 200 if len(calls) == 2 else 404, csv if len(calls) == 2 else "no")
    monkeypatch.setattr(m._client, "get", get)
    out = m.qcew({"county": {"state": "37", "county": "183"}})
    assert out["employment"] == 655755 and out["avg_weekly_wage_usd"] == 1635 and out["employment_yoy_pct"] == 1.4
    assert out["period"] == "2025 Q1"


def test_cdc_places_maps_measures(monkeypatch):
    _route(monkeypatch, {"data.cdc.gov": [{"measureid": "OBESITY", "data_value": "29.3", "year": "2023"}, {"measureid": "ZZZ", "data_value": "1"}]})
    out = m.cdc_places({"county": {"name": "Wake County"}}, "NC")
    assert out == {"obesity_pct": 29.3, "year": "2023", "places_source": "CDC PLACES (county model estimates, adults)"}


def test_acis_anomaly_uses_temperature_station(monkeypatch):
    def post(url, json=None, **kw):
        if url.endswith("StnMeta"):
            return _R({"meta": [{"name": "RAIN ONLY", "sids": ["X 6"], "ll": [-78.64, 35.78], "valid_daterange": [[], [], ["2024-01-01", "2026-09-18"]]},
                                {"name": "CARY", "sids": ["CARY 6"], "ll": [-78.8, 35.8],
                                 "valid_daterange": [["2000-01-01", "2026-09-18"], ["2000-01-01", "2026-09-18"], ["2000-01-01", "2026-09-18"]]}]})
        assert json["sid"] == "CARY"
        return _R({"data": [["2026-09-17", "90", "70", "0.10", "80", "60", "0.20"], ["2026-09-18", "88", "68", "T", "80", "60", "0.20"]]})
    monkeypatch.setattr(m._client, "post", post)
    out = m.acis(35.75, -78.7, 35.82, -78.6)
    assert out["station"] == "CARY" and out["high_anomaly_f"] == 9.0 and out["low_anomaly_f"] == 9.0
    assert out["precip_7d_in"] == 0.1 and out["normal_precip_7d_in"] == 0.4


def test_fema_declarations_dedupes_and_counts(monkeypatch):
    rows = [{"disasterNumber": 1, "declarationDate": "2026-01-24T00:00:00.000Z", "declarationTitle": "SEVERE WINTER STORM", "incidentType": "Winter Storm"},
            {"disasterNumber": 1, "declarationDate": "2026-01-24T00:00:00.000Z", "declarationTitle": "SEVERE WINTER STORM", "incidentType": "Winter Storm"},
            {"disasterNumber": 2, "declarationDate": "2010-01-01T00:00:00.000Z", "declarationTitle": "OLD", "incidentType": "Flood"}]
    _route(monkeypatch, {"fema.gov": {"DisasterDeclarationsSummaries": rows}})
    out = m.fema_declarations({"county": {"county": "183"}}, "NC")
    assert out["disaster_declarations_10y"] == 1 and out["disasters_by_type_10y"] == {"Winter Storm": 1}
    assert out["latest_declarations"][0] == "2026-01-24 Severe Winter Storm"


def test_attention_prefers_busier_article(monkeypatch):
    def get(url, params=None, **kw):
        n = 300 if "Chicago,_Illinois" in url else 5000
        return _R({"items": [{"views": n, "timestamp": f"202609{d:02d}00"} for d in range(1, 11)]})
    monkeypatch.setattr(m._client, "get", get)
    out = m.attention(41.9, -87.6, {"address": {"city": "Chicago", "state": "Illinois"}})
    assert out["wikipedia_article"] == "Chicago" and out["attention_ratio"] == 1.0
    assert m.attention(0, 0, {"address": {}}) == {}


def test_snotel_is_nonblocking_until_stations_warm(monkeypatch):
    m._awdb_cache.clear()
    started = []
    monkeypatch.setattr(m.threading if hasattr(m, "threading") else __import__("threading"), "Thread",
                        lambda **kw: type("T", (), {"start": lambda self: started.append(kw["name"])})())
    assert m.snotel(39.5, -106.3, 39.7, -106.0) == {}
    assert started == ["awdb-warm"] and m._awdb_cache.get("loading") is True
    m._awdb_cache.clear()


def test_notable_flags_new_lanes():
    from apb.context import facts
    flags = facts.notable({"activity": {"attention_ratio": 4.2}, "climate": {"high_anomaly_f": -12.0},
                           "safety": {"alerts": [{"event": "Tornado Warning", "severity": "Extreme"}]},
                           "environment": {"significant_violators": 3}}, {})
    assert "Wikipedia attention 4.2x its 30-day median" in flags
    assert "highs 12.0°F below normal this week" in flags and "NWS Tornado Warning" in flags
    assert "3 EPA significant violators in the box" in flags
