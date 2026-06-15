"""PoliceToCitizen (Tyler/CentralSquare) public CAD-calls ingest — adds POLICE
coverage for agencies that publish a P2C citizen portal (often where there's no
open-data feed).

Self-bootstraps the session (no manual cookies):
  1. GET /                          -> F5 WAF cookies
  2. GET /api/Agency/InitialSettings -> AgencyId + ASP.NET antiforgery + XSRF-TOKEN cookie
  3. POST /api/CADCalls/{id} with X-XSRF-TOKEN header -> the live calls list

Be respectful: this is a public citizen portal, but poll slowly and cache the session
(the wider pipeline caches results 60s and rotates feeds).
"""
from __future__ import annotations

import time

import httpx

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/141.0 Safari/537.36")

# request body that returns recent open+closed calls, newest first
_BODY = {
    "IncludeOpenCalls": True, "IncludeClosedCalls": True, "IncludeCount": True,
    "PagingOptions": {
        "SortOptions": [{"Name": "StartTime", "SortDirection": "Descending",
                         "Sequence": 1}],
        "Take": 100, "Skip": 0,
    },
    "FilterOptionsParameters": {"IntersectionSearch": True, "SearchText": "",
                                "Parameters": []},
}

_SESSION_TTL = 240.0  # re-bootstrap a subdomain's session every few minutes


class P2C:
    def __init__(self):
        # subdomain -> (client, agency_id, agency_name, ts)
        self._sessions: dict[str, tuple] = {}

    def _base(self, sub: str) -> str:
        return f"https://{sub}.policetocitizen.com"

    def _session(self, sub: str):
        cached = self._sessions.get(sub)
        if cached and time.time() - cached[3] < _SESSION_TTL:
            return cached
        base = self._base(sub)
        c = httpx.Client(timeout=20.0, follow_redirects=True,
                         headers={"User-Agent": _UA,
                                  "Accept": "application/json, text/plain, */*"})
        c.get(base + "/")
        ini = c.get(base + "/api/Agency/InitialSettings").json()
        sess = (c, ini.get("AgencyId"), ini.get("Name") or sub, time.time())
        self._sessions[sub] = sess
        return sess

    def initial_settings(self, sub: str) -> dict:
        """Agency id/name + whether CAD calls are enabled (for discovery)."""
        c, aid, name, _ = self._session(sub)
        base = self._base(sub)
        try:
            ads = c.get(f"{base}/api/CADCalls/ADSSettings/{aid}").json()
            enabled = bool(ads.get("OpenCallsEnabled") or ads.get("ClosedCallsEnabled"))
        except Exception:
            enabled = False
        return {"agency_id": aid, "name": name, "cad_enabled": enabled}

    def incidents(self, sub: str) -> list[dict]:
        c, aid, name, _ = self._session(sub)
        base = self._base(sub)
        xsrf = c.cookies.get("XSRF-TOKEN")
        if not aid or not xsrf:
            return []
        r = c.post(f"{base}/api/CADCalls/{aid}", json=_BODY, headers={
            "Content-Type": "application/json", "X-XSRF-TOKEN": xsrf,
            "Origin": base, "Referer": base + "/CADCalls"})
        if r.status_code != 200 or r.text[:1] != "{":
            self._sessions.pop(sub, None)   # session likely stale; drop to re-bootstrap
            return []
        out = []
        for i in r.json().get("CADCalls", []):
            if not i.get("HasLocation") or i.get("Latitude") in (None, 0):
                continue
            out.append({
                "call_id": str(i.get("IncidentId")),
                "type_raw": i.get("Nature") or i.get("CallType") or "",
                "address": i.get("Address"),
                "at": i.get("StartTime"),
                "lat": float(i["Latitude"]), "lon": float(i["Longitude"]),
            })
        return out
