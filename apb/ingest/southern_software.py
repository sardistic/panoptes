"""Southern Software "Citizen Connect" public CAD calls-for-service ingest.

A multi-tenant police/sheriff CAD portal (cc.southernsoftware.com) keyed by AgencyID
(e.g. HarnettCoNC, DaphnePDAL) — a CentralSquare/P2C analog that tends to cover smaller
Southeastern agencies. Like P2C it self-bootstraps a session (the calls endpoint reads
the agency from the PHP session, not a query param):
  1. GET /CADCFS_Public/index.php?AgencyID=<id>      -> PHPSESSID with agency context
  2. GET /CADCFS_Public/fetchesforajax/resttest.php   -> server-rendered calls table

The table is HTML (no JSON API); each row carries data-lat/data-lng/data-calltype plus
a title= address and a time-badge. No incident id is exposed, so call_id is a content
hash. Received time is MM/DD/YYYY HH:MM in agency-local time (no tz) — treated as UTC
downstream, which only shifts the freshness window by a few hours.

Be respectful: public citizen portal; the wider pipeline caches results ~60s and
rotates feeds, and sessions are cached a few minutes.
"""
from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime

import httpx

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")
_BASE = "https://cc.southernsoftware.com"
_SESSION_TTL = 240.0

_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_LINK_RE = re.compile(
    r'data-lat="([^"]+)"\s+data-lng="([^"]+)"\s+data-calltype="([^"]*)"', re.S)
_TITLE_RE = re.compile(r'class="location-cell"\s+title="([^"]*)"')
_TIME_RE = re.compile(r'class="time-badge">\s*([^<]+?)\s*<')


def _iso(s: str | None) -> str | None:
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%m/%d/%Y %H:%M").isoformat()
    except ValueError:
        return s


class SouthernSoftware:
    def __init__(self):
        # agency_id -> (client, ts)
        self._sessions: dict[str, tuple] = {}

    def _session(self, aid: str) -> httpx.Client:
        cached = self._sessions.get(aid)
        if cached and time.time() - cached[1] < _SESSION_TTL:
            return cached[0]
        c = httpx.Client(timeout=20.0, follow_redirects=True,
                         headers={"User-Agent": _UA})
        c.get(f"{_BASE}/CADCFS_Public/index.php", params={"AgencyID": aid})
        self._sessions[aid] = (c, time.time())
        return c

    def incidents(self, aid: str) -> list[dict]:
        c = self._session(aid)
        ref = {"Referer": f"{_BASE}/CADCFS_Public/index.php?AgencyID={aid}",
               "X-Requested-With": "XMLHttpRequest"}
        try:
            r = c.get(f"{_BASE}/CADCFS_Public/fetchesforajax/resttest.php",
                      params={"t": int(time.time() * 1000)}, headers=ref)
            if r.status_code != 200:
                raise httpx.HTTPError(f"status {r.status_code}")
        except httpx.HTTPError:
            self._sessions.pop(aid, None)   # drop session; re-bootstrap next time
            return []
        out: list[dict] = []
        for row in _ROW_RE.findall(r.text):
            m = _LINK_RE.search(row)
            if not m:
                continue
            try:
                lat, lon = float(m.group(1)), float(m.group(2))
            except ValueError:
                continue
            if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
                continue
            ctype = m.group(3).strip()
            addr_m = _TITLE_RE.search(row)
            addr = addr_m.group(1).strip() if addr_m else None
            time_m = _TIME_RE.search(row)
            when = time_m.group(1).strip() if time_m else None
            cid = hashlib.md5(
                f"{aid}|{ctype}|{when}|{lat},{lon}".encode()).hexdigest()[:16]
            out.append({"call_id": cid, "type_raw": ctype, "address": addr,
                        "at": _iso(when), "lat": lat, "lon": lon})
        return out
