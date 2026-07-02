"""ACLED civil-unrest events — protests, riots, political violence (key, opt-in).

A domain nothing else in the pipeline covers: geolocated protest / riot / armed-clash
/ political-violence events. A protest or unrest cluster is both an event and a strong
cause signal for CAD/traffic surges in the same place/time.

ACLED migrated off the legacy `key`+`email` query-param API to OAuth: you POST your
myACLED account email + password to /oauth/token for a 24h bearer token, then call
/api/acled/read with it. Opt-in via the ACLED_EMAIL and ACLED_PASSWORD env vars, so
the lean core never depends on it. Defaults to US events in the last week.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="acled"; see apb.ingest.cad.load_acled.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_API = "https://acleddata.com/api/acled/read"
_TOKEN_URL = "https://acleddata.com/oauth/token"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

# Event type -> (incident_type, base threat).
_TYPE = {
    "battles": ("assault", 0.9),
    "explosions/remote violence": ("fire", 0.9),
    "violence against civilians": ("assault", 0.85),
    "riots": ("assault", 0.75),
    "protests": ("suspicious", 0.5),
    "strategic developments": ("other", 0.4),
}


def creds() -> tuple[str | None, str | None]:
    """(email, password) for the myACLED OAuth login."""
    return os.environ.get("ACLED_EMAIL"), os.environ.get("ACLED_PASSWORD")


def _epoch(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").replace(
            tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


class AcledIngest:
    """Fetches recent unrest events; mirrors the CadIngest fetch contract."""

    def __init__(self, country: str = "United States", days: int = 7):
        self._client = httpx.Client(timeout=30.0, headers=_UA, follow_redirects=True)
        self._country = country
        self._days = days
        self._token: str | None = None
        self._token_exp: float = 0.0

    def _bearer(self) -> str | None:
        """Return a cached OAuth access token, refreshing ~60s before expiry."""
        if self._token and time.time() < self._token_exp - 60:
            return self._token
        email, password = creds()
        if not email or not password:
            return None
        data = {
            "username": email, "password": password, "grant_type": "password",
            "client_id": "acled", "scope": "authenticated",
        }
        try:
            resp = self._client.post(_TOKEN_URL, data=data)
            resp.raise_for_status()
            tok = resp.json()
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"oauth token failed: {e}")
            self._token = None
            return None
        self._token = tok.get("access_token")
        self._token_exp = time.time() + float(tok.get("expires_in") or 0)
        return self._token

    def fetch(self) -> list[dict]:
        token = self._bearer()
        if not token:
            return []
        start = (datetime.now(timezone.utc) - timedelta(days=self._days)).strftime("%Y-%m-%d")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        params = {
            "country": self._country,
            "event_date": f"{start}|{today}", "event_date_where": "BETWEEN",
            "limit": 500,
        }
        try:
            payload = self._client.get(
                _API, params=params,
                headers={"Authorization": f"Bearer {token}"}).json()
        except (httpx.HTTPError, ValueError) as e:
            log.warning(f"fetch failed: {e}")
            return []
        out: list[dict] = []
        for r in payload.get("data", []) if isinstance(payload, dict) else []:
            try:
                lat, lon = float(r.get("latitude")), float(r.get("longitude"))
            except (TypeError, ValueError):
                continue
            etype = str(r.get("event_type") or "").lower()
            itype, threat = _TYPE.get(etype, ("other", 0.5))
            try:                               # fatalities escalate threat
                if int(r.get("fatalities") or 0) > 0:
                    threat = min(0.97, threat + 0.1)
            except (TypeError, ValueError):
                pass
            ts = _epoch(r.get("event_date"))
            sub = r.get("sub_event_type") or etype.title()
            where = ", ".join(x for x in (r.get("location"), r.get("admin1")) if x)
            out.append({
                "call_id": f"acled:{r.get('event_id_cnty') or r.get('data_id')}",
                "metro": "acled", "type": itype,
                "summary": f"{sub}: {(r.get('notes') or where)}".strip()[:280],
                "location": where or None, "source": "acled",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": threat >= 0.9,
                "lat": lat, "lon": lon,
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out
