"""Aircraft emergency squawks — keyless, highest signal-per-row lane in the system.

An aircraft squawking 7700 (general emergency), 7600 (radio failure), or 7500
(unlawful interference) is a crew formally declaring an emergency on their
transponder. adsb.lol / airplanes.live expose these as a national one-call query.
Zero rows almost all the time; when a row exists it matters.

Returns the normalized incident dict, registered as a hidden feed of kind="squawk".
"""
from __future__ import annotations

import logging
import time

import httpx

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_HOSTS = ("https://api.adsb.lol/v2/sqk/{code}",
          "https://api.airplanes.live/v2/sqk/{code}")

_CODES = {
    "7700": ("Emergency declared", 0.85),
    "7600": ("Radio failure", 0.5),
    "7500": ("Unlawful interference", 0.98),
}

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]


class SquawkIngest:
    """Fetches aircraft currently squawking emergency codes."""

    def __init__(self):
        self._client = httpx.Client(timeout=15.0, headers=_UA, follow_redirects=True)

    def _one(self, code: str) -> list[dict]:
        for host in _HOSTS:
            try:
                r = self._client.get(host.format(code=code))
                r.raise_for_status()
                return r.json().get("ac", []) or []
            except (httpx.HTTPError, ValueError):
                continue
        return []

    def fetch(self) -> list[dict]:
        now = time.time()
        out: list[dict] = []
        for code, (label, threat) in _CODES.items():
            for ac in self._one(code):
                lat, lon = ac.get("lat"), ac.get("lon")
                if lat is None or lon is None:
                    continue
                callsign = (ac.get("flight") or "").strip() or ac.get("r") or ac.get("hex")
                desc = ac.get("desc") or ac.get("t") or "aircraft"
                alt = ac.get("alt_baro")
                out.append({
                    "call_id": f"sqk:{code}:{ac.get('hex')}",
                    "metro": "squawk", "type": "other", "squawk": code,
                    "summary": (f"SQUAWK {code} — {label}: {callsign} ({desc})"
                                f"{f' at {alt} ft' if isinstance(alt, (int, float)) else ''}"),
                    "location": None, "source": "squawk",
                    "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                    "threat_score": threat, "emerging": code in ("7500", "7700"),
                    "lat": float(lat), "lon": float(lon),
                    "at": None, "ts": now,   # positions are live at fetch time
                })
        return out
