"""NOAA NWPS river-gauge flood status — keyless flood-event signal.

Complements the NWS flood *warnings* already pulled in apb.ingest.hazard with
gauge-level *observed* flood category (action / minor / moderate / major) on major
rivers — i.e. water is actually high here right now, not just forecast.

The NWPS bulk/list endpoints don't serve a national query (they time out), but the
per-gauge endpoint is reliable, so we poll a curated watch list of high-impact
river gauges and emit only those currently at/above "action" stage. The GAUGES list
is a seed — extend it with more AHPS/NWPS gauge IDs (lids) over time.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="usgs_flood"; see apb.ingest.cad.load_usgs_flood.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import httpx

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_GAUGE = "https://api.water.noaa.gov/nwps/v1/gauges/{lid}"

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]

# Curated high-impact river gauges (NWPS lids). Seed set — extend freely.
GAUGES: tuple[str, ...] = (
    "EADM7",  # Mississippi R at St. Louis, MO
    "MEMT1",  # Mississippi R at Memphis, TN
    "BTRL1",  # Mississippi R at Baton Rouge, LA
    "NORL1",  # Mississippi R at New Orleans, LA
    "HARP1",  # Susquehanna R at Harrisburg, PA
    "OMAN1",  # Missouri R at Omaha, NE
    "ALBN6",  # Hudson R at Albany, NY
    "TREN4",  # Delaware R at Trenton, NJ
    "MCGI4",  # Mississippi R at McGregor, IA
    "NAST1",  # Cumberland R at Nashville, TN
    "SACC1",  # Sacramento R at Sacramento, CA
)

# Observed flood category -> (threat, label). "no_flooding"/"" are skipped.
_CAT = {
    "action": (0.45, "Action stage"),
    "minor": (0.6, "Minor flooding"),
    "moderate": (0.75, "Moderate flooding"),
    "major": (0.9, "Major flooding"),
}


def _epoch(s: str | None) -> float | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except ValueError:
        return None


class UsgsFloodIngest:
    """Polls the curated NWPS gauge watch list; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=10.0, headers=_UA, follow_redirects=True)

    def _one(self, lid: str) -> dict | None:
        try:
            g = self._client.get(_GAUGE.format(lid=lid)).json()
        except (httpx.HTTPError, ValueError):
            return None
        obs = (g.get("status") or {}).get("observed") or {}
        cat = (obs.get("floodCategory") or "").lower()
        if cat not in _CAT:
            return None
        lat, lon = g.get("latitude"), g.get("longitude")
        if lat is None or lon is None:
            return None
        threat, label = _CAT[cat]
        ts = _epoch(obs.get("validTime"))
        stage = f"{obs.get('primary')} {obs.get('primaryUnit','')}".strip()
        return {
            "call_id": f"flood:{lid}", "metro": "usgs_flood", "type": "weather",
            "summary": f"{label}: {g.get('name','river gauge')} ({stage})"[:280],
            "location": f"{g.get('county','')}, {(g.get('state') or {}).get('abbreviation','')}".strip(", "),
            "source": "usgs_flood",
            "sentiment": _SENTIMENT[min(4, int(threat * 5))],
            "threat_score": round(threat, 2), "emerging": cat == "major",
            "lat": float(lat), "lon": float(lon),
            "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                   if ts else None),
            "ts": ts,
        }

    def fetch(self) -> list[dict]:
        out: list[dict] = []
        with ThreadPoolExecutor(max_workers=6) as ex:
            for row in ex.map(self._one, GAUGES):
                if row:
                    out.append(row)
        return out
