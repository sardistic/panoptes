"""Amtrak live train delays — keyless national rail-anomaly signal.

api-v3.amtraker.com mirrors Amtrak's live train tracker (positions + per-station
schedule vs actual). A train running hours late mid-route is an operational
anomaly that frequently corresponds to a mappable incident — trespasser strikes,
grade-crossing collisions, freight derailments blocking the corridor, weather.

We deliberately emit only trains >= _MIN_DELAY_MIN late (on-time rail is not an
event signal), positioned at the train's live lat/lon.

Returns the normalized incident dict, registered as a hidden feed of kind="amtrak".
"""
from __future__ import annotations

import logging
from datetime import datetime

import httpx

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_URL = "https://api-v3.amtraker.com/v3/trains"
_MIN_DELAY_MIN = 60.0

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]


def _dt(s: str | None) -> datetime | None:
    try:
        return datetime.fromisoformat(s) if s else None
    except ValueError:
        return None


def _delay_min(train: dict) -> float | None:
    """Worst schedule slip (minutes) across stations already reached."""
    worst = None
    for s in train.get("stations") or []:
        for sch_k, act_k in (("schArr", "arr"), ("schDep", "dep")):
            sch, act = _dt(s.get(sch_k)), _dt(s.get(act_k))
            if not sch or not act or s.get("status") not in ("Departed", "Arrived", "Station"):
                continue
            d = (act - sch).total_seconds() / 60.0
            if worst is None or d > worst:
                worst = d
    return worst


def _threat(delay_min: float) -> float:
    return min(0.6, 0.3 + delay_min / 600.0)     # 1h → 0.4, 3h+ → ~0.6 cap


class AmtrakIngest:
    """Fetches all live trains, emits the significantly-delayed ones."""

    def __init__(self):
        self._client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)

    def fetch(self) -> list[dict]:
        try:
            data = self._client.get(_URL).json()
        except (httpx.HTTPError, ValueError) as e:
            log.warning("fetch failed: %s", e)
            return []
        out: list[dict] = []
        for trains in (data.values() if isinstance(data, dict) else []):
            for t in trains if isinstance(trains, list) else []:
                lat, lon = t.get("lat"), t.get("lon")
                if not lat or not lon:
                    continue
                delay = _delay_min(t)
                if delay is None or delay < _MIN_DELAY_MIN:
                    continue
                threat = _threat(delay)
                hrs, mins = divmod(int(delay), 60)
                upd = _dt(t.get("updatedAt") or t.get("lastValTS"))
                out.append({
                    "call_id": f"amtrak:{t.get('trainID')}",
                    "metro": "amtrak", "type": "traffic",
                    "summary": (f"Amtrak {t.get('routeName')} #{t.get('trainNum')} "
                                f"running {hrs}h{mins:02d}m late "
                                f"({t.get('origCode')}→{t.get('destCode')})"),
                    "location": t.get("eventName") or None,   # last/next station
                    "source": "amtrak", "delay_min": round(delay),
                    "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                    "threat_score": round(threat, 2), "emerging": False,
                    "lat": float(lat), "lon": float(lon),
                    "at": t.get("updatedAt"),
                    "ts": upd.timestamp() if upd else None,
                })
        return out
