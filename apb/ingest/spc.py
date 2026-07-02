"""NOAA SPC preliminary storm reports — keyless severe-weather ground truth.

The Storm Prediction Center publishes the day's filtered local storm reports
(tornado / hail / wind) as CSV with coordinates. Unlike NWS *warnings* (what may
happen), these are observed events — a confirmed tornado touchdown or 2" hail at a
point, which is exactly the kind of high-confidence corroboration the fusion layer
wants.

Returns the same normalized incident dict the CAD/hazard layers emit. Registered as a
hidden feed of kind="spc"; see apb.ingest.cad.load_spc.
"""
from __future__ import annotations

import csv
import io
from datetime import datetime, timezone

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "apb/0.1 (panoptes.run; public-safety map)"}
_BASE = "https://www.spc.noaa.gov/climo/reports/today_{kind}.csv"
_KINDS = ("torn", "hail", "wind")

_SENTIMENT = ["calm", "routine", "elevated", "urgent", "distress"]


def _threat(kind: str, magnitude: str) -> float:
    """Scale threat by report magnitude where available."""
    if kind == "torn":
        return 0.85
    try:
        m = float(magnitude)
    except (TypeError, ValueError):
        return 0.5
    if kind == "hail":                 # CSV magnitude is hundredths of an inch
        return 0.7 if m >= 200 else (0.55 if m >= 100 else 0.45)
    if kind == "wind":                 # mph (UNK -> handled above)
        return 0.7 if m >= 65 else 0.5
    return 0.5


def _ts(hhmm: str) -> float | None:
    """SPC report times are UTC HHMM on the current (UTC) convective day."""
    try:
        now = datetime.now(timezone.utc)
        return now.replace(hour=int(hhmm[:2]), minute=int(hhmm[2:]), second=0,
                           microsecond=0).timestamp()
    except (ValueError, IndexError):
        return None


class SpcIngest:
    """Fetches today's tornado/hail/wind reports; mirrors the CadIngest fetch contract."""

    def __init__(self):
        self._client = httpx.Client(timeout=20.0, headers=_UA, follow_redirects=True)

    def _one(self, kind: str) -> list[dict]:
        try:
            text = self._client.get(_BASE.format(kind=kind)).text
        except httpx.HTTPError as e:
            log.warning(f"{kind} fetch failed: {e}")
            return []
        out: list[dict] = []
        for r in csv.DictReader(io.StringIO(text)):
            try:
                lat, lon = float(r["Lat"]), float(r["Lon"])
            except (KeyError, ValueError):
                continue
            mag = r.get("F_Scale") or r.get("Size") or r.get("Speed") or ""
            threat = _threat(kind, mag)
            label = {"torn": "Tornado", "hail": "Hail", "wind": "Wind"}[kind]
            extra = ""
            if kind == "hail" and mag.isdigit():
                extra = f" {int(mag)/100:.2f}\""
            elif kind == "wind" and mag.isdigit():
                extra = f" {mag} mph"
            ts = _ts(r.get("Time", ""))
            out.append({
                "call_id": f"spc:{kind}:{r.get('Time')}:{lat:.2f}:{lon:.2f}",
                "metro": "spc", "type": "weather", "report_type": kind,
                "summary": (f"{label} report{extra}: {r.get('Location','')}, "
                            f"{r.get('State','')} — {r.get('Comments','')}").strip()[:280],
                "location": f"{r.get('Location','')}, {r.get('State','')}".strip(", "),
                "source": "spc",
                "sentiment": _SENTIMENT[min(4, int(threat * 5))],
                "threat_score": round(threat, 2), "emerging": kind == "torn",
                "lat": lat, "lon": lon,
                "at": (datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None),
                "ts": ts,
            })
        return out

    def fetch(self) -> list[dict]:
        out: list[dict] = []
        for kind in _KINDS:
            out.extend(self._one(kind))
        return out
