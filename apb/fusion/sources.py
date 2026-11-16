"""Source collection helpers for the fusion layer."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterable

from apb.common.models import EventSignal, SignalKind
from apb.fusion.places import resolve_place
from apb.fusion.signals import incident_to_signal, text_signal
from apb.ingest.cad import CadIngest
from apb.store import snapshots


def cad_signals(
    cad: CadIngest,
    *,
    limit_per: int = 60,
    max_age_hours: float = 24.0,
    include_live: bool = True,
) -> list[EventSignal]:
    """Fetch live CAD plus accumulated snapshot history as normalized signals."""
    merged = {(d["metro"], str(d["call_id"])): d for d in snapshots.query(max_age_hours)}
    if include_live:
        live = cad.overview(limit_per=limit_per, max_age_hours=max_age_hours)
        for d in live:
            merged[(d["metro"], str(d["call_id"]))] = d
    return [incident_to_signal(d) for d in merged.values()]


def gather_signals(
    cad: CadIngest,
    *,
    limit_per: int = 60,
    max_age_hours: float = 24.0,
    include_seed: bool = True,
    include_live: bool = True,
    kinds: set[str] | None = None,
) -> list[EventSignal]:
    """The fusion substrate: CAD live+history, seed rows, social + news buffers —
    all time-windowed to max_age_hours and optionally filtered by sensor family.
    Single source of truth for /live/signals and /live/fused."""
    from apb.fusion import news_store, social_store
    signals = cad_signals(cad, limit_per=limit_per, max_age_hours=max_age_hours,
                          include_live=include_live)
    if include_seed:
        signals.extend(seed_recent(max_age_hours))
    signals.extend(social_store.recent(max_age_hours))   # live Bluesky + social RSS
    signals.extend(news_store.recent(max_age_hours))     # live news RSS
    if include_live:
        signals.extend(derived_signals(max_age_hours))   # camera checks + NOTABLE cells
    from apb.fusion.dedupe import dedupe_signals
    signals = dedupe_signals(signals)   # one phenomenon != multi-source corroboration
    if kinds is not None:
        signals = [s for s in signals if s.source_kind.value in kinds]
    return signals


# NOTABLE flags that describe human activity at a place (as opposed to weather or
# climate leaving its range): only these can corroborate an incident cluster.
_ACTIVITY_FLAGS = ("attention", "outage", "connectivity", "crime", "noise", "traffic",
                   "vehicles", "pedestrians", "delay")


def derived_signals(max_age_hours: float = 6.0) -> list[EventSignal]:
    """The other two detectors, as signals, so all three vote in one place:

    camera checks   a tasked look whose frames support the event -> `camera` kind,
                    which corroborates on its own (a different sensor saw it)
    NOTABLE cells   a place's metrics leaving their baseline -> `sensor` kind; it
                    corroborates only for activity-type flags on a cell that is not
                    flagged all the time, and is context otherwise
    """
    from apb.common.models import IncidentType
    out: list[EventSignal] = []
    try:
        from apb.context import tasking
        for c in tasking.recent_checks(min(max_age_hours, 6.0)):
            if c.get("overall") != "yes" or c.get("lat") is None:
                continue
            kind = c.get("top_type") or "other"
            out.append(EventSignal(
                signal_id=f"camera_check:{c['uid']}:{int(c['ts'])}",
                source="camera_check", source_kind=SignalKind.camera,
                observed_at=datetime.fromtimestamp(c["ts"], tz=timezone.utc),
                lat=c["lat"], lon=c["lon"],
                normalized_type=IncidentType(kind if kind in IncidentType._value2member_map_ else "other"),
                summary=f"Cameras confirm: {c.get('verdict') or 'frames show the reported incident'}"[:280],
                confidence=min(0.9, 0.55 + 0.1 * (c.get("supports") or 1)),
                severity=0.6, dedupe_key=f"camera_check:{c['uid']}:{int(c['ts'])}",
                metadata={"cameras": c.get("cameras"), "supports": c.get("supports"),
                          "event_uid": c["uid"]}))
    except Exception:
        pass                                    # stores are optional in tests / followers
    try:                                        # radio systems far above their own normal
        from apb.ingest import rdio
        for r in rdio.activity(min(max_age_hours * 3600.0, 3 * rdio.EVERY)):
            if r["z"] is None or r["z"] < 3.0 or r["calls"] < 8 or r["lat"] is None:
                continue
            tags = " ".join(r["tags"]).lower()
            kind = ("fire" if "fire" in tags else "medical" if "ems" in tags or "medic" in tags
                    else "other")
            out.append(EventSignal(
                signal_id=f"radio:{r['id']}:{int(r['ts'])}",
                source="rdio", source_kind=SignalKind.radio_metadata,
                observed_at=datetime.fromtimestamp(r["ts"], tz=timezone.utc),
                lat=r["lat"], lon=r["lon"], location_text=r["system"],
                normalized_type=IncidentType(kind),
                summary=(f"Radio surge: {r['system']} — {r['per10']} calls/10 min vs "
                         f"{r['normal']} normal ({', '.join(r['tags'])})")[:280],
                # a system placed only by its operator's town is weaker evidence of WHERE
                confidence=0.45 if r["approx"] else 0.65,
                severity=min(0.85, 0.4 + 0.05 * r["z"]),
                dedupe_key=f"radio:{r['id']}",
                metadata={"z": r["z"], "calls": r["calls"], "approx": r["approx"]}))
    except Exception:
        pass
    try:
        from apb.store import metrics as mstore
        for cell in mstore.notable_cells(min(max_age_hours, 6.0)):
            if cell.get("lat") is None:
                continue
            flags = [str(f) for f in cell.get("flags") or []]
            text = "; ".join(flags)
            activity = any(k in text.lower() for k in _ACTIVITY_FLAGS)
            fresh = (cell.get("streak") or 0) <= 2          # not this cell's usual state
            out.append(EventSignal(
                signal_id=f"notable:{cell['cell']}:{int(cell['ts'])}",
                source="notable", source_kind=SignalKind.sensor,
                observed_at=datetime.fromtimestamp(cell["ts"], tz=timezone.utc),
                lat=cell["lat"], lon=cell["lon"],
                summary=f"Notable: {text}"[:280],
                confidence=0.6, severity=0.45 if (activity and fresh) else 0.25,
                dedupe_key=f"notable:{cell['cell']}",
                metadata={"flags": flags, "streak": cell.get("streak")}))
    except Exception:
        pass
    return out


def seed_recent(max_age_hours: float) -> list[EventSignal]:
    """Seed rows are offline test data — drop stale ones so live windows stay live."""
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_hours * 3600
    return [s for s in load_seed_signals() if s.observed_at.timestamp() >= cutoff]


def social_text_signals(rows: Iterable[dict]) -> list[EventSignal]:
    """Normalize already-collected social/news rows.

    Row shape is intentionally loose:
    {"source","text","lat","lon","metro","location","url","created_at"}.
    This lets early scrapers, manual seed files, or future stream consumers plug in
    without changing the fusion endpoint.
    """
    out: list[EventSignal] = []
    for r in rows:
        text = r.get("text") or r.get("summary") or ""
        if not text:
            continue
        created = r.get("created_at") or r.get("at")
        observed = _parse_dt(created)
        place = None
        if r.get("lat") is None or r.get("lon") is None:
            place = resolve_place(text)
        lat = r.get("lat") if r.get("lat") is not None else (place.lat if place else None)
        lon = r.get("lon") if r.get("lon") is not None else (place.lon if place else None)
        metro = r.get("metro") or (place.metro if place else None)
        location = r.get("location") or (place.name if place else None)
        out.append(text_signal(
            source=r.get("source") or "social",
            source_kind=SignalKind(r.get("source_kind") or SignalKind.social.value),
            text=text,
            observed_at=observed,
            lat=lat,
            lon=lon,
            metro=metro,
            location_text=location,
            url=r.get("url"),
            confidence=float(r.get("confidence", place.confidence if place else 0.45)),
            metadata={k: v for k, v in r.items()
                      if k not in {"text", "summary", "lat", "lon", "url"}},
        ))
    return out


def load_seed_signals(path: str = "data/social_seed.jsonl") -> list[EventSignal]:
    """Load optional local social/news seed rows for offline fusion experiments."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            rows = [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []
    return social_text_signals(rows)


def _parse_dt(v) -> datetime | None:
    if not v:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(float(v), tz=timezone.utc)
    try:
        s = str(v).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
