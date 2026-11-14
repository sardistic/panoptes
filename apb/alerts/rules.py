"""Alert rules — which events are worth a message, and to whom.

Rules come from a JSON file (`APB_ALERT_RULES`, default `config/alert_rules.json`):

    [
      {"name": "home", "center": [35.78, -78.64], "radius_km": 40, "min_score": 4},
      {"name": "gunfire", "types": ["shots_fired", "pursuit"], "min_score": 5,
       "min_sources": 2, "webhook": "https://discord.com/api/webhooks/..."},
      {"name": "west-coast-fire", "bbox": [-125, 32, -114, 49], "types": ["fire"]}
    ]

Fields (all optional except name): `min_score`, `min_sources`, `types`, `bbox`
(w,s,e,n), `center` + `radius_km`, `max_age_hours` (ignore events older than this
unless they are rising), `escalate` (re-alert when the score reaches this multiple
of the last alerted score), `webhook` (overrides APB_WEBHOOK_URL for this rule).

With no file there is one global rule built from the environment:
APB_ALERT_SCORE (6.0) and APB_ALERT_SOURCES (2 — independent source kinds).
"""
from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class Rule:
    name: str
    min_score: float = 6.0
    min_sources: int = 1
    types: frozenset[str] | None = None
    bbox: tuple[float, float, float, float] | None = None      # w, s, e, n
    center: tuple[float, float] | None = None                  # lat, lon
    radius_km: float = 50.0
    max_age_hours: float = 12.0
    escalate: float = 1.5
    webhook: str | None = None


def _float(env: str, default: float) -> float:
    try:
        return float(os.environ.get(env, default))
    except ValueError:
        return default


def default_rule() -> Rule:
    return Rule(name="default", min_score=_float("APB_ALERT_SCORE", 6.0),
                min_sources=int(_float("APB_ALERT_SOURCES", 2)))


def parse(raw: list) -> list[Rule]:
    out: list[Rule] = []
    for i, r in enumerate(raw):
        if not isinstance(r, dict):
            continue
        try:
            bbox = tuple(float(x) for x in r["bbox"]) if r.get("bbox") else None
            center = tuple(float(x) for x in r["center"]) if r.get("center") else None
            if (bbox and len(bbox) != 4) or (center and len(center) != 2):
                raise ValueError("bbox needs 4 numbers, center needs 2")
            out.append(Rule(
                name=str(r.get("name") or f"rule{i + 1}")[:40],
                min_score=float(r.get("min_score", 6.0)),
                min_sources=int(r.get("min_sources", 1)),
                types=frozenset(str(t) for t in r["types"]) if r.get("types") else None,
                bbox=bbox, center=center,
                radius_km=float(r.get("radius_km", 50.0)),
                max_age_hours=float(r.get("max_age_hours", 12.0)),
                escalate=max(1.05, float(r.get("escalate", 1.5))),
                webhook=(str(r["webhook"]).strip() or None) if r.get("webhook") else None,
            ))
        except (TypeError, ValueError, KeyError) as e:
            log.warning("alert rule %s ignored: %s", r.get("name", i), e)
    return out


_cache: tuple[str, float, list[Rule]] | None = None


def load_rules() -> list[Rule]:
    """Rules from the configured file (re-read when it changes), else the env default."""
    global _cache
    path = Path(os.environ.get("APB_ALERT_RULES", "config/alert_rules.json"))
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return [default_rule()]
    if _cache and _cache[0] == str(path) and _cache[1] == mtime:
        return _cache[2]
    try:
        rules = parse(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, ValueError) as e:
        log.warning("alert rules unreadable (%s): %s", path, e)
        rules = []
    rules = rules or [default_rule()]
    _cache = (str(path), mtime, rules)
    return rules


def _types(event: dict) -> dict:
    t = event.get("types") or {}
    if isinstance(t, str):
        try:
            t = json.loads(t)
        except ValueError:
            t = {}
    return t if isinstance(t, dict) else {}


def matches(rule: Rule, event: dict) -> bool:
    """Place / kind / corroboration gates. Score is judged by the caller (a first
    alert looks at the peak, an escalation at the latest value)."""
    if (event.get("source_count") or 0) < rule.min_sources:
        return False
    lat, lon = event.get("lat"), event.get("lon")
    if rule.bbox:
        w, s, e, n = rule.bbox
        if lat is None or lon is None or not (s <= lat <= n and w <= lon <= e):
            return False
    if rule.center:
        if lat is None or lon is None:
            return False
        coslat = max(0.2, math.cos(math.radians(lat)))
        km = math.hypot(lat - rule.center[0], (lon - rule.center[1]) * coslat) * 111.0
        if km > rule.radius_km:
            return False
    if rule.types and not (rule.types & set(_types(event))):
        return False
    age_h = (event.get("age_min") or 0) / 60.0
    if age_h > rule.max_age_hours and event.get("trend") != "rising":
        return False                    # a standing cluster is that place's normal
    return True
