"""Push notifications: event alerts, escalations, feed-health changes, daily digest.

Events are matched against the alert rules (apb/alerts/rules.py). A rule alerts an
event once when its score crosses the rule's threshold, and again each time the
score reaches `escalate` x the last alerted score — so a growing incident produces
"update" messages instead of going quiet after the first ping.

Every alert is written to the alert log whether or not a webhook exists; with no
`APB_WEBHOOK_URL` the log is a dry run that the scorecard can still grade.

  APB_WEBHOOK_URL      default destination (Discord URLs get the `content` wrapper,
                       anything else receives {"text", "event"} JSON)
  APB_OPS_WEBHOOK_URL  destination for feed-health changes (falls back to the above)
  APB_DIGEST_HOUR_UTC  hour (0-23) for the daily digest; unset = no digest
"""
from __future__ import annotations

import json
import logging
import os
import time

import httpx

log = logging.getLogger(__name__)

_MIN_UPDATE_GAP_SEC = 600.0
_SITE = "https://panoptes.run"


def _url() -> str | None:
    return os.environ.get("APB_WEBHOOK_URL", "").strip() or None


def _config() -> tuple[str | None, float]:        # kept for callers of the old API
    from apb.alerts.rules import default_rule
    return _url(), default_rule().min_score


def _top_type(e: dict) -> str:
    types = e.get("types") or {}
    if isinstance(types, str):
        try:
            types = json.loads(types)
        except ValueError:
            types = {}
    if not isinstance(types, dict) or not types:
        return "activity"
    ranked = sorted(types.items(), key=lambda kv: -kv[1])
    return next((k for k, _ in ranked if k != "other"), ranked[0][0])


def _summary_line(e: dict, kind: str = "new", rule: str = "default", prev: float | None = None) -> str:
    heads = e.get("summaries") or []
    if isinstance(heads, str):
        try:
            heads = json.loads(heads)
        except ValueError:
            heads = []
    head = f' — "{str(heads[0])[:120]}"' if heads else ""
    lat, lon = round(e.get("lat", 0), 4), round(e.get("lon", 0), 4)
    tag = "" if rule == "default" else f" [{rule}]"
    if kind == "update":
        lead = (f"▲ Panoptes update{tag}: {_top_type(e)} now {e.get('latest_score')} "
                f"(was {prev}) x{e.get('latest_count')}")
    else:
        lead = (f"⚠ Panoptes surge{tag} {e.get('latest_score')}: {_top_type(e)} "
                f"x{e.get('latest_count')}")
    trend = f", {e['trend']}" if e.get("trend") in ("rising", "falling") else ""
    return (f"{lead} ({e.get('source_count')} source kinds{trend}) at "
            f"{round(lat, 3)},{round(lon, 3)}{head} {_SITE}/#v={lat},{lon},12")


def _post(client: httpx.Client, url: str, text: str, event: dict | None = None) -> bool:
    body = ({"content": text[:1900]} if "discord.com/api/webhooks" in url
            else {"text": text, **({"event": event} if event else {})})
    try:
        r = client.post(url, json=json.loads(json.dumps(body, default=str)))
        r.raise_for_status()
        return True
    except httpx.HTTPError as err:
        log.warning("webhook failed: %s", err)
        return False


def send_pending() -> int:
    """Evaluate the rules over active events; returns how many messages were sent."""
    from apb.alerts.rules import load_rules, matches
    from apb.store import alertlog
    from apb.store import events as event_store
    rules = load_rules()
    active = event_store.query(max_age_hours=1.0, limit=500)
    if not active:
        return 0
    now = time.time()
    sent = 0
    first_alerted: list[str] = []
    with httpx.Client(timeout=10.0) as client:
        for e in active:
            for rule in rules:
                if not matches(rule, e):
                    continue
                prev = alertlog.last(e["uid"], rule.name)
                if prev is None:
                    # judged on where the event is now, not on a peak it may have left
                    if (e.get("latest_score") or 0) < rule.min_score:
                        continue
                    kind = "new"
                    if e.get("notified") == 1:      # alerted before the log existed
                        alertlog.add(e["uid"], rule.name, e.get("latest_score") or 0, "legacy", False)
                        continue
                else:
                    if ((e.get("latest_score") or 0) < (prev["score"] or 0) * rule.escalate
                            or now - prev["ts"] < _MIN_UPDATE_GAP_SEC):
                        continue
                    kind = "update"
                url = rule.webhook or _url()
                line = _summary_line(e, kind, rule.name, prev["score"] if prev else None)
                ok = _post(client, url, line, e) if url else False
                if url and not ok:
                    continue                        # not logged: retried next cycle
                score = e.get("latest_score")
                alertlog.add(e["uid"], rule.name, score or 0, kind, ok, line)
                if kind == "new":
                    alertlog.open_outcome(e, rule.name)
                    first_alerted.append(e["uid"])
                sent += 1 if ok else 0
    if first_alerted:
        try:
            event_store.mark_notified(sorted(set(first_alerted)))
        except Exception as err:                    # the log is the source of truth now
            log.info("mark_notified skipped: %s", err)
    if sent:
        log.info("notified %d event message(s)", sent)
    return sent


def send_ops(changes: list[dict]) -> bool:
    """One message for this cycle's feed-health transitions."""
    url = os.environ.get("APB_OPS_WEBHOOK_URL", "").strip() or _url()
    if not url or not changes:
        return False
    broke = [c for c in changes if c["to"] != "ok"]
    healed = [c for c in changes if c["to"] == "ok"]
    lines = []
    if broke:
        lines.append(f"🩺 Panoptes feeds: {len(broke)} went dark")
        lines += [f"• {c['name'] or c['slug']} ({c['kind']}) {c['to']}: {c['reason']}"
                  for c in broke[:12]]
        if len(broke) > 12:
            lines.append(f"• …and {len(broke) - 12} more")
    if healed:
        lines.append(f"✅ recovered: " + ", ".join((c["name"] or c["slug"]) for c in healed[:12])
                     + (f" +{len(healed) - 12}" if len(healed) > 12 else ""))
    lines.append(f"{_SITE}/status/health")
    with httpx.Client(timeout=10.0) as client:
        return _post(client, url, "\n".join(lines))


def digest_text(hours: float = 24.0) -> str:
    """The daily digest body: top events, alert results, feed health."""
    from apb.store import alertlog, feedhealth
    from apb.store import events as event_store
    rows = event_store.query(max_age_hours=hours, limit=1000)
    rows.sort(key=lambda r: -(r.get("peak_score") or 0))
    corroborated = [r for r in rows if (r.get("source_count") or 0) >= 2]
    lines = [f"📋 Panoptes — last {int(hours)} h: {len(rows)} events, "
             f"{len(corroborated)} corroborated by 2+ source kinds"]
    for r in (corroborated or rows)[:6]:
        lines.append(f"• {r.get('peak_score')} {_top_type(r)} x{r.get('peak_count')} "
                     f"({r.get('source_count')} kinds, {round((r.get('age_min') or 0) / 60, 1)} h) "
                     f"{_SITE}/#v={round(r['lat'], 4)},{round(r['lon'], 4)},12")
    card = alertlog.scorecard(days=hours / 24.0)
    if card["alerts"]:
        lead = card["median_lead_min"]
        lines.append(f"Alerts: {card['alerts']} — {card['confirmed']} ahead of news, "
                     f"{card['trailing']} behind, {card['unconfirmed']} unconfirmed, "
                     f"{card['pending']} open"
                     + (f"; median lead {round(lead)} min" if lead is not None else ""))
    health = feedhealth.summary(limit=5)
    if health["feeds"]:
        bad = health["problems_total"]
        lines.append(f"Feeds: {health['feeds'] - bad}/{health['feeds']} healthy"
                     + (" — dark: " + ", ".join(p["name"] or p["slug"] for p in health["problems"])
                        if bad else ""))
    return "\n".join(lines)


def send_digest_if_due(now: float | None = None) -> bool:
    """Send the digest once per UTC day at/after APB_DIGEST_HOUR_UTC."""
    raw = os.environ.get("APB_DIGEST_HOUR_UTC", "").strip()
    url = _url()
    if not raw or not url:
        return False
    try:
        hour = int(raw) % 24
    except ValueError:
        return False
    from apb.store import alertlog
    t = time.gmtime(now or time.time())
    day = time.strftime("%Y-%m-%d", t)
    if t.tm_hour < hour or alertlog.kv_get("digest_day") == day:
        return False
    with httpx.Client(timeout=10.0) as client:
        ok = _post(client, url, digest_text())
    if ok:
        alertlog.kv_set("digest_day", day)
    return ok
