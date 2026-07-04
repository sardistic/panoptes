"""Push notifications for high-score fused events.

Set APB_WEBHOOK_URL to any HTTP endpoint; new events whose surge score crosses
APB_ALERT_SCORE (default 6.0) are POSTed exactly once (the event registry marks
them notified). Discord webhook URLs get the Discord `content` wrapper; anything
else receives the raw event JSON. No URL configured = no-op.
"""
from __future__ import annotations

import json
import logging
import os

import httpx

log = logging.getLogger(__name__)


def _config() -> tuple[str | None, float]:
    return (os.environ.get("APB_WEBHOOK_URL", "").strip() or None,
            float(os.environ.get("APB_ALERT_SCORE", "6.0")))


def _summary_line(e: dict) -> str:
    types = e.get("types") or {}
    top = max(types, key=types.get) if isinstance(types, dict) and types else "activity"
    heads = e.get("summaries") or []
    head = f' — "{heads[0][:120]}"' if heads else ""
    return (f"⚠ Panoptes surge {e.get('peak_score')}: {top} x{e.get('latest_count')} "
            f"({e.get('source_count')} source kinds) at "
            f"{round(e.get('lat', 0), 3)},{round(e.get('lon', 0), 3)}{head} "
            f"https://panoptes.run/#{round(e.get('lat', 0), 4)},{round(e.get('lon', 0), 4)}")


def send_pending() -> int:
    """POST unnotified events over the threshold. Returns how many were sent."""
    url, min_score = _config()
    if not url:
        return 0
    from apb.store import events as event_store
    pending = event_store.unnotified(min_score)
    if not pending:
        return 0
    sent: list[str] = []
    with httpx.Client(timeout=10.0) as client:
        for e in pending:
            body = ({"content": _summary_line(e)} if "discord.com/api/webhooks" in url
                    else {"text": _summary_line(e), "event": e})
            try:
                r = client.post(url, json=json.loads(json.dumps(body, default=str)))
                r.raise_for_status()
                sent.append(e["uid"])
            except httpx.HTTPError as err:
                log.warning("webhook failed for %s: %s", e["uid"], err)
    event_store.mark_notified(sent)
    if sent:
        log.info("notified %d event(s)", len(sent))
    return len(sent)
