"""Email through Resend (https://resend.com) — the API platform's notices.

  RESEND_API_KEY          Resend API key ("re_..."; sending access is enough)
  APB_MAIL_FROM           sender on a domain verified in Resend
                          (default "Panoptes API <api@panoptes.run>")
  APB_MAIL_REPLY_TO       where replies go (default api@panoptes.run)
  APB_OWNER_EMAIL         the owner's inbox for new-application notices
  APB_API_NOTIFY_WEBHOOK  optional: owner notices also go to this webhook (Discord gets `content`)

Sending happens on a short-lived thread so a slow mail API never delays a request.
With no key configured a notice is only logged; applications still wait in the admin
queue and applicants still have their claim link on the page.
"""
from __future__ import annotations

import logging
import os
import threading

import httpx

log = logging.getLogger(__name__)
_API = "https://api.resend.com/emails"
_sent = {"ok": 0, "failed": 0, "last_error": None}


def _from() -> str:
    return os.environ.get("APB_MAIL_FROM", "").strip() or "Panoptes API <api@panoptes.run>"


def reply_to() -> str:
    return os.environ.get("APB_MAIL_REPLY_TO", "").strip() or "api@panoptes.run"


def configured() -> dict:
    return {"email": bool(os.environ.get("RESEND_API_KEY", "").strip()),
            "owner": bool(os.environ.get("APB_OWNER_EMAIL", "").strip()),
            "webhook": bool(os.environ.get("APB_API_NOTIFY_WEBHOOK", "").strip()), **_sent}


def _resend(to: str, subject: str, body: str) -> bool:
    key = os.environ.get("RESEND_API_KEY", "").strip()
    if not key or not to:
        return False
    r = httpx.post(_API, headers={"Authorization": f"Bearer {key}"}, timeout=20, json={
        "from": _from(), "to": [to], "reply_to": reply_to(), "subject": subject[:180], "text": body})
    if r.status_code >= 300:
        raise RuntimeError(f"resend {r.status_code}: {r.text[:160]}")
    return True


def _webhook(subject: str, body: str) -> bool:
    url = os.environ.get("APB_API_NOTIFY_WEBHOOK", "").strip()
    if not url:
        return False
    text = f"**{subject}**\n{body}"
    payload = {"content": text[:1900]} if "discord.com/api/webhooks" in url else {"text": text}
    httpx.post(url, json=payload, timeout=15).raise_for_status()
    return True


def _run(jobs) -> None:
    for fn, args in jobs:
        try:
            if fn(*args):
                _sent["ok"] += 1
        except Exception as e:                       # never let a notice break the caller
            _sent["failed"] += 1
            _sent["last_error"] = f"{fn.__name__}: {str(e)[:160]}"
            log.warning("mail via %s failed: %s", fn.__name__, e)


def _go(jobs, wait: bool) -> None:
    if wait:
        _run(jobs)
    else:
        threading.Thread(target=_run, args=(jobs,), daemon=True, name="mail").start()


def notify_owner(subject: str, body: str, wait: bool = False) -> None:
    log.info("owner notice: %s", subject)
    owner = os.environ.get("APB_OWNER_EMAIL", "").strip()
    _go([(_resend, (owner, subject, body)), (_webhook, (subject, body))], wait)


def send(to: str, subject: str, body: str, wait: bool = False) -> None:
    """Email one applicant (plain text, from APB_MAIL_FROM, replies to APB_MAIL_REPLY_TO)."""
    log.info("mail to applicant: %s", subject)
    _go([(_resend, (to, subject, body))], wait)
