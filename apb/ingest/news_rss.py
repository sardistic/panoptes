"""Keyless news ingestion via RSS — local-granularity correlation signal.

GDELT (apb.context.gdelt) is great for global/event-typed news but coarse locally.
This pulls Google News RSS search feeds (free, no key) plus any extra local-outlet
feeds, so breaking local-news chatter can corroborate a CAD/radio spike in the same
place/time. Items are geo-resolved with the same coarse place matcher used for social.

stdlib-only XML parsing (no feedparser dependency). Returns loose dict rows shaped for
apb.fusion.sources.social_text_signals (source_kind="news").
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

import httpx

import logging

log = logging.getLogger(__name__)

_UA = {"User-Agent": "Mozilla/5.0 apb-news/0.1 (panoptes.run)"}
_GNEWS = "https://news.google.com/rss/search"

# Breaking-safety search terms; each becomes a Google News RSS query. Kept tight to
# event-bearing topics so the buffer stays high-signal.
DEFAULT_QUERIES = (
    "shooting", "police standoff", "active shooter", "explosion", "evacuation",
    "wildfire", "shelter in place", "mass casualty", "officer involved",
    "building fire", "hazmat", "police chase",
)

# Optional fixed local-outlet RSS feeds (add reliable regional outlets here).
LOCAL_FEEDS: tuple[str, ...] = ()

_TAG = re.compile(r"<[^>]+>")


def _clean(s: str | None) -> str:
    return _TAG.sub("", s or "").strip()


def _pubdate(s: str | None) -> str | None:
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).isoformat()
    except (TypeError, ValueError):
        return None


class NewsRSS:
    def __init__(self):
        self._client = httpx.Client(timeout=15.0, headers=_UA, follow_redirects=True)

    def _parse(self, xml: str, source: str) -> list[dict]:
        out: list[dict] = []
        try:
            root = ET.fromstring(xml)
        except ET.ParseError:
            return out
        for item in root.iter("item"):
            title = _clean(item.findtext("title"))
            if not title:
                continue
            out.append({
                "source": source, "source_kind": "news", "text": title,
                "url": (item.findtext("link") or "").strip(),
                "created_at": _pubdate(item.findtext("pubDate")),
                "confidence": 0.4,
            })
        return out

    def query(self, q: str, max_items: int = 20) -> list[dict]:
        try:
            r = self._client.get(_GNEWS, params={"q": q, "hl": "en-US",
                                                 "gl": "US", "ceid": "US:en"})
            r.raise_for_status()
        except httpx.HTTPError as e:
            log.warning(f"query '{q}' failed: {e}")
            return []
        return self._parse(r.text, f"gnews:{q}")[:max_items]

    def feed(self, url: str, max_items: int = 20) -> list[dict]:
        try:
            r = self._client.get(url)
            r.raise_for_status()
        except httpx.HTTPError as e:
            log.warning(f"feed {url} failed: {e}")
            return []
        return self._parse(r.text, f"rss:{url}")[:max_items]

    def collect(self, queries: tuple[str, ...] = DEFAULT_QUERIES,
                feeds: tuple[str, ...] = LOCAL_FEEDS) -> list[dict]:
        """Pull every configured query + local feed; dedupe by URL."""
        seen: set[str] = set()
        out: list[dict] = []
        for q in queries:
            for row in self.query(q):
                u = row.get("url") or row["text"]
                if u in seen:
                    continue
                seen.add(u)
                out.append(row)
        for f in feeds:
            for row in self.feed(f):
                u = row.get("url") or row["text"]
                if u in seen:
                    continue
                seen.add(u)
                out.append(row)
        return out
