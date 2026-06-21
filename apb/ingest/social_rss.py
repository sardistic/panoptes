"""Keyless social ingestion via RSS/Atom — broadens the social lane beyond Bluesky.

Reddit (per-subreddit `/new/.rss`) and Mastodon (per-instance `/tags/<tag>.rss`) both
expose public Atom/RSS without auth. Posts are geo-resolved with the same coarse place
matcher used for Bluesky/news, so local chatter can corroborate a CAD/radio spike in the
same place/time.

stdlib-only XML parsing (handles both Atom <entry> and RSS <item>). Returns loose dict
rows shaped for apb.fusion.sources.social_text_signals (source_kind="social"); see
apb.fusion.social_store.start_rss.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

import httpx

_UA = {"User-Agent": "Mozilla/5.0 (compatible; apb-social/0.1; +panoptes.run)"}

# Local-news / safety subreddits tend to surface breaking incidents fast.
DEFAULT_SUBREDDITS = (
    "news", "CrimeScene", "Scanner", "DispatchAudio", "NewsOfTheStupid",
)
# Mastodon instance + hashtag pairs. (instance host, tag)
DEFAULT_MASTODON = (
    ("mastodon.social", "breakingnews"),
    ("mastodon.social", "wildfire"),
)

_TAG = re.compile(r"<[^>]+>")
_NS = re.compile(r"\{.*?\}")           # strip XML namespaces from tags


def _clean(s: str | None) -> str:
    return _TAG.sub("", s or "").strip()


def _local(tag: str) -> str:
    return _NS.sub("", tag)


def _ts(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    try:                               # Atom: ISO 8601
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).isoformat()
    except ValueError:
        pass
    try:                               # RSS: RFC 822
        dt = parsedate_to_datetime(s)
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).isoformat()
    except (TypeError, ValueError):
        return None


def _link(node) -> str:
    # RSS <link>text</link>, or Atom <link href="..."/>.
    for ch in node:
        if _local(ch.tag) == "link":
            return (ch.get("href") or ch.text or "").strip()
    return ""


class SocialRSS:
    def __init__(self):
        self._client = httpx.Client(timeout=15.0, headers=_UA, follow_redirects=True)

    def _parse(self, xml: str, source: str) -> list[dict]:
        out: list[dict] = []
        try:
            root = ET.fromstring(xml)
        except ET.ParseError:
            return out
        for node in root.iter():
            if _local(node.tag) not in ("item", "entry"):
                continue
            fields = {_local(c.tag): c for c in node}
            title = _clean(fields["title"].text if "title" in fields else None)
            body = _clean((fields.get("summary") or fields.get("content")
                           or fields.get("description")).text
                          if (fields.get("summary") or fields.get("content")
                              or fields.get("description")) is not None else None)
            if title and body and body != title:
                text = f"{title} — {body}"
            else:
                text = title or body
            if not text:
                continue
            pub = (fields.get("published") or fields.get("updated")
                   or fields.get("pubDate"))
            out.append({
                "source": source, "source_kind": "social", "text": text[:400],
                "url": _link(node), "confidence": 0.3,
                "created_at": _ts(pub.text if pub is not None else None),
            })
        return out

    def _get(self, url: str, source: str, max_items: int) -> list[dict]:
        try:
            r = self._client.get(url)
            r.raise_for_status()
        except httpx.HTTPError as e:
            print(f"[social_rss] {source} failed: {e}")
            return []
        return self._parse(r.text, source)[:max_items]

    def subreddit(self, sub: str, max_items: int = 25) -> list[dict]:
        return self._get(f"https://www.reddit.com/r/{sub}/new/.rss",
                         f"reddit:{sub}", max_items)

    def mastodon_tag(self, host: str, tag: str, max_items: int = 25) -> list[dict]:
        return self._get(f"https://{host}/tags/{tag}.rss",
                         f"mastodon:{tag}", max_items)

    def collect(self, subreddits: tuple[str, ...] = DEFAULT_SUBREDDITS,
                mastodon: tuple[tuple[str, str], ...] = DEFAULT_MASTODON) -> list[dict]:
        """Pull every configured subreddit + Mastodon tag; dedupe by URL/text."""
        seen: set[str] = set()
        out: list[dict] = []
        for sub in subreddits:
            for row in self.subreddit(sub):
                key = row.get("url") or row["text"]
                if key in seen:
                    continue
                seen.add(key)
                out.append(row)
        for host, tag in mastodon:
            for row in self.mastodon_tag(host, tag):
                key = row.get("url") or row["text"]
                if key in seen:
                    continue
                seen.add(key)
                out.append(row)
        return out
