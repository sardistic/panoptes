"""Unified web-search backend for source discovery dorks.

Two backends, auto-selected:
- google_cse  — Google Programmable Search JSON API (if GOOGLE_API_KEY + GOOGLE_CX
  are set). Highest quality, 100 queries/day free.
- ddg         — keyless DuckDuckGo HTML endpoint (POST + result__a scrape). No key
  required, so dorking works out of the box; lower volume / occasional throttling.

Everything in apb.discover that needs "find me hosts matching this dork" goes through
search() here, so the backend choice is a single decision, not per-caller.

Usage:
  from apb.discover.websearch import search
  for url in search('site:policetocitizen.com inurl:CADCalls'):
      ...
"""
from __future__ import annotations

import re
import time
import urllib.parse as _up

import httpx

from apb.common.config import settings

_CSE = "https://www.googleapis.com/customsearch/v1"
_DDG = "https://html.duckduckgo.com/html/"
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")
_HEADERS = {"User-Agent": _UA, "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9"}

# result anchors in the DDG HTML endpoint (and the redirect-wrapped variant)
_DDG_A = re.compile(r'result__a[^>]*href="([^"]+)"')
_DDG_REDIR = re.compile(r'uddg=([^&"]+)')


def backend() -> str:
    return "google_cse" if (settings.google_api_key and settings.google_cx) else "ddg"


def _google(query: str, max_results: int) -> list[str]:
    client = httpx.Client(timeout=20.0)
    urls: list[str] = []
    for start in range(1, min(max_results, 100) + 1, 10):
        r = client.get(_CSE, params={"key": settings.google_api_key,
                                     "cx": settings.google_cx, "q": query, "start": start})
        if r.status_code != 200:
            print(f"[cse] stop at start={start}: {r.status_code} {r.text[:120]}")
            break
        items = r.json().get("items", [])
        urls += [it["link"] for it in items]
        if len(items) < 10:
            break
        time.sleep(0.3)
    return urls[:max_results]


def _unwrap(href: str) -> str:
    """DDG sometimes wraps results in /l/?uddg=<encoded> redirects."""
    if href.startswith("//duckduckgo.com/l/") or "uddg=" in href:
        m = _DDG_REDIR.search(href)
        if m:
            return _up.unquote(m.group(1))
    return href


def _ddg(query: str, max_results: int, pages: int) -> list[str]:
    client = httpx.Client(timeout=20.0, headers=_HEADERS, follow_redirects=True)
    seen: list[str] = []
    for p in range(pages):
        data = {"q": query, "kl": "us-en"}
        if p:                       # crude pagination via result offset
            data["s"] = str(p * 25)
            data["dc"] = str(p * 25 + 1)
        try:
            r = client.post(_DDG, data=data)
        except httpx.HTTPError as e:
            print(f"[ddg] '{query[:40]}' p{p} failed: {e}")
            break
        hrefs = [_unwrap(h) for h in _DDG_A.findall(r.text)]
        hrefs += [_up.unquote(u) for u in _DDG_REDIR.findall(r.text)]
        new = [h for h in hrefs if h.startswith("http") and h not in seen]
        seen += new
        if len(new) < 5 or len(seen) >= max_results:   # last page reached
            break
        time.sleep(1.2)             # be polite; DDG throttles aggressively
    # de-dup preserving order
    out, s = [], set()
    for u in seen:
        if u not in s:
            s.add(u)
            out.append(u)
    return out[:max_results]


def search(query: str, max_results: int = 30, pages: int = 3) -> list[str]:
    """Return result URLs for a search/dork query, via the best available backend."""
    if backend() == "google_cse":
        return _google(query, max_results)
    return _ddg(query, max_results, pages)
