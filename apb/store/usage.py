"""Model usage meter — what every Gemini call cost, by feature, per day.

Each successful call records its token counts from the response's usageMetadata
(input split by modality: text, image, audio; output including any thinking tokens)
against a feature name ("camera_sampler", "radio_stt", "sitrep", ...) and the model
that answered. `summary()` turns that into estimated dollars with a price table that
can be overridden per deployment, because prices change and differ by model:

  APB_PRICE_IN_TEXT   USD per million input text / image / video tokens (0.30)
  APB_PRICE_IN_AUDIO  USD per million input audio tokens (1.00)
  APB_PRICE_OUT       USD per million output tokens, thinking included (2.50)

Defaults are the Gemini 2.5 Flash list prices as reported at the time of writing.
These are estimates; the provider's billing page is the source of truth. A key on
the free tier costs nothing. SQLite in the snapshot store's file.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time

from apb.store import snapshots

log = logging.getLogger(__name__)
_lock = snapshots.db_lock
_ready = False


def _conn() -> sqlite3.Connection:
    global _ready
    c = snapshots.conn()
    if not _ready or c is not _ready:          # re-check whenever the connection changes
        c.execute("""
        CREATE TABLE IF NOT EXISTS llm_usage (
            day TEXT NOT NULL, feature TEXT NOT NULL, model TEXT NOT NULL,
            calls INTEGER DEFAULT 0, in_text INTEGER DEFAULT 0, in_image INTEGER DEFAULT 0,
            in_audio INTEGER DEFAULT 0, out INTEGER DEFAULT 0,
            PRIMARY KEY (day, feature, model))""")
        c.commit()
        _ready = c
    return c


def prices() -> dict[str, float]:
    def f(name: str, default: float) -> float:
        try:
            return float(os.environ.get(name, default))
        except ValueError:
            return default
    return {"in_text": f("APB_PRICE_IN_TEXT", 0.30), "in_audio": f("APB_PRICE_IN_AUDIO", 1.00),
            "out": f("APB_PRICE_OUT", 2.50)}


def split(usage: dict) -> dict[str, int]:
    """Token counts by kind from a Gemini usageMetadata object."""
    out = {"in_text": 0, "in_image": 0, "in_audio": 0, "out": 0}
    details = usage.get("promptTokensDetails") or []
    for d in details:
        n = int(d.get("tokenCount") or 0)
        m = str(d.get("modality", "")).upper()
        if m == "AUDIO":
            out["in_audio"] += n
        elif m in ("IMAGE", "VIDEO", "DOCUMENT"):
            out["in_image"] += n
        else:
            out["in_text"] += n
    if not details:
        out["in_text"] = int(usage.get("promptTokenCount") or 0)
    out["out"] = int(usage.get("candidatesTokenCount") or 0) + int(usage.get("thoughtsTokenCount") or 0)
    return out


def record(feature: str, model: str, usage: dict, now: float | None = None) -> None:
    """Add one call. Never raises: a meter must not break the feature it measures."""
    try:
        t = split(usage or {})
        day = time.strftime("%Y-%m-%d", time.gmtime(now or time.time()))
        with _lock:
            c = _conn()
            c.execute("""
                INSERT INTO llm_usage (day, feature, model, calls, in_text, in_image, in_audio, out)
                VALUES (?,?,?,1,?,?,?,?)
                ON CONFLICT(day, feature, model) DO UPDATE SET calls = calls + 1,
                    in_text = in_text + excluded.in_text, in_image = in_image + excluded.in_image,
                    in_audio = in_audio + excluded.in_audio, out = out + excluded.out""",
                      (day, feature[:40], (model or "?")[:60], t["in_text"], t["in_image"],
                       t["in_audio"], t["out"]))
            c.commit()
    except Exception as e:
        log.info("usage meter: %s", e)


def _usd(row: dict, p: dict) -> float:
    return ((row["in_text"] + row["in_image"]) * p["in_text"] + row["in_audio"] * p["in_audio"]
            + row["out"] * p["out"]) / 1_000_000


def summary(days: int = 30) -> dict:
    """Per day and per feature, with estimated USD; newest day first."""
    since = time.strftime("%Y-%m-%d", time.gmtime(time.time() - (days - 1) * 86400))
    with _lock:
        rows = [dict(r) for r in _conn().execute(
            "SELECT * FROM llm_usage WHERE day >= ? ORDER BY day DESC", (since,))]
    p = prices()
    by_day: dict[str, dict] = {}
    by_feature: dict[str, dict] = {}
    for r in rows:
        usd = _usd(r, p)
        d = by_day.setdefault(r["day"], {"day": r["day"], "calls": 0, "usd": 0.0, "features": {}})
        d["calls"] += r["calls"]
        d["usd"] += usd
        f = d["features"].setdefault(r["feature"], {"calls": 0, "usd": 0.0})
        f["calls"] += r["calls"]
        f["usd"] += usd
        g = by_feature.setdefault(r["feature"], {"calls": 0, "in_text": 0, "in_image": 0,
                                                 "in_audio": 0, "out": 0, "usd": 0.0, "models": set()})
        for k in ("calls", "in_text", "in_image", "in_audio", "out"):
            g[k] += r[k]
        g["usd"] += usd
        g["models"].add(r["model"])
    for d in by_day.values():
        d["usd"] = round(d["usd"], 4)
        for f in d["features"].values():
            f["usd"] = round(f["usd"], 4)
    for g in by_feature.values():
        g["usd"] = round(g["usd"], 4)
        g["models"] = sorted(g["models"])
    today = time.strftime("%Y-%m-%d", time.gmtime())
    total = round(sum(d["usd"] for d in by_day.values()), 4)
    full_days = [d for d in by_day.values() if d["day"] != today]
    return {
        "days": days, "prices_per_million": p, "total_usd": total,
        "today_usd": by_day.get(today, {}).get("usd", 0.0),
        "avg_day_usd": round(sum(d["usd"] for d in full_days) / len(full_days), 4) if full_days else None,
        "by_feature": dict(sorted(by_feature.items(), key=lambda kv: -kv[1]["usd"])),
        "by_day": list(by_day.values()),
        "note": "Estimates from token counts and the price table above; the provider's billing "
                "page is authoritative, and a free-tier key costs nothing.",
    }
