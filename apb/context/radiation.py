"""Radiation lane: EPA RadNet + Safecast sensors, per-sensor baselines, and an LLM digest.

Every EVERY seconds (default 900) one conditional GET fetches all Safecast real-time
sensors (apb/ingest/safecast.py), and a few EPA RadNet stations are refreshed in rotation
(apb/ingest/radnet.py; each about every APB_RADNET_EVERY_H = 6 h, a station's first fetch
backfilling the cap period). New readings fold into hourly buckets
(apb/store/radiation.py) kept for KEEP_DAYS (default 30). Background radiation differs a lot between places, so
each sensor is judged only against its own history:

  baseline  median and MAD of the sensor's hourly means over the cap period, leaving
            out the last 6 h; needs MIN_HOURS hours, else the sensor is "learning"
  elevated  the last two hourly means both >= max(1.5 x median, median + 0.05 µSv/h,
            median + 4 MAD): sustained, so one noisy reading or a short rain washout
            bump does not count
  high      >= 1.0 µSv/h sustained, whatever the baseline (far above any environment)
  corroborated  another sensor within 30 km is also >= 1.2 x its own baseline

Elevated sensors become hazard rows (kind "safecast") that the map and fusion read.

The digest: a cheap text model (pinned Flash-Lite) gets compact statistics, not raw
data: sensors reporting, network medians now / 24 h / 7 d / cap period, elevated
sensors by place, and its own last summaries, and writes a headline, a short summary,
what changed, and a level. It runs only when the picture changed (a digest of the
state) and at least MIN_GAP apart, or once a day as a heartbeat, within
APB_RAD_SUMMARY_DAILY calls (default 8). Summaries are kept for the cap period, so
/live/radiation/summary shows how things moved.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import statistics
import threading
import time

log = logging.getLogger(__name__)

EVERY = float(os.environ.get("APB_RAD_EVERY", "900"))
KEEP_DAYS = float(os.environ.get("APB_RAD_KEEP_DAYS", "30"))
MODEL = os.environ.get("APB_RAD_MODEL", "gemini-3.5-flash-lite")
MIN_HOURS = 48
RECENT_H = 6                       # left out of the baseline (the period being judged)
MIN_GAP = 2 * 3600.0               # between summaries when things change
HEARTBEAT = 24 * 3600.0            # a summary at least this often
FRESH = 3 * 3600.0                 # a Safecast sensor counts as reporting if heard within this
NEAR_KM = 30.0
RADNET_EVERY_H = float(os.environ.get("APB_RADNET_EVERY_H", "6"))   # each RadNet station refreshed this often
RADNET_FRESH = (RADNET_EVERY_H + 3) * 3600.0

_stop = threading.Event()
_thread: threading.Thread | None = None
_state: dict = {"sensors": [], "at": 0.0, "baseline_at": 0.0, "baselines": {}}
_stats = {"polls": 0, "not_modified": 0, "new_readings": 0, "errors": 0, "summaries": 0,
          "summary_skips": 0, "budget_stops": 0, "last_poll": 0.0, "last_error": None}


def off() -> bool:
    return os.environ.get("APB_RAD_OFF", "").strip().lower() in ("1", "true", "yes", "on")


def daily_cap() -> int:
    try:
        return max(0, int(os.environ.get("APB_RAD_SUMMARY_DAILY", "8")))
    except ValueError:
        return 8


def _km(a: tuple, b: tuple) -> float:
    return math.hypot(a[0] - b[0], (a[1] - b[1]) * math.cos(math.radians((a[0] + b[0]) / 2))) * 111.0


def _place(lat: float, lon: float) -> str | None:
    try:
        from apb.api.main import _place_name
        return _place_name(lat, lon)
    except Exception:
        return None


# ---- assessment -----------------------------------------------------------------------

def baselines(now: float) -> dict[str, tuple[float, float, int]]:
    """device -> (median, MAD, hours) of hourly means over the cap period, minus the last RECENT_H."""
    from apb.store import radiation as store
    cut = int(now // 3600) - RECENT_H
    by: dict[str, list[float]] = {}
    for dev, hour, mean, _mx in store.hourly(int((now - KEEP_DAYS * 86400) // 3600)):
        if hour < cut:
            by.setdefault(dev, []).append(mean)
    out = {}
    for dev, v in by.items():
        med = statistics.median(v)
        out[dev] = (med, statistics.median(abs(x - med) for x in v), len(v))
    return out


def assess(now: float | None = None) -> list[dict]:
    """Every sensor heard within FRESH, with its baseline, ratio and status."""
    from apb.store import radiation as store
    now = now or time.time()
    if now - _state["baseline_at"] > 3600 or not _state["baselines"]:
        _state["baselines"], _state["baseline_at"] = baselines(now), now
    base = _state["baselines"]
    by_hour: dict[str, dict[int, float]] = {}
    for dev, hour, mean, _mx in store.hourly(int((now - RADNET_FRESH) // 3600) - 1):
        by_hour.setdefault(dev, {})[hour] = mean
    names = _radnet_names()
    out = []
    for s in store.sensors(now - max(FRESH, RADNET_FRESH)):
        radnet = s["device"].startswith("radnet:")
        if not radnet and s["last_ts"] < now - FRESH:
            continue
        b = base.get(s["device"])
        h_last = int(s["last_ts"] // 3600)                    # the sensor's own last two hours
        hrs = by_hour.get(s["device"], {})
        r = [hrs[h] for h in (h_last - 1, h_last) if h in hrs] or [s["last_usvh"]]
        sustained = min(r) if len(r) >= 2 else None
        row = {"id": s["device"], "source": "radnet" if radnet else "safecast",
               "name": names.get(s["device"]), "lat": round(s["lat"], 2), "lon": round(s["lon"], 2),
               "usvh": round(s["last_usvh"], 3), "cpm": s["last_cpm"], "tube": s["tube"], "ts": s["last_ts"],
               "baseline": None, "ratio": None, "status": "learning", "corroborated": False}
        if b and b[2] >= MIN_HOURS:
            med, mad, _n = b
            thr = max(1.5 * med, med + 0.05, med + 4 * mad)
            row["baseline"] = round(med, 3)
            row["ratio"] = round((sum(r) / len(r)) / med, 2) if med > 0 else None
            row["status"] = "elevated" if sustained is not None and sustained >= thr else "normal"
            if row["status"] == "elevated" and sustained >= 1.0:
                row["status"] = "high"                      # far above any environment, and above its own normal
        elif sustained is not None and sustained >= 5.0:
            row["status"] = "high"                          # no baseline yet: only the extreme counts
        out.append(row)
    hot = [x for x in out if x["status"] in ("elevated", "high")]
    for x in hot:
        x["corroborated"] = any(y is not x and (y["ratio"] or 0) >= 1.2 and _km((x["lat"], x["lon"]), (y["lat"], y["lon"])) <= NEAR_KM
                                for y in out)
    return out


def hazard_rows() -> list[dict]:
    """Elevated sensors as incident rows for the map's hazard layer and fusion (no upstream call)."""
    out = []
    for x in _state["sensors"]:
        if x["status"] not in ("elevated", "high"):
            continue
        threat = 0.8 if x["status"] == "high" or (x["corroborated"] and (x["ratio"] or 0) >= 2) else \
            0.6 if x["corroborated"] else 0.45
        place = x.get("name") or _place(x["lat"], x["lon"])
        ratio = f", {x['ratio']}× this sensor's normal" if x["ratio"] else ""
        out.append({
            "call_id": f"safecast:{x['id']}", "metro": "safecast", "type": "public", "source": "safecast",
            "summary": f"Radiation {x['usvh']:.2f} µSv/h{ratio}" + (" — nearby sensor agrees" if x["corroborated"] else "")
                       + (f" ({place})" if place else ""),
            "location": place, "lat": x["lat"], "lon": x["lon"], "ts": x["ts"], "threat_score": threat,
            "sentiment": "urgent" if threat >= 0.8 else "elevated", "emerging": threat >= 0.8,
            "usvh": x["usvh"], "baseline": x["baseline"], "ratio": x["ratio"], "status": x["status"],
            "corroborated": x["corroborated"],
            "url": "https://www.epa.gov/radnet/radnet-near-real-time-air-data" if x["source"] == "radnet"
                   else "https://map.safecast.org/"})
    return out


# ---- digest ---------------------------------------------------------------------------

def _means(hours_ago_lo: int, hours_ago_hi: int, now: float) -> dict[str, float]:
    """device -> mean of its hourly means between hours_ago_hi and hours_ago_lo ago."""
    from apb.store import radiation as store
    h_now = int(now // 3600)
    per: dict[str, list[float]] = {}
    for dev, hour, mean, _mx in store.hourly(h_now - hours_ago_hi):
        if hour <= h_now - hours_ago_lo:
            per.setdefault(dev, []).append(mean)
    return {d: sum(v) / len(v) for d, v in per.items()}


def _change(now_means: dict[str, float], then: dict[str, float]) -> dict | None:
    """Median ratio now / then over sensors present in both — the same sensors compared,
    so a network that gained or lost sensors does not look like a change."""
    r = [now_means[d] / then[d] for d in now_means.keys() & then.keys() if then[d] > 0]
    return {"median_ratio": round(statistics.median(r), 3), "sensors_compared": len(r)} if r else None


def stats(now: float, sensors: list[dict]) -> dict:
    hot = sorted((x for x in sensors if x["status"] in ("elevated", "high")),
                 key=lambda x: -(x["ratio"] or x["usvh"]))
    us = [x for x in sensors if 18 < x["lat"] < 72 and -170 < x["lon"] < -50]
    cur = _means(0, int(RADNET_EVERY_H) + 4, now)              # each sensor's recent level
    ratios = [x["ratio"] for x in sensors if x["ratio"]]
    return {
        "reporting": len(sensors), "reporting_north_america": len(us),
        "epa_radnet_us": sum(x["source"] == "radnet" for x in sensors),
        "safecast": sum(x["source"] == "safecast" for x in sensors),
        "learning": sum(x["status"] == "learning" for x in sensors),
        "median_usvh_now": {src: (round(statistics.median(v), 4) if v else None) for src, v in
                            (("radnet", [x["usvh"] for x in sensors if x["source"] == "radnet"]),
                             ("safecast", [x["usvh"] for x in sensors if x["source"] == "safecast"]))},
        # same sensors compared (now = last RadNet refresh window): ratio now / then
        "change_vs_24h": _change(cur, _means(22, 34, now)),
        "change_vs_7d": _change(cur, _means(166, 178, now)),
        "vs_own_baseline": {"median_ratio": round(statistics.median(ratios), 3) if ratios else None,
                            "sensors_with_baseline": len(ratios)},
        "cap_days": KEEP_DAYS,
        "elevated": [{"place": x.get("name") or _place(x["lat"], x["lon"]) or f"{x['lat']},{x['lon']}",
                      "network": x["source"], "usvh": x["usvh"],
                      "baseline": x["baseline"], "ratio": x["ratio"], "status": x["status"],
                      "corroborated": x["corroborated"]} for x in hot[:12]],
        "elevated_count": len(hot), "corroborated_count": sum(x["corroborated"] for x in hot),
        "drive_surveys": _survey_stats(now)}


def _survey_stats(now: float) -> dict | None:
    try:
        from apb.context import radsurvey
        return radsurvey.digest_stats(now)
    except Exception:
        return None


def _survey_key() -> list:
    """Flagged drives in the last week (ids): a newly flagged drive is a change worth a note."""
    try:
        from apb.store import radiation as store
        return sorted(r["id"] for r in store.survey_logs(time.time() - 7 * 86400)
                      if (r.get("flagged_cells") or 0) >= 5)
    except Exception:
        return []


def digest(sensors: list[dict]) -> str:
    """What counts as a change, from the in-memory state only (no history scan): who is
    elevated (ratio to the nearest 0.5), the reporting count to the nearest 25, and the
    median current reading to the nearest 0.005 µSv/h."""
    hot = sorted((x["id"], round((x["ratio"] or 0) * 2) / 2, x["corroborated"], x["status"])
                 for x in sensors if x["status"] in ("elevated", "high"))
    med = statistics.median(x["usvh"] for x in sensors) if sensors else 0
    key = {"e": hot, "r": round(len(sensors) / 25), "m": round(med / 0.005), "s": _survey_key()}
    return hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()[:16]


PROMPT = """You write a short public status note about environmental radiation measured by \
two networks: EPA RadNet (about 140 official fixed monitors across the U.S., hourly data \
refreshed every few hours) and Safecast (volunteer real-time sensors, mostly Japan and \
Europe, a few in North America). "drive_surveys" are Safecast volunteer drives with \
mobile counters, uploaded days after the drive; a drive is flagged when several road \
cells read well above what earlier drives saw there. You get statistics (µSv/h = microsieverts per hour), \
not raw data, and your own previous notes. Typical background is about 0.05-0.20 µSv/h; rain \
can briefly raise readings by washing natural radon decay products down. A sensor is "elevated" \
only against its own history and only when sustained for two hours.
Answer with ONLY one JSON object:
{"headline": at most 90 characters,
 "summary": at most 500 characters: the overall picture, anything elevated and where, \
how it compares with 24 h, 7 days and the whole period,
 "changes": at most 200 characters: what changed since your last note, or "No material change.",
 "level": "normal" | "watch" | "elevated"}
Use "watch" for isolated or uncorroborated elevations, "elevated" only for corroborated or \
very high readings. Be calm and factual; do not speculate about causes beyond what the \
numbers support. Quote numbers exactly as given and never compute new ones (do not average \
the two networks: their sensors differ). "change_vs_24h" / "change_vs_7d" are median \
ratios of the same sensors now versus then (1.00 = unchanged); "vs_own_baseline" compares \
each sensor with its own normal over the whole period. Never invent places or values."""


def summarise(now: float | None = None, gen=None, force: bool = False) -> dict | None:
    """Write a new digest if the picture changed (or the heartbeat is due) and budget allows."""
    from apb.context import explain
    from apb.store import radiation as store
    from apb.store import spend
    now = now or time.time()
    dg = digest(_state["sensors"])
    prev = store.summaries(now - KEEP_DAYS * 86400, limit=3)
    last = prev[0] if prev else None
    if not force and last:
        age = now - last["ts"]
        if (last["digest"] == dg and age < HEARTBEAT) or (last["digest"] != dg and age < MIN_GAP):
            _stats["summary_skips"] += 1
            return None
    if not _state["sensors"]:
        return None
    cap = daily_cap()
    if cap <= 0 or not spend.allow("radiation_summary_calls", 1, monthly_cap=cap * 31, daily_cap=cap):
        _stats["budget_stops"] += 1
        return None
    st = stats(now, _state["sensors"])                # the history scan, only when writing
    history = "\n".join(f"- {time.strftime('%Y-%m-%d %H:%MZ', time.gmtime(p['ts']))} [{p['level']}] "
                        f"{p['headline']} — {p['summary']}" for p in reversed(prev)) or "(none yet)"
    text = (PROMPT + "\n\nNow: " + time.strftime("%Y-%m-%d %H:%MZ", time.gmtime(now))
            + "\n\nStatistics:\n" + json.dumps(st, separators=(",", ":"))
            + "\n\nYour previous notes (oldest first):\n" + history)
    call = gen or (lambda parts, max_tokens: explain.generate_usage(
        parts, max_tokens, feature="radiation_summary", model=MODEL, thinking_level="MINIMAL",
        json_output=True, timeout=30.0))
    try:
        raw, _ = call([{"text": text}], max_tokens=500)
    except Exception:
        spend.refund("radiation_summary_calls", 1)     # a failed call does not use the day's budget
        raise
    try:
        s = json.loads(raw.strip().removeprefix("```json").removesuffix("```"))
    except ValueError:
        _stats["errors"] += 1
        _stats["last_error"] = "summary: unparseable reply"
        return None
    s = {"headline": str(s.get("headline") or "")[:120], "summary": str(s.get("summary") or "")[:700],
         "changes": str(s.get("changes") or "")[:260],
         "level": s.get("level") if s.get("level") in ("normal", "watch", "elevated") else "normal"}
    if not s["headline"] or not s["summary"]:
        return None
    store.add_summary(now, s, st, dg, MODEL)
    _stats["summaries"] += 1
    return s


# ---- worker ---------------------------------------------------------------------------

def _radnet_names() -> dict[str, str]:
    from apb.ingest import radnet
    return {radnet.device_id(k): v["name"] for k, v in radnet.stations().items()}


def radnet_rotation(now: float, fetch=None) -> int:
    """Refresh the RadNet stations whose data is stalest: enough per cycle that each is
    refreshed about every RADNET_EVERY_H hours. A station's first fetch backfills the
    cap period, so it has a baseline at once. Returns new readings stored."""
    from apb.ingest import radnet
    from apb.store import radiation as store
    keys = list(radnet.stations())
    if not keys or os.environ.get("APB_RADNET_OFF", "").strip().lower() in ("1", "true", "yes", "on"):
        return 0
    per = max(1, math.ceil(len(keys) / max(1.0, RADNET_EVERY_H * 3600 / EVERY)))
    last = {s["device"]: s["last_ts"] for s in store.sensors(0) if s["device"].startswith("radnet:")}
    tried = _state.setdefault("radnet_tried", {})
    due = sorted(keys, key=lambda k: max(last.get(radnet.device_id(k), 0), tried.get(k, 0)))[:per]
    new = 0
    for k in due:
        tried[k] = now
        since = last.get(radnet.device_id(k)) or now - KEEP_DAYS * 86400
        year = time.gmtime(now).tm_year
        try:
            rows = []
            if time.gmtime(since).tm_year < year:          # early January: last year's file too
                rows += (fetch or radnet.fetch_station)(k, year - 1, since)
            rows += (fetch or radnet.fetch_station)(k, year, since)
            new += store.add_readings(rows)
            _stats["radnet_fetches"] = _stats.get("radnet_fetches", 0) + 1
        except Exception as e:
            _stats["radnet_errors"] = _stats.get("radnet_errors", 0) + 1
            _stats["last_error"] = f"radnet {k}: {str(e)[:120]}"
    return new


def run_once(now: float | None = None, gen=None, fetch=None, radnet_fetch=None) -> None:
    from apb.ingest import safecast
    from apb.store import radiation as store
    now = now or time.time()
    try:
        rows, changed = (fetch or safecast.fetch)(now)
        if changed:
            _stats["new_readings"] += store.add_readings(rows)
        else:
            _stats["not_modified"] += 1
    except Exception as e:
        _stats["errors"] += 1
        _stats["last_error"] = f"safecast: {str(e)[:160]}"
    _stats["polls"] += 1
    _stats["last_poll"] = now
    _stats["new_readings"] += radnet_rotation(now, radnet_fetch)
    if _stats.get("radnet_fetches") and not _state.get("rebased"):    # first backfill: baselines now
        _state["baseline_at"], _state["rebased"] = 0.0, True
    _state["sensors"], _state["at"] = assess(now), now
    if int(now // 3600) != int((_state.get("pruned") or 0) // 3600):
        store.prune(KEEP_DAYS, now)
        _state["pruned"] = now
    from apb.context import explain
    if gen or explain.api_key():
        summarise(now, gen=gen)


def _loop() -> None:
    _stop.wait(30)
    while not _stop.is_set():
        try:
            run_once()
        except Exception as e:
            _stats["errors"] += 1
            _stats["last_error"] = str(e)[:200]
            log.info("radiation run failed: %s", str(e)[:160])
        _stop.wait(EVERY)


def start() -> bool:
    global _thread
    if _thread or off():
        return False
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="radiation")
    _thread.start()
    log.info("radiation: on, every %d s, %g-day history, %d summaries a day max", EVERY, KEEP_DAYS, daily_cap())
    return True


def stop() -> None:
    _stop.set()


def status() -> dict:
    from apb.store import spend
    s = _state["sensors"]
    return {**_stats, "running": bool(_thread and _thread.is_alive()), "reporting": len(s),
            "elevated": sum(x["status"] in ("elevated", "high") for x in s),
            "learning": sum(x["status"] == "learning" for x in s), "keep_days": KEEP_DAYS,
            "daily_cap": daily_cap(), "used": spend.used("radiation_summary_calls"), **_counts()}


def _counts() -> dict:
    from apb.store import radiation as store
    try:
        return store.counts()
    except Exception as e:                      # /status must not fail because of this lane
        return {"store_error": str(e)[:120]}


# ---- published views ------------------------------------------------------------------

def public_sensors(flagged_only: bool = False) -> list[dict]:
    s = _state["sensors"] or (assess() if _state["at"] == 0 else [])
    return [x for x in s if not flagged_only or x["status"] in ("elevated", "high")]


def public_summary(days: float) -> dict:
    from apb.store import radiation as store
    hist = store.summaries(time.time() - min(days, KEEP_DAYS) * 86400)
    return {"latest": hist[0] if hist else None, "history": hist, "keep_days": KEEP_DAYS,
            "source": "EPA RadNet (public domain) and Safecast (CC0) sensors; summary by Panoptes"}


def sensor_history(device: str, days: float) -> list[dict]:
    from apb.store import radiation as store
    since = int((time.time() - min(days, KEEP_DAYS) * 86400) // 3600)
    return [{"t": h * 3600, "mean": round(m, 4), "max": round(mx, 4)} for _d, h, m, mx in store.hourly(since, device)]
