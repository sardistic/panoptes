"""Safecast drive surveys, ingested: road-level radiation history in ~100 m cells.

Two jobs on one thread:

  forward   every FORWARD_EVERY s, the newest pages of the import list; each newly
            approved log anywhere in the world is downloaded and folded in
  backfill  the whole import list oldest first (BACKFILL_PAGES pages a cycle, cursor kept
            in rad_survey_meta). Each approved log's first GPS fix is probed with an HTTP
            Range request; North American logs are downloaded and folded in, others are
            recorded as skipped (with where they were). Logs whose city text is Japanese
            are skipped without a request.

A log's readings fold into cells (lat/lon x 1000). Before folding, each cell this drive
touched is compared with what earlier drives saw there: a cell with >= 20 earlier
readings from >= 2 earlier logs counts as raised when this drive's mean there is
>= max(1.5 x earlier mean, earlier mean + 0.05 µSv/h). A log with >= 5 raised cells is
"flagged" and reaches the radiation digest. Survey data arrives days after the drive,
so it informs the digest and the map, not live alerts.
"""
from __future__ import annotations

import logging
import os
import re
import statistics
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

log = logging.getLogger(__name__)

FORWARD_EVERY = float(os.environ.get("APB_SURVEY_EVERY", "3600"))
CYCLE = float(os.environ.get("APB_SURVEY_CYCLE", "20"))
BACKFILL_PAGES = int(os.environ.get("APB_SURVEY_BACKFILL_PAGES", "4"))
BACKFILL_REGIONS = {"north_america"}
MIN_PRIOR_N, MIN_PRIOR_LOGS, FLAG_CELLS = 20, 2, 5
_CJK = re.compile(r"[぀-ヿ㐀-鿿]")

_stop = threading.Event()
_thread: threading.Thread | None = None
_stats = {"forward_runs": 0, "backfill_pages": 0, "probes": 0, "ingested": 0, "skipped": 0,
          "errors": 0, "last_error": None, "last_forward": 0.0, "backfill_done": False}


def off() -> bool:
    v = lambda k: os.environ.get(k, "").strip().lower() in ("1", "true", "yes", "on")   # noqa: E731
    return v("APB_SURVEY_OFF") or v("APB_RAD_OFF")


def _iso(s) -> float | None:
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _approved(imp: dict) -> bool:
    return imp.get("status") == "done" and bool(imp.get("approved")) and not imp.get("rejected") \
        and bool((imp.get("source") or {}).get("url"))


def ingest(imp: dict, text: str | None = None, now: float | None = None) -> dict:
    """Download (unless given), parse, flag and fold one approved log. Returns its row."""
    from apb.ingest import bgeigie
    from apb.store import radiation as store
    now = now or time.time()
    pts = bgeigie.parse(text if text is not None else bgeigie.fetch(imp["source"]["url"]))
    base = {"id": int(imp["id"]), "created": _iso(imp.get("created_at")), "cities": (imp.get("cities") or "")[:200],
            "credits": (imp.get("credits") or "")[:120], "ingested": now}
    if not pts:
        row = {**base, "state": "empty", "n": 0}
        store.add_survey_log(row, {})
        return row
    cells: dict[tuple[int, int], list] = {}
    for p in pts:
        k = (round(p["lat"] * 1000), round(p["lon"] * 1000))
        c = cells.get(k)
        if c is None:
            cells[k] = [1, p["usvh"], p["usvh"], p["ts"], p["ts"]]
        else:
            c[0] += 1
            c[1] += p["usvh"]
            c[2] = max(c[2], p["usvh"])
            c[3] = min(c[3], p["ts"])
            c[4] = max(c[4], p["ts"])
    prior = store.survey_cells_prior(list(cells))
    flagged = 0
    for k, (n, s, _mx, _f, _l) in cells.items():
        pn = prior.get(k)
        if pn and pn[0] >= MIN_PRIOR_N and pn[2] >= MIN_PRIOR_LOGS:
            mean, before = s / n, pn[1]
            if mean >= max(1.5 * before, before + 0.05):
                flagged += 1
    mid = sorted(pts, key=lambda p: p["ts"])[len(pts) // 2]
    usv = [p["usvh"] for p in pts]
    row = {**base, "state": "ingested", "first_ts": min(p["ts"] for p in pts), "last_ts": max(p["ts"] for p in pts),
           "kind": Counter(p["kind"] for p in pts).most_common(1)[0][0], "device": pts[0]["device"][:20],
           "region": bgeigie.region(mid["lat"], mid["lon"]), "lat": round(mid["lat"], 3), "lon": round(mid["lon"], 3),
           "n": len(pts), "cells": len(cells), "mean": round(statistics.fmean(usv), 4), "max": round(max(usv), 4),
           "flagged_cells": flagged}
    store.add_survey_log(row, cells)
    _stats["ingested"] += 1
    return row


def _skip(imp: dict, region: str, pt: tuple | None, now: float) -> None:
    from apb.store import radiation as store
    store.add_survey_log({"id": int(imp["id"]), "created": _iso(imp.get("created_at")), "region": region,
                          "lat": round(pt[0], 3) if pt else None, "lon": round(pt[1], 3) if pt else None,
                          "cities": (imp.get("cities") or "")[:200], "state": "skipped", "ingested": now}, {})
    _stats["skipped"] += 1


def forward(now: float | None = None, max_pages: int = 8) -> int:
    """Newest approved logs (any region) not seen yet."""
    from apb.ingest import bgeigie
    from apb.store import radiation as store
    now = now or time.time()
    done = 0
    for n in range(1, max_pages + 1):
        imps = bgeigie.page(n, newest_first=True)
        if not imps:
            break
        known = store.survey_known([int(i["id"]) for i in imps])
        fresh = [i for i in imps if int(i["id"]) not in known and _approved(i)]
        for imp in fresh:
            try:
                ingest(imp, now=now)
                done += 1
            except Exception as e:
                _stats["errors"] += 1
                _stats["last_error"] = f"log {imp.get('id')}: {str(e)[:120]}"
        oldest = _iso(imps[-1].get("created_at")) or now
        if not fresh and all(int(i["id"]) in known or not _approved(i) for i in imps) and now - oldest > 3 * 86400:
            break                                     # caught up with what we have
        if now - oldest > 30 * 86400:
            break
    _stats["forward_runs"] += 1
    _stats["last_forward"] = now
    return done


def backfill_step(now: float | None = None, pages: int = BACKFILL_PAGES) -> int:
    """Advance the oldest-first crawl by `pages` list pages. Returns logs ingested."""
    from apb.ingest import bgeigie
    from apb.store import radiation as store
    now = now or time.time()
    cur = int(store.survey_meta("backfill_page", 1))
    done = 0
    for _ in range(pages):
        imps = bgeigie.page(cur)
        if not imps:
            store.set_survey_meta("backfill_done", now)
            _stats["backfill_done"] = True
            return done
        known = store.survey_known([int(i["id"]) for i in imps])
        todo = [i for i in imps if int(i["id"]) not in known and _approved(i)]
        ja = [i for i in todo if _CJK.search(i.get("cities") or "")]
        for i in ja:
            _skip(i, "japan", None, now)
        rest = [i for i in todo if i not in ja]

        def where(i):
            try:
                return i, bgeigie.probe(i["source"]["url"])
            except Exception as e:
                return i, e
        with ThreadPoolExecutor(4) as ex:
            located = list(ex.map(where, rest))
        _stats["probes"] += len(rest)
        for imp, pt in located:
            if isinstance(pt, Exception):
                _stats["errors"] += 1
                _stats["last_error"] = f"probe {imp.get('id')}: {str(pt)[:120]}"
                continue                               # retried when the crawl is rerun
            reg = bgeigie.region(*pt) if pt else "unknown"
            if reg in BACKFILL_REGIONS:
                try:
                    ingest(imp, now=now)
                    done += 1
                except Exception as e:
                    _stats["errors"] += 1
                    _stats["last_error"] = f"log {imp.get('id')}: {str(e)[:120]}"
            else:
                _skip(imp, reg, pt, now)
        cur += 1
        store.set_survey_meta("backfill_page", cur)
        _stats["backfill_pages"] += 1
    return done


def _loop() -> None:
    from apb.store import radiation as store
    _stop.wait(90)
    _stats["backfill_done"] = bool(store.survey_meta("backfill_done"))
    while not _stop.is_set():
        try:
            if time.time() - _stats["last_forward"] >= FORWARD_EVERY:
                forward()
            if not _stats["backfill_done"]:
                backfill_step()
        except Exception as e:
            _stats["errors"] += 1
            _stats["last_error"] = str(e)[:200]
            log.info("radiation survey run failed: %s", str(e)[:160])
        _stop.wait(CYCLE if not _stats["backfill_done"] else 600)


def start() -> bool:
    global _thread
    if _thread or off():
        return False
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="radsurvey")
    _thread.start()
    log.info("radiation surveys: on (forward every %d s, backfill %d pages / %d s)", FORWARD_EVERY,
             BACKFILL_PAGES, CYCLE)
    return True


def stop() -> None:
    _stop.set()


def status() -> dict:
    from apb.store import radiation as store
    try:
        extra = {**store.survey_counts(), "backfill_page": store.survey_meta("backfill_page", 1)}
    except Exception as e:
        extra = {"store_error": str(e)[:120]}
    return {**_stats, "running": bool(_thread and _thread.is_alive()), **extra}


# ---- published views ------------------------------------------------------------------

def _step(z: int) -> int:
    """Cell size in 1/1000 degree for a map zoom."""
    return 200 if z <= 4 else 50 if z <= 6 else 20 if z <= 8 else 5 if z <= 10 else 2 if z <= 12 else 1


def tile(z: int, x: int, y: int) -> list[list]:
    """Cells in one web-mercator tile, aggregated for its zoom: [lat, lon, mean µSv/h, max, n, last_ts]."""
    import math
    from apb.store import radiation as store
    nz = 2 ** z
    w, e = x / nz * 360 - 180, (x + 1) / nz * 360 - 180
    lat = lambda yy: math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * yy / nz))))   # noqa: E731
    n, s = lat(y), lat(y + 1)
    return [[round(a, 4), round(b, 4), round(m, 4), round(mx, 4), cnt, int(ts or 0)]
            for a, b, m, mx, cnt, ts in store.survey_cells(w, s, e, n, _step(z))]


def recent(days: float, region: str | None = None) -> list[dict]:
    from apb.store import radiation as store
    keep = ("id", "first_ts", "last_ts", "kind", "region", "lat", "lon", "n", "cells", "mean", "max",
            "flagged_cells", "cities", "credits")
    return [{k: r.get(k) for k in keep} for r in store.survey_logs(time.time() - days * 86400, region)]


def digest_stats(now: float) -> dict:
    """Compact survey facts for the radiation digest."""
    week = recent(7)
    return {"drives_7d": len(week), "drives_7d_by_region": dict(Counter(r["region"] for r in week)),
            "readings_7d": sum(r["n"] or 0 for r in week),
            "flagged_drives_7d": [{"region": r["region"], "lat": r["lat"], "lon": r["lon"], "cities": r["cities"][:80],
                                   "raised_cells": r["flagged_cells"], "mean_usvh": r["mean"], "max_usvh": r["max"]}
                                  for r in week if (r["flagged_cells"] or 0) >= FLAG_CELLS][:6]}
