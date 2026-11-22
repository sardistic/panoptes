"""Radio codes and jargon: what the transcribed lines say that a listener may not know.

Three layers, all keyed by the cleaned line the map already publishes:

1. A seed glossary (config/radio_glossary.yaml): APCO ten-codes, response codes, fire,
   EMS, police and traffic jargon. Numeric codes ("10-N", "11-N", "code N", "signal N",
   "priority N") are always highlighted; the ones the seed does not know carry a generic
   note ("meanings differ between agencies").
2. A per-system history in `radio_terms` (apb/store/radio.py): every code, seed term
   and unknown acronym heard on a system, with how many times, first and last heard and
   the newest cleaned example line. It outlives the 3-day transcripts, so a tooltip can
   say "heard 212 times on this system since 18 Nov".
3. Learned meanings: every LEARN_EVERY seconds one small text call (pinned Flash-Lite,
   APB_RADIO_TERMS_DAILY calls a day, default 24) asks what one system's most frequent
   undefined terms mean there, from a few cleaned example lines. Ten-codes vary between
   agencies, so frequent seeded codes are asked too and a local meaning overrides the
   seed for that system. The model returns null for names, places, unit identifiers and
   anything unclear; meanings that name a medical condition are dropped.

What is published: term keys with character spans on each line, the seed glossary,
learned meanings, and per-system counts/examples for terms that have a meaning or are
numeric codes. Unknown acronyms are counted but never served.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path

log = logging.getLogger(__name__)

EVERY = float(os.environ.get("APB_RADIO_TERMS_EVERY", "120"))
LEARN_EVERY = float(os.environ.get("APB_RADIO_TERMS_LEARN_EVERY", "1800"))
MODEL = os.environ.get("APB_RADIO_TERMS_MODEL", "gemini-3.5-flash-lite")
MIN_ASK = 3                   # heard at least this often on a system before asking
MIN_ASK_SEEDED = 5            # a seeded (varying) code needs a little more evidence
MAX_ASK = 20                  # terms per model call (one system per call)
RETRY_SEC = 14 * 86400.0      # an unanswered term is asked again after this

_GLOSSARY_PATH = Path(__file__).resolve().parents[2] / "config" / "radio_glossary.yaml"
_stop = threading.Event()
_thread: threading.Thread | None = None
_stats = {"scans": 0, "lines": 0, "learn_calls": 0, "learned": 0, "errors": 0,
          "budget_stops": 0, "last_scan": 0.0, "last_learn": 0.0, "last_error": None}

_STATES = set("AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV "
              "NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC".split())
_STOP = _STATES | set("I US SR CR FM RT HWY OK AM PM TV NO ON IN TO AT OF OR AND THE A AN IS IT "
                      "INAUDIBLE REDACTED UNKNOWN".split())

_CODE = re.compile(r"(?<![\w-])(10|11)(?:-|\s)(\d{1,3})(?![\w-])"
                   r"(?!\s*(?:minutes?|mins?|miles?|seconds?|feet|ft|hours?|years?|yards?|%|percent|"
                   r"units?|people|persons?|cars?|vehicles?|o'?clock)\b)", re.I)
_TEN_FOUR = re.compile(r"\bten[- ]four\b", re.I)
_WORD_CODE = re.compile(r"\b(code|signal|priority)\s+(\d{1,3})\b", re.I)
_MPDS = re.compile(r"\b(alpha|bravo|charlie|delta|echo|omega)[- ](?:level\s+)?(?:response|call|level)\b", re.I)
_ACRO = re.compile(r"(?<![A-Za-z0-9])([A-Z][A-Z0-9]{1,5})(?![A-Za-z0-9])")

_seed: dict | None = None
_learned: dict[str, tuple[str, str, int]] = {}
_learned_at = 0.0
_phrase_re: re.Pattern | None = None
_acro_re: re.Pattern | None = None
_known_acros: frozenset = frozenset()


def seed() -> dict:
    """{"codes": {key: {m, cat}}, "terms": {key: {m, cat}}, "generic": {prefix: note}}."""
    global _seed
    if _seed is None:
        import yaml
        try:
            raw = yaml.safe_load(_GLOSSARY_PATH.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError) as e:
            log.warning("radio glossary unreadable: %s", e)
            raw = {}
        _seed = {"codes": {k.lower(): v for k, v in (raw.get("codes") or {}).items()},
                 "terms": dict(raw.get("terms") or {}),
                 "generic": dict(raw.get("generic") or {})}
    return _seed


def _is_acro(k: str) -> bool:
    return k == k.upper() and any(ch.isalpha() for ch in k)


def _refresh(force: bool = False) -> None:
    """(Re)build the matchers from the seed plus every meaning learned so far."""
    global _learned, _learned_at, _phrase_re, _acro_re, _known_acros
    if not force and _phrase_re is not None and time.time() - _learned_at < 120:
        return
    try:
        from apb.store import radio
        _learned = radio.learned_terms()
    except Exception as e:                       # store not ready: seed only
        log.debug("radio terms: learned unavailable: %s", e)
    _learned_at = time.time()
    terms = seed()["terms"]
    phrases = sorted((k for k in terms if not _is_acro(k)), key=len, reverse=True)
    pat = "|".join(re.escape(p).replace(r"\-", "[- ]").replace(r"\ ", r"[- ]") for p in phrases)
    _phrase_re = re.compile(rf"(?<![\w-])(?:{pat})(?![\w-])", re.I) if pat else re.compile(r"(?!x)x")
    acros = {k for k in terms if _is_acro(k)} | {k for k in _learned if _is_acro(k) and not _CODE.fullmatch(k)}
    _known_acros = frozenset(acros)
    alt = "|".join(re.escape(a) for a in sorted(acros, key=len, reverse=True))
    _acro_re = re.compile(rf"(?<![A-Za-z0-9])(?:{alt})(?![A-Za-z0-9])") if alt else re.compile(r"(?!x)x")


def _phrase_key(m: str) -> str:
    low = re.sub(r"[- ]+", " ", m.lower())
    for k in seed()["terms"]:
        if not _is_acro(k) and re.sub(r"[- ]+", " ", k.lower()) == low:
            return k
    return low


def _matches(line: str) -> list[tuple[int, int, str, bool]]:
    """(start, end, key, known) for every code / term / acronym in a line, overlapping."""
    _refresh()
    out = []
    for m in _CODE.finditer(line):
        out.append((m.start(), m.end(), f"{m.group(1)}-{int(m.group(2))}", True))
    for m in _TEN_FOUR.finditer(line):
        out.append((m.start(), m.end(), "10-4", True))
    for m in _WORD_CODE.finditer(line):
        out.append((m.start(), m.end(), f"{m.group(1).lower()} {int(m.group(2))}", True))
    for m in _MPDS.finditer(line):
        out.append((m.start(), m.end(), f"{m.group(1).lower()} response", True))
    for m in _phrase_re.finditer(line):
        out.append((m.start(), m.end(), _phrase_key(m.group(0)), True))
    for m in _acro_re.finditer(line):
        out.append((m.start(), m.end(), m.group(0), True))
    words = re.findall(r"[A-Za-z]{2,}", line)
    if words and sum(w.isupper() for w in words) / len(words) < 0.5:     # not an all-caps line
        for m in _ACRO.finditer(line):
            a = m.group(1)
            if a not in _STOP and a not in _known_acros and sum(ch.isalpha() for ch in a) >= 2:
                out.append((m.start(), m.end(), a, False))
    return out


def _pick(ms: list[tuple[int, int, str, bool]]) -> list[tuple[int, int, str, bool]]:
    """Leftmost-longest, non-overlapping."""
    ms = sorted(ms, key=lambda x: (x[0], -(x[1] - x[0])))
    keep, end = [], -1
    for m in ms:
        if m[0] >= end:
            keep.append(m)
            end = m[1]
    return keep


def spans(line: str | None) -> list[list]:
    """[[start, end, key], ...] for the known terms in a cleaned line (what the map
    highlights). Unknown acronyms are left out."""
    if not line:
        return []
    return [[s, e, k] for s, e, k, known in _pick(_matches(line)) if known]


def _seed_meaning(key: str) -> dict | None:
    s = seed()
    if key in s["codes"]:
        return s["codes"][key]
    return s["terms"].get(key)


def _varies(key: str) -> bool:
    return bool(_CODE.fullmatch(key) or _WORD_CODE.fullmatch(key))


# ---- counting -------------------------------------------------------------------------

def scan(limit: int = 5000) -> int:
    """Count terms in transcripts finished since the cursor. Returns lines read."""
    from apb.context.radio_areas import clean
    from apb.store import radio
    rows = radio.done_since(radio.term_cursor(), limit)
    if not rows:
        return 0
    hits: dict[tuple[str, str], list] = {}
    for r in rows:
        line = clean(r.get("text") or r.get("summary"))
        if not line:
            continue
        scope = f"{r['key']}:{r['system']}"
        for _, _, k, _known in _pick(_matches(line)):
            h = hits.get((scope, k))
            if h is None:
                hits[(scope, k)] = [1, r["ts"], r["ts"], line[:160]]
            else:
                h[0] += 1
                h[1] = min(h[1], r["ts"])
                if r["ts"] >= h[2]:
                    h[2], h[3] = r["ts"], line[:160]
    radio.bump_terms({k: tuple(v) for k, v in hits.items()}, rows[-1]["done_at"])
    _stats["lines"] += len(rows)
    return len(rows)


# ---- learning -------------------------------------------------------------------------

PROMPT = """You help people understand public-safety radio traffic. Below are codes and \
terms heard repeatedly on one radio system, each with a few example lines (machine \
transcripts, already cleaned of addresses and unit numbers). Say what each one means ON \
THIS SYSTEM, judged from the examples and what agencies in this area use.
Answer with ONLY one JSON object: {"terms": [{"term": exactly as given, "meaning": plain \
English, at most 90 characters, or null, "confidence": 0.0 to 1.0}]}.
Give null when the term is a person's name, a place, street, business, unit or aircraft \
identifier, call sign, licence plate or a transcription error, or when you cannot tell. \
Agency and radio-system names may be defined ("Ohio statewide radio system"). Ten-codes and \
signal codes differ between agencies: if the examples show a local meaning that differs \
from the usual one, give the local one. A code for a medical problem is just "medical \
call" — never name the condition, injury or a death."""


def daily_cap() -> int:
    try:
        return max(0, int(os.environ.get("APB_RADIO_TERMS_DAILY", "24")))
    except ValueError:
        return 24


def _fixed_seed_terms() -> set[str]:
    """Seed entries whose meaning does not vary — never worth a model call."""
    return set(seed()["terms"])


def _choose(rows: list[dict]) -> tuple[str, list[dict]] | None:
    fixed = _fixed_seed_terms()
    by_scope: dict[str, list[dict]] = {}
    for r in rows:
        t = r["term"]
        if t in fixed:
            continue
        if _seed_meaning(t) and r["n"] < MIN_ASK_SEEDED:
            continue
        by_scope.setdefault(r["scope"], []).append(r)
    if not by_scope:
        return None
    scope = max(by_scope, key=lambda s: sum(r["n"] for r in by_scope[s][:MAX_ASK]))
    return scope, by_scope[scope][:MAX_ASK]


def _parse(raw: str) -> list[dict]:
    raw = (raw or "").strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw)
    try:
        obj = json.loads(raw)
    except ValueError:
        m = re.search(r"\{.*\}", raw, re.S)
        if not m:
            return []
        try:
            obj = json.loads(m.group(0))
        except ValueError:
            return []
    items = obj.get("terms") if isinstance(obj, dict) else obj
    return [x for x in (items or []) if isinstance(x, dict)]


def _safe_meaning(m) -> str | None:
    from apb.context.radio_areas import _MEDICAL
    m = re.sub(r"\s+", " ", str(m or "")).strip().strip('"')[:100]
    if not m or m.lower() in ("null", "none", "unknown"):
        return None
    if _MEDICAL.search(m):
        return "Medical call code"
    return m


def learn_once(gen=None, now: float | None = None) -> int:
    """One model call for the system with the most frequent undefined terms."""
    from apb.context import explain
    from apb.context.radio_areas import clean
    from apb.ingest import rdio
    from apb.store import radio, spend
    picked = _choose(radio.terms_to_ask(min(MIN_ASK, MIN_ASK_SEEDED), RETRY_SEC))
    if not picked:
        return 0
    cap = daily_cap()
    if cap <= 0 or not spend.allow("radio_term_calls", 1, monthly_cap=cap * 31, daily_cap=cap):
        _stats["budget_stops"] += 1
        return 0
    scope, rows = picked
    key, _, system = scope.partition(":")
    inst = rdio.instance(key) or {}
    label = rdio._label(inst, int(system)) if inst else system
    pl = rdio.place_info(inst, label, int(system)) if inst else {}
    head = (f"System: {label}. Listener near {inst.get('place') or 'unknown'}"
            + (f"; the system serves {pl['area']}" if pl.get("area") else "") + ".")
    blocks = []
    for r in rows:
        t = r["term"]
        needle = t.split()[-1] if " " in t else t
        exs = [clean(x)[:160] for x in radio.examples(key, int(system), needle, 3)] or [r.get("example") or ""]
        hint = _seed_meaning(t)
        blocks.append(f'- "{t}" (heard {r["n"]} times)'
                      + (f'; usual meaning: {hint["m"]}' if hint else "")
                      + "".join(f"\n    e.g. {e}" for e in exs if e))
    text = PROMPT + "\n\n" + head + "\n\nTerms:\n" + "\n".join(blocks)
    call = gen or (lambda parts, max_tokens: explain.generate_usage(
        parts, max_tokens, feature="radio_terms", model=MODEL, json_output=True))
    raw, _ = call([{"text": text}], max_tokens=1200)
    got = {str(x.get("term") or "").strip(): x for x in _parse(raw)}
    now = now or time.time()
    learned = 0
    for r in rows:
        x = got.get(r["term"]) or {}
        try:
            conf = max(0.0, min(1.0, float(x.get("confidence") or 0.0)))
        except (TypeError, ValueError):
            conf = 0.0
        meaning = _safe_meaning(x.get("meaning")) if conf >= 0.5 else None
        radio.set_term(r["scope"], r["term"], meaning, conf if meaning else None, asked=now)
        learned += bool(meaning)
    _stats["learn_calls"] += 1
    _stats["learned"] += learned
    _stats["last_learn"] = now
    _refresh(force=True)
    return learned


# ---- published views ------------------------------------------------------------------

def glossary() -> dict:
    """What /live/radio/glossary serves: the seed and every learned meaning (from the
    system that heard the term most)."""
    _refresh()
    s = seed()
    learned = {t: {"m": m, "n": n} for t, (m, _scope, n) in _learned.items()
               if not _seed_meaning(t)}
    return {"codes": s["codes"], "terms": s["terms"], "generic": s["generic"], "learned": learned}


def for_system(scope: str) -> list[dict]:
    """What /live/radio/{rid}/terms serves: counts, first/last heard, newest example and
    the local meaning, for every term heard on a system that has a meaning (seed or
    learned) or is a numeric code. Unknown acronyms stay private."""
    from apb.store import radio
    _refresh()
    out = []
    for r in radio.terms_for(scope):
        t = r["term"]
        if not (r.get("meaning") or _seed_meaning(t) or _varies(t) or t in _learned):
            continue
        out.append({"t": t, "n": r["n"], "first": r["first_ts"], "last": r["last_ts"],
                    "ex": r.get("example"), "m": r.get("meaning")})
    return out


# ---- worker ---------------------------------------------------------------------------

def run_once(now: float | None = None, gen=None) -> None:
    now = now or time.time()
    while scan() >= 5000:                         # backfill in batches on first start
        pass
    _stats["scans"] += 1
    _stats["last_scan"] = now
    from apb.context import explain
    if (gen or explain.api_key()) and now - _stats["last_learn"] >= LEARN_EVERY:
        _stats["last_learn"] = now
        learn_once(gen=gen, now=now)


def _loop() -> None:
    _stop.wait(60)
    while not _stop.is_set():
        try:
            run_once()
        except Exception as e:
            _stats["errors"] += 1
            _stats["last_error"] = str(e)[:200]
            log.info("radio terms run failed: %s", str(e)[:160])
        _stop.wait(EVERY)


def off() -> bool:
    return os.environ.get("APB_RADIO_TERMS_OFF", "").strip().lower() in ("1", "true", "yes", "on")


def start() -> bool:
    global _thread
    if _thread or off():
        return False
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="radio-terms")
    _thread.start()
    log.info("radio terms: on, scan every %d s, %d learning calls a day max", EVERY, daily_cap())
    return True


def stop() -> None:
    _stop.set()


def status() -> dict:
    from apb.store import spend
    return {**_stats, "running": bool(_thread and _thread.is_alive()), "daily_cap": daily_cap(),
            "used": spend.used("radio_term_calls"), "learned_terms": len(_learned)}
