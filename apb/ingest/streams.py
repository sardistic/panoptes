"""Scanner audio streams: public Icecast feeds of police / fire / EMS radio, cut into calls.

Many scanner hobbyists publish a live MP3 stream to an Icecast server listed in the
public Xiph directory (dir.xiph.org/yp.xml) — FDNY boroughs, DC Fire, Maricopa County,
Charlotte Fire and dozens more that no Rdio Scanner instance covers. This lane:

  * reads the directory once a day and keeps the public-safety streams
    (state/streams.json — out of git like the Rdio list: hobbyists' hosts);
  * listens to up to APB_STREAMS_MAX of them continuously, through the same egress
    SOCKS proxy as the Rdio lane (APB_RDIO_PROXY), never from the home address;
  * cuts each stream into calls at the squelch silence between transmissions (decode
    MP3 -> 8 kHz mono, RMS per 50 ms against a running noise floor);
  * stores each call's original MP3 bytes for 6 h (state/streams/<key>/<id>.mp3) and
    records it like any Rdio call: radio_recent (system 1, talkgroup 0), counts every
    10 min into radio_calls (baseline + surge), so transcription, area summaries,
    incident threads, the map and the live feed treat a stream like a scanner.

A stream looks to the rest of the code like an Rdio instance with one system whose one
talkgroup is the stream itself: rdio.instance()/_label()/talkgroup()/fetch_calls() and
activity() know about it. Where it is comes from the usual system profile (the model
reads the stream's name), checked against the Census state.
"""
from __future__ import annotations

import array
import asyncio
import hashlib
import json
import logging
import math
import os
import re
import ssl
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

import httpx

log = logging.getLogger(__name__)

YP_URL = os.environ.get("APB_STREAMS_YP", "https://dir.xiph.org/yp.xml")
UA = "apb/0.1 (+https://panoptes.run; public-safety map; listening)"
RATE = 8000
FRAME = 400                       # 50 ms at 8 kHz
PRE_ROLL_S = 0.6
HANG_S = 1.6                      # silence that ends a call
MIN_CALL_S = 1.2
MAX_CALL_S = 90.0
KEEP_SEC = 6 * 3600.0
COUNT_EVERY = 600.0

_SAFETY = re.compile(r"\bscanner\b|police|sheriff|\bfire\b|fire ?dept|fire/ems|dispatch|public safety|\bems\b|"
                     r"\bp25\b|\b911\b|\bfdny\b|\bpd\b|highway patrol|\bdps\b|rescue|emergency", re.I)
_SKIP = re.compile(r"\btest|repeater|amateur|\bham\b|public works|penndot|music|weather radio|noaa|"
                   r"\bwx\b|aviation|\batc\b|air traffic|railroad|\brail\b|\bcb\b|\blsb\b|\busb\b|"
                   r"old time|\botr\b|drama|podcast|comedy|\bdmr\b|\bhub\b|air ?band|\bgmrs\b|\bfrs\b", re.I)
# an amateur callsign in the name (W8CMN, KD2ABC) marks a ham net, not public safety;
# business licences (KCF415: letters then digits) do not match
_HAM_CALL = re.compile(r"\b[AKNW][A-Z]?\d[A-Z]{1,3}\b")
# the directory genre must say it is a scanner feed ("She On Fire", genre "various", is not)
_GENRE = re.compile(r"scanner|public safety|police|sheriff|fire|\bems\b|dispatch|911|emergency", re.I)
_NOT_GENRE = re.compile(r"music|rock|jazz|hits|\bpop\b|estilo|talk radio", re.I)


def wanted(name: str, genre: str) -> bool:
    text = f"{name} {genre}"
    return bool(_SAFETY.search(text) and _GENRE.search(genre or "") and not _SKIP.search(text)
                and not _HAM_CALL.search(name) and not _NOT_GENRE.search(genre or ""))


def probed_ok(name: str, genre: str) -> bool:
    """Mounts found by icecast_probe were kept on their names; re-check what the rules
    learned since (ham callsigns, DMR hubs, air band)."""
    return not _SKIP.search(f"{name} {genre}") and not _HAM_CALL.search(name)

_stop = threading.Event()
_thread: threading.Thread | None = None
_streams: list[dict] = []
_stats = {"listed": 0, "listening": 0, "connects": 0, "drops": 0, "calls": 0, "bytes": 0,
          "last_call": 0.0, "last_error": None, "discovered": 0}
_live: dict[str, dict] = {}       # key -> {"calls": n since last count, "since": ts, "up": bool}


def off() -> bool:
    return os.environ.get("APB_STREAMS_OFF", "").strip().lower() in ("1", "true", "yes", "on")


def max_streams() -> int:
    try:
        return max(0, int(os.environ.get("APB_STREAMS_MAX", "120")))
    except ValueError:
        return 120


def state_dir() -> Path:
    from apb.store import snapshots
    return snapshots.DB_PATH.parent


def list_path() -> Path:
    return Path(os.environ.get("APB_STREAMS_LIST", "").strip() or state_dir() / "streams.json")


def extra_path() -> Path:
    """Mounts found on Icecast servers outside the directory (apb.discover.icecast_probe)."""
    return Path(os.environ.get("APB_STREAMS_EXTRA", "").strip() or state_dir() / "streams_extra.json")


def key_of(url: str) -> str:
    return hashlib.sha1(("stream:" + url).encode()).hexdigest()[:10]


def tag_for(name: str) -> str:
    n = name.lower()
    fire = bool(re.search(r"\bfire\b|fdny|rescue", n))
    law = bool(re.search(r"police|sheriff|\bpd\b|highway patrol|\bdps\b|state police|trooper", n))
    ems = bool(re.search(r"\bems\b|medic|ambulance", n))
    if (fire + law + ems) > 1 or not (fire or law or ems):
        return "Multi-Dispatch"
    return "Fire Dispatch" if fire else "Law Dispatch" if law else "EMS Dispatch"


# ── directory ────────────────────────────────────────────────────────────────

def parse_yp(xml: str) -> list[dict]:
    """Public-safety MP3 streams from the Xiph yp.xml, one per distinct name."""
    from html import unescape
    out, seen = [], set()
    for e in re.findall(r"<entry>(.*?)</entry>", xml, re.S):
        def f(tag: str) -> str:
            m = re.search(fr"<{tag}>(.*?)</{tag}>", e, re.S)
            return unescape(m.group(1).strip()) if m else ""
        name, genre, url = f("server_name"), f("genre"), f("listen_url")
        if f("server_type") != "audio/mpeg" or not url.startswith(("http://", "https://")):
            continue
        if not wanted(name, genre):
            continue
        norm = re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()
        if not norm or norm in seen:
            continue
        seen.add(norm)
        try:
            br = int(f("bitrate") or 0)
        except ValueError:
            br = 0
        out.append({"key": key_of(url), "name": re.sub(r"\s+", " ", name)[:80], "genre": genre[:60],
                    "url": url, "bitrate": br, "tag": tag_for(name)})
    return out


def discover(fetch=None) -> int:
    """Refresh the stream list from the directory. Returns how many are listed."""
    text = fetch() if fetch else httpx.get(YP_URL, timeout=60, headers={"User-Agent": UA},
                                           follow_redirects=True).text
    found = parse_yp(text)
    if not found:
        return 0
    path = list_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"fetched": time.time(), "streams": found}, indent=1), encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    tmp.replace(path)
    _stats["discovered"] = len(found)
    return len(found)


def load() -> list[dict]:
    """The directory list, then the extra (probed) mounts; one entry per key."""
    out, seen = [], set()
    for path in (list_path(), extra_path()):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for st in data.get("streams") or []:
            if st.get("key") and st.get("url") and st["key"] not in seen:
                seen.add(st["key"])
                out.append(st)
    return out


def instances() -> list[dict]:
    """The listened streams, shaped like Rdio instances (one system: the stream)."""
    return [{"key": s["key"], "kind": "stream", "host": urlsplit(s["url"]).hostname, "port": None,
             "place": None, "loc": None, "labels": {1: s["name"]}} for s in _streams]


def cfg_for(s: dict) -> dict:
    """What rdio._cfg holds for an instance: {system: {label, tags, tg}}."""
    tg = {"label": s["name"][:40], "name": s["name"], "tag": s.get("tag") or "Multi-Dispatch", "group": "Stream"}
    return {1: {"label": s["name"], "tags": {0: tg["tag"]}, "tg": {0: tg}}}


# ── cutting calls out of a stream ────────────────────────────────────────────

class Cutter:
    """Squelch-gated call detection on 8 kHz mono int16 audio, fed in pieces.

    A call starts when 2 frames (100 ms) run above the on-threshold and ends after
    HANG_S below the off-threshold. Thresholds ride a noise floor that only learns
    while idle. Calls shorter than MIN_CALL_S are dropped; longer than MAX_CALL_S are
    cut. Returns (start_s, end_s) in stream seconds for each finished call."""

    def __init__(self):
        self.t = 0.0
        self.floor = None
        self.active = False
        self.start = 0.0
        self.last_loud = 0.0
        self.hot = 0
        self.rest = array.array("h")

    def feed(self, pcm: array.array) -> list[tuple[float, float]]:
        buf = self.rest + pcm
        out = []
        n = len(buf) // FRAME
        for i in range(n):
            fr = buf[i * FRAME:(i + 1) * FRAME]
            rms = _rms(fr)
            out += self._frame(rms)
            self.t += FRAME / RATE
        self.rest = buf[n * FRAME:]
        return out

    def _frame(self, rms: float) -> list[tuple[float, float]]:
        if self.floor is None:
            self.floor = rms
        on, off_ = max(self.floor * 3.5, 350.0), max(self.floor * 2.0, 220.0)
        done = []
        if not self.active:
            self.floor = min(rms, self.floor * 1.003 + 0.5) if rms < on else self.floor
            self.hot = self.hot + 1 if rms > on else 0
            if self.hot >= 2:
                self.active, self.start, self.last_loud = True, self.t - 0.1, self.t
        else:
            if rms > off_:
                self.last_loud = self.t
            if self.t - self.last_loud >= HANG_S or self.t - self.start >= MAX_CALL_S:
                end = self.last_loud + 0.05
                if end - self.start >= MIN_CALL_S:
                    done.append((max(0.0, self.start - PRE_ROLL_S), end + 0.3))
                self.active, self.hot = False, 0
        return done


try:
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        import audioop as _audioop                     # C speed; gone in 3.13

    def _rms(fr: array.array) -> float:
        return float(_audioop.rms(fr.tobytes(), 2))
except ImportError:                                    # pragma: no cover
    def _rms(fr: array.array) -> float:
        return math.sqrt(sum(x * x for x in fr) / max(1, len(fr)))


def decode_pcm(mp3: bytes) -> array.array:
    """MP3 bytes (any cut) -> 8 kHz mono int16. Partial frames at the edges are dropped."""
    import miniaudio
    try:
        d = miniaudio.decode(mp3, output_format=miniaudio.SampleFormat.SIGNED16, nchannels=1, sample_rate=RATE)
    except Exception:
        return array.array("h")
    return array.array("h", d.samples)


# ── storage ──────────────────────────────────────────────────────────────────

def call_path(key: str, call_id: int) -> Path:
    return state_dir() / "streams" / key / f"{int(call_id)}.mp3"


def save_call(s: dict, start_ts: float, mp3: bytes, now: float | None = None) -> int:
    """Store one cut call and list it like an Rdio call. Returns its id."""
    from apb.store import radio
    call_id = int(start_ts * 10)
    p = call_path(s["key"], call_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(mp3)
    radio.add_recent(s["key"], [(call_id, start_ts, 1, 0)])
    st = _live.setdefault(s["key"], {"calls": 0, "since": now or time.time(), "up": True})
    st["calls"] += 1
    _stats["calls"] += 1
    _stats["last_call"] = now or time.time()
    return call_id


def fetch(key: str, ids: list[int]) -> dict:
    """{id: (bytes, mime)} for stored calls — what rdio.fetch_calls returns."""
    out = {}
    for i in ids:
        try:
            out[int(i)] = (call_path(key, i).read_bytes(), "audio/mpeg")
        except OSError:
            pass
    return out


def prune(now: float | None = None) -> int:
    cut = (now or time.time()) - KEEP_SEC
    n = 0
    root = state_dir() / "streams"
    if not root.exists():
        return 0
    for p in root.glob("*/*.mp3"):
        try:
            if int(p.stem) / 10.0 < cut:
                p.unlink()
                n += 1
        except (ValueError, OSError):
            pass
    return n


def record_counts(now: float | None = None) -> None:
    """Every COUNT_EVERY: each listened stream's calls into radio_calls (baseline, surge)."""
    from apb.store import radio
    now = now or time.time()
    for s in _streams:
        st = _live.get(s["key"])
        if not st or not st.get("up"):
            continue
        window = now - st["since"]
        if window < COUNT_EVERY * 0.9:
            continue
        radio.record(s["key"], {1: (st["calls"], {s.get("tag") or "Multi-Dispatch": st["calls"]} if st["calls"] else {})},
                     window, now=now)
        st["calls"], st["since"] = 0, now


# ── listening ────────────────────────────────────────────────────────────────

async def _open(url: str, proxy: str | None):
    """An HTTP/1.0 GET of the stream, through the SOCKS proxy when one is set."""
    u = urlsplit(url)
    host, port = u.hostname, u.port or (443 if u.scheme == "https" else 80)
    path = (u.path or "/") + (f"?{u.query}" if u.query else "")
    ctx = ssl.create_default_context() if u.scheme == "https" else None
    if proxy:
        from python_socks.async_.asyncio import Proxy
        purl = "socks5://" + proxy[len("socks5h://"):] if proxy.startswith("socks5h://") else proxy
        sock = await Proxy.from_url(purl, rdns=True).connect(host, port, timeout=15)
        reader, writer = await asyncio.open_connection(sock=sock, ssl=ctx, server_hostname=host if ctx else None)
    else:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port, ssl=ctx), 15)
    writer.write((f"GET {path} HTTP/1.0\r\nHost: {host}\r\nUser-Agent: {UA}\r\n"
                  "Icy-MetaData: 0\r\nAccept: */*\r\n\r\n").encode())
    await writer.drain()
    head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 15)
    status = head.split(b"\r\n", 1)[0]
    if b" 200" not in status:
        writer.close()
        raise ConnectionError(status.decode(errors="replace")[:60])
    return reader, writer


async def listen(s: dict, proxy: str | None) -> None:
    """Listen to one stream until stopped, reconnecting with backoff."""
    backoff = 15.0
    bps = max(1, s.get("bitrate") or 16) * 125            # bytes per second at the stated bitrate
    while not _stop.is_set():
        writer = None
        try:
            reader, writer = await _open(s["url"], proxy)
            _stats["connects"] += 1
            _live.setdefault(s["key"], {"calls": 0, "since": time.time(), "up": True})["up"] = True
            backoff = 15.0
            cut = Cutter()
            ring: list[tuple[float, float, bytes]] = []     # (stream_t0, stream_t1, mp3 bytes)
            wall0 = time.time()
            chunk = max(1024, bps)                          # ~1 s per piece
            while not _stop.is_set():
                data = await asyncio.wait_for(reader.read(chunk), 30)
                if not data:
                    raise ConnectionError("stream ended")
                _stats["bytes"] += len(data)
                t0 = cut.t
                pcm = decode_pcm(data)
                calls = cut.feed(pcm)
                ring.append((t0, cut.t, data))
                horizon = cut.t - MAX_CALL_S - 5
                while ring and ring[0][1] < horizon:
                    ring.pop(0)
                for a, b in calls:
                    mp3 = b"".join(d for x0, x1, d in ring if x1 > a and x0 < b)
                    if mp3:
                        save_call(s, wall0 + a, mp3)
        except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError, ValueError) as e:
            _stats["drops"] += 1
            _stats["last_error"] = f"{s['name'][:30]}: {type(e).__name__} {str(e)[:60]}"
            st = _live.get(s["key"])
            if st:
                st["up"] = False
        finally:
            if writer is not None:
                writer.close()
        await asyncio.sleep(backoff)
        backoff = min(1800.0, backoff * 2)


async def _main() -> None:
    from apb.ingest import rdio
    proxy = rdio.proxy()
    tasks = [asyncio.create_task(listen(s, proxy)) for s in _streams]
    _stats["listening"] = len(tasks)
    last_count = last_prune = time.time()
    while not _stop.is_set():
        await asyncio.sleep(5)
        now = time.time()
        if now - last_count >= 60:
            record_counts(now)
            last_count = now
        if now - last_prune >= 3600:
            prune(now)
            last_prune = now
    for t in tasks:
        t.cancel()


def _run() -> None:
    try:
        if time.time() - (list_path().stat().st_mtime if list_path().exists() else 0) > 86400:
            discover()
    except Exception as e:
        log.info("streams: directory: %s", str(e)[:160])
    choose()
    if _streams:
        asyncio.run(_main())


def choose() -> list[dict]:
    """Pick which streams to listen to: APB_STREAMS_ONLY keys if set, else the list's
    first APB_STREAMS_MAX (directory order). Registers them with the Rdio lane."""
    global _streams
    from apb.ingest import rdio
    # rules may have tightened; probed mounts were filtered on their names by icecast_probe
    found = [s for s in load() if (probed_ok(s.get("name") or "", s.get("genre") or "") if s.get("via") == "shodan"
                                   else wanted(s.get("name") or "", s.get("genre") or ""))]
    only = {k.strip() for k in os.environ.get("APB_STREAMS_ONLY", "").split(",") if k.strip()}
    picked = [s for s in found if s["key"] in only] if only else found[:max_streams()]
    _streams = picked
    for s in picked:
        rdio._cfg[s["key"]] = (time.time() + 10 * 365 * 86400.0, cfg_for(s))
    _stats["listed"] = len(found)
    return picked


def stream(key: str) -> dict | None:
    return next((s for s in _streams if s["key"] == key), None)


def start() -> bool:
    global _thread
    from apb.ingest import rdio
    if _thread or off() or max_streams() == 0:
        return False
    if not rdio.proxy() and os.environ.get("APB_RDIO_DIRECT", "") != "1":
        log.info("streams: no egress proxy (APB_RDIO_PROXY) — not listening from this address")
        return False
    _stop.clear()
    _thread = threading.Thread(target=_run, daemon=True, name="streams")
    _thread.start()
    return True


def stop() -> None:
    _stop.set()


def status() -> dict:
    up = sum(1 for v in _live.values() if v.get("up"))
    return {**_stats, "up": up, "running": bool(_thread and _thread.is_alive()), "max": max_streams()}
