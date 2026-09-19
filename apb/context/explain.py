"""Scene explainer — "what am I looking at?" for a user-drawn map rectangle.

The browser sends the abstracted view (bounds, active layers, filters, the incident /
hazard / surge / cluster rows inside the rectangle, the environment readout) plus the
ids of live cameras inside it. This module adds keyless weather at the rectangle's
center, pulls the camera stills through the registry (never client-supplied URLs),
and asks the newest Gemini Flash model for a grounded summary: what is happening,
why the map looks the way it does, what the cameras actually show (including any
on-image text such as timestamps and signage), and what is uncertain.

Credentials: GEMINI_API_KEY (falls back to GOOGLE_API_KEY). GEMINI_MODEL pins a
model; otherwise the ListModels endpoint is queried and the highest-version
`*flash*` chat model is chosen (cached for an hour).
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import httpx

log = logging.getLogger(__name__)

_GEN = os.environ.get("GEMINI_API_BASE", "https://generativelanguage.googleapis.com/v1beta")  # test hook
_WEATHER = "https://api.open-meteo.com/v1/forecast"
_UA = {"User-Agent": "panoptes/0.1 (panoptes.run; scene explainer)"}
_client = httpx.Client(timeout=60.0, headers=_UA, follow_redirects=True)
_model_cache: dict = {"names": None, "at": 0.0}
_benched: dict[str, float] = {}          # model -> until (unix); set on 429/503/timeouts
_BENCH_SEC = 15 * 60.0
_FALLBACK_MODEL = "gemini-2.5-flash"

# WMO weather interpretation codes -> plain words (Open-Meteo `weather_code`).
_WMO = {0: "clear", 1: "mainly clear", 2: "partly cloudy", 3: "overcast", 45: "fog",
        48: "rime fog", 51: "light drizzle", 53: "drizzle", 55: "heavy drizzle",
        61: "light rain", 63: "rain", 65: "heavy rain", 66: "freezing rain",
        67: "heavy freezing rain", 71: "light snow", 73: "snow", 75: "heavy snow",
        77: "snow grains", 80: "rain showers", 81: "heavy showers", 82: "violent showers",
        85: "snow showers", 86: "heavy snow showers", 95: "thunderstorm",
        96: "thunderstorm with hail", 99: "severe thunderstorm with hail"}


def api_key() -> str:
    return (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or "").strip()


def _version(name: str) -> tuple:
    m = re.search(r"gemini-(\d+)(?:\.(\d+))?", name)
    return (int(m.group(1)), int(m.group(2) or 0)) if m else (0, 0)


def candidate_models(key: str) -> list[str]:
    """General-purpose Flash chat models for this key, newest first. The newest one
    is sometimes capacity-limited (503 "high demand"), so callers walk the list."""
    pinned = os.environ.get("GEMINI_MODEL", "").strip()
    if pinned:
        return [pinned]
    now = time.time()
    if _model_cache["names"] and now - _model_cache["at"] < 3600:
        return _model_cache["names"]
    chosen = [_FALLBACK_MODEL]
    try:
        r = _client.get(f"{_GEN}/models", params={"key": key, "pageSize": 200}, timeout=20.0)
        r.raise_for_status()
        names = [m["name"].split("/", 1)[-1] for m in r.json().get("models", [])
                 if "generateContent" in m.get("supportedGenerationMethods", [])]
        flash = [n for n in names if "flash" in n
                 and not re.search(r"lite|image|tts|audio|live|8b|native|thinking|exp", n)]
        if flash:
            # highest version first; among equals prefer a stable name over preview/dated
            flash.sort(key=lambda n: (_version(n), 0 if re.search(r"preview|\d{2}-\d{2}", n) else 1),
                       reverse=True)
            chosen = flash
        _model_cache.update(names=chosen, at=now)     # cache only a real discovery
    except (httpx.HTTPError, ValueError, KeyError) as e:
        log.warning("gemini model discovery failed (%s); using %s", e, chosen)
    return chosen


def healthy_models(key: str) -> list[str]:
    """candidate_models with recently-failing models moved to the back, so a busy
    newest model costs one slow call every 15 minutes instead of every request."""
    now = time.time()
    names = candidate_models(key)
    return ([n for n in names if _benched.get(n, 0) <= now]
            + [n for n in names if _benched.get(n, 0) > now])


def pick_model(key: str) -> str:
    """Newest general-purpose Flash chat model available to this key."""
    return candidate_models(key)[0]


def weather_at(lat: float, lon: float) -> dict:
    """Current conditions at a point (keyless Open-Meteo), already in plain words."""
    try:
        r = _client.get(_WEATHER, params={
            "latitude": round(lat, 3), "longitude": round(lon, 3), "timezone": "auto",
            "current": ("temperature_2m,apparent_temperature,relative_humidity_2m,"
                        "precipitation,weather_code,wind_speed_10m,wind_gusts_10m,"
                        "wind_direction_10m,visibility,is_day,cloud_cover"),
        }, timeout=15.0)
        r.raise_for_status()
        c = r.json().get("current") or {}
        code = c.get("weather_code")
        c["conditions"] = _WMO.get(int(code), "unknown") if code is not None else "unknown"
        c["local_time"] = c.get("time")
        c["timezone"] = r.json().get("timezone")
        return c
    except (httpx.HTTPError, ValueError) as e:
        log.info("weather lookup failed: %s", e)
        return {}


# Drill-down modes. "overview" is the first answer; the others re-ask the same box
# with one facet in the foreground (and, for cameras, more stills + per-camera notes).
FOCUS_BRIEFS = {
    "overview": (
        "A user drew a rectangle on the map and asked: what am I looking at? Answer in "
        "plain English, 90-160 words, no markdown headers. Lead with the single most "
        "important thing in the box, then the rest by importance. Be concrete (names, "
        "roads, counts, ages). Separate what the data shows from what you infer. If the "
        "camera images show anything relevant (traffic state, weather, smoke, flooding, "
        "emergency vehicles, signage or on-image text such as timestamps), say so and cite "
        "which camera. If the box is quiet, say that plainly and describe the ambient picture. "
        "ALWAYS finish with one line starting 'Cameras:' that says in a sentence what the "
        "attached stills show (or 'Cameras: none in the box')."),
    "cameras": (
        "The user wants to inspect the live cameras inside the rectangle. For EACH attached "
        "still, one short line prefixed by its number: what the camera shows right now — "
        "traffic density and flow, stopped or emergency vehicles, weather visible in frame "
        "(wet road, fog, snow, smoke/haze), lighting/time of day, any on-image text (timestamps, "
        "camera labels, signs) read verbatim, and anything unusual. Then a two-sentence "
        "synthesis: what the cameras together say about conditions in the box, and which "
        "camera deserves a closer look. Say 'frame unavailable/black' if an image is blank. "
        "FINALLY append one line `OBS: ` followed by a JSON array, one object per camera still "
        "in order: {\"i\": n, \"vehicles\": int, \"pedestrians\": int, \"road_wet\": bool, "
        "\"visibility\": \"good|reduced|poor\", \"notable\": \"short phrase or empty\"}."),
    "incidents": (
        "Focus on the incidents (CAD/911/DOT rows) inside the rectangle. Group them by what "
        "is going on (same road, same block, same type, converging types), call out the "
        "most severe and the newest, note any pattern versus the surge/cluster signals, and "
        "say what a dispatcher would want to know. 120-200 words, plain English."),
    "hazards": (
        "Focus on hazards and warnings inside the rectangle: NWS products, quakes, fire, "
        "flood, smoke, air quality, traffic hazards, aviation/marine notices. Say what is "
        "active, how severe, when it expires or started, and how it interacts with the "
        "weather and the incidents. 120-200 words."),
    "social": (
        "Focus on the social posts and news headlines placed inside or near the rectangle. "
        "What are people reporting, does it corroborate or contradict the official data, "
        "what is rumor versus confirmed, and what is missing. Quote sparingly. 120-200 words."),
    "weather": (
        "Focus on weather and environment: current conditions at the box center, the map's "
        "environment readout, visibility, wind, precipitation, air quality, day/night, and "
        "what the camera stills show about actual conditions. Explain how the weather bears "
        "on the incidents/hazards present. 100-180 words."),
    "facts": (
        "The user asked for the FACTS of this rectangle. If a QUESTION is given, answer it "
        "first and directly, using only the sections that bear on it, then add whatever "
        "else is notable. You are given a structured facts "
        "digest (place, population, economy, terrain, water, sky, air, nature, activity — "
        "each labelled with its source). Write a tight briefing, 150-260 words, organised "
        "as short labelled lines (Place:, People:, Economy:, Safety:, Health:, Terrain & water:, "
        "Sky & atmosphere:, Air:, Nature:, Activity:, Connectivity:). Quote the numbers with units; say which are estimates "
        "(WorldPop, tract-level ACS, model-derived weather). Note anything notable or "
        "contradictory. Do not invent metrics that are not in the digest."),
    "street": (
        "The user asked what this area looks like AT GROUND LEVEL. You are given street-"
        "level frames (Mapillary / KartaView / Google Street View), each labelled with "
        "provider, capture date and position, plus any live camera stills. Describe the "
        "built environment: road type and width, lanes, sidewalks, storefronts vs housing "
        "vs industrial, vegetation, signage you can read, condition, anything notable. "
        "Say how old each frame is and where in the box it sits (north/south/east/west). "
        "Then relate it to the live data: does the ground view explain the incidents or "
        "cameras? 140-240 words, plain English."),
    "place": (
        "Focus on the place itself: what area/city/roads/landmarks the rectangle covers "
        "(infer from coordinates, incident locations, roadway names, camera names and news), "
        "what kind of environment it is (urban core, highway corridor, rural, coastal), and "
        "the baseline you would expect there at this hour versus what the data shows. "
        "120-180 words."),
}


_GIBS_WMS = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi"
_IEM_WMS = "https://mesonet.agron.iastate.edu/cgi-bin/wms/nexrad/n0q.cgi"


def sky_urls(bounds: dict) -> dict[str, str]:
    """WMS crops of the sky over the box: GOES GeoColor (the map's satellite layer)
    padded to a regional view so clouds/smoke/lights read, and CONUS NEXRAD base
    reflectivity (the map's radar layer). Same public services the map itself uses."""
    try:
        s, n = float(bounds["south"]), float(bounds["north"])
        w, e = float(bounds["west"]), float(bounds["east"])
    except (KeyError, TypeError, ValueError):
        return {}
    clat, clon = (s + n) / 2, (w + e) / 2
    out: dict[str, str] = {}

    def pad(min_span: float) -> tuple[float, float, float, float]:
        hs = max(min_span, (n - s)) / 2
        hw = max(min_span * 1.3, (e - w)) / 2
        return (max(-85, clat - hs), clon - hw, min(85, clat + hs), clon + hw)

    if -170 <= clon <= -20 and -60 <= clat <= 70:          # GOES East/West footprint
        layer = "GOES-West_ABI_GeoColor" if clon < -105 else "GOES-East_ABI_GeoColor"
        ps, pw, pn, pe = pad(1.6)
        out["goes"] = (f"{_GIBS_WMS}?SERVICE=WMS&REQUEST=GetMap&VERSION=1.3.0&LAYERS={layer}"
                       f"&CRS=EPSG:4326&BBOX={ps:.3f},{pw:.3f},{pn:.3f},{pe:.3f}&WIDTH=640"
                       f"&HEIGHT=640&FORMAT=image/png&TIME=default")
    if -130 <= clon <= -60 and 22 <= clat <= 52:           # NEXRAD CONUS mosaic
        ps, pw, pn, pe = pad(1.0)
        out["radar"] = (f"{_IEM_WMS}?SERVICE=WMS&REQUEST=GetMap&VERSION=1.1.1&LAYERS=nexrad-n0q"
                        f"&SRS=EPSG:4326&BBOX={pw:.3f},{ps:.3f},{pe:.3f},{pn:.3f}&WIDTH=640"
                        f"&HEIGHT=640&FORMAT=image/png&TRANSPARENT=true")
    return out


def sky_images(bounds: dict) -> list[tuple[str, bytes, str]]:
    """Fetch the sky crops as image parts (label, bytes, mime); failures are skipped."""
    labels = {"goes": "GOES GeoColor satellite crop of the region around the box (latest ~10-min "
                      "frame; at night city lights are orange, clouds blue-white)",
              "radar": "NEXRAD base-reflectivity radar crop around the box (transparent = no echoes; "
                       "greens light rain, yellows/reds heavy, purples hail-capable)"}
    out = []
    for key, url in sky_urls(bounds).items():
        try:
            r = _client.get(url, timeout=20.0)
            if r.status_code == 200 and r.content[:4] == bytes.fromhex("89504e47") and len(r.content) < 1_500_000:
                out.append((labels[key], r.content, "image/png"))
        except httpx.HTTPError as e:
            log.info("sky crop %s failed: %s", key, e)
    return out


def build_prompt(ctx: dict, weather: dict, camera_names: list[str]) -> str:
    b = ctx.get("bounds") or {}
    view = ctx.get("view") or {}
    focus = ctx.get("focus") or "overview"
    lines = [
        "You are the analyst behind Panoptes, a live public-safety / hazard map. "
        + FOCUS_BRIEFS.get(focus, FOCUS_BRIEFS["overview"]),
        "",
        f"UTC now: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}. "
        f"User's local time: {ctx.get('local_time', '?')}.",
        f"Rectangle: lat {b.get('south')}..{b.get('north')}, lon {b.get('west')}..{b.get('east')} "
        f"(center {view.get('lat')}, {view.get('lon')}; map zoom {view.get('zoom')}).",
        f"Area selection: {ctx.get('metro', 'national overview')}; time window {ctx.get('window', '?')}; "
        f"type filter: {ctx.get('type') or 'all'}; severity floor {ctx.get('severity', 0)}.",
        f"Layers on: {', '.join(ctx.get('layers') or []) or 'none'}.",
    ]
    if weather:
        lines.append("Weather at center (Open-Meteo, now): " + json.dumps(
            {k: weather.get(k) for k in ("conditions", "temperature_2m", "apparent_temperature",
                                         "relative_humidity_2m", "precipitation", "wind_speed_10m",
                                         "wind_gusts_10m", "wind_direction_10m", "visibility",
                                         "cloud_cover", "is_day", "local_time", "timezone")}))
    if ctx.get("environment"):
        lines.append(f"Map environment readout: {ctx['environment']}")
    for key, label in (("incidents", "Incidents in box (CAD/911/DOT, newest/highest first)"),
                       ("hazards", "Hazards & NWS warnings in box"),
                       ("surges", "Surge anomalies in box (rate vs baseline)"),
                       ("clusters", "Emerging clusters in box"),
                       ("social", "Social posts in/near box"),
                       ("news", "Placed news headlines near box"),
                       ("counts", "Counts")):
        val = ctx.get(key)
        if val:
            lines.append(f"{label}: {json.dumps(val, ensure_ascii=False)[:6000]}")
    if ctx.get("question"):
        lines.append("QUESTION from the user: " + ctx["question"][:300])
    if ctx.get("facts"):
        lines.append("Facts digest for the box (JSON; 'notable' and 'baseline_vs_30d' compare against this "
                     "place's own recent history — cite percentiles when present): " + ctx["facts"])
    lines.append("Whenever two sources disagree (e.g. METAR says clear but a camera shows fog, radar shows "
                 "echoes but the station reports dry), say so explicitly — the disagreement is information.")
    if ctx.get("prior"):
        lines.append("Earlier looks at this same area (your own previous answers; note what "
                     "changed since): " + json.dumps(ctx["prior"], ensure_ascii=False)[:2500])
    if ctx.get("stream_cameras"):
        lines.append("Live-stream-only cameras inside the box (no frame attached unless listed "
                     "below): " + "; ".join(ctx["stream_cameras"]))
    if ctx.get("sky_labels"):
        lines.append("The FIRST attached images are sky/weather crops, in order: "
                     + "; ".join(f"[S{i + 1}] {n}" for i, n in enumerate(ctx["sky_labels"]))
                     + ". Use them for cloud cover, storms, smoke and lights; say what they show.")
    if ctx.get("street_frames"):
        lines.append(f"{ctx['street_frames']} street-level frames are attached (labelled 'street level'); "
                     "they are static imagery, months or years old — say so.")
    if camera_names:
        lines.append("Then live camera stills, in order: "
                     + "; ".join(f"[{i + 1}] {n}" for i, n in enumerate(camera_names)))
    else:
        lines.append("No camera stills were available inside the box"
                     + (" (only the stream-only cameras above)." if ctx.get("stream_cameras") else "."))
    return "\n".join(lines)


def generate(parts: list[dict], max_tokens: int = 800) -> str:
    """One model call with the same model fallback as explain(); returns the text."""
    key = api_key()
    if not key:
        raise RuntimeError("GEMINI_API_KEY is not configured")
    body = {"contents": [{"role": "user", "parts": parts}],
            "generationConfig": {"temperature": 0.2, "maxOutputTokens": max_tokens,
                                 "thinkingConfig": {"thinkingBudget": 0}}}
    r = None
    for model in healthy_models(key)[:3]:
        try:
            r = _client.post(f"{_GEN}/models/{model}:generateContent", params={"key": key}, json=body, timeout=25.0)
            if r.status_code == 400 and "thinking" in r.text.lower():
                slim = {**body, "generationConfig": {k: v for k, v in body["generationConfig"].items() if k != "thinkingConfig"}}
                r = _client.post(f"{_GEN}/models/{model}:generateContent", params={"key": key}, json=slim, timeout=25.0)
        except httpx.TimeoutException:
            _benched[model] = time.time() + _BENCH_SEC
            continue
        if r.status_code == 200:
            break
        if r.status_code in (429, 503):
            _benched[model] = time.time() + _BENCH_SEC
            continue
        break
    if r is None or r.status_code != 200:
        raise RuntimeError(f"gemini {r.status_code if r else '?'}")
    return "".join(p.get("text", "") for c in r.json().get("candidates", [])[:1]
                   for p in (c.get("content") or {}).get("parts", []))


def explain(ctx: dict, stills: list[tuple[str, bytes, str]]) -> dict:
    """stills: (camera name, image bytes, content-type). Returns {model, text}."""
    key = api_key()
    if not key:
        raise RuntimeError("GEMINI_API_KEY is not configured")
    view = ctx.get("view") or {}
    weather = weather_at(float(view.get("lat", 0)), float(view.get("lon", 0))) if view else {}
    sky = sky_images(ctx.get("bounds") or {}) if ctx.get("focus", "overview") in (
        "overview", "weather", "hazards", "place") else []
    ctx["sky_labels"] = [s[0].split(" (")[0] for s in sky]
    parts: list[dict] = [{"text": build_prompt(ctx, weather, [s[0] for s in stills])}]
    for _, data, ctype in sky + stills:
        parts.append({"inline_data": {"mime_type": ctype, "data": base64.b64encode(data).decode()}})
    # Thinking is disabled: on Gemini 3.x Flash the hidden thinking tokens otherwise
    # consume the output budget (MAX_TOKENS after one sentence) and triple latency.
    body = {"contents": [{"role": "user", "parts": parts}],
            "generationConfig": {"temperature": 0.4, "maxOutputTokens": 2048,
                                 "thinkingConfig": {"thinkingBudget": 0}}}
    r = None
    for model in healthy_models(key)[:3]:          # newest healthy first; step down when busy
        try:
            r = _client.post(f"{_GEN}/models/{model}:generateContent", params={"key": key},
                             json=body, timeout=25.0)
            if r.status_code == 400 and "thinking" in r.text.lower():   # model without the knob
                slim = {**body, "generationConfig": {k: v for k, v in body["generationConfig"].items()
                                                     if k != "thinkingConfig"}}
                r = _client.post(f"{_GEN}/models/{model}:generateContent", params={"key": key},
                                 json=slim, timeout=25.0)
        except httpx.TimeoutException:
            log.info("gemini %s timed out; benching it", model)
            _benched[model] = time.time() + _BENCH_SEC
            continue
        if r.status_code == 200:
            break
        if r.status_code not in (429, 503):
            break
        log.info("gemini %s busy (%d); benching it", model, r.status_code)
        _benched[model] = time.time() + _BENCH_SEC
    if r is None or r.status_code != 200:
        msg = ""
        try:
            msg = r.json().get("error", {}).get("message", "")
        except (ValueError, AttributeError):
            pass
        raise RuntimeError(f"gemini {r.status_code if r else '?'}: {msg[:200]}")
    data = r.json()
    text = "".join(p.get("text", "") for c in data.get("candidates", [])[:1]
                   for p in (c.get("content") or {}).get("parts", []))
    obs = None
    m = re.search(r"\n?OBS:\s*(\[.*\])\s*$", text, re.S)
    if m:
        try:
            obs = json.loads(m.group(1))
            text = text[:m.start()].rstrip()
        except ValueError:
            obs = None
    return {"model": model, "text": text.strip(), "weather": weather,
            "focus": ctx.get("focus") or "overview", "cameras": [s[0] for s in stills], "camera_obs": obs,
            "sky": sky_urls(ctx.get("bounds") or {}) if sky else {}}
