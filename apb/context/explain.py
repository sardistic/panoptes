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
_model_cache: dict = {"name": None, "at": 0.0}
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


def pick_model(key: str) -> str:
    """Newest general-purpose Flash chat model available to this key."""
    pinned = os.environ.get("GEMINI_MODEL", "").strip()
    if pinned:
        return pinned
    now = time.time()
    if _model_cache["name"] and now - _model_cache["at"] < 3600:
        return _model_cache["name"]
    chosen = _FALLBACK_MODEL
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
            chosen = flash[0]
        _model_cache.update(name=chosen, at=now)      # cache only a real discovery
    except (httpx.HTTPError, ValueError, KeyError) as e:
        log.warning("gemini model discovery failed (%s); using %s", e, chosen)
    return chosen


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
        "plain English, 120-220 words, no markdown headers. Lead with the single most "
        "important thing in the box, then the rest by importance. Be concrete (names, "
        "roads, counts, ages). Separate what the data shows from what you infer. If the "
        "camera images show anything relevant (traffic state, weather, smoke, flooding, "
        "emergency vehicles, signage or on-image text such as timestamps), say so and cite "
        "which camera. If the box is quiet, say that plainly and describe the ambient picture."),
    "cameras": (
        "The user wants to inspect the live cameras inside the rectangle. For EACH attached "
        "still, one short line prefixed by its number: what the camera shows right now — "
        "traffic density and flow, stopped or emergency vehicles, weather visible in frame "
        "(wet road, fog, snow, smoke/haze), lighting/time of day, any on-image text (timestamps, "
        "camera labels, signs) read verbatim, and anything unusual. Then a two-sentence "
        "synthesis: what the cameras together say about conditions in the box, and which "
        "camera deserves a closer look. Say 'frame unavailable/black' if an image is blank."),
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
    "place": (
        "Focus on the place itself: what area/city/roads/landmarks the rectangle covers "
        "(infer from coordinates, incident locations, roadway names, camera names and news), "
        "what kind of environment it is (urban core, highway corridor, rural, coastal), and "
        "the baseline you would expect there at this hour versus what the data shows. "
        "120-180 words."),
}


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
    if camera_names:
        lines.append("Attached images are live stills from these cameras, in order: "
                     + "; ".join(f"[{i + 1}] {n}" for i, n in enumerate(camera_names)))
    else:
        lines.append("No camera stills were available inside the box.")
    return "\n".join(lines)


def explain(ctx: dict, stills: list[tuple[str, bytes, str]]) -> dict:
    """stills: (camera name, image bytes, content-type). Returns {model, text}."""
    key = api_key()
    if not key:
        raise RuntimeError("GEMINI_API_KEY is not configured")
    model = pick_model(key)
    view = ctx.get("view") or {}
    weather = weather_at(float(view.get("lat", 0)), float(view.get("lon", 0))) if view else {}
    parts: list[dict] = [{"text": build_prompt(ctx, weather, [s[0] for s in stills])}]
    for _, data, ctype in stills:
        parts.append({"inline_data": {"mime_type": ctype, "data": base64.b64encode(data).decode()}})
    body = {"contents": [{"role": "user", "parts": parts}],
            "generationConfig": {"temperature": 0.4, "maxOutputTokens": 1200}}
    r = _client.post(f"{_GEN}/models/{model}:generateContent", params={"key": key}, json=body)
    if r.status_code != 200:
        msg = ""
        try:
            msg = r.json().get("error", {}).get("message", "")
        except ValueError:
            pass
        raise RuntimeError(f"gemini {r.status_code}: {msg[:200]}")
    data = r.json()
    text = "".join(p.get("text", "") for c in data.get("candidates", [])[:1]
                   for p in (c.get("content") or {}).get("parts", []))
    return {"model": model, "text": text.strip(), "weather": weather,
            "focus": ctx.get("focus") or "overview", "cameras": [s[0] for s in stills]}
