"""Compact national weather/air-quality field for the map.

Samples a deliberately coarse CONUS grid in two multi-coordinate requests. The
browser interpolates these broad cells visually; this is context, not station-grade
observation data. Cached by the API so every client shares the same model snapshot.
"""
from __future__ import annotations

import httpx

_WEATHER = "https://api.open-meteo.com/v1/forecast"
_AIR = "https://air-quality-api.open-meteo.com/v1/air-quality"
_UA = {"User-Agent": "panoptes/0.1 (panoptes.run; environmental context map)"}
_client = httpx.Client(timeout=25.0, headers=_UA, follow_redirects=True)

# A land-shaped 30-cell CONUS mesh: enough to reveal regional structure without
# spending calls on ocean cells or implying street-level model precision.
GRID = (
    [(27.0, lon) for lon in (-98.0, -90.0, -82.0)]
    + [(32.25, lon) for lon in (-118.0, -110.0, -102.0, -94.0, -86.0, -78.0)]
    + [(lat, lon) for lat in (37.5, 42.75, 48.0)
       for lon in (-122.0, -114.0, -106.0, -98.0, -90.0, -82.0, -74.0)]
)


def _many(payload):
    return payload if isinstance(payload, list) else [payload]


def fetch_environment() -> list[dict]:
    latitudes = ",".join(str(p[0]) for p in GRID)
    longitudes = ",".join(str(p[1]) for p in GRID)
    common = {"latitude": latitudes, "longitude": longitudes, "timezone": "UTC"}
    weather = _many(_client.get(_WEATHER, params={
        **common,
        "current": ("temperature_2m,relative_humidity_2m,dew_point_2m,"
                    "apparent_temperature,precipitation,rain"),
        "hourly": "precipitation", "past_hours": 6, "forecast_hours": 1,
    }).json())
    air = _many(_client.get(_AIR, params={
        **common, "current": "us_aqi,pm2_5,uv_index",
    }).json())

    out = []
    for i, (lat, lon) in enumerate(GRID):
        w = weather[i] if i < len(weather) else {}
        a = air[i] if i < len(air) else {}
        current = w.get("current") or {}
        aq = a.get("current") or {}
        hourly = w.get("hourly") or {}
        rain_6h = sum(float(v or 0) for v in (hourly.get("precipitation") or [])[-7:])
        out.append({
            "lat": lat, "lon": lon, "at": current.get("time") or aq.get("time"),
            "temperature_c": current.get("temperature_2m"),
            "apparent_c": current.get("apparent_temperature"),
            "humidity": current.get("relative_humidity_2m"),
            "dewpoint_c": current.get("dew_point_2m"),
            "precipitation_mm": current.get("precipitation"),
            "rain_mm": current.get("rain"), "rain_6h_mm": round(rain_6h, 2),
            "uv": aq.get("uv_index"), "us_aqi": aq.get("us_aqi"),
            "pm2_5": aq.get("pm2_5"),
        })
    return out
