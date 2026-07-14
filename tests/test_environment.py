from apb.ingest import environment


class _Response:
    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body


class _Client:
    def get(self, url, params):
        rows = []
        for _ in environment.GRID:
            if "air-quality" in url:
                rows.append({"current": {"time": "2026-07-14T03:00", "us_aqi": 72,
                                         "pm2_5": 18.5, "uv_index": 7.2}})
            else:
                rows.append({
                    "current": {"time": "2026-07-14T03:00", "temperature_2m": 31,
                                "relative_humidity_2m": 70, "dew_point_2m": 24,
                                "apparent_temperature": 37, "precipitation": 1,
                                "rain": 1},
                    "hourly": {"precipitation": [0, 1, 2, 3]},
                })
        return _Response(rows)


def test_environment_merges_weather_air_and_recent_rain(monkeypatch):
    monkeypatch.setattr(environment, "_client", _Client())
    rows = environment.fetch_environment()
    assert len(rows) == len(environment.GRID)
    assert rows[0]["apparent_c"] == 37
    assert rows[0]["dewpoint_c"] == 24
    assert rows[0]["rain_6h_mm"] == 6
    assert rows[0]["uv"] == 7.2
    assert rows[0]["us_aqi"] == 72
