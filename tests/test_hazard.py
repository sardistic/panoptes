from apb.ingest.hazard import HazardIngest


class _Response:
    def json(self):
        return {
            "features": [{
                "id": "https://api.weather.gov/alerts/test",
                "geometry": {"type": "Point", "coordinates": [-97.0, 35.0]},
                "properties": {
                    "id": "urn:oid:test",
                    "event": "Tornado Warning",
                    "areaDesc": "Test County",
                    "effective": "2026-07-13T20:00:00+00:00",
                    "expires": "2026-07-13T21:00:00+00:00",
                    "severity": "Extreme",
                    "certainty": "Observed",
                    "urgency": "Immediate",
                    "headline": "Tornado observed near Testville",
                    "description": "A confirmed tornado is moving northeast.",
                    "instruction": "Take shelter now.",
                },
            }]
        }


class _Client:
    def get(self, *_args, **_kwargs):
        return _Response()


def test_nws_alert_keeps_actionable_cap_intelligence():
    ingest = HazardIngest()
    ingest._client.close()
    ingest._client = _Client()
    row = ingest._nws()[0]
    assert row["event"] == "Tornado Warning"
    assert row["headline"] == "Tornado observed near Testville"
    assert row["instruction"] == "Take shelter now."
    assert row["severity"] == "Extreme"
    assert row["certainty"] == "Observed"
    assert row["urgency"] == "Immediate"
    assert row["expires"] == "2026-07-13T21:00:00+00:00"
    assert row["geometry"] == {"type": "Point", "coordinates": [-97.0, 35.0]}
