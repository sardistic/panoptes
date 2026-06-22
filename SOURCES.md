# Source registration — unlocking more coverage

Panoptes runs every **keyless** lane out of the box (CAD/911 catalogs, hazards,
weather, FAA, traffic, NDBC buoys, plus the Bluesky / news RSS / Reddit-Mastodon /
ADS-B collectors). The lanes below stay **dark until you register a free key** and
set it in the environment (Railway → Variables, or your local `.env`). Each one
adds an independent signal source, which directly improves `/live/fused` coverage
and surge-score source-diversity.

## Live-map coverage keys (register these first)

| Lane | Env var(s) | What it adds | Register (all free) |
|---|---|---|---|
| NASA FIRMS | `FIRMS_MAP_KEY` | Per-pixel satellite wildfire/hotspot detections (VIIRS, CONUS) | https://firms.modaps.eosdis.nasa.gov/api/map_key/ — instant email key |
| EPA AirNow | `AIRNOW_KEY` | Official AQI by station; smoke/hazmat proxy | https://docs.airnowapi.org/ → "Request an API key" |
| OpenAQ | `OPENAQ_KEY` | PM2.5 air-quality spikes (v3 API) | https://openaq.org/ → account → API key (https://docs.openaq.org/) |
| aisstream.io | `AISSTREAM_KEY` | Live maritime AIS vessel positions (websocket) | https://aisstream.io/ → free signup → API key |
| ACLED | `ACLED_KEY` + `ACLED_EMAIL` | Protests, riots, political-violence events | https://acleddata.com/ → register → access key (use your registered email) |

Notes:
- **AISStream** is a websocket firehose. `websockets` ships with `uvicorn[standard]`,
  so it runs on the lean Railway image once the key is set.
- **ACLED** needs *both* `ACLED_KEY` and `ACLED_EMAIL` (the email you registered with).
- These lanes self-register only when their key is present — no key, no extra load.

## Backend / pipeline keys (local work, not the live map)

You do **not** need these for the deployed map. They unlock the heavier
local pipeline and source discovery:

| Purpose | Env var(s) | When you need it | Register |
|---|---|---|---|
| Source discovery (dorking) | `GOOGLE_API_KEY` + `GOOGLE_CX` | Re-running `apb/discover/*` locally to regenerate the committed `data/*.json` catalogs | API key: console.cloud.google.com (enable Custom Search API) · CX: programmablesearchengine.google.com (search entire web). Free 100 queries/day |
| Incident extraction / sentiment | `ANTHROPIC_API_KEY` | Transcription→inference enrichment (`scripts/run_pipeline.py`) | console.anthropic.com |
| Type-learner (alt) | `OPENAI_API_KEY` | Optional alternative provider for the JSON type-mapping task | platform.openai.com |
| Radio-call fallback | `BROADCASTIFY_API_KEY` + `_USERNAME` + `_PASSWORD` | Broadcastify Calls ingestion when not using your own SDR node | https://www.broadcastify.com/calls/ |

## Priority for maximum availability

1. `FIRMS_MAP_KEY`, `AIRNOW_KEY`, `OPENAQ_KEY` — instant, no review, three new lanes.
2. `AISSTREAM_KEY` — instant signup, adds the maritime layer.
3. `ACLED_KEY` + `ACLED_EMAIL` — quick account, adds civil-unrest events.

Everything else is keyless and already on. See `.env.example` for the full variable
list and the `*_OFF` opt-out flags.
