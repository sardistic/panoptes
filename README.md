# Panoptes — live event intelligence

[panoptes.run](https://panoptes.run) · All-seeing event intelligence

Panoptes fuses dozens of public, mostly keyless data feeds — CAD/911 dispatch,
public-safety radio activity, hazards, aircraft/maritime traffic, weather, air
quality, and news/social posts — into a single national map of what's happening
right now. Each source normalizes into an `EventSignal`; nearby signals are
clustered and ranked by a surge score (volume, recency, severity, confidence,
and independent-source diversity).

> The Python package is still named `apb` (the project's original name,
> "Aggregated Public Broadcast"); the product and repo are now Panoptes.

## What it ingests

Sources auto-register at API startup. Most are keyless and **on by default**; a few
only register when a free key is present. See [SOURCES.md](SOURCES.md) for what to
register to unlock the keyed lanes.

- **CAD / 911 dispatch** — Socrata + ArcGIS open-data catalogs, PulsePoint
  (AES-decrypted), P2C, Southern Software, ODIN, CHP statewide live incidents
  (California Highway Patrol, keyless). Catalogs are discovered offline
  (see *Source discovery*) and committed under `data/`.
- **Public-safety radio activity** — trunk-recorder / OpenMHz metadata,
  Broadcastify fallback. Activity-first: cheap, fast, and works on encrypted
  systems. Transcription is a deferred enrichment layer.
- **Hazards & weather** — USGS, NWS, EONET, USGS flood, SPC storm reports, NHC
  cyclones, volcano activity, NIFC active wildfires, NASA FIRMS (`FIRMS_MAP_KEY`),
  HMS smoke, FEMA declarations, EMSC global earthquakes, GDACS Orange/Red global
  disaster alerts, AWC SIGMETs (hazardous airspace weather).
- **Air quality** — AirNow (`AIRNOW_KEY`), OpenAQ (`OPENAQ_KEY`).
- **Traffic & transport** — 511 traffic (NY keyless; GA/LA/PA/ID/VA/New England
  unlock with free `T511_*_KEY`s), FAA TFRs, FAA airport delays, Amtrak trains
  running 1h+ late (rail-corridor anomaly signal).
- **Aircraft & maritime** — ADS-B (`APB_ADSB`, heavier/opt-in), AIS stream, NDBC buoys.
- **Civil unrest** — ACLED (`ACLED_KEY` + `ACLED_EMAIL`).
- **News & social** — news RSS, social RSS (Reddit/Mastodon), Bluesky/ATProto
  Jetstream collector, GDELT correlation.

## API

`uvicorn apb.api.main:app` serves the dissemination layer. Selected endpoints:

- `/live/overview` — national rollup across all lanes.
- `/live/fused` — source-diverse, surge-ranked event clusters (the "Fused Events" panel).
- `/live/signals` — normalized `EventSignal` rows from CAD/history + optional
  `data/social_seed.jsonl`.
- `/live/incidents`, `/live/hazards`, `/live/traffic`, `/live/aircraft`,
  `/live/maritime`, `/live/fire`, `/live/wildfires`, `/live/flood`,
  `/live/airquality`, `/live/airnow`, `/live/storm_reports`, `/live/cyclones`,
  `/live/volcano`, `/live/smoke`, `/live/marine`, `/live/airport_delays`,
  `/live/declarations`, `/live/outages`, `/live/unrest`, `/live/social` —
  per-lane live feeds.
- `/live/hazards/all` — cached aggregate of every hazard family, used by the UI to
  avoid a large fan-out of browser requests.
- `/live/stream` — Server-Sent Events snapshots for the selected metro/window. The
  map uses this instead of fixed 15-second polling, with a slow compatibility fallback.
- `/live/emerging`, `/emerging`, `/baseline/anomalies` — surge / anomaly detection
  against rolling, seasonally-bucketed baselines.
- `/events` — persisted fused events with lifecycle (stable uid, first_seen, age,
  peak vs latest score, growing flag). New events over `APB_ALERT_SCORE` POST to
  `APB_WEBHOOK_URL` exactly once (Discord webhooks supported).
- `/status` — per-lane operational health (rows, freshness, backoff, buffers).
- `/correlate`, `/feeds` — keyless news/context correlation (GDELT, BigDataCloud).
- `/incidents`, `/activity` — stored, **PII-redacted** records.
- `/health` (liveness), `/health/ready` (database/worker readiness), `/db/stats`,
  `/live/metros`.

Public numeric query parameters are bounded to protect upstream feeds and clustering
work. Responses include a request id, timing, and browser security headers. Text from
upstream feeds is treated as untrusted and escaped before UI rendering.

The live map UI is served from `web/` at the API root.

## Runtime state

SQLite remains the zero-configuration default for snapshots, fused-event lifecycle,
social/news buffers, and live vessel positions. For multiple web instances, set
`APB_STATE_DATABASE_URL` to
a native PostgreSQL DSN such as `postgresql://user:pass@host:5432/db`. All instances
then share runtime state; a PostgreSQL advisory lock elects exactly one upstream
poller/collector process, follower instances read the shared buffers, event writes are
serialized, and webhook notification rows are claimed with `FOR UPDATE SKIP LOCKED`
to prevent concurrent duplicate delivery.

`APB_DATABASE_URL` is separate: it configures the optional SQLAlchemy/PostGIS radio
pipeline. A deployment may point both variables at the same PostgreSQL service.

## Source discovery (build-time)

`apb/discover/*` finds new CAD/open-data sources via web search and vendor "dork"
registries (Socrata, ArcGIS hubs, PulsePoint, P2C, Southern Software). This runs
**locally** to regenerate the committed `data/*.json` catalogs
(`sources_catalog.json`, `arcgis_catalog.json`, `pulsepoint_agencies.json`,
`p2c_agencies.json`, `southern_agencies.json`, `type_map.json`). Production just
serves those catalogs — no discovery keys needed in prod.

## Radio activity pipeline (activity-first)

The radio foundation is **activity/metadata**, not transcripts.

```
own trunk-recorder node ─┐                       ┌─► aggregate → ActivityWindow
Broadcastify (fallback) ─┴─► ingest (metadata) ──┤   + rolling baseline + anomaly
                                                 └─► store (PostGIS) → API /activity
                              (later enrichment)
                              transcribe → infer (Claude: incident+sentiment+geocode)
```

- `scripts/run_activity.py` — metadata → anomaly → store.
- `scripts/run_pipeline.py` — optional transcription/extraction enrichment (bookmarked).
- `scripts/run_bluesky.py` — bounded Bluesky collector appending event-like posts
  to `data/social_seed.jsonl`.
- Geocoding: self-hosted Nominatim (`APB_NOMINATIM_URL`), metro-bbox constrained.
- Sourcing prefers your own SDR nodes + OpenMHz over Broadcastify (avoid lock-in).

## Legal / ethical guardrails (read first)

- Receiving public-safety radio is legal federally and in most states. **Encrypted
  systems are off-limits** — do not ingest or attempt to decrypt (ECPA/CALEA).
- Source feeds (Broadcastify, OpenMHz, etc.) have **API terms / licenses** — respect them.
- The pipeline **redacts PII** (names, addresses, phone, medical, victim details)
  before any record is exposed via the API. Keep redaction on.

## Quick start

```bash
python -m venv .venv && . .venv/Scripts/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env            # optional keys (FIRMS_MAP_KEY, AIRNOW_KEY, etc.)

# Live map + fused events (keyless, no DB needed):
uvicorn apb.api.main:app --reload    # open http://localhost:8000/

# Full radio/incident pipeline (needs postgres+postgis):
python -m apb.store.db --init
python scripts/run_activity.py --metro nyc
```

## Deploy

Deployed on **Railway** from a lean Docker image:

- `Dockerfile` (python:3.11-slim) installs `requirements-web.txt` only and runs
  `uvicorn apb.api.main:app --host 0.0.0.0 --port $PORT`.
- `requirements-web.txt` is the pinned minimal serving set (FastAPI, HTTPX,
  cryptography, Pydantic, and psycopg for optional shared state). PostGIS and the
  transcription pipeline stay in `requirements.txt` and aren't needed to serve the map.
- The container runs as an unprivileged user. `railway.toml` uses the Dockerfile
  builder with readiness healthcheck `/health/ready`.
- No secrets are required for one instance. For persistent single-instance history,
  set `APB_DB_PATH=/app/state/apb.sqlite` and mount a volume at `/app/state`. For
  horizontal scaling, provision Railway PostgreSQL and set `APB_STATE_DATABASE_URL`.
