# APB — Aggregated Public Broadcast intelligence

Ingests public-safety radio (police/fire/EMS) across major US metros, transcribes
audio, and extracts structured incidents + sentiment/threat signals for query and
alerting.

## Pipeline (activity-first)

The foundation is radio **activity/metadata**, not transcripts — it's cheap, fast,
and works even on **encrypted** systems. Transcription is a deferred enrichment layer.

```
own trunk-recorder node ─┐                       ┌─► aggregate → ActivityWindow
Broadcastify (fallback) ─┴─► ingest (metadata) ──┤   + rolling baseline + anomaly
                                                 └─► store (PostGIS) → API /activity
                              (later enrichment)
                              transcribe → infer (Claude: incident+sentiment+geocode)
```

- `scripts/run_activity.py` — the day-one pipeline: metadata → anomaly → store.
- `scripts/run_pipeline.py` — optional transcription/extraction enrichment (bookmarked).
- Sourcing prefers your own SDR nodes + OpenMHz over Broadcastify (avoid lock-in).
- Geocoding: self-hosted Nominatim (`APB_NOMINATIM_URL`), metro-bbox constrained.

## Fusion layer

APB now treats CAD, radio activity, social posts, news, traffic, and weather as
event sensors. Each source normalizes into an `EventSignal`, then `/live/fused`
clusters nearby signals and ranks them by surge score: volume, recency, severity,
confidence, and independent source diversity.

- `/live/signals` — normalized source rows from CAD/history plus optional
  `data/social_seed.jsonl` rows for offline social/news experiments.
- `/live/fused` — source-diverse event clusters for the national "Fused Events"
  panel.
- `apb.ingest.bluesky.BlueskyJetstream` — optional Bluesky/ATProto Jetstream
  collector scaffold. Install `websockets` only when running that collector.
- `scripts/run_bluesky.py` — bounded collector that appends event-like public posts
  to `data/social_seed.jsonl`. Posts without exact geo are resolved only to coarse
  metro centroids when the text explicitly names a known place.

## Legal / ethical guardrails (read first)

- Receiving public-safety radio is legal federally and in most states. **Encrypted
  systems are off-limits** — do not ingest or attempt to decrypt (ECPA/CALEA).
- Source feeds (Broadcastify, OpenMHz) have **API terms / licenses** — respect them.
- The pipeline **redacts PII** (names, addresses, phone, medical, victim details)
  before any record is exposed via the API. Dissemination is the highest-liability
  stage; keep redaction on.

## Quick start

```bash
python -m venv .venv && . .venv/Scripts/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env            # fill in keys
# start postgres+postgis (see docker-compose), then:
python -m apb.store.db --init
python scripts/run_activity.py --metro nyc      # activity-first, no transcription
uvicorn apb.api.main:app --reload               # GET /activity?anomalous_only=true
```

## Status

First milestone: one-metro-at-a-time vertical slice across major metros, with
sentiment analysis. Single-process orchestrator for now; swap the in-process
queue in `scripts/run_pipeline.py` for Redis/RabbitMQ before scaling.
