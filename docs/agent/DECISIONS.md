# Project Decisions

Record durable architectural and operational decisions here.

### Date — Decision title

Context:

Decision:

Consequences:

### 2026-09-17 — Public live-camera layer: server-side registry + same-origin snapshot proxy

Context: the map needed a "live cameras" view across every public DOT/traffic camera
source. Camera inventories are ~12k slow-changing points spread over many vendors,
many of which serve stills over plain http or without CORS, and several (DelDOT,
Maryland CHART) publish HLS only.

Decision: cameras are a separate registry (`apb/ingest/cameras.py`), not incidents
and not a `CadIngest` lane — per-vendor parsers, 45-min inventory TTL, background
refresh, bbox + hash-sampled `/live/cameras`. Browsers never load vendor image URLs
directly: `/live/cameras/{id}/image` resolves only registry ids (no arbitrary URL
fetch = no SSRF) and caches each still ~6s. HLS is the one thing fetched directly
by the browser; the allowed streamer hosts live in `cameras.STREAM_HOSTS` and the
API's CSP `media-src`/`connect-src` is built from that list so a new streaming
vendor cannot silently break in production. `hls.js` (unpkg) loads lazily on first
stream play.

Consequences: adding a keyless vendor = one parser + one `SOURCES` entry (+ a
`STREAM_HOSTS` entry if it streams). Keyed Carmanah states reuse `T511_*_KEY`.
Snapshot bandwidth flows through the host; the ~6s per-camera cache bounds it to
one upstream fetch per camera per cache window regardless of viewers.
`APB_CAMERAS_OFF=1` disables the lane. UI: the classification banner is gone and
panel offsets now derive from the command bar's measured height (`--deck-top`).

### 2026-09-17 — Scene explainer ("the clip"): client abstracts, server grounds, Gemini answers

Context: users wanted "what the $!@# am I looking at?" for any drawn box, combining
map layers, time, weather, place and image analysis of the live cameras, with the
ability to dive into one facet (cameras/incidents/hazards/social/weather/place).

Decision: the browser sends only what is on screen (bounded pydantic model — rows
inside the rectangle, filters, layer toggles, environment readout, registry camera
ids). `apb/context/explain.py` adds keyless Open-Meteo conditions at the box
center, placed news headlines from the live buffer, and up to 5 (10 for the cameras
focus) stills fetched through the registry — never a client-supplied URL — and
calls Gemini Flash. The model is discovered from ListModels (highest-version
`*flash*` chat model, stable over preview at equal version, 1h cache, `GEMINI_MODEL`
override) so "newest Flash" stays true without code changes. `/explain` is
throttled 6/min/IP because every call costs money, and returns 503 until
`GEMINI_API_KEY` exists. The UI renders a basemap snippet of the box with the
grounding data overlaid so the user sees exactly what the model was given.

Consequences: the existing `GOOGLE_API_KEY` in prod is Custom-Search-restricted and
returns 403 for the Generative Language API — a separate `GEMINI_API_KEY` must be
added to `/srv/panoptes/.env` before the clip answers in production. Shareable view
state now lives in the URL hash (`v`, `m`, `w`, `t`, `s`, `l`).

### 2026-09-17 — Facts aggregator streams fastest-first; Overpass is best-effort

Context: the user wants every public metric for a drawn box (population, income,
elevation, flood zone, tides, sky, air, wildlife, POIs…). Sources vary from 200 ms
(Open-Meteo) to 30 s+ (Overpass mirrors, WorldPop tasks).

Decision: `apb/context/facts.py` runs every source in parallel and `facts_stream`
yields each result the moment it lands (NDJSON via `/facts?stream=1`); the bubble's
table fills in arrival order but displays in canonical section order. A 15 s deadline
ends the stream; stragglers keep running and land in the 10-min cache, so the next
ask is complete. Census ACS needs a key (the API refuses keyless now) — gated on
`CENSUS_API_KEY`; WorldPop covers headcount keylessly. Overpass's main server 406s
our client; a mirror (`APB_OVERPASS_URL`) is used and treated as best-effort.
Google Places / reviews are not wired: Places is a billed API with no keyless tier;
OSM POI counts stand in for "business activity".

Consequences: `/facts` is cheap to add to (one function + one tuple in `_tasks`).
Windy webcams are looked up per view, not preloaded (100k+ cams).

### 2026-09-18 — Street-level imagery via token proxy; Street View only on the Street facet

Context: the clip's looks benefit from ground-level context. Providers: Mapillary
(free token), KartaView (keyless but its public API mostly refuses/timeouts),
Google Street View Static (billed per image).

Decision: `apb/context/street.py` samples a 3x2 grid across the box, takes the newest
frame per cell, and hands frames to the model as image parts. Browser never sees
provider URLs (or the Google key): frames are served by `/street/{token}` from a
short-lived token→URL map. Street View is only requested for the Street facet
(`paid=True`) and only after its free metadata call confirms coverage; overview and
place get up to 3 free frames. Frames are cached 6 h per rounded box and saved with
the look. Consequence: cost is bounded to ≤6 billed images per Street-facet look
(~4¢), zero otherwise.
