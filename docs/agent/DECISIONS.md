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
