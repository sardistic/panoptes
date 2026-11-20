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

### 2026-09-19 — Background sampler fills baselines; NOTABLE is a stored layer

Context: per-cell baselines and "notable" flags only got samples when someone drew a
box, so they were empty in practice.

Decision: `apb/context/sampler.py` runs on the poller leader only. Every 45 min it
runs `facts.facts(..., lite=True)` over a rotating batch of 30 cells (cells of looks
from the last 7 days first, then the CAD metro centers); `lite` drops the quota lanes
(Synoptic, TomTom, Transitland) and bypasses the 10-min cache. Every hour it reads 50
rotating camera stills, 10 per Gemini call, with the structured `OBS:` prompt only.
Each facts pass upserts its flags into `cell_notable`; `/live/notable` serves the
last 6 h and the UI draws them as a NOTABLE layer (on by default) whose click runs a
Facts look on the cell. YouTube (units) and Windy (requests) go through the spend
store with daily caps so `/status.quotas` shows usage and a runaway view loop cannot
exhaust a day's quota. Consequence: ~1.4k lite facts cells/day and ~1.2k camera
reads/day of load, all keyless or free-tier; baselines mature after ~5 passes per
cell (a few days for watched cells).

### 2026-09-19 — Weekly re-sniff in CI

Session-bound camera maps (511NJ, GoAkamai HI, OKtraffic, TDOT) ship as static
inventories in `data/camera_discoveries.json`. `.github/workflows/resniff.yml` runs
the Playwright sniffer weekly and commits the file when it changes, so they do not
silently rot; prod picks it up on the next deploy.

### 2026-09-19 — Notable persistence and camera-activity series

Context: a single flag says little; whether a cell keeps flagging does.

Decision: `cell_notable_log` records every facts pass per cell (flagged or not, one
per 10 min, 30-day retention). `notable_history()` yields passes / flagged / flagged
within the last 5 / recurring flag names; it rides the facts stream event, the model
digest (`notable_persistence`) and the NOTABLE tooltip (`streak`). `camera_obs_series()`
bins camera reads into 12 slots over 24 h; `/live/camera_activity` returns it and the
Cameras facet draws it. The sampler watchlist adds the 50 largest US metros so the
layer covers the map, and skips the camera pass while inventories are still loading.

### 2026-09-19 — Second facts expansion lives in facts_more.py

Context: "look for more data sources". 35 candidates probed live; 13 keyless lanes
verified (table in SOURCES.md).

Decision: new lanes go in `apb/context/facts_more.py` (own httpx client, same
contract: return {} for nothing, raise for failure); facts.py only registers them in
`_tasks` / `run_us_dependents` / the geocode hook. New sections: infrastructure,
radio, environment, climate. Lane-specific `*_source` keys instead of a shared
"source" so lanes sharing a section do not overwrite each other. HIFLD is reached via
the `services5/HDRa0B57OVrv2E1q` mirror (the canonical `services1/Hp6G80Pky0om7QvQ`
host now 400s on every layer). ACIS picks the nearest station whose *temperature*
record is current (CoCoRaHS sites are rain-only). Wikipedia attention keeps whichever
of "City, State" / "City" has the higher median (the former is often a redirect stub).
New tracked scalars: wikipedia_views, temp_anomaly_f, nws_alerts,
snow_water_equivalent; new notable flags: attention ≥3× median, |anomaly| ≥10 °F,
Extreme/Severe NWS alerts, ≥3 EPA significant violators.

### 2026-09-19 — Sampler stays inside Open-Meteo's daily allowance

Context: the first full sampler day exhausted Open-Meteo's 10k weighted calls/day
("Daily API request limit exceeded") for the whole home IP — every sky / air /
elevation lane then 429'd for users too.

Decision: lite passes drop the marine, solar and pressure-level lanes (only sky, air
and flood feed tracked scalars) and the facts cadence is hourly (~120 Open-Meteo calls
per pass, ~3k/day). If the layer still starves, the next step is an Open-Meteo API key
(commercial tier) or self-hosting.

### 2026-11-14 — Overwatch layer: one ranked board over three detectors

Context: production showed the fused-event layer was not trustworthy. A far-future
timestamp drove the recency term to ~29,000 (scores near 500,000); the registry
matched each cluster to the first open row the index returned — the stalest — so
duplicate rows refreshed each other and lived for weeks; name-placed posts stacked on
metro centroids and "corroborated" whatever dispatch was there; and nothing said
whether a quiet area was calm or its feed was dead (five camera inventories were
failing unnoticed).

Decision:
- `fusion/cluster.detect` drops signals dated more than 15 min ahead, clamps recency
  to 1, counts a syndicated headline once, weights a centroid-placed mention at 0.25,
  and lets it corroborate only when a located signal of the same kind is in the
  cluster. New source kinds `camera` and `sensor` corroborate on severity alone.
- `store/events` pairs clusters with the nearest unclaimed open row (strongest cluster
  first) and logs one history point per event per cycle (`fused_event_history`, SQLite
  on the leader); `trend`/`spark`/`vs_median` come from it. Legacy peaks over 1000 are
  reset to the latest score at init.
- `store/feedhealth` learns rows-per-fetch and the usual peak record age per feed and
  names feeds `failing` / `silent` / `stale`; baselines freeze during a problem state.
  The peak lag is learned only when freshness advances, otherwise the baseline chases
  a frozen feed. Lanes where empty is ordinary (`BURSTY`) are exempt from `silent`.
- `context/tasking` reads the nearest cameras when an event crosses `APB_TASK_SCORE`.
  The model's own yes/no/unclear bottom line gates the `camera` signal: per-frame
  "supports" alone was too lenient (one car on the shoulder "supported" a scene the
  same reply called normal).
- `/live/board` ranks by score x novelty x trend x corroboration x incident kind.
  Single-source clusters are multiplied by 0.6 and standing clusters by their own
  history, because raw volume makes every busy downtown the top item forever.
- Alerts are rule-based (`alerts/rules.py`), judged on the current score, escalate at
  1.5x, and are logged with no webhook so `/scorecard` can grade a dry run. The global
  default now needs 2 source kinds — at the old default (any event >= 6.0) the median
  production event would have alerted.
- `infer/baseline` uses a Poisson tail and a MAD-based z, takes the smaller, uses local
  solar time for day-parts, and caps the z of fewer than three calls at 1.5.

Consequences: three detectors (fused events, NOTABLE cells, camera reads) vote in one
place. Vision spend rises by at most `APB_TASK_DAILY` small calls a day plus six
sitreps an hour. NOTABLE cells are context unless the flag is an activity kind on a
cell that is not flagged all the time — weather leaving its range must not corroborate
a medical call. History, feed health, camera checks and the alert log are leader-local
SQLite even when shared Postgres state is on (same precedent as the box-metric store).

### 2026-11-14 — Cloudflare Radar lane; Shodan is discovery only

Radar's outage annotations and verified traffic anomalies are a hidden lane
(`ingest/radar.py`, kind `radar`, country coordinates from Radar's own entity list).
Unlike the other singleton lanes it raises on upstream failure so the ingest layer
backs off and feed health can tell a bad token from a quiet internet. The desktop
`CLOUDFLARE_API_TOKEN` already reads Radar, but it can also edit zone rulesets — the
deployed service gets its own Radar-read-only token (`CLOUDFLARE_RADAR_TOKEN`).
Shodan is used only by `discover/shodan_sweep.py` to find intentionally public
services for the existing discoverers; it is not a live lane and is not used to find
cameras or devices exposed by accident.

### 2026-11-17 — Radio audio is replayable; dispatch is transcribed on a budget

Context: PulsePoint's API went behind a bot challenge (2026-11-14), taking 488 fire/EMS
feeds with it. The owner asked for an alternative before shelving it, for more radio
coverage, to be able to hear the radio, and for budgeted speech-to-text on the calls
that matter. The API header and memory said raw audio is never served and that
transcription was deferred until the activity layer worked; it now works.

Decision:
- Audio: a call already published by a volunteer's open Rdio Scanner is relayed on
  request (`/live/radio/audio`), only for calls the lane listed, through the WARP egress,
  cached 24 MB, 60/min per viewer and `APB_RDIO_CAL_PER_MIN` per source. No archive.
  Viewers never connect to an operator's home server, and operators' addresses never
  reach the browser (systems are keyed by a 10-hex hash).
- Transcription: Gemini (already keyed, accepts audio, extracts structure in the same
  call) rather than local Whisper (a 1 GB container that was being OOM-killed a week
  ago). Selection by talkgroup tag/label: dispatch first; boosts for surging systems and
  nearby events. Daily cap in audio seconds through the spend store. The model redacts
  per the README guardrail; only redacted text is stored; radio unit ids are dropped.
- PulsePoint replacement: transcribed dispatches with a block-level address are geocoded
  (Census geocoder, must fall within 80 km of the system, 150 km if the system's place
  is approximate) and written to the incident store, so they render and fuse like a
  dispatch feed. Fusion counts radio activity and radio transcripts as ONE sensor family.

Consequences: coverage follows where hobbyists publish (about 160 systems, about 55
instances, uneven). Audio contains PII the transcripts redact — accepted by the owner;
the guardrail still governs stored and exposed records. Cost at the default cap is
roughly two hours of audio a day (Gemini bills about 32 audio tokens per second).

### 2026-11-18 — Radio transcripts are internal; systems are placed by evidence

Context: the owner asked not to publish the transcribed data, only to use transcribed
addresses to place incidents in their general area; and asked why many radio systems
stacked on one spot (a volunteer in Economy, PA carries Franklin MARCS, which serves
Columbus, OH — every system fell back to the listener's town).

Decision: transcripts (redacted) stay in the server's store. Nothing served contains
their words, addresses or units: incidents carry the type, urgency and the centre of a
~2 km cell; the calls list shows only "fire dispatch, placed / not placed". Already
published radio incidents were coarsened at startup. Systems are placed by, in order:
the median of their own geocoded dispatches (>= 3), a one-time model profile from the
system and talkgroup names (point checked against the Census state it names), the label
in the gazetteer near the listener, the listener's town. The profile also names the
dispatch talkgroups of systems that tag nothing; transcripts then promote (>= 40 %
dispatch over >= 3 calls) or demote (< 10 % over >= 8) a talkgroup.

Consequences: audio playback is unchanged (the owner asked to hear the radio; it still
carries what the transcripts redact). Profiling costs one small text call per system
(capped 300 a day). Placement improves as dispatches accumulate.

### 2026-11-18 — Camera sampler cut from 50 to 20 stills an hour

The new cost meter showed the background camera reads at about $0.0044 per call of ten
stills (each still is ~1,100 image tokens, not the ~260 assumed), about $0.53 a day and
the second-largest Gemini line. The owner asked to reduce it. Default
`APB_SAMPLER_CAMS_BATCH` is now 20 (two full calls an hour, ~$0.21 a day). Camera
baselines fill more slowly; tasked checks on live events are unaffected.

## 2026-11-19 — Radio area summaries are published; identifying details are not

The owner clarified that "don't publish the data" meant identifying metadata (addresses,
names), not everything. Transcripts themselves stay internal, but a per-system summary
of what is going on in each area (one line per town / county / road: call types, tempo,
agencies) is now published at `/live/radio/areas` and on the map. House numbers become
blocks, unit and aircraft identifiers are stripped, and areas are placed on ~2 km cells.
Cost test on NC VIPER (12 clips): $0.0004 per transcribed clip, $0.0006 per summary;
VIPER in full is about $0.20 a day. `APB_RADIO_FOCUS` makes chosen systems transcribe
every talkgroup (VIPER's highway-patrol channel weighs 0.2 and was never picked).

## 2026-11-19 — Radio calls are linked into incident threads and placed

The owner's standing goal: use calls to locate where incidents happen, and link calls
with each other. The area-summary call now also returns incidents: a dispatch and the
traffic about it (responding, on scene, updates, cleared), continuing open threads by
label (T1, T2...) so later calls join the same incident. Each thread is placed by the
best evidence — a geocoded block address (Census), else the node where two named roads
meet near the town (Overpass, keyless; "NC 280 at Deep Woods"), else the town — and
published as the ~2 km cell plus the precision word. The place text stays internal.
Threads placed by address or intersection go to fusion (metro `radio_<key>`, call_id
`rthr<tid>`) unless a call already did. Cleaned per-call lines are now published
(`clean()`: no house/block numbers, unit numbers, conditions or [REDACTED] markers).

### 2026-11-19 — Intersections from a local junction index, not live Overpass

The first prod run of incident threads placed nothing by intersection: every public
Overpass mirror (kumi, overpass-api.de, private.coffee) timed out at 40 s or answered
"at capacity" for the per-incident name-regex query. Now `apb/context/roads.py` keeps
its own index: 0.1° tiles of named/ref'd roads (`out body geom`, one query per tile)
fetched when an incident first needs them, reduced to junction rows (road key a, road
key b, lat, lon) in `state/roads.sqlite`. Spoken and mapped names normalise to the same
key ("Deep Woods Dr" / "Deep Woods Drive" -> `n:deep woods`, "NC 280" / ref "NC 280;US 64"
-> `r:280`). Lookups are local and instant; tiles download one at a time with backoff
and mirror rotation, refreshed after 90 days. Threads still at town level are re-placed
for 6 h as tiles arrive (no model or geocoder calls). Rejected: Geofabrik state
extracts (GBs per state; a node-location index needs more than the 1 GB container).
Search radius: 10 km around the town, 20 km around a known system location.

### 2026-11-19 — Live radio indicators: polled feed, honest about latency

Owner asked for live visual indicators of new radio calls and transcriptions. The server
only learns of a call when it polls the scanner (every 10 min per instance), so "live" is
bounded by that; focus-system instances are now polled every 60 s (`APB_RDIO_FOCUS_EVERY`)
— one tiny "newest calls" request a minute. `radio_recent.seen` records when the lane saw
a call; `/live/radio/feed?since=` returns calls seen, transcriptions done and threads
written after the cursor (server time, clamped to 10 min). The browser polls every 5 s
(plain JSON, no SSE — simpler through the Cloudflare tunnel). A 10-minute burst is spread
over one poll interval and capped at 8 pings per system so it reads as activity, not a
flash. Transcription is one Gemini reply per clip; the typing effect is presentation
only. Lines are `clean()`ed exactly like `/calls`.

### 2026-11-19 — Roads from Census TIGERweb; block numbers that exist; street precision

Supersedes the Overpass tile index above, which never fetched a tile on prod: kumi and
private.coffee timed out or 502'd, and overpass-api.de answered 406 or hung (even a
one-node query). TIGERweb's Transportation MapServer (layers 2 primary, 6 secondary,
8 local; fields BASENAME, RTTYP) answers in ~0.2 s from prod. A lookup asks for both
roads in the search box, intersects the centre lines (plus 12 m end-point touches for
T-junctions) and caches the answer (hits 90 d, misses 7 d; errors not cached). US only
— the Toronto systems stay town-level.

Most town-level threads were block addresses the Census geocoder rejected: "600 block of
Bannon St" became "600 Bannon St", outside every address range, while "601" matches.
`address_variants` now tries n, n+1, n+50, n+51 (0-block -> 1, 2, 50, 51) and Census
answers are cached 6 h. New precision `street` (between intersection and town): one named
road whose stretch in the search box spans <= 3 km. `split_where` drops travel words
(westbound, ramp, exit) and also splits on "east of" / "between". Dry run on a copy of
prod data moved 6 of ~40 unplaced threads on the first pass; Census had transient
timeouts there, which the 6 h retry covers.

### 2026-11-19 — Layers by domain; radio incidents drawn as events; replayed live feed

Owner: radio icons were too much, radio should read as events, the live strip flickered,
updates arrived in pulses, and the layer deck was organised by feed ("Storm" holding
weather, radar separate). Now:
- Deck: one button per domain — Activity, Weather, Water, Fire & Air, Earth, Transport,
  Infrastructure, Civil — each opening a panel of its layers; Radio is a direct toggle.
  Radar, satellite, NWS warnings and the conditions field live under Weather; every
  hazard category is its own toggle (no HAZARDS master, no separate filter bar). The
  layer buttons keep their ids/handlers and are moved into the panels; a
  MutationObserver keeps domain counts honest when a layer switches itself. URL `l=`
  keeps old keys and adds `h.<cat>`; an old `hazards` link turns every category on.
- Radio systems: a small quiet mast (hidden below zoom 7 unless surging); no waves, no
  area squares on the map (area lines stay in the system popup). Radio incidents use
  the incident tick in severity colour with one pair of angular brackets; fused radio
  incidents are not drawn twice while Radio is on, and open the call timeline.
- Live feed: each system's burst replays at the pace the calls were made (a 10-minute
  poll plays back over ~10 minutes), one ping per system per 0.7 s; transcribed lines
  are released one every 1.2-3.2 s; the strip is built once and only new rows animate;
  bubbles only from zoom 7. New incident pings spread over up to 12 s.

### 2026-11-19 — Radio expansion: our own stream recorder for Icecast scanner feeds

Owner: "expand radios; if we need to operate our own crawler for specific broadcasters
so be it." Options checked: Shodan filtered search needs paid credits on the free key;
crt.sh (CT logs) returned 502s and mostly noise; OpenMHz stays out (Cloudflare + ToS).
The public Xiph Icecast directory (dir.xiph.org/yp.xml, keyless) lists ~90 public-safety
MP3 scanner streams (~40 distinct after de-duplication), mostly 16 kbps — FDNY boroughs,
DC Fire, Maricopa County police/DPS, Charlotte, St. Louis and Baltimore City Fire,
Louisville, Ohio HP — none covered by our Rdio instances.

`apb/ingest/streams.py` listens through the WARP egress (same rule as Rdio: never from
the home address), decodes with `miniaudio` (pip wheel, no ffmpeg) to 8 kHz mono, cuts
calls on a running noise floor (on 3.5x floor, off 2x, 1.6 s hang, 1.2-90 s), stores
each call's original MP3 bytes for 6 h and lists it in radio_recent (system 1,
talkgroup 0), with counts into radio_calls every 10 min. To the rest of the code a
stream is an Rdio instance with one system: `rdio.instance/_label/talkgroup/
fetch_calls/activity` know about it, so STT, profiles (placement from the stream name),
threads, the map and the live feed need no stream-specific code. Trial on prod through
WARP: 4 streams, 2 min, 8 calls, no drops; Gemini transcripts were clean dispatch
traffic ("Respond for Box 1664 … West 142nd Street … odor of gas"). ~1.5 Mbps and light
CPU for 40 streams; transcription stays inside the existing daily STT budget.

