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
| Gemini (scene explainer) | `GEMINI_API_KEY` (or an unrestricted `GOOGLE_API_KEY`) | "What am I looking at?" — the clip's box summaries, camera image analysis, drill-downs. `GEMINI_MODEL` pins a model; otherwise the newest Flash is auto-picked | https://aistudio.google.com/apikey — the key must allow the Generative Language API (the Custom-Search-restricted key returns 403) |
| Census ACS (facts) | `CENSUS_API_KEY` | Tract/county population, median income, income bands, age, home value, poverty, education, renters | https://api.census.gov/data/key_signup.html — instant |
| eBird (facts) | `EBIRD_API_KEY` | Notable bird sightings near the box (7 days) | https://ebird.org/api/keygen — instant |
| Windy Webcams (cameras) | `WINDY_WEBCAMS_KEY` | World's largest public webcam index, looked up per map view | https://api.windy.com/keys — free tier |
| WSDOT (cameras) | `WSDOT_ACCESS_CODE` | Washington statewide cameras | https://wsdot.wa.gov/traffic/api/ — free |
| OHGO (cameras) | `OHGO_API_KEY` | Ohio statewide cameras | https://publicapi.ohgo.com — free |
| ACLED | `ACLED_EMAIL` + `ACLED_PASSWORD` | Protests, riots, political-violence events | https://acleddata.com/user/register → myACLED account (OAuth login; the old key+email API is retired) |
| 511 Georgia | `T511_GA_KEY` | Statewide GA DOT traffic incidents | https://511ga.org → developer resources (free key) |
| 511 Louisiana | `T511_LA_KEY` | Statewide LA DOT traffic incidents | https://511la.org → developer resources |
| 511 Pennsylvania | `T511_PA_KEY` | Statewide PA DOT traffic incidents | https://www.511pa.com → developer resources |
| 511 Idaho | `T511_ID_KEY` | Statewide ID DOT traffic incidents | https://511.idaho.gov → developer resources |
| New England 511 | `T511_NE_KEY` | CT/ME/MA/NH/RI/VT traffic incidents (one key, six states) | https://newengland511.org → developer resources |
| FL511 | `T511_FL_KEY` | Statewide FL DOT traffic incidents + cameras | https://fl511.com → developer resources |
| UDOT / NV Roads / 511WI / AZ511 / CT Travel Smart / 511 Alaska / 511 Alberta cameras | `T511_UT_KEY`, `T511_NV_KEY`, `T511_WI_KEY`, `T511_AZ_KEY`, `T511_CT_KEY`, `T511_AK_KEY`, `T511_AB_KEY` | That state's live traffic cameras (same Carmanah `/api/v2/get/cameras` platform) | each site → developer resources (free key) |

Virginia does **not** run the `getevents` platform — VDOT's data portal is
https://smarterroads.org (free account, different API; not wired up yet).
North Carolina needs no key: NCDOT TIMS incidents are already flowing through the
ArcGIS catalog (`ag_ncdot_timsincidentsbylanesaffected`).

Notes:
- **AISStream** is a websocket firehose. `websockets` ships with `uvicorn[standard]`,
  so it runs on the lean Railway image once the key is set.
- **ACLED** uses OAuth: set `ACLED_EMAIL` + `ACLED_PASSWORD` (your myACLED login).
  The lane fetches a 24h bearer token automatically.
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
3. `ACLED_EMAIL` + `ACLED_PASSWORD` — myACLED account, adds civil-unrest events.
4. `T511_*_KEY` — each free 511 key lights up a whole state's DOT incident feed
   (New England's covers six states at once) **and** its live traffic cameras on
   the same platform. 511NY needs no key and is already on.

## Live cameras (keyless, on by default)

**One-network (Iteris) 511 states — keyless GraphQL, found by the sniffer:** IA, IN,
MN, NE, MA, KS (`ITERIS` in cameras.py; the public map's `MapFeatures` query with the
`normalCameras` layer, whole-state bbox at zoom 14 → every camera un-clustered).
**Iteris ATIS states (SC, MT, SD, VA)** via `{st}.cdn.iteris-atis.com/geojson/icons/
metadata/icons.cameras.geojson` (VDOT: `511.vdot.virginia.gov/services/map/array/cameras`),
found by the sniffer's `--dump` of blob-fetched payloads. **Colorado** via the CARS "511x" GeoJSON API (`api-511x-co.carsprogram.org`).

**Finding new feeds is automated:** `python -m apb.discover.camera_sniff` (needs
Playwright + Chromium, build-time only) opens each DOT/511 map in `SEEDS`, captures
the JSON the page loads, recognises camera lists by shape, validates a still or HLS
playlist, and writes replayable specs to `data/camera_discoveries.json`; every
`enabled` spec becomes a live source (`dx_<key>`) with no code. `--url X --key Y`
sniffs a new site. Sweep of 2026-09-17: MoDOT (880, HLS) and NMRoads (183) enabled;
511NJ (679) and GoAkamai HI (336) found but session-gated (replay 401/403); OK/KY
found but their image hosts refuse us; 26 other maps exposed nothing camera-shaped
(vector tiles, keyed APIs, or cameras loaded only per-tile).

`apb/ingest/cameras.py` — verified 2026-09-17: Caltrans (12 districts, ~3.6k),
NYC DOT TMC (~1k), 511NY (~2.9k, most with HLS), DelDOT (HLS only), Maryland CHART
(HLS only), Seattle SDOT+WSDOT (~650), Ontario 511 (~1.7k views), TfL JamCams
(London, ~900), NZTA (~300), ODOT TripCheck (~1.1k), ALGO Alabama (~650, stills +
HLS), TravelMidwest gateway (IDOT/Tollway/Lake County/InDOT/WisDOT/KYTC, ~2.1k
views), Austin (~1k), Baton Rouge (118), ALERTCalifornia wildfire cams (~1.3k, kind
`wildfire`), Calgary (216), Ottawa (428), Vancouver (~840 views), Fintraffic
weather cams (~2.3k presets, kind `weather`), Singapore LTA, Hong Kong TD (~1k),
Transport NSW (147 via ArcGIS mirror). ~22k cameras total. Found via endpoint
probing plus ArcGIS Hub / Socrata catalog searches for "traffic cameras" /
"webcam" — re-run those searches when hunting for more. Stills are served through `/live/cameras/{id}/image`;
HLS plays directly from the DOT streamer (hosts allow-listed in the API CSP via
`STREAM_HOSTS`). Keyed platforms in the table above add cameras once their key is
set. Evaluated and not usable server-side: DriveBC (connection reset from non-BC
clients), 511NJ / Quebec 511 / Montreal (bot protection), MnDOT IRIS camera.xml
(WAF-rejected; stills at video.dot.state.mn.us work if you have ids), KYTC ArcGIS
(snapshot host refuses connections), Honolulu open data (image host gone), Toronto
(API method retired), WSDOT / OHGO / COtrip / Alberta+Atlantic 511s (keyed, not yet
registered), FAA WeatherCams (auth required), Germany Autobahn API (webcam lists
empty), TDOT SmartWay (401).

## Evaluated and not viable (so far)

- **Waze live-map GeoRSS** — every host variant (www/embed, live-map api, rtserver)
  returns 403 to non-browser clients; bot protection blocks server-side use.
- **CBP border wait times** — keyless JSON but no coordinates and mostly
  "Update Pending" rows.
- **NGA maritime navigational warnings** — keyless but offshore DMS-string
  positions with little map value for this product.

Everything else is keyless and already on. See `.env.example` for the full variable
list and the `*_OFF` opt-out flags.
