# CA Fire Production VolumeStore Handoff

This is the production handoff for deploying the HRRR pressure VolumeStore and
arbitrary cross-section rendering path on the CA Fire Hetzner node. It is
separate from the GOES18 satellite lane.

## Live Node

- Host: `cafire.wxsection.com`
- Hetzner: `ubuntu-32gb-nbg1-1`
- IPv4: `178.104.59.253`
- SSH: `ssh root@178.104.59.253`
- Service dir: `/opt/cafire-weather-service`
- Data dir: `/opt/cafire-weather-service/data`
- Cache: `/opt/cafire-weather-service/data/cache`
- Artifacts: `/opt/cafire-weather-service/data/artifacts`
- RAM: 16 vCPU, about 30 GiB Linux RAM
- Disk: about 601 GB root disk

Do not put passwords, SSH keys, R2 keys, or service API keys into the repo,
prompt, docs, shell history, or logs. Use the existing remote `.env` and any SSH
access Drew provides out of band.

Before touching production:

```bash
cd /opt/cafire-weather-service
docker compose ps
docker compose logs --tail=100 api
docker compose logs --tail=100 static-worker
docker compose logs --tail=100 warmer
df -h
free -h
```

Do not stop Caddy or the API unless absolutely necessary. This is live
production.

## Goal

Deploy the local pressure VolumeStore cross-section path safely for CA Fire:

- HRRR California pressure store, f000-f048, for 00/06/12/18Z synoptic cycles.
- All wxsection cross-section products except smoke.
- Users can draw arbitrary California cross sections.
- Users can render a 48 hour WebP loop for one product.
- Render jobs are bounded and cached, not per-user unlimited.
- Keep completed synoptic stores and recent hourly stores according to the
  retention policy below.
- Avoid double-downloading HRRR data already present for static plots and
  meteograms.

Do not publish local proof/test names such as `codex_top_pressure_test` in
production. Public route ids should be deterministic hashes over the request,
cycle, style version, dimensions, hours, product, and spacing.

## Local Verified Facts

The local full California enhanced store was built with:

- Cycle: `2026-04-28T00:00:00Z`
- Coverage: f000-f048
- Vars: `TMP`, `SPFH`, `UGRD`, `VGRD`, `HGT`, `VVEL`, `ABSV`, `CLWMR`, `ICMR`,
  `RWMR`, `SNMR`, `GRLE`
- Store size: 6.27 GB
- Build time before the parallel patch: 58.27 min
- 49-frame `wind_speed` WebP loop: 7.3s renderer / 7.9s HTTP
- All non-smoke products at f024: 19 rendered, smoke skipped, 1.45s renderer

The builder supports:

- `--load-parallelism N`
- `RUSTWX_VOLUME_STORE_LOAD_PARALLELISM=N`

Use `--load-parallelism 4` for California on the 32 GB node. Start lower for
CONUS.

## Build Binaries

On the node, use a build tree, not the live service directory as a scratchpad.

```bash
cargo build -p rustwx-cli --release \
  --bin hrrr_pressure_volume_store \
  --bin volume_store_dashboard \
  --bin volume_store_cross_section_render
```

Production code must include the local changes for:

- `crates/rustwx-cli/src/bin/hrrr_pressure_volume_store.rs`
- `crates/rustwx-cli/src/bin/volume_store_cross_section_render.rs`
- `crates/rustwx-products/src/gridded.rs`
- `crates/rustwx-products/src/cross_section.rs`
- `crates/rustwx-products/src/volume_store/`

Use `deploy/cafire-weather-service/local_proof_demo/proof_wall.py` only as a
reference for request validation, route hashing, renderer invocation, and local
artifact serving. Production endpoints belong in FastAPI.

## Avoid Double Downloads

The current rustwx cache is keyed partly by variable pattern. Static maps and
meteograms may already have HRRR files on disk, but the VolumeStore builder may
still fetch a new 12-variable pressure subset if the existing cached file was a
different subset.

First inspect production cache layout:

```bash
find /opt/cafire-weather-service/data/cache/hrrr -name fetch_meta.json | head -100
```

Use the same cache root:

```bash
--cache-dir /opt/cafire-weather-service/data/cache
```

Production path in this service: run `pressure-volume-builder` as the only
automatic VolumeStore builder. It uses the same latest-full HRRR discovery as
the warmer, the same `/data/cache` root as static maps and meteograms, and a
single lock under `/data/volume-stores`. By default it waits for the static-map
manifest for the target synoptic cycle before starting, so it does not race the
regular render/download loop during cycle catch-up.

Static maps mainly use surface HRRR files; the pressure VolumeStore legitimately
needs pressure-level HRRR files too. Those are not always the same downloaded
payload, but they must still flow through the same cache and the same single
VolumeStore builder. If cache keys do not share the same raw GRIB/subset, add a
shared staging cache keyed by `(model, cycle, fhr, product, source, url)` so
static maps, meteograms, and VolumeStore can reuse any identical fetched GRIB
payload. Do not let multiple VolumeStore builders independently pull the same
synoptic pressure files forever.

## Build Store

Build into a new cycle directory. Never overwrite the active store in place.

```bash
STORE_ROOT=/opt/cafire-weather-service/data/volume-stores
DATE=YYYYMMDD
CYCLE=00

target/release/hrrr_pressure_volume_store \
  --date "$DATE" \
  --cycle "$CYCLE" \
  --start-hour 0 \
  --end-hour 48 \
  --source nomads \
  --west=-125.2 --east=-113.5 --south=31.0 --north=43.0 \
  --cache-dir /opt/cafire-weather-service/data/cache \
  --out-dir "$STORE_ROOT/hrrr_ca_${DATE}_${CYCLE}z_wxsection" \
  --load-parallelism 4
```

Validate before publishing:

```bash
jq '.request.forecast_hours | length' "$STORE_ROOT/hrrr_ca_${DATE}_${CYCLE}z_wxsection/report.json"
jq '.files' "$STORE_ROOT/hrrr_ca_${DATE}_${CYCLE}z_wxsection/report.json"
du -sh "$STORE_ROOT/hrrr_ca_${DATE}_${CYCLE}z_wxsection/store"
```

Expected: 49 hours, 12 variables, around 6-8 GB for California.

## Retention And Cadence

Use atomic pointers and never replace an active store until the replacement
passes validation.

The desired serving model:

- Keep the latest completed 00/06/12/18Z synoptic cycle with full f000-f048
  coverage active until the next synoptic cycle is fully complete.
- Keep the previous completed synoptic cycle as fallback.
- For hourly cycles between synoptic cycles, publish a `latest-partial` store as
  soon as a contiguous f000..fNN pressure range is available and, by default,
  the regular static-map manifest has caught up through fNN. This lets users
  render f002 cross sections from a 04Z cycle without waiting for the full
  cycle range.
- Replace an older partial/hourly store only when the newer partial store has
  completed its intended available range.
- Keep one newly building temp directory. Garbage collect older successful
  stores only after the new active/fallback pointers are correct.

Examples:

- At 06Z, the completed 00Z f000-f048 store should remain available until the
  06Z f000-f048 store is fully complete.
- At 08Z, the completed 06Z f000-f048 store should remain available; the 07Z
  hourly store can remain available until the 08Z hourly store completes its
  available forecast range.
- The 06Z synoptic store should not be replaced until the 12Z synoptic store is
  fully complete.

Recommended pointer layout:

```text
/opt/cafire-weather-service/data/volume-stores/current
/opt/cafire-weather-service/data/volume-stores/previous
/opt/cafire-weather-service/data/volume-stores/latest-partial
/opt/cafire-weather-service/data/volume-stores/building-<cycle>
```

Publish a completed synoptic store with a symlink swap:

```bash
ln -sfn "$STORE_ROOT/hrrr_ca_${DATE}_${CYCLE}z_wxsection/store" "$STORE_ROOT/current"
```

Only move `previous` after `current` points at a validated replacement.

## Sidecar

Run `volume_store_dashboard` bound only to localhost or Docker internal network:

```bash
target/release/volume_store_dashboard \
  --store /opt/cafire-weather-service/data/volume-stores/current \
  --host 127.0.0.1 \
  --port 8797
```

API env:

```dotenv
PRESSURE_VOLUME_ENABLED=1
PRESSURE_VOLUME_BASE_URL=http://pressure-volume:8797
PRESSURE_VOLUME_STORE_PATH=/data/volume-stores/current
PRESSURE_VOLUME_PARTIAL_STORE_PATH=/data/volume-stores/latest-partial
PRESSURE_VOLUME_RENDERER_PATH=/app/bin/volume_store_cross_section_render
PRESSURE_CROSS_SECTION_RENDER_MAX_ACTIVE=3
PRESSURE_CROSS_SECTION_LOOP_MAX_ACTIVE=2
PRESSURE_CROSS_SECTION_DEFAULT_TOP_HPA=100
PRESSURE_VOLUME_BUILDER_ENABLED=true
PRESSURE_VOLUME_BUILDER_INTERVAL_SEC=900
PRESSURE_VOLUME_BUILDER_LOAD_PARALLELISM=4
PRESSURE_VOLUME_BUILDER_REQUIRE_STATIC_MANIFEST=true
PRESSURE_VOLUME_PARTIAL_ENABLED=true
PRESSURE_VOLUME_PARTIAL_REQUIRE_STATIC_MANIFEST=true
PRESSURE_VOLUME_PARTIAL_MAX_HOUR=18
```

Do not expose the sidecar port publicly.

The builder status is exposed through:

```http
GET /api/v1/public/pressure-volume-builder/status
```

and included in `/health` and `/api/v1/public/warm-status`.

## Production API Endpoints

The FastAPI service now exposes:

```http
GET  /api/v1/public/cross-section-products
POST /api/v1/public/cross-section-render
POST /api/v1/public/cross-section-loop
```

The artifact renderer chooses the newest configured VolumeStore that contains
the requested hour(s), considering both `latest-partial` and `current`. Explicit
requests like f002 will use the newest partial store when available. Full-loop
requests can still use the stable f000-f048 synoptic store.

Required behavior:

- California bounds only.
- Product whitelist: 19 non-smoke products.
- `spacing_km` bounded, probably 1-80.
- `top_pressure_hpa` bounded, probably 50-1000, default 100.
- Hours bounded to 0-48.
- Route hash includes cycle, product, hours, spacing, width/height, style
  version, top pressure, and route endpoints.
- Return cached results immediately.
- Upload generated artifacts and manifests to R2 using existing artifact
  patterns.

Artifact layout:

```text
cross-sections/hrrr/YYYYMMDD/HHZ/<route_hash>/volume_store_<route_hash>_f000_wind_speed_cross_section.webp
cross-sections/hrrr/YYYYMMDD/HHZ/<route_hash>/manifest.json
```

Cap active render jobs:

- Full-loop active renders: 2-4 max.
- If saturated, queue or return a clear 429/queued response.
- Do not spawn unlimited renderer processes for concurrent users.

Local observed memory:

- Sidecar full store: about 440 MB working set, about 36 MB private.
- Render process: budget 0.5-1 GB transient.
- Build with `--load-parallelism 4`: budget 6-10 GB.

## Verification

After deployment:

```bash
curl http://127.0.0.1:8797/api/metadata
```

Test one loop:

```bash
target/release/volume_store_cross_section_render \
  --store /opt/cafire-weather-service/data/volume-stores/current \
  --out-dir /tmp/xs-loop-test \
  --products wind_speed \
  --hours all \
  --spacing-km 5 \
  --top-pressure-hpa 100 \
  --width 1400 \
  --height 820 \
  --route-id prod_test \
  --route-name prod_test \
  --start-lat 40.5865 --start-lon -122.3917 \
  --end-lat 39.0968 --end-lon -120.0324
```

Expected: 49 PNG + 49 WebP frames.

Test all products for one hour:

```bash
target/release/volume_store_cross_section_render \
  --store /opt/cafire-weather-service/data/volume-stores/current \
  --out-dir /tmp/xs-all-test \
  --products all \
  --hour 24 \
  --spacing-km 5 \
  --top-pressure-hpa 100 \
  --width 1400 \
  --height 820 \
  --route-id prod_all_test \
  --route-name prod_all_test \
  --start-lat 40.5865 --start-lon -122.3917 \
  --end-lat 39.0968 --end-lon -120.0324
```

Expected: 19 rendered, smoke skipped.

After the node has a validated store and the API endpoints are wired, use
`https://cafire.wxsection.com` as the public test surface for arbitrary cross
sections. Keep the sidecar private and treat the public site/API as the test
suite.

## GOES18 Note

GOES18 is a separate satellite lane. Do not mix satellite ingest/rendering into
this VolumeStore deployment except for resource planning and shared R2/artifact
conventions.

Relevant satellite docs:

- `docs/cafire_satellite_pipeline.md`
- `docs/cafire_satellite_hetzner_integration_guide.md`

The satellite worker must remain independent from HRRR static maps, meteograms,
and the VolumeStore sidecar.
