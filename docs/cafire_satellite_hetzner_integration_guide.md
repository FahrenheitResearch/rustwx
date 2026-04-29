# CA Fire Satellite Hetzner Integration Guide

This is the handoff for integrating the all-Rust GOES satellite lane into the
live CA Fire weather service on the Hetzner node.

## Goal

Expose the rustwx GOES-West satellite products through the existing CA Fire API
and R2 artifact flow without scraping or proxying NOAA STAR JPGs.

Generic satellite ingest/rendering stays in:

- `crates/rustwx-products/src/satellite`
- `crates/rustwx-render`
- `crates/rustwx-python` bindings

CA Fire service wiring stays in:

- `deploy/cafire-weather-service`

Do not change the existing public schemas or behavior for:

- `/api/v1/public/latest-artifacts`
- `/api/v1/public/latest-diurnal-artifacts`
- `/api/v1/public/latest-lightning-artifacts`
- `/api/v1/public/meteogram`
- `/api/v1/public/meteogram.png`

Satellite should be additive through `/api/v1/public/latest-satellite-artifacts`.

## Live Service

- Host path: `/opt/cafire-weather-service`
- Public API: `https://cafire.wxsection.com`
- Compose service: `satellite-worker`
- Raw cache inside container: `/data/cache/satellite`
- Artifact root inside container: `/data/artifacts`
- Public latest manifest key: `satellite/latest.json`
- Per-scan artifact layout: `satellite/g18/pacific_southwest/<scan-time>/...`

Do not print `.env`, API keys, R2 secrets, or shell history containing secrets.

## Preflight

On the Hetzner node:

```bash
cd /opt/cafire-weather-service
git status --short
docker compose ps
docker compose logs --tail=120 api
docker compose logs --tail=120 satellite-worker
df -h /
du -sh data/cache data/artifacts 2>/dev/null || true
```

If the worktree is dirty, do not revert unrelated files. Preserve the live
`.env`.

## Required Config

Confirm these non-secret settings exist in `.env`, matching
`deploy/cafire-weather-service/.env.example`:

```dotenv
SATELLITE_ENABLED=true
SATELLITE_INTERVAL_SEC=300
SATELLITE_DOMAIN=pacific_southwest
SATELLITE_LABEL=Pacific Southwest Satellite
SATELLITE_SATELLITE=goes18
SATELLITE_ABI_PRODUCT=ABI-L2-CMIPC
SATELLITE_DOWNLOAD_GLM=true
SATELLITE_SCAN_LOOKBACK_HOURS=6
SATELLITE_DISCOVERY_RETRIES=2
SATELLITE_RETRY_SLEEP_MS=20000
SATELLITE_STILL_WIDTHS=600
SATELLITE_STILL_FORMATS=webp
SATELLITE_LOOP_ENABLED=true
SATELLITE_LOOP_DURATIONS_MIN=30
SATELLITE_LOOP_WIDTHS=600
SATELLITE_LOOP_FORMATS=webp
SATELLITE_LOOP_GIF_MAX_DURATION_MIN=0
SATELLITE_LOOP_MAX_FRAMES=6
SATELLITE_LOOP_FRAME_MS=450
```

The default product list should include:

```text
goes_geocolor
goes_glm_fed_geocolor
goes_airmass_rgb
goes_sandwich_rgb
goes_day_night_cloud_micro_combo_rgb
goes_fire_temperature_rgb
goes_dust_rgb
goes_abi_band_01 through goes_abi_band_16
```

R2 must be configured with bucket write access and a public artifact base URL:

```dotenv
R2_ACCOUNT_ID=...
R2_BUCKET=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_ENDPOINT_URL=https://<account-id>.r2.cloudflarestorage.com
PUBLIC_ARTIFACT_BASE_URL=https://...
```

Prefer a Cloudflare custom domain for production browser traffic. Keep `r2.dev`
only as a temporary public asset URL.

## Deploy Steps

Use release builds through Docker, not debug binaries.

```bash
cd /opt/cafire-weather-service
docker compose build api satellite-worker
docker compose up -d api satellite-worker
```

If the Python extension or Rust satellite entry point was missing in the old
image, rebuild without cache:

```bash
docker compose build --no-cache api satellite-worker
docker compose up -d api satellite-worker
```

Leave `static-worker`, `lightning-worker`, `warmer`, and `caddy` running unless
their images also changed.

## First Run Smoke Test

Run one satellite pass before relying on the loop service:

```bash
docker compose run --rm satellite-worker python -m app.satellite_service
```

Expected behavior:

- Finds the latest complete GOES-West `ABI-L2-CMIPC` scan in `noaa-goes18`.
- Downloads required ABI NetCDF channels into `/data/cache/satellite`.
- Downloads recent GLM `GLM-L2-LCFA` files when GLM overlay is enabled.
- Renders all configured products from raw NetCDF.
- Writes native stills and configured lightweight WebP still variants.
- Publishes `satellite/latest.json` as soon as stills are ready.
- Builds recent WebP loops if previous scans exist, then updates the manifest
  with loop metadata.
- Uploads immutable artifacts and `satellite/latest.json` to R2.

Then start or restart the loop worker:

```bash
docker compose up -d satellite-worker
docker compose logs -f satellite-worker
```

## API Checks

Check the satellite endpoint without disturbing HRRR or lightning:

```bash
curl -fsS https://cafire.wxsection.com/api/v1/public/latest-satellite-artifacts | jq '.kind,.scan_time_utc,.product_catalog | length'
curl -fsS https://cafire.wxsection.com/api/v1/public/latest-artifacts >/dev/null
curl -fsS https://cafire.wxsection.com/api/v1/public/latest-diurnal-artifacts >/dev/null
curl -fsS https://cafire.wxsection.com/api/v1/public/latest-lightning-artifacts >/dev/null
curl -fsS https://cafire.wxsection.com/health | jq .
```

The satellite manifest should include clean metadata:

- `satellite`
- `abi_product`
- `scan_time_utc`
- `generated_at_utc`
- `domain`
- `domain_label`
- `products`
- `product_catalog`
- `artifacts`
- `loops`
- `source_keys`
- `bounds`
- Map overlay metadata with image URL, bounds, opacity recommendation, and
  timestamp where available

## Cadence And Capacity

The target polling interval is 5 minutes. That means the worker must complete a
normal changed-scan pass in less than 300 seconds on the Hetzner node. Practical
public latency is usually one polling interval plus NOAA product availability,
roughly 5-15 minutes for GOES-West CONUS ABI products.

The first scan is slower because it fills the raw NetCDF cache. Later scans
should be faster because unchanged scans are skipped and repeated raw files are
cache hits.

Loops are built from local recent stills. A new deployment will publish one-frame
loops until enough successful scans accumulate.

The production target is still-first publication in under one GOES cadence.
Keep only a lightweight 600 px WebP still variant and a 30 minute 600 px WebP
loop by default. Longer loops, GIFs, and additional still sizes are allowed only
when they do not make the worker fall behind latest scans.

Do not disable HRRR, meteogram, or lightning workers to make satellite fit.

## Failure Isolation

Satellite failures must only affect:

- `satellite-worker`
- `/api/v1/public/latest-satellite-artifacts`
- `satellite/latest.json`

They must not degrade existing HRRR maps, diurnal maps, GLM lightning artifacts,
meteograms, or Caddy/API health.

If satellite is broken in production:

```bash
cd /opt/cafire-weather-service
docker compose stop satellite-worker
```

Leave the API and existing workers running. Fix satellite independently, then
restart only `satellite-worker`.

## Common Pitfalls

- Incomplete ABI scan discovery: the worker has bounded retries; do not publish
  partial channel sets.
- Stale Docker image: rebuild `api` and `satellite-worker` together when the
  rustwx Python extension changes.
- Wrong artifact base URL: browser-visible URLs should use
  `PUBLIC_ARTIFACT_BASE_URL`, not private R2 S3 endpoints.
- Missing R2 CORS/custom domain: cafire.org needs browser-readable image URLs.
- Disk growth: raw NetCDF can be large. Keep cache cleanup enabled and monitor
  `/opt/cafire-weather-service/data/cache`.
- Secrets in logs: never echo `.env`; only check variable names or masked values.
