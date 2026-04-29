# CA Fire Weather Hosting Ops Handoff

Last verified: 2026-04-28 04:52 UTC.

This is the operational handoff for the live `cafire.wxsection.com` pilot:
Hetzner compute, Docker Compose service, Cloudflare DNS, and Cloudflare R2
artifact storage.

Do not commit secrets to this repo. The live R2 credentials and service API key
belong only in the remote `.env` file or a secret manager.

## Live Deployment

Public API/site:

```text
https://cafire.wxsection.com
```

Public R2 artifact base:

```text
https://pub-8665dc80b0aa4f1d8a3e91eda3a5c01f.r2.dev
```

Important public endpoints:

```text
GET  https://cafire.wxsection.com/health
GET  https://cafire.wxsection.com/api/v1/public/products
GET  https://cafire.wxsection.com/api/v1/public/latest-artifacts
GET  https://cafire.wxsection.com/api/v1/public/latest-diurnal-artifacts
GET  https://cafire.wxsection.com/api/v1/public/latest-lightning-artifacts
GET  https://cafire.wxsection.com/api/v1/public/latest-lightning.geojson
GET  https://cafire.wxsection.com/api/v1/public/warm-status
POST https://cafire.wxsection.com/api/v1/public/meteogram
POST https://cafire.wxsection.com/api/v1/public/meteogram.png
```

The public integration guide for website/frontend agents is:

```text
docs/cafire_api_integration_guide.md
```

## Hetzner Server

Current box:

```text
Provider: Hetzner Cloud
Server name: ubuntu-32gb-nbg1-1
IPv4: 178.104.59.253
IPv6 prefix: 2a01:4f8:1c19:9295::/64
Location: Nuremberg DC Park 1 / NBG1
OS: Ubuntu
CPU: 16 vCPU
RAM: about 30 GiB available to Linux
Disk: about 601 GB root disk
```

Current access pattern from this workstation:

```bash
ssh root@178.104.59.253
```

The root password was provided out of band during setup. Do not write it into
the repo. Prefer SSH keys. Rotate the root password and any pasted tokens before
calling this production.

Live service path:

```text
/opt/cafire-weather-service
```

Current Rust build tree used for patched local wheels:

```text
/opt/rustwx-build-20260427231954
```

Current mounted data paths under the service directory:

```text
/opt/cafire-weather-service/data/cache
/opt/cafire-weather-service/data/artifacts
/opt/cafire-weather-service/data/glm
```

As of verification, local disk usage was roughly:

```text
data/cache      241 GB
data/artifacts  4.0 GB
root disk       263 GB used / 314 GB free
```

## Docker Services

The service runs with Docker Compose in `/opt/cafire-weather-service`.

```bash
cd /opt/cafire-weather-service
docker compose ps
docker compose logs --tail=200 api
docker compose logs --tail=200 static-worker
docker compose logs --tail=200 lightning-worker
docker compose logs --tail=200 warmer
```

Services:

```text
caddy          HTTPS reverse proxy for cafire.wxsection.com
api            FastAPI app, public site, public JSON endpoints, meteogram PNGs
static-worker  long-running HRRR static plot renderer/uploader
lightning-worker dedicated GOES-West GLM fetch/render/uploader
warmer         background point-timeseries warm cache
batch          one-shot render command profile
```

The API binds only to localhost on the host:

```text
127.0.0.1:8000 -> api:8000
```

Caddy exposes ports `80` and `443` publicly and reverse-proxies to `api:8000`.

## Cloudflare

Cloudflare zone currently used:

```text
Domain: wxsection.com
Zone ID: d4f78c289eaf0a05618888cba8280841
Account ID: de348a5190a7a5676bd2f6f19d506eec
```

DNS should point `cafire.wxsection.com` at the Hetzner IPv4:

```text
cafire.wxsection.com -> 178.104.59.253
```

Caddy handles TLS on the box.

## Cloudflare R2

Current bucket:

```text
Bucket: cafire
Region/location: Western North America (WNAM)
S3 endpoint: https://de348a5190a7a5676bd2f6f19d506eec.r2.cloudflarestorage.com
Public development URL: https://pub-8665dc80b0aa4f1d8a3e91eda3a5c01f.r2.dev
```

The live service uses a bucket-scoped R2 token with read/write/list access for
the `cafire` bucket. The S3 credentials are stored only in:

```text
/opt/cafire-weather-service/.env
```

Never commit these values:

```text
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
SERVICE_API_KEY
```

Current non-secret R2 settings are mirrored in:

```text
deploy/cafire-weather-service/.env.example
```

The current public setup uses the `r2.dev` public URL. For a real production
site, assign a custom R2 domain and set `PUBLIC_ARTIFACT_BASE_URL` to that
custom hostname.

## Current Runtime Config

Current `rustwx` package in the containers:

```text
rustwx 0.4.4
```

Current data source:

```text
DEFAULT_SOURCE=nomads
```

Static maps:

```text
DEFAULT_DOMAIN=california
DEFAULT_WIDTH=1400
DEFAULT_HEIGHT=1600
STATIC_MAP_WORKER_INTERVAL_SEC=30
STATIC_MAP_WORKER_PARALLELISM=3
STATIC_MAP_CYCLE_LOOKBACK_HOURS=8
STATIC_MAP_BACKFILL_BATCH_HOURS=3
STATIC_MAP_SMOKE_INTERVAL_HOURS=1
STATIC_MAP_BRAND_TEXT=California Wildfire Tracking
STATIC_MAP_WEBP_ENABLED=true
STATIC_MAP_WEBP_QUALITY=72
```

Current hourly products:

```text
2m_temperature_10m_winds
2m_relative_humidity_10m_winds
2m_dewpoint_10m_winds
10m_wind_gusts
vpd_2m
fire_weather_composite
qpf_1h
10m_wind_1h_max
visibility
smoke_pm25_native
smoke_column
```

Current diurnal products:

```text
2m_temp_0_24h_range
2m_temp_24_48h_range
2m_temp_0_48h_range
```

Meteograms:

```text
METEOGRAM_WARM_ENABLED=true
METEOGRAM_WARM_IN_API=false
METEOGRAM_WARM_HOURS=0-48
FAST_METEOGRAM_STORE_ENABLED=true
FAST_METEOGRAM_STORE_BOUNDS=-125.2,-113.5,31.0,43.0
```

The meteogram warmers now select the newest 00/06/12/18Z HRRR run where f048 is
actually available on NOMADS. They no longer wait a fixed 6 hours before moving
from 18Z to 00Z.

Lightning:

```text
LIGHTNING_ENABLED=true
LIGHTNING_INTERVAL_SEC=30
LIGHTNING_DOMAIN=california
LIGHTNING_LABEL=California GLM Lightning
LIGHTNING_SATELLITE=goes18
LIGHTNING_FETCH_COUNT=90
LIGHTNING_LOOKBACK_HOURS=3
LIGHTNING_MAX_AGE_MIN=30
LIGHTNING_WIDTH=1400
LIGHTNING_HEIGHT=1100
```

The dedicated `lightning-worker` fetches recent GOES-West GLM LCFA files from
the public NOAA S3 bucket, keeps only the selected recent file set in
`/data/glm`, renders with `rustwx.render_glm_lightning_json`, writes
PNG/WebP/JSON artifacts, uploads them to R2, and updates:

```text
lightning/latest.json
```

The worker polls every 30 seconds and skips re-render/upload work when NOAA has
not posted a newer GLM object. Expected normal latency is roughly 1-2 minutes
behind the latest NOAA object availability on this setup.

Manual run:

```bash
cd /opt/cafire-weather-service
docker compose run --rm lightning-worker python -m app.lightning_service
```

Public manifest:

```text
https://cafire.wxsection.com/api/v1/public/latest-lightning-artifacts
```

The site gallery has a `Lightning` tab. It can legitimately show zero
California-domain flashes even when the GOES-West full-disk file set contains
many flashes.

Cache janitor:

```text
CACHE_CLEANUP_ENABLED=true
CACHE_CLEANUP_INTERVAL_SEC=1800
CACHE_CLEANUP_MAX_AGE_HOURS=30
CACHE_CLEANUP_MAX_CACHE_GB=200
CACHE_CLEANUP_TARGET_CACHE_GB=185
CACHE_CLEANUP_MIN_FREE_GB=160
CACHE_CLEANUP_TARGET_FREE_GB=220
CACHE_CLEANUP_EMERGENCY_MIN_AGE_HOURS=4
```

The static worker runs `app.cache_cleanup` periodically. The live cap is 200G,
and cleanup trims toward 185G when the cap is exceeded so active HRRR writes
have headroom and the status does not flap while a batch is running. Manual
checks:

```bash
cd /opt/cafire-weather-service
docker compose run --rm static-worker python -m app.cache_cleanup --dry-run
docker compose run --rm static-worker python -m app.cache_cleanup
```

The janitor only operates on a directory named `cache`, `rustwx-cache`, or
`rustwx_cache`; this is intended to prevent accidental broad deletes from a bad
`RUSTWX_CACHE_DIR`.

## Artifact Layout

Public R2 object layout:

```text
hrrr/latest.json
hrrr/latest-diurnal.json
hrrr/runs/YYYYMMDD/HHZ/manifest.json
hrrr/runs/YYYYMMDD/HHZ/diurnal-manifest.json
hrrr/runs/YYYYMMDD/HHZ/f000/california/...
hrrr/runs/YYYYMMDD/HHZ/f001/california/...
lightning/latest.json
lightning/california/YYYYMMDDTHHMMZ/raw/glm_flashes.png
lightning/california/YYYYMMDDTHHMMZ/raw/glm_flashes.webp
lightning/california/YYYYMMDDTHHMMZ/raw/glm_flashes.json
meteograms/hrrr/YYYYMMDD/HHZ/...
```

Static plots are published as both PNG and WebP. PNG is the full-quality
inspection artifact. WebP is the lightweight browser/scrub-bar artifact.

As of verification:

```text
Latest hourly manifest: 207 PNG + 207 WebP
Latest diurnal manifest: 4 PNG + 4 WebP
Hourly WebP byte size: about 7.4% of PNG byte size
Diurnal WebP byte size: about 5.6% of PNG byte size
```

## Deploy Commands

After editing files under `deploy/cafire-weather-service/app`, copy them to the
server and rebuild/restart the affected service:

```powershell
scp deploy/cafire-weather-service/app/<file>.py root@178.104.59.253:/opt/cafire-weather-service/app/
ssh root@178.104.59.253 "cd /opt/cafire-weather-service && docker compose build api static-worker lightning-worker warmer && docker compose up -d api static-worker lightning-worker warmer"
```

For static frontend JS:

```powershell
scp deploy/cafire-weather-service/app/static/app.js root@178.104.59.253:/opt/cafire-weather-service/app/static/app.js
ssh root@178.104.59.253 "cd /opt/cafire-weather-service && docker compose build api && docker compose up -d api"
```

For one-shot manual render:

```bash
cd /opt/cafire-weather-service
docker compose run --rm batch python -m app.batch render-latest --hours 0-2
```

For full restart:

```bash
cd /opt/cafire-weather-service
docker compose up -d --build
```

For health checks:

```bash
curl -sS https://cafire.wxsection.com/health
curl -sS https://cafire.wxsection.com/api/v1/public/latest-artifacts
curl -sS https://cafire.wxsection.com/api/v1/public/latest-diurnal-artifacts
```

For public meteogram performance logs:

```bash
cd /opt/cafire-weather-service
docker compose logs -f api | grep meteogram_performance
```

Each public `/api/v1/public/meteogram` and `/api/v1/public/meteogram.png`
request emits one JSON log event with endpoint, status, total request
milliseconds, sample path (`fast_store`, `rustwx_chunked`, `rustwx_latest`, or
`artifact_cache`), sample/render milliseconds when available, cache hit flags,
forecast-hour count/range, rounded coordinates, run date/cycle, fetch count, and
blocker count. User-provided labels are intentionally not logged.

For rolling API/meteogram traffic metrics:

```bash
cd /opt/cafire-weather-service
python3 - <<'PY'
import urllib.request
from pathlib import Path
env = {}
for line in Path(".env").read_text().splitlines():
    if line.strip() and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v
req = urllib.request.Request(
    "http://127.0.0.1:8000/api/v1/metrics",
    headers={"x-api-key": env.get("SERVICE_API_KEY", "")},
)
print(urllib.request.urlopen(req, timeout=20).read().decode())
PY
```

The metrics endpoint is protected by the service API key and keeps only
in-process rolling windows (`5m`, `15m`, `1h`). It resets on API container
restart. It summarizes route counts/RPS, status groups, p50/p90/p95/p99/max
latency, slowest requests, meteogram cache/store hit rates, sample/render
latency, fetch/blocker counts, and slowest meteograms.

For a local browser ops dashboard from this workstation:

```powershell
python deploy/cafire-weather-service/ops/local_metrics_dashboard.py
```

Then open:

```text
http://127.0.0.1:8787
```

The local dashboard reads the remote API key from `/opt/cafire-weather-service/.env`
inside an SSH command and does not print the key. It shows Compose service
state, Docker CPU/memory, root disk, cache/artifact/GLM directory sizes,
summarized `/health`, latest hourly/diurnal/lightning manifests, and rolling
API/meteogram metrics. It refreshes every 10 seconds.

The dashboard exposes its raw local snapshot at:

```text
http://127.0.0.1:8787/snapshot
```

It does not include direct R2 object-download traffic; use Cloudflare/R2
analytics for static map bandwidth and CDN cache-hit analysis.

## Rebuilding The Local rustwx Wheel

The live Docker image installs `rustwx==0.4.4` from a wheel in:

```text
/opt/cafire-weather-service/wheels/
```

If a local Rust patch is needed before a PyPI release, rebuild on the server:

```bash
source /root/.cargo/env
cd /opt/rustwx-build-20260427231954
/opt/maturin-venv/bin/maturin build --release \
  --manifest-path crates/rustwx-python/Cargo.toml \
  --features python \
  --out target/wheels
```

Then copy the resulting cp312 wheel into:

```text
/opt/cafire-weather-service/wheels/
```

and rebuild/restart the Docker services.

## Known Current Limits

- Static worker currently publishes only the `california` domain.
- Other built-in rustwx domains exist, including `pacific_northwest`,
  `rockies_high_plains`, `california_southwest`, and `conus`, but the hosted
  service still needs a multi-domain manifest/worker pass before publishing
  them cleanly.
- Public meteogram endpoints are currently California-bounds guarded.
- The local HRRR cache is large but now has the static-worker cache janitor
  enabled. Recheck `df -h /` and `/opt/cafire-weather-service/data/cache`
  before adding another site to this disk.
- The current public R2 URL is an `r2.dev` URL. Use a custom domain for a
  polished public product.

## CONUS Site Notes

For a separate personal CONUS weather site, prefer a second Hetzner box or at
least a separate Compose project and R2 bucket. Do not overload the cafire
branding/API contract.

Recommended first CONUS shape:

```text
Domain: weather.<your-domain>
R2 bucket: rustwx-weather or similar
DEFAULT_DOMAIN=conus
STATIC_MAP_BRAND_TEXT=
DEFAULT_WIDTH=2200
DEFAULT_HEIGHT=1400
STATIC_MAP_WEBP_ENABLED=true
STATIC_MAP_WEBP_QUALITY=72
```

Start with a reduced product set, then widen:

```text
2m_temperature_10m_winds
2m_relative_humidity_10m_winds
2m_dewpoint_10m_winds
10m_wind_gusts
vpd_2m
fire_weather_composite
qpf_1h
visibility
smoke_pm25_native
smoke_column
```

CONUS is reasonable for static WebP-first maps. Full-CONUS point-click
meteogram memmaps are a different problem; use regional stores or on-demand
sampling first.
