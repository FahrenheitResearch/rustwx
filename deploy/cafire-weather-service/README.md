# CA Fire Weather Hosted Service Pilot

This is a thin deployment scaffold around the stable `rustwx` Python/CLI API.
It is intentionally outside the Rust crates so the hosted service can evolve
without destabilizing the weather engine.

## Shape

- `api`: FastAPI service for health, catalogs, latest run, and point time-series.
- `batch`: one-shot HRRR California map renderer/uploader.
- `lightning-worker`: GOES-West GLM lightning artifact publisher.
- `satellite-worker`: GOES-West ABI/GLM NetCDF ingest and satellite artifact publisher.
- `R2`: public static artifact store for plots and manifests.
- local disk: transient HRRR fetch/decode cache, GOES raw NetCDF cache, and generated artifacts.

The normal path is:

```text
HRRR run available
-> batch renders California products by forecast hour
-> batch uploads immutable run artifacts to R2
-> batch writes latest.json
-> website reads static artifacts from R2/CDN
-> point clicks call /api/v1/public/meteogram.png for cached PNG meteograms
```

Satellite runs independently from HRRR workers:

```text
NOAA noaa-goes18 ABI/GLM NetCDF available
-> satellite-worker discovers the latest complete ABI-L2-CMIPC scan
-> rustwx downloads raw NetCDF into /data/cache/satellite
-> rustwx renders PSW PNGs from ABI/GLM fields
-> worker writes WebP, manifest, and satellite/latest.json to R2
```

## Hetzner Box

Recommended pilot settings:

```text
Provider: Hetzner Cloud
Location: Ashburn, VA first; Hillsboro, OR is also fine if preferred
Architecture: x86_64, not ARM, so the PyPI rustwx Linux wheel works directly
Image: Ubuntu 24.04 LTS
Plan: 8 vCPU / 16 GB RAM minimum
Disk: 160 GB absolute minimum; 250-320 GB preferred for HRRR cache headroom
Networking: public IPv4 + IPv6
Firewall inbound: 22 from your IP only, 80/443 from anywhere
Firewall inbound closed: 8000, Redis/Postgres, Docker internals
Backups: optional for pilot; snapshot before major deploy changes
```

If batch rendering falls behind, scale the worker first. The API can stay small.

Basic server bootstrap:

```bash
sudo apt update
sudo apt install -y ca-certificates curl git ufw
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```

Then clone the repo, copy `.env.example` to `.env`, fill the R2 values, and run:

```bash
mkdir -p data/cache data/artifacts data/glm
docker compose build
docker compose up -d
docker compose run --rm batch python -m app.batch render-latest --hours 1-2
```

## Cloudflare R2

Create one bucket for the pilot, for example:

```text
cafire-weather-prod
```

Recommended public hostname:

```text
weather-assets.cafire.org
```

Do not use the `r2.dev` URL for production traffic. Connect a custom domain to
the bucket and use Cloudflare cache rules:

```text
/hrrr/latest.json                 TTL 30-60 seconds
/hrrr/runs/*                      cache long, immutable
```

Create a bucket-scoped R2 API token with object read/write for the service. Put
the resulting S3-compatible values into `.env`:

```text
R2_ACCOUNT_ID=...
R2_BUCKET=cafire-weather-prod
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_ENDPOINT_URL=https://<account-id>.r2.cloudflarestorage.com
PUBLIC_ARTIFACT_BASE_URL=https://weather-assets.cafire.org
```

Apply the CORS policy in `cors.r2.json`, adjusted to the final cafire.org
origins.

## Local Smoke

```bash
cp .env.example .env
docker compose build
docker compose up api
curl http://localhost:8000/health
```

Render a small local batch without R2:

```bash
docker compose run --rm batch python -m app.batch render-latest \
  --hours 1-2 \
  --products 2m_temperature_10m_winds,2m_relative_humidity_10m_winds
```

## Production Notes

Use a public R2 custom domain for browser assets. Keep `r2.dev` only for
development. Give the service a bucket-scoped token with object read/write.

Recommended first bucket object layout:

```text
hrrr/latest.json
hrrr/runs/YYYYMMDD/HHZ/manifest.json
hrrr/runs/YYYYMMDD/HHZ/f001/california/<rustwx outputs>
satellite/latest.json
satellite/goes18/pacific_southwest/YYYYMMDDTHHMMSSZ/<goes outputs>
```

`latest.json` should have a short CDN TTL. Immutable run paths can use long
cache lifetimes.

## HTTP Endpoints

```text
GET  /health
GET  /api/v1/latest
GET  /api/v1/capabilities
GET  /api/v1/domains?kind=region
GET  /api/v1/products
GET  /api/v1/public/latest-artifacts
GET  /api/v1/public/latest-diurnal-artifacts
GET  /api/v1/public/latest-lightning-artifacts
GET  /api/v1/public/latest-satellite-artifacts
POST /api/v1/meteogram
POST /api/v1/public/meteogram
POST /api/v1/public/meteogram.png
```

`POST /api/v1/meteogram` returns native `rustwx.sample_point_timeseries_json`
output. `POST /api/v1/public/meteogram.png` renders the six-panel HRRR
meteogram, stores it under `meteograms/hrrr/YYYYMMDD/HHz/f000-f048/`, uploads it
to R2 when configured, and returns the static artifact URL. Repeat requests for
the same run, point, label, and hour range return the cached artifact without
resampling.

## Satellite Cadence

`satellite-worker` defaults to `SATELLITE_INTERVAL_SEC=300`. Expected practical
latency is usually one polling interval plus NOAA ABI product availability,
roughly 5-15 minutes for the latest GOES-West CONUS scan. Raw ABI and GLM
NetCDF files live under `RUSTWX_CACHE_DIR/satellite` and are pruned by the same
cache cleanup policy as other transient service cache files.

The satellite manifest publishes STAR-style product metadata for GeoColor, GLM
FED3+GeoColor, the RGB products, and ABI Bands 1-16. It also writes per-product
still-size variants when `SATELLITE_STILL_WIDTHS` is set. Defaults are
`600` in WebP, without upscaling past the native render. The native render is
also retained for large inspection.

When `SATELLITE_LOOP_ENABLED=true`, the worker publishes animated loop variants
from recent successful local scans after the latest still manifest has already
been published. Defaults are animated WebP loops at 30 minutes, 600 px wide,
capped at 6 frames. Keep GIFs, longer loops, and additional still sizes disabled
unless they fit within the GOES scan cadence.

The worker is separate from HRRR static maps, meteogram warming, and lightning;
satellite failures should only affect `/api/v1/public/latest-satellite-artifacts`.
