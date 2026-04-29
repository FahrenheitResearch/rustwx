# CA Fire GOES Satellite Pipeline

The satellite lane uses raw NOAA GOES NetCDF, not NOAA STAR JPG scraping. The
generic ingest/render code lives in `crates/rustwx-products/src/satellite`; the
CA Fire service only supplies polling, domain, product list, upload, and public
manifest configuration.

## Operational Defaults

- Satellite: GOES-West / `goes18`
- ABI source: `noaa-goes18` bucket, `ABI-L2-CMIPC`
- GLM source: `noaa-goes18` bucket, `GLM-L2-LCFA`
- Domain: Pacific Southwest, `[-127, -111, 30, 44.5]`
- Polling: `SATELLITE_INTERVAL_SEC=300`
- Raw cache: `RUSTWX_CACHE_DIR/satellite`
- Public manifest: `/api/v1/public/latest-satellite-artifacts`
- Artifact pointer: `satellite/latest.json`

## Products

The default still manifest includes:

- `goes_geocolor`
- `goes_glm_fed_geocolor`
- `goes_airmass_rgb`
- `goes_sandwich_rgb`
- `goes_day_night_cloud_micro_combo_rgb`
- `goes_fire_temperature_rgb`
- `goes_dust_rgb`
- `goes_abi_band_01` through `goes_abi_band_16`

The service manifest also includes a website-facing `product_catalog` so the UI
can show NOAA STAR-style names and descriptions while keeping stable rustwx
slugs in the API.

Lightweight loops are generated per product from recent local stills after the
latest still manifest has already been published. Defaults:

- `SATELLITE_LOOP_ENABLED=true`
- `SATELLITE_LOOP_DURATIONS_MIN=30`
- `SATELLITE_LOOP_MAX_FRAMES=6`
- `SATELLITE_LOOP_FRAME_MS=450`
- `SATELLITE_LOOP_WIDTHS=600`
- `SATELLITE_LOOP_FORMATS=webp`

The first published scan only has a one-frame loop. Once the worker has two or
more successful scans in the local artifact tree, each loop uses the recent
scan sequence for that product.

## Expected Runtime

The first run has to download the selected ABI channels and recent GLM files.
After that, unchanged scans are skipped by `scan_id`, and repeated downloads are
cache hits. A full configured batch should be treated as CPU and memory heavier
than the standalone GLM lightning worker because GeoColor and visible bands use
higher-resolution ABI grids. Plan for several GB of memory headroom on the
worker host and measure release-mode batch time after deployment with the
manifest `timing` block.

Satellite can run independently from HRRR workers. If NOAA satellite discovery
or rendering fails, the failure is logged by `satellite-worker` and does not
degrade HRRR static maps, meteograms, or the existing GLM lightning endpoint.
