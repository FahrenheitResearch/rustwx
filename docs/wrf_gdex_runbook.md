# WRF GDEX Runbook

This is the minimal setup to get `rustwx` running on a fresh Linux node for UCAR GDEX WRF data.

## Fresh node setup

```bash
git clone https://github.com/FahrenheitResearch/rustwx.git
cd rustwx
cargo build --release -p rustwx-cli --bin forecast_now --bin direct_batch --bin derived_batch
```

Recommended cache root:

```bash
mkdir -p /root/rustwx-cache
```

## Historical `d612005` example

This runs the full CONUS non-ECAPE stack for the historical branch.

```bash
target/release/forecast_now \
  --models wrf-gdex \
  --hours 12,15 \
  --regions conus \
  --date 20110427 \
  --cycle 0 \
  --source gdex \
  --out-dir /root/runs/apr27_2011_hist \
  --cache-dir /root/rustwx-cache \
  --all-supported \
  --skip-ecape \
  --allow-large-heavy-domain \
  --job-concurrency 2 \
  --render-threads 16
```

`--date` and `--cycle` define the model start time. `--hours 21` means valid time `cycle + 21h`.

## Future `d612005` example

The future files use the same WRF/GDEX adapter. Pass the future product
families explicitly; no cache alias workaround is required.

### Seed future cache

```bash
python3 scripts/seed_wrf_gdex_cache.py \
  --cache-root /root/rustwx-cache \
  --dataset d612005 \
  --family future2d \
  --date 20800427 \
  --cycle-hour 0 \
  --forecast-hour 21

python3 scripts/seed_wrf_gdex_cache.py \
  --cache-root /root/rustwx-cache \
  --dataset d612005 \
  --family future3d \
  --date 20800427 \
  --cycle-hour 0 \
  --forecast-hour 21
```

### Future full non-ECAPE run

```bash
target/release/forecast_now \
  --models wrf-gdex \
  --hours 21 \
  --regions conus \
  --date 20800427 \
  --cycle 0 \
  --source gdex \
  --out-dir /root/runs/apr27_2080_future \
  --cache-dir /root/rustwx-cache \
  --all-supported \
  --skip-ecape \
  --surface-product d612005-future2d \
  --pressure-product d612005-future3d \
  --allow-large-heavy-domain \
  --job-concurrency 2 \
  --render-threads 16
```

### Future derived-only run

```bash
target/release/derived_batch \
  --model wrf-gdex \
  --date 20800427 \
  --cycle 0 \
  --forecast-hour 21 \
  --source gdex \
  --region conus \
  --all-supported \
  --surface-product d612005-future2d \
  --pressure-product d612005-future3d \
  --allow-large-heavy-domain \
  --out-dir /root/runs/apr27_2080_future \
  --cache-dir /root/rustwx-cache
```

### Future direct-only run

```bash
target/release/direct_batch \
  --model wrf-gdex \
  --date 20800427 \
  --cycle 0 \
  --forecast-hour 21 \
  --source gdex \
  --region conus \
  --all-supported \
  --product-override d612005-hist2d=d612005-future2d \
  --product-override d612005-hist3d=d612005-future3d \
  --out-dir /root/runs/apr27_2080_future \
  --cache-dir /root/rustwx-cache
```

This produces the direct surface maps like:

- `2m_relative_humidity`
- `2m_temperature_10m_winds`
- `2m_dewpoint_10m_winds`
- `mslp_10m_winds`

## Custom GDEX WRF products

Use product names as the adapter contract:

- `dNNNNNN-hist2d` maps to `.../g/dNNNNNN/hist2D/YYYYMM/wrf2d_d01_...nc`
- `dNNNNNN-hist3d` maps to `.../g/dNNNNNN/hist3D/YYYYMM/wrf3d_d01_...nc`
- `dNNNNNN-future2d` maps to `.../g/dNNNNNN/future2D/YYYYMM/wrf2d_d01_...nc`
- `dNNNNNN-future3d` maps to `.../g/dNNNNNN/future3D/YYYYMM/wrf3d_d01_...nc`
- `dNNNNNN-d01` maps to legacy `wrfout_d01_...nc`

For one-off government datasets that are still WRF but have odd native
fields, keep the product family explicit and add dataset-specific extraction
logic in `rustwx-wrf` instead of forking the runner.

## Pulling results back

```bash
cd /root/runs/apr27_2080_future
tar -czf /root/apr27_2080_future.tar.gz rustwx_wrf_gdex_20800427_0z_f021_conus_*
scp -P 10094 root@HOST:/root/apr27_2080_future.tar.gz .
```

## Current caveats

- `forecast_now` is the clean path for whole-hour production runs.
- `direct_batch` uses `--product-override planned=actual` for direct-only WRF/GDEX future/custom families.
- `derived_batch`, `forecast_now`, `non_ecape_hour`, and `production_runner` accept `--surface-product` and `--pressure-product`.
- If THREDDS is flaky, prefer `scripts/seed_wrf_gdex_cache.py` because it downloads from the OSDF direct namespace.
