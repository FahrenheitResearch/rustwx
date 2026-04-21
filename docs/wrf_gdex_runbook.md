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

The future files exist, but two extra steps are useful:

1. Seed the fetch cache from the OSDF direct namespace instead of THREDDS when UCAR is returning `502/503`.
2. Alias the future cache entries onto the historical family names for `direct_batch`, because the current direct planner still assumes the historical WRF/GDEX family names.

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

### Future derived run

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

### Future direct run

Alias future cache entries onto the historical family names:

```bash
python3 scripts/alias_wrf_gdex_cache_entry.py \
  --cache-root /root/rustwx-cache \
  --date 20800427 \
  --cycle-hour 0 \
  --forecast-hour 21 \
  --source-product d612005-future2d \
  --target-product d612005-hist2d

python3 scripts/alias_wrf_gdex_cache_entry.py \
  --cache-root /root/rustwx-cache \
  --date 20800427 \
  --cycle-hour 0 \
  --forecast-hour 21 \
  --source-product d612005-future3d \
  --target-product d612005-hist3d
```

Then run direct products:

```bash
target/release/direct_batch \
  --model wrf-gdex \
  --date 20800427 \
  --cycle 0 \
  --forecast-hour 21 \
  --source gdex \
  --region conus \
  --all-supported \
  --out-dir /root/runs/apr27_2080_future \
  --cache-dir /root/rustwx-cache
```

This produces the direct surface maps like:

- `2m_relative_humidity`
- `2m_temperature_10m_winds`
- `2m_dewpoint_10m_winds`
- `mslp_10m_winds`

## Pulling results back

```bash
cd /root/runs/apr27_2080_future
tar -czf /root/apr27_2080_future.tar.gz rustwx_wrf_gdex_20800427_0z_f021_conus_*
scp -P 10094 root@HOST:/root/apr27_2080_future.tar.gz .
```

## Current caveats

- `forecast_now` is the clean path for historical `d612005`.
- Future `derived_batch` works with explicit `future2d` and `future3d` product overrides.
- Future `direct_batch` still needs the cache alias workaround until the direct planner is taught to respect future WRF/GDEX families directly.
- If THREDDS is flaky, prefer `scripts/seed_wrf_gdex_cache.py` because it downloads from the OSDF direct namespace.
