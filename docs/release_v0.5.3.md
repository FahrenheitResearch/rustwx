# rustwx v0.5.3 Release Notes

v0.5.3 is a map-throughput release for the forecast rendering operators.

## Changes

- `forecast_now` now exposes `--png-compression default|fast|fastest` and defaults to `fast`.
- `hrrr_non_ecape_hour` now exposes the same `--png-compression` option and defaults to `fast`.
- Direct and derived product rendering now use dynamic work queues, which reduces tail latency when recipes have uneven render costs.
- Warm-cache benchmark guidance: HRRR non-ECAPE CONUS all-supported maps are best packed with multiple forecast jobs and smaller render pools, for example `--job-concurrency 4 --render-threads 4 --png-compression fast`.

## Validation

- `cargo check -p rustwx-cli --bin forecast_now`
- `cargo check -p rustwx-cli --bin hrrr_non_ecape_hour`
- `cargo build --release -p rustwx-cli --bin forecast_now --bin hrrr_non_ecape_hour`
- `cargo test -p rustwx-products`
- `cargo test -p rustwx-cli --bin forecast_now --bin hrrr_non_ecape_hour`

## Notes

PNG compression mode is lossless. The `fast` mode preserves image pixels while spending less CPU on compression; files may be larger than the default encoder output.
