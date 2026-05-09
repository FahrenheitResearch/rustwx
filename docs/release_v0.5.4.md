# rustwx v0.5.4 Release Notes

v0.5.4 extends the lossless fast PNG path to the direct and ensemble-stat map binaries used by production runners.

## Changes

- `direct_batch` now exposes `--png-compression default|fast|fastest` and defaults to `fast`.
- `hrrr_direct_batch` now exposes the same `--png-compression` option and defaults to `fast`.
- `grib_ensemble_reduce` now exposes the same `--png-compression` option and defaults to `fast`.
- The option is lossless; `fast` preserves pixels while spending less CPU on PNG compression.

## Validation

- `cargo check -p rustwx-cli --bin direct_batch --bin hrrr_direct_batch --bin grib_ensemble_reduce`
- `cargo build --release -p rustwx-cli --bin direct_batch --bin hrrr_direct_batch --bin grib_ensemble_reduce`
- `cargo test -p rustwx-cli --bin direct_batch --bin hrrr_direct_batch --bin grib_ensemble_reduce`
