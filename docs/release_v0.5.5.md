# rustwx v0.5.5 Release Notes

v0.5.5 promotes the shared global-model non-ECAPE hour runner for GFS and ECMWF production use, and adds native GOES ABI sector rendering for full-disk and mesoscale satellite workflows.

## Changes

- `non_ecape_hour` now accepts `--all-supported` to request every supported non-ECAPE direct and derived recipe for the selected model.
- `non_ecape_hour --domain-set global-model` is documented as the preferred fast lane for GFS and ECMWF-style global models: one prepared model hour can fan out to global, CONUS, North America, Europe, and the other continent-scale domains.
- Explicit opt-in ensemble/stat recipes remain excluded from `--all-supported` so model-specific ensemble products stay in their own lanes.
- `goes_satellite_batch` now supports GOES ABI sector shortcuts for full disk, CONUS, Meso-1, and Meso-2.
- Full-disk and mesoscale ABI single-band renders can use native fixed-grid output, preserving the actual satellite disk or mesoscale scene instead of forcing a lat/lon map projection.
- Mesoscale sequences can render the latest N complete scans and write an animated GIF, useful for one-minute GOES proof loops.
- The Python agent API exposes the same GOES sector, auto-bounds, sequence-count, and sequence-GIF controls.

## Validation

- `cargo check -p rustwx-cli --bin non_ecape_hour`
- `cargo test -p rustwx-cli --bin non_ecape_hour`
- `cargo build --release -p rustwx-cli --bin non_ecape_hour`
- `cargo test -p rustwx-products satellite::batch::tests`
- `cargo test -p rustwx-cli --bin goes_satellite_batch`
- `cargo check -p rustwx-python --features python`
