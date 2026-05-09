# rustwx v0.5.6 Release Notes

v0.5.6 adds native GOES ABI sector rendering for full-disk and mesoscale satellite workflows.

## Changes

- `goes_satellite_batch` supports GOES ABI sector shortcuts for full disk, CONUS, Meso-1, and Meso-2.
- Full-disk and mesoscale ABI single-band renders can use native fixed-grid output, preserving the actual satellite disk or mesoscale scene instead of forcing a lat/lon map projection.
- Mesoscale sequences can render the latest N complete scans and write an animated GIF, useful for one-minute GOES proof loops.
- The Python agent API exposes GOES sector, auto-bounds, sequence-count, and sequence-GIF controls.

## Validation

- `cargo test -p rustwx-products satellite::batch::tests`
- `cargo test -p rustwx-cli --bin goes_satellite_batch`
- `cargo check -p rustwx-python --features python`
