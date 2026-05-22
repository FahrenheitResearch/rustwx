# rustwx v0.5.9

v0.5.9 is a sounding-table correctness patch for the HRRR sounding renderer.

## Changes

- Fixed sounding parcel tables so standard CAPE, CINH, 3CAPE, 6CAPE, LCL,
  LFC, and EL use native `sharprs` parcel values.
- Kept ECAPE and NCAPE sourced from the verified `ecape-rs` bridge.
- Prevented ECAPE bridge parcel-origin LFC values from overriding the
  SHARPpy-style native LFC clamp.
- Synced the same ECAPE table behavior into the vendored `sharprs` renderer.

## Validation

```text
cargo test -p rustwx-sounding
cargo test --manifest-path vendor/sharprs/Cargo.toml
cargo run -p rustwx-cli --bin sounding_plot -- --model hrrr --date 20260522 --cycle 16 --forecast-hour 0 --source aws --lat 35.48 --lon -97.51 --station-id OKC --crop-radius-deg 1.0 --sample-method nearest --include-column
```

The regenerated HRRR 2026-05-22 16Z F000 Oklahoma City sounding now reports
surface and most-unstable LFC at the native SHARPpy-style LCL instead of zero.
