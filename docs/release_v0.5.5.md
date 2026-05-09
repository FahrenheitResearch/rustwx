# rustwx v0.5.5 Release Notes

v0.5.5 promotes the shared global-model non-ECAPE hour runner for GFS and ECMWF production use.

## Changes

- `non_ecape_hour` now accepts `--all-supported` to request every supported non-ECAPE direct and derived recipe for the selected model.
- `non_ecape_hour --domain-set global-model` is documented as the preferred fast lane for GFS and ECMWF-style global models: one prepared model hour can fan out to global, CONUS, North America, Europe, and the other continent-scale domains.
- Explicit opt-in ensemble/stat recipes remain excluded from `--all-supported` so model-specific ensemble products stay in their own lanes.

## Validation

- `cargo check -p rustwx-cli --bin non_ecape_hour`
- `cargo test -p rustwx-cli --bin non_ecape_hour`
- `cargo build --release -p rustwx-cli --bin non_ecape_hour`
