# Vendored Dependencies

This directory contains upstream code copied into `rustwx` so the
workspace builds and runs from a fresh clone without sibling checkouts.

Imported upstreams in this migration pass:

- `metrust`, `wx-core`, `wx-field`, `wx-math`, `wx-radar`
  Source repo: `C:\Users\drew\metrust-py`
  Upstream commit: `a1664c245051c363a9fc2462d6ed1f2596a21e02`
  License metadata in upstream `Cargo.toml`: `MIT`
- `grib-core`
  Source repo: `C:\Users\drew\cfrust`
  Upstream commit: `d326eca4615df9cb3b081181e5e56a9026bb7c3f`
  License metadata in upstream `Cargo.toml`: no explicit license string
- `ecape-rs`
  Source repo: `C:\Users\drew\.cargo\git\checkouts\ecape-rs-e4f5fdaa7c8f9a1b\8292253`
  Upstream commit: `82922534c02a888e773c50463b5a49d535606276`
  License: `MIT`
  Preserved file: [vendor/ecape-rs/LICENSE](/abs/path/C:/Users/drew/rustwx/vendor/ecape-rs/LICENSE)
- `sharprs`
  Source repo: `C:\Users\drew\sharprs`
  Upstream commit: `16cf0757304eb690d0208c304e32a4676178f00a`
  License: `BSD-3-Clause`
  Preserved file: [vendor/sharprs/LICENSE](/abs/path/C:/Users/drew/rustwx/vendor/sharprs/LICENSE)

Basemap assets were copied from `wrf-rust-plots/rustbox-fresh/assets/basemap`
into `rustwx/assets/basemap`. The upstream asset lockfile was preserved as
`assets/basemap/upstream-lock.json`, and the Natural Earth asset README remains
in `assets/basemap/natural_earth_110m/README.md`.
