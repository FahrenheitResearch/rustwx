# rustwx-cli

`rustwx-cli` is the command-line entrypoint plus the proof binaries used to exercise the stack end to end.

## Main CLI

Current top-level commands:

- `list`
- `show`
- `url`
- `latest`
- `hours`
- `probe`
- `fetch`

## Proof binaries that matter right now

- `hrrr_native_proof`
  - Current bounded HRRR weather-native proof runner.
  - Default suite currently selects only `conus_contour`.
  - Add `--case` to widen coverage with `midwest-core` and `southern-plains-severe`.
  - `--mode custom` keeps ad hoc single-region reruns.
  - Also runs the bounded HRRR cross-section proof lane and writes summary plus cross-section hook JSON.
- `hrrr_temperature_xsection`
  - Standalone real-data HRRR cross-section proof runner.
  - Supports `temperature`, `relative_humidity`, `theta_e`, and `wind_speed` through the shared `rustwx_cli::cross_section_proof` module.
  - Exposes optional `--palette` overrides on top of the public `rustwx-cross-section` palette catalog.
- `weather_native_bench`
  - Release-mode benchmark/profiling runner for the native contour map lane.
  - Compares Rust native contour render timings against forced legacy raster renders and Python `matplotlib/cartopy` equivalents on the same cached HRRR fields.
  - Current default benchmark set is `stp_fixed`, `sbcape`, and `srh_0_1km`; writes PNGs plus summary JSON/Markdown under `proof/bench/`.
- `hrrr_derived_batch` / `derived_batch`
  - Best lane for iterating derived weather-native maps.
  - Native projected contour-filled derived products are currently `stp_fixed`, `sbcape`, `mlcape`, `srh_0_1km`, `srh_0_3km`, `ehi_0_1km`, and `ehi_0_3km`.
- `hrrr_direct_batch` / `direct_batch`
  - Direct field proof lane.
  - Useful for contour-sensitive projected products such as `mslp_10m_winds` that still use the standard `rustwx-render` contour/overlay path.
- `product_catalog` and `proof_gallery`
  - Small inspection helpers for published proof output.

## Current limits

- The main CLI and the proof binaries are still separate surfaces.
- Native projected contour-fill is live for the derived products above, not yet for every direct/synoptic contour product.
- The real-data cross-section proof lane now covers a small multi-product family, but it is still pressure-axis only and not yet the full `wxsection_ref` product inventory.

## Minimal examples

```powershell
cargo run -p rustwx-cli -- list
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_native_proof -- --date 20260414 --cycle 23 --forecast-hour 0 --out-dir proof
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_native_proof -- --case conus-contour,southern-plains-severe --date 20260414 --cycle 23 --forecast-hour 0 --out-dir proof
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_native_proof -- --mode custom --date 20260414 --cycle 23 --forecast-hour 0 --region southern-plains --direct-recipe 500mb_temperature_height_winds --derived-recipe stp_fixed,sbcape --out-dir proof
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_temperature_xsection -- --date 20260414 --cycle 23 --forecast-hour 0 --out-dir proof
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_temperature_xsection -- --product wind-speed --date 20260414 --cycle 23 --forecast-hour 0 --out-dir proof
```

```powershell
cargo run -p rustwx-cli --release --bin weather_native_bench -- --date 20260414 --cycle 23 --forecast-hour 0 --region southern-plains --product stp_fixed,sbcape,srh_0_1km --rust-runs 5 --python-runs 3 --out-dir proof
```

```powershell
cargo run -p rustwx-cli --bin hrrr_derived_batch -- --recipe stp_fixed,sbcape,mlcape,srh_0_1km,ehi_0_1km
```

```powershell
cargo run -p rustwx-cli --bin hrrr_direct_batch -- --recipe mslp_10m_winds,500mb_temperature_height_winds,composite_reflectivity
```

```powershell
cargo run -p rustwx-cli --bin product_catalog -- --out C:\Users\drew\rustwx\proof\product_catalog.json
```

```powershell
cargo run -p rustwx-cli --bin proof_gallery -- --proof-root C:\Users\drew\rustwx\proof --out-dir C:\Users\drew\rustwx\proof\viewer
```
