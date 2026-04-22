# Proof artifacts

This directory holds end-to-end proof outputs for the current engine direction. It also contains older reruns and working artifacts, so start with the curated lanes below instead of treating every file in `proof/` as a current gold master.

## Current lanes

- `hrrr_native_proof` is the bounded HRRR weather-native map proof runner.
  - Default suite currently runs only `conus_contour`.
  - Optional suite cases are `midwest_core` and `southern_plains_severe`.
  - It also runs the bounded HRRR cross-section proof lane and writes a suite summary plus a hook-style cross-section JSON.
- `hrrr_temperature_xsection` is the standalone real-data HRRR temperature cross-section proof runner.
  - Despite the legacy binary name, it now supports multiple pressure-section products with `--product temperature|relative-humidity|theta-e|wind-speed`.
- `weather_native_bench` is the bounded native-contour benchmark/profiling runner.
  - It compares Rust native contour renders, forced legacy raster renders, and Python `matplotlib/cartopy` equivalents on the same cached HRRR fields.
  - Current default benchmark set is `stp_fixed`, `sbcape`, and `srh_0_1km`.
  - It writes a summary JSON/Markdown plus the Rust and Python comparison PNGs under `proof/bench/`.

## What is implemented

- Projected native contour-filled derived products are live for `stp_fixed`, `sbcape`, `mlcape`, `srh_0_1km`, `srh_0_3km`, `ehi_0_1km`, and `ehi_0_3km`.
- The committed native proof lane currently pins `stp_fixed`. `mslp_10m_winds` stays in the suite as a contour-sensitive projected comparison case on the standard render path.
- The cross-section crate now has a palette/style catalog informed by `wxsection_ref`, and the real-data HRRR proof lane now exercises `temperature`, `relative_humidity`, `theta_e`, and `wind_speed`.
- The old `cross-section hook placeholder` wording is no longer quite right: the hook JSON now points at a real proof lane, but it is still a hook-style summary rather than an inline map artifact.

## Inspect first

- `proof/rustwx_hrrr_20260414_23z_f000_suite_native_proof_summary.json`
- `proof/rustwx_hrrr_20260414_23z_f000_suite_native_proof_cross_section_hook.json`
- `proof/conus/rustwx_hrrr_20260414_23z_f000_conus_mslp_10m_winds.png`
- `proof/conus/rustwx_hrrr_20260414_23z_f000_conus_500mb_temperature_height_winds.png`
- `proof/conus/rustwx_hrrr_20260414_23z_f000_conus_sbcape.png`
- `proof/conus/rustwx_hrrr_20260414_23z_f000_conus_stp_fixed.png`
- `proof/rustwx_hrrr_20260414_23z_f000_amarillo_chicago_temperature_cross_section.png`
- `proof/rustwx_hrrr_20260414_23z_f000_amarillo_chicago_temperature_cross_section.json`
- `proof/rustwx_hrrr_20260414_23z_f000_amarillo_chicago_rh_cross_section.png`
- `proof/rustwx_hrrr_20260414_23z_f000_amarillo_chicago_theta_e_cross_section.png`
- `proof/rustwx_hrrr_20260414_23z_f000_amarillo_chicago_wind_speed_cross_section.png`
- `proof/bench/rustwx_hrrr_20260414_23z_f000_southern_plains_weather_native_benchmark_summary.md`
- `proof/bench/stp_fixed_rust_native.png`
- `proof/bench/stp_fixed_rust_legacy.png`
- `proof/bench/stp_fixed_python_matplotlib.png`
- If you want a widened rerun that is already checked in, inspect `proof/southern_plains/`.

## Generate and inspect

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
cargo run -p rustwx-cli --release --bin hrrr_temperature_xsection -- --product theta-e --date 20260414 --cycle 23 --forecast-hour 0 --out-dir proof
```

```powershell
cargo run -p rustwx-cli --release --bin weather_native_bench -- --date 20260414 --cycle 23 --forecast-hour 0 --region southern-plains --product stp_fixed,sbcape,srh_0_1km --rust-runs 5 --python-runs 3 --out-dir proof
```

```powershell
Get-Content proof/rustwx_hrrr_20260414_23z_f000_suite_native_proof_summary.json
Get-Content proof/rustwx_hrrr_20260414_23z_f000_suite_native_proof_cross_section_hook.json
Get-Content proof/bench/rustwx_hrrr_20260414_23z_f000_southern_plains_weather_native_benchmark_summary.md
```
