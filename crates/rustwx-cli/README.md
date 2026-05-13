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
- `sounding_plot`
  - Standalone native Rust model sounding renderer.
  - Extracts a model column at lat/lon and writes a SHARPpy-style PNG through `rustwx-sounding`.
- `weather_native_bench`
  - Release-mode benchmark/profiling runner for the native contour map lane.
  - Compares Rust native contour render timings against forced legacy raster renders and Python `matplotlib/cartopy` equivalents on the same cached HRRR fields.
  - Current default benchmark set is `stp_fixed`, `sbcape`, and `srh_0_1km`; writes PNGs plus summary JSON/Markdown under the selected output directory.
- `hrrr_derived_batch` / `derived_batch`
  - Best lane for iterating derived weather-native maps.
  - Native projected contour-filled derived products are currently `stp_fixed`, `sbcape`, `mlcape`, `srh_0_1km`, `srh_0_3km`, `ehi_0_1km`, `ehi_0_3km`, `tehi`, `tts`, and `vtp_mod`.
- `hrrr_direct_batch` / `direct_batch`
  - Direct field proof lane.
  - Useful for contour-sensitive projected products such as `mslp_10m_winds` that still use the standard `rustwx-render` contour/overlay path.
- `surface_mesoanalysis`
  - Native Rust, model-agnostic surface objective-analysis lane.
  - Loads the requested model's surface fields, reads rustwx-runner `latest_observations.json` artifacts, filters observations through an explicit profile such as `surface_meso_conus`, QC's observation increments, and writes surface analysis JSON reports for 2 m temperature, 2 m dew point/q2, and 10 m winds.
  - The run report includes `model_load_mode = surface_only`, the selected model, the observation profile, and surface fetch/decode timing so agents can audit exactly what was used.
  - `--case-tag` attaches calibration labels such as `regime=dryline`, `hazard=severe`, or `domain=ok_plains` to the run report and compact agent packet, giving later matrices a machine-readable regime axis.
  - Report validation samples each accepted observation against the nearest model grid cell and compares background vs analyzed absolute error for temperature, dew point, and wind speed.
  - Validation summaries include per-station confidence, low/medium/high confidence buckets, ranked low/high confidence terciles, high-minus-low MAE when buckets are represented, and confidence-vs-absolute-error correlation so confidence grids can be calibrated rather than merely plotted.
  - Validation also emits `strata_summaries` for source-quality class, representativeness class, correction role, terrain-pressure class, and observation-age bucket. These are the machine-readable hooks for finding regime/representativeness failures instead of trusting one whole-domain score.
  - Each temperature/dewpoint/wind validation summary also emits `confidence.reliability` with schema `rustwx.surface_mesoanalysis.confidence_reliability.v1`. The semantic label is `calibrated_reliability` only when ranked high-confidence holdouts have MAE no worse than ranked low-confidence holdouts with sufficient bucket coverage; otherwise agents should treat grid confidence as `uncalibrated_support` or `support_index` metadata.
  - By default writes a WxStore-compatible grid export manifest under `out_dir/wxstore_grid_export` with corrected fields, increments, confidence fields, and neighbor counts; use `--no-grid-export` to skip this artifact or `--grid-export-dir` to choose another output directory.
  - Includes an operational validation gate with thresholds for sampled station count, skipped validation count, nearest-grid distance, and temperature/dewpoint/wind-speed MAE; use `--fail-on-validation-gate` to make the command fail when the gate fails.
  - Defaults to two-pass Barnes analysis with bounded final increments; tune with `--barnes-passes` and `--second-pass-gamma`.
  - Defaults to model-valid-time observation filtering with `--max-obs-age-minutes 90`; use `--obs-reference-time` to override the reference time or `--no-time-filter` only for mechanical/cache smokes. Accepted observations are also time-weighted by age before correction, with `--obs-time-weight-half-life-minutes` and `--obs-max-time-error-inflation-factor` controlling quality-weight decay and observation-error inflation.
  - Runner observations are de-duplicated after source/profile/time filtering so the same station from multiple feeds does not receive multiple independent OI votes. Source summaries report `duplicate_filtered_count`.
  - Optional `--external-reference-model rtma` or `--external-reference-model rtma,urma` loads the matching valid-time NOAA surface-analysis grid and writes `external_reference_comparisons` that score raw background, RustWX OI, and the reference against the same validation observations.
  - Optional `--compare-isotropic-oi-baseline` runs a no-flow/no-terrain OI ablation (`oi_flow_anisotropy_ratio = 1`, effectively disabled terrain-pressure damping) and writes `covariance_ablation_comparison` so agents can see whether the professional covariance terms helped the case.
  - OI gross-error rejection includes nearby buddy-observation rescue for supported mesoscale extremes. Tune with `--oi-gross-error-buddy-radius-km`, `--oi-gross-error-buddy-min-neighbors`, and `--oi-gross-error-buddy-agreement-sigma`; variable diagnostics report `gross_error_rescued_observations`.
  - `hrrr_mesoanalysis` remains as a legacy binary alias for old scripts, but it should not be the primary agent-facing name.
- `surface_mesoanalysis_calibration`
  - Aggregates one or more `surface_mesoanalysis` `run_report.json` files into `rustwx.surface_mesoanalysis.calibration_matrix.v1`.
  - Prefers repeated holdout benchmark summaries, falls back to holdout and then same-observation summaries, and emits quality flags when validation modes are mixed or reports are skipped.
  - Reports domain-wide and per-source aggregate deltas for OI vs raw background and OI vs Barnes so tuning sweeps can be judged across cases and providers instead of by a single smoke.
  - Aggregates validation strata under `aggregate.strata` so tuning sweeps can be audited by source quality, exposure/representativeness, terrain-pressure class, and age bucket.
  - Strata can be gated directly with `--gate-stratum` plus `--gate-max-stratum-oi-minus-raw-mae`, `--gate-max-stratum-oi-minus-barnes-mae`, and stratum confidence-reliability thresholds. This lets CI fail on a weak terrain/source/age bucket even when the domain mean still looks acceptable.
  - Aggregates compact station-level innovation summaries under `aggregate.stations`, keyed by source and station ID, so persistent station bias or isolated bad validation behavior can be found without scanning raw validation samples.
  - Station gates can target one or more `source::station_id` keys with `--gate-station`, `--gate-min-station-observation-count`, `--gate-max-station-oi-minus-raw-mae`, `--gate-max-station-analysis-mae`, and `--gate-max-station-abs-analysis-bias`.
  - Optional `--innovation-history-out` writes `rustwx.surface_mesoanalysis.innovation_history.v1`, a compact station/source time-series sidecar with per-case entries and aggregate rollups for station/provider monitoring.
  - Add `--innovation-history-in` to merge an existing sidecar before writing and `--innovation-history-max-entries-per-series` to retain only the newest N entries for each station/source series.
  - Innovation histories include compact `station_watchlist` and `source_watchlist` arrays that rank persistent station/source problems by field, error, bias, background degradation, and tail error so agents can inspect the worst offenders first.
  - Query-only mode reads an existing history with `--innovation-query-history` and writes `rustwx.surface_mesoanalysis.innovation_query.v1` via `--innovation-query-out`, with optional station/source/variable/min-case/top-N filters.
  - `--innovation-wxstore-index-dir` writes a WxStore-shaped innovation index directory with `manifest.json`, `station_index.jsonl`, `source_index.jsonl`, and watchlist JSON files for durable station/source lookup.
  - Aggregates run diagnostics such as candidate/accepted/rejected observations, gross-error buddy rescues, solver failures, and truncated OI neighborhoods by analyzed variable.
  - Can aggregate optional `external_reference_comparison` blocks, e.g. `rtma` or `urma`, and gate OI-minus-reference MAE when NOAA analysis reference validation is available.
  - Can aggregate optional `covariance_ablation_comparison` blocks and gate terrain/flow OI against the isotropic/no-terrain OI baseline.
  - Aggregates domain and per-source confidence reliability, including bucket coverage, ranked confidence-tercile skill, high-minus-low confidence MAE, confidence-vs-error correlation, and the stable `calibrated_reliability` / `uncalibrated_support` / `support_index` semantic label used by the agent packet.
  - Tracks case breadth with model/source/date/cycle/forecast-hour/signature counts plus `case_tag_counts`, and can require unique case signatures, cycles, dates, forecast hours, holdout strategies, required case tags, and minimum unique case-tag counts.
  - Can attach and enforce `rustwx.surface_mesoanalysis.calibration_gate.v1` with domain/source/station/reference/ablation MAE thresholds, confidence-reliability thresholds, breadth checks, and per-case compute-time limits for CI or tuning sweeps.
- `product_catalog` and `proof_gallery`
  - Small inspection helpers for generated validation output.

## Current limits

- The main CLI and the proof binaries are still separate surfaces.
- Native projected contour-fill is live for the derived products above, not yet for every direct/synoptic contour product.
- The real-data cross-section proof lane now covers a small multi-product family, but it is still pressure-axis only and not yet the full `wxsection_ref` product inventory.

## Minimal examples

```powershell
cargo run -p rustwx-cli -- list
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_native_proof -- --date 20260414 --cycle 23 --forecast-hour 0 --out-dir target/artifacts/hrrr_native
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_native_proof -- --case conus-contour,southern-plains-severe --date 20260414 --cycle 23 --forecast-hour 0 --out-dir target/artifacts/hrrr_native
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_native_proof -- --mode custom --date 20260414 --cycle 23 --forecast-hour 0 --region southern-plains --direct-recipe 500mb_temperature_height_winds --derived-recipe stp_fixed,sbcape --out-dir target/artifacts/hrrr_native
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_temperature_xsection -- --date 20260414 --cycle 23 --forecast-hour 0 --out-dir target/artifacts/hrrr_temperature_xsection
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_temperature_xsection -- --product wind-speed --date 20260414 --cycle 23 --forecast-hour 0 --out-dir target/artifacts/hrrr_temperature_xsection
```

```powershell
cargo run -p rustwx-cli --release --bin sounding_plot -- --model hrrr --date 20260424 --cycle 22 --forecast-hour 1 --source aws --lat 35.222 --lon -97.439 --station-id "Norman OK" --out-dir target/artifacts/soundings
```

Use `--sample-method box-mean --box-radius-km 25` for a box-averaged model
sounding around the target point instead of a single nearest grid point.

```powershell
cargo run -p rustwx-cli --release --bin weather_native_bench -- --date 20260414 --cycle 23 --forecast-hour 0 --region southern-plains --product stp_fixed,sbcape,srh_0_1km --rust-runs 5 --python-runs 3 --out-dir target/artifacts/weather_native_bench
```

```powershell
cargo run -p rustwx-cli --bin hrrr_derived_batch -- --recipe stp_fixed,sbcape,mlcape,srh_0_1km,ehi_0_1km
```

```powershell
cargo run -p rustwx-cli --bin hrrr_direct_batch -- --recipe mslp_10m_winds,500mb_temperature_height_winds,composite_reflectivity
```

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 0 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --analysis-method oi --compare-barnes-baseline --holdout-strategy spatial_block --holdout-repeat-count 3
```

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 0 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --analysis-method oi --holdout-strategy spatial_block --external-reference-model rtma --no-grid-export --out-dir target\surface_mesoanalysis_rtma_reference_smoke
```

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 2 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --analysis-method oi --holdout-strategy spatial_block --compare-isotropic-oi-baseline --no-grid-export --out-dir target\surface_mesoanalysis_covariance_ablation_smoke_03z
```

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --reports-root target --out target\surface_mesoanalysis_calibration\calibration_matrix.json --min-case-count 1
```

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_oi_spatial_holdout_smoke\run_report.json --run-report target\surface_mesoanalysis_oi_source_holdout_smoke\run_report.json --out target\surface_mesoanalysis_calibration\repeated_gate_matrix.json --min-case-count 2 --fail-on-calibration-gate --require-benchmark-mode repeated_holdout_validation --gate-max-domain-oi-minus-raw-mae 0.0 --gate-max-domain-oi-minus-barnes-mae 0.0 --gate-max-source-oi-minus-raw-mae 0.75 --gate-max-source-oi-minus-barnes-mae 0.75
```

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_spatial_index_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\spatial_index_speed_gate_matrix.json --min-case-count 1 --require-covariance-ablation IsotropicOiNoTerrain --gate-variable temperature_c,dewpoint_c,wind_speed_ms --gate-max-covariance-ablation-oi-minus-baseline-mae 0.0 --gate-max-case-mesoanalysis-compute-ms 2000
```

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_rtma_reference_smoke\run_report.json --run-report target\surface_mesoanalysis_spatial_index_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\diverse_two_cycle_gate_matrix.json --min-case-count 2 --require-holdout-strategy spatial_block --gate-min-unique-case-signatures 2 --gate-min-unique-cycles 2 --gate-min-unique-dates 1 --gate-min-unique-forecast-hours 1 --gate-max-case-mesoanalysis-compute-ms 2000
```

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_confidence_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\confidence_bucket_coverage_gate_matrix.json --min-case-count 1 --require-holdout-strategy spatial_block --require-covariance-ablation IsotropicOiNoTerrain --gate-variable temperature_c,dewpoint_c --gate-min-domain-low-confidence-observation-count 1 --gate-min-domain-high-confidence-observation-count 1 --gate-max-domain-high-minus-low-confidence-mae 0.0 --gate-max-domain-confidence-abs-error-correlation 0.0 --gate-max-case-mesoanalysis-compute-ms 2000
```

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_confidence_ranked_station_hash_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\confidence_ranked_station_hash_gate_matrix.json --min-case-count 1 --require-holdout-strategy station_hash --require-covariance-ablation IsotropicOiNoTerrain --gate-variable temperature_c,dewpoint_c,wind_speed_ms --gate-max-domain-ranked-high-minus-low-confidence-mae 0.0 --gate-max-case-mesoanalysis-compute-ms 2000
```

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_case_tag_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\station_gate_pass_matrix.json --innovation-history-out target\surface_mesoanalysis_calibration\innovation_history_smoke.json --min-case-count 1 --require-case-tag regime=dryline --gate-min-unique-case-tags 3 --gate-variable temperature_c --gate-station aviation_weather_metar_conus::KP69 --gate-min-station-observation-count 1 --gate-max-station-analysis-mae 6.0 --gate-max-station-abs-analysis-bias 6.0 --fail-on-calibration-gate
```

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_case_tag_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\station_gate_rolling_matrix.json --innovation-history-in target\surface_mesoanalysis_calibration\innovation_history_smoke.json --innovation-history-out target\surface_mesoanalysis_calibration\innovation_history_rolling_smoke.json --innovation-history-max-entries-per-series 365 --min-case-count 1 --require-case-tag regime=dryline --gate-min-unique-case-tags 3 --gate-variable temperature_c --gate-station aviation_weather_metar_conus::KP69 --gate-min-station-observation-count 1 --gate-max-station-analysis-mae 6.0 --gate-max-station-abs-analysis-bias 6.0 --fail-on-calibration-gate
```

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --innovation-query-history target\surface_mesoanalysis_calibration\innovation_history_rolling_smoke.json --innovation-query-station aviation_weather_metar_conus::KP69 --innovation-query-variable temperature_c --innovation-query-top 3 --innovation-query-out target\surface_mesoanalysis_calibration\innovation_query_kp69_temperature.json --innovation-wxstore-index-dir target\surface_mesoanalysis_calibration\innovation_wxstore_index_smoke
```

```powershell
cargo run -p rustwx-cli --bin product_catalog -- --out target/artifacts/product_catalog.json
```

```powershell
cargo run -p rustwx-cli --bin proof_gallery -- --proof-root target/artifacts --out-dir target/artifacts/viewer
```
