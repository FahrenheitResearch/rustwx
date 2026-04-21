# RESULT

## What changed

- Replaced `crates/rustwx-cli/src/bin/hrrr_ecape8.rs` with a thin wrapper around `rustwx_products::ecape::run_ecape_batch`.
- Deprecated legacy `--write-proof-artifacts` in `hrrr_ecape8`; it now errors clearly instead of keeping the uncropped compute path alive.
- Added a source-level regression test so `hrrr_ecape8.rs` cannot directly call `compute_ecape8_panel_fields` or rebuild the legacy ECAPE assembly path.
- Added shared heavy-domain guardrails in `crates/rustwx-products/src/heavy.rs` and wired `--allow-large-heavy-domain` / `--allow-conus-heavy` into `ecape8_batch`, `severe_batch`, `hrrr_ecape8`, `hrrr_severe_proof`, and `hrrr_batch`.
- Added heavy timing/report fields for ECAPE and severe: `full_cells`, `cropped_cells`, `pressure_levels`, `crop_kind`, `crop_ms`, `prepare_height_agl_ms`, `broadcast_pressure_ms`, `pressure_3d_bytes`, `ecape_triplet_ms`, `severe_fields_ms`, `render_ms`, `total_ms`.
- Changed severe calc to use 1D pressure levels instead of broadcasting a full 3D pressure volume. Verified in runtime reports: `broadcast_pressure_ms=0` and `pressure_3d_bytes=0`.
- Added unified product-layer heavy runner `run_heavy_panel_hour` plus CLI `heavy_panel_hour`.
- Added generic CLI `non_ecape_hour` and made both non-ECAPE hour runners default to a curated non-heavy recipe bundle when no explicit recipes are supplied.
- Updated README examples to use `ecape8_batch --model hrrr` instead of `hrrr_ecape8 --region conus`.
- Removed legacy severe/ECAPE credit/footer strings from shared rendering and added a regression test that fails if they reappear under `crates/`.

## HRRR ECAPE wrapper before/after

Warm-cache comparison for `2026-04-14 23Z F001 midwest`:

| wrapper | total_ms | prepare_ms | compute_ms | project_ms | render_ms | notes |
|---|---:|---:|---:|---:|---:|---|
| old `hrrr_ecape8` | 151219 | 19366 | 130059 | 288 | 1494 | legacy full-grid proof path |
| new `hrrr_ecape8` | 93161 | n/a | 73281 | 749 | 1068 | shared cropped batch path |

New wrapper heavy sub-breakdown:

- `fetch_ms`: 1734
- `crop_ms`: 360
- `prepare_height_agl_ms`: 129
- `ecape_triplet_ms`: 71835
- `render_ms`: 1068
- `cropped_cells`: `1111544`
- `pressure_levels`: `40`
- `crop_kind`: `crop`

Measured improvement: `151219 ms -> 93161 ms` (`-58058 ms`, about `-38.4%`).

## Regional heavy timings

- HRRR ECAPE (`midwest`, `2026-04-14 23Z F001`): total `78216 ms`; fetch `1569 ms`; project `605 ms`; crop `280 ms`; height AGL `104 ms`; ECAPE triplet `59889 ms`; render `1027 ms`; cells `1111544 x 40`; `crop_kind=crop`; `input_fetches=2`.
- HRRR severe (`midwest`, `2026-04-14 23Z F001`): total `21562 ms`; fetch `1563 ms`; project `612 ms`; crop `278 ms`; height AGL `100 ms`; severe fields `3215 ms`; render `1061 ms`; cells `1111544 x 40`; `crop_kind=crop`; `input_fetches=2`.
- GFS ECAPE (`midwest`, `2026-04-14 18Z F012`): total `21393 ms`; fetch `4472 ms`; project `314 ms`; crop `9 ms`; height AGL `2 ms`; ECAPE triplet `6214 ms`; render `683 ms`; cells `23650 x 41`; `crop_kind=crop`; `input_fetches=1`.
- GFS severe (`midwest`, `2026-04-14 18Z F012`): total `12492 ms`; fetch `3114 ms`; project `304 ms`; crop `10 ms`; height AGL `3 ms`; severe fields `89 ms`; render `693 ms`; cells `23650 x 41`; `crop_kind=crop`; `input_fetches=1`.
- ECMWF ECAPE (`midwest`, `2026-04-14 12Z F006`): total `7160 ms`; fetch `1080 ms`; project `361 ms`; crop `6 ms`; height AGL `1 ms`; ECAPE triplet `1224 ms`; render `724 ms`; cells `23650 x 13`; `crop_kind=crop`; `input_fetches=1`.
- ECMWF severe (`midwest`, `2026-04-14 12Z F006`): total `4690 ms`; fetch `718 ms`; project `306 ms`; crop `6 ms`; height AGL `1 ms`; severe fields `53 ms`; render `724 ms`; cells `23650 x 13`; `crop_kind=crop`; `input_fetches=1`.

Shared-file verification:

- GFS regional heavy runs used exactly one physical input fetch: `gfs.t18z.pgrb2.0p25.f012`.
- ECMWF regional heavy runs used exactly one physical input fetch: `20260414120000-6h-oper-fc.grib2`.
- HRRR still legitimately uses two physical inputs (`wrfsfc` + `wrfprs`).

## What the timings prove

- The remaining expensive HRRR heavy path is ECAPE parcel compute, not rendering.
- Regional heavy runs are now actually cropped: every verified heavy regional run reported `crop_kind=crop`.
- Severe no longer pays for 3D pressure broadcast on the verified runs: `broadcast_pressure_ms=0`, `pressure_3d_bytes=0`.
- GFS/ECMWF severe math is cheap at regional scale; fetch/shared overhead dominates the instrumented phases there.
- Render is not the bottleneck on any of the verified heavy runs.

## Acceptance runs

- `cargo fmt`: pass
- `cargo test`: pass
- `cargo run -p rustwx-cli --release --bin product_catalog`: pass
- `cargo run -p rustwx-cli --release --bin hrrr_non_ecape_hour -- --date 20260414 --cycle 23 --forecast-hour 1 --region conus`: pass
- `cargo run -p rustwx-cli --release --bin ecape8_batch -- --model hrrr --date 20260414 --cycle 23 --forecast-hour 1 --source nomads --region midwest`: pass
- `cargo run -p rustwx-cli --release --bin severe_batch -- --model hrrr --date 20260414 --cycle 23 --forecast-hour 1 --source nomads --region midwest`: pass
- `cargo run -p rustwx-cli --release --bin ecape8_batch -- --model gfs --date 20260414 --cycle 18 --forecast-hour 12 --region midwest`: pass
- `cargo run -p rustwx-cli --release --bin severe_batch -- --model gfs --date 20260414 --cycle 18 --forecast-hour 12 --region midwest`: pass
- ECMWF smoke runs on `2026-04-14 12Z F006` for both ECAPE and severe: pass
- `cargo run -p rustwx-cli --release --bin heavy_panel_hour -- --model hrrr --date 20260414 --cycle 23 --forecast-hour 1 --source nomads --region midwest`: pass

## Remaining blockers / caveats

- The reports now expose the heavy sub-phases the pass asked for, but `total_ms` still includes time outside those split heavy buckets. The data now makes that visible; it does not yet break every remaining intermediate helper into its own line item.
- Default heavy-domain guardrail is set to `1_500_000` cropped cells (`RUSTWX_MAX_HEAVY_CELLS` override available). That keeps Midwest-scale regional heavy runs working while still forcing explicit opt-in for larger heavy domains.
- Vendor crates still emit build warnings; unchanged in this pass.
