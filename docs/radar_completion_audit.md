# rustwx Radar Completion Audit

Status date: 2026-05-11

This audit maps the active goal to concrete evidence and open work. It is intentionally conservative: the goal is not complete while any "world best-looking and fastest" claim lacks external comparison, broad fixture coverage, or repeatable CI evidence.

## Objective Restated

Build first-party, all-Rust radar capabilities that can support an AI-agent-first GR2Analyst-style application:

- Correctly decode NEXRAD Level-II radar data.
- Render reflectivity, velocity, spectrum width, dual-pol products, and all tilts as transparent web tiles.
- Produce visually excellent radar PNGs and tiles at native scale without fake overzoom detail.
- Support velocity dealiasing with safe metadata and rejection paths.
- Process radar fast enough for operational runner publishing and interactive wxstore use.
- Ground quality decisions in radar research and open-science baselines.
- Keep outputs inspectable as PNGs while work proceeds.

## Prompt-to-Artifact Checklist

| Requirement | Current artifact/evidence | Status |
| --- | --- | --- |
| All-Rust radar crate, not vendored-only radar logic | `crates/rustwx-radar/src/dealias.rs`, `crates/rustwx-radar/src/tile.rs`, and `crates/rustwx-cli/src/bin/radar_web_tiles.rs` own decode-adjacent QC, dealiasing, PNG tile rendering, and CLI export | In progress, strong slice |
| Correct Level-II Message 31 decode basics | `crates/rustwx-radar/src/nexrad/products.rs` normalizes velocity/spectrum-width units; `crates/rustwx-radar/src/nexrad/level2.rs` decodes RRAD Nyquist from the signed field | Implemented, needs more fixtures |
| Native-scale rendering instead of misleading overzoom | Tile manifests include `native_gate_size_m`, `native_azimuth_spacing_deg`, and `maxzoom_site_meters_per_pixel`; KSJT z9 native-scale evidence is recorded in `docs/radar_sota_roadmap.md` | Implemented for manifests/viewer |
| Product provenance metadata | Tile manifests include `product_provenance`; runner frame/tilt indexes preserve it, and wxstore serves it in radar frame JSON. `radar_quality_gate` can require provenance fields, and the KDP smoke requires `source=derived`, `inputs=["phi"]`, and `method=centered_phi_range_derivative`. | Implemented and gate-enforced for derived KDP |
| Multiple tilts | `radar_web_tiles --all-tilts`, runner all-tilt publish records, and wxstore tilt selector evidence in `C:/Users/drew/wxstore/src/main.rs` | Implemented, needs all-site smoke |
| Velocity support | `RadarProduct::Velocity`, Message 31 unit fixes, velocity QC, and stable KTLX/KSJT PNG artifacts | Implemented, quality still expanding |
| Dealiasing | `DealiasMethod::{Radial,Sweep,Staged}`, QC reports, low-alias skip, forced-debug rendering, high-shear acceptance for Moore 2013, staged-network acceptance, and staged extreme-jump cleanup across KSJT hard low/mid tilts | Implemented for current fixtures; broader validation remains |
| Velocity quality filtering | `--velocity-quality-filter` masks gates failing reflectivity/spectrum-width checks and records `velocity_quality_qc` in tile manifests | Implemented as explicit render option; paired with staged cleanup for hard KSJT cases |
| Reflectivity cleanup | Opt-in polar despeckle with manifest QC and quality gate caps | Implemented conservative pass; clutter QC incomplete |
| Dual-pol product rendering | Correlation coefficient (`cc`/`rho`), differential reflectivity (`zdr`), differential phase (`phi`), and PHI-derived specific differential phase (`kdp`) render from the KSJT real fixture with product-QC and speed gates. | First CC/ZDR/PHI/KDP product smokes implemented; deeper science QC and HCA/raw-KDP products remain |
| Fast tile generation | `radar_benchmark_gate` plus release benchmark artifacts under `radar_outputs/*benchmark*.json`; `.github/workflows/rustwx-ci.yml` tests/builds radar gate and tile binaries; `.github/workflows/radar-smoke.yml` renders pinned public KTLX Moore 2013 velocity, KSJT 2026 hard all-tilt velocity, KSJT 2026 despeckled reflectivity, and KSJT 2026 CC/ZDR/PHI/KDP dual-pol fixtures and runs quality/benchmark gates | Implemented for scheduled/manual real-fixture velocity, reflectivity, and first dual-pol smokes; broader fixture matrix still needed |
| Repeatable dealias method comparison | `crates/rustwx-cli/src/bin/radar_dealias_compare.rs` writes JSON reports comparing `raw`, `radial`, `sweep`, and `staged` on the same sweep, with explicit `--require-method-pass`, `--expect-best-method`, `--max-method-ms`, and `--max-total-method-ms` regression checks; CI now runs its unit tests | Implemented for current fixtures |
| Runner integration | `C:/Users/drew/rustwx-runner/src/main.rs` has `radar-run-once`, all-site expansion, quality-gate invocation, despeckle/dealias/velocity-filter args, native/QC metadata carry-through, and a live KSJT staged/QC quality-gated publish at `20260511T051237Z` | Implemented, needs long-running operational soak |
| wxstore integration | `C:/Users/drew/wxstore/src/main.rs` serves radar frames/tilts, has a tilt selector, preserves native/QC metadata in JSON, and shows native/velocity-filter hints in the radar legend | Implemented, browser smoke passed locally |
| PNGs shown while working | Stable copies and contact sheets under `radar_outputs/stable_gallery/` | Implemented for current snapshots |
| Research-grounded SOTA direction | `docs/radar_sota_roadmap.md` links NOAA ICD, Py-ART, wradlib/Gabella, UNRAVEL, and ORPG-derived deep-learning dealiasing references; `scripts/radar_pyart_baseline.py` now generates optional Py-ART comparison JSON and resolves reflectivity/velocity split sweeps | Documented; first KTLX and KSJT Py-ART baselines exist, more comparison needed |

## Evidence Commands

Useful focused checks already exercised during this pass:

- `cargo test -p rustwx-radar --lib prepared_sweep_reports_native_resolution_metadata`
- `cargo test -p rustwx-radar --lib reflectivity_despeckle_removes_isolated_gate_only`
- `cargo test -p rustwx-radar --lib dealias`
- `cargo test -p rustwx-radar --lib staged`
- `cargo test radar_dealias_method_accepts_staged` in `C:/Users/drew/rustwx-runner`
- `radar_quality_gate` against staged KTLX Moore/current KTLX manifests.
- `cargo test -p rustwx-cli --bin radar_dealias_compare`
- `cargo build --release -p rustwx-cli --bin radar_web_tiles --bin radar_quality_gate --bin radar_dealias_compare`
- `.github/workflows/rustwx-ci.yml` now includes `cargo test -p rustwx-radar --lib`, radar CLI gate tests, and a release build for `radar_web_tiles`, `radar_quality_gate`, `radar_benchmark_gate`, and `radar_dealias_compare`.
- `.github/workflows/radar-smoke.yml` now runs two real-fixture smoke jobs on manual or weekly scheduled runs:
  - KTLX Moore 2013 velocity: downloads `https://unidata-nexrad-level2.s3.amazonaws.com/2013/05/20/KTLX/KTLX20130520_201643_V06.gz`, unpacks it, renders bounded staged velocity tiles, gates velocity QC, gates render speed, and uploads PNG/JSON artifacts.
  - KSJT hard all-tilt velocity, reflectivity, CC, ZDR, PHI, and derived KDP: downloads `https://unidata-nexrad-level2.s3.amazonaws.com/2026/05/11/KSJT/KSJT20260511_043240_V06`, renders bounded all-tilt staged velocity tiles, bounded GR2Analyst/despeckled reflectivity tiles, and bounded CC/ZDR/PHI/KDP dual-pol tiles, gates every velocity tilt's QC, gates reflectivity removal fraction, gates dual-pol finite-gate/value range QC, gates render speed, and uploads PNG/JSON artifacts.
- `radar_dealias_compare` fixture reports:
  - `radar_outputs/dealias_compare_ktlx_moore_sweep1_01.json`
  - `radar_outputs/dealias_compare_ktlx_current_sweep0_01.json`
  - `radar_outputs/dealias_compare_ksjt_sweep0_01.json`
  - `radar_outputs/dealias_compare_ksjt_043240_all_sweeps_network_accepted_01.json`
  - `radar_outputs/dealias_compare_ktlx_moore_sweep1_network_accepted_01.json`
  - `radar_outputs/dealias_compare_ktlx_current_sweep0_network_accepted_01.json`
  - `radar_outputs/dealias_compare_ksjt_043240_all_sweeps_release_network_opt_01.json`
  - `radar_outputs/dealias_compare_ktlx_moore_sweep1_release_network_opt_01.json`
  - `radar_outputs/dealias_compare_ktlx_current_sweep0_release_network_opt_01.json`
  - `radar_outputs/dealias_compare_ksjt_050734_all_sweeps_cleanup_release_01.json`
  - `radar_outputs/dealias_compare_ksjt_043240_require_staged_01.json`
  - `radar_outputs/dealias_compare_ksjt_050734_require_staged_01.json`
  - `radar_outputs/dealias_compare_ksjt_043240_require_staged_speed_01.json`
  - `radar_outputs/dealias_compare_ksjt_050734_require_staged_speed_01.json`
- KSJT velocity filter experiments:
  - `radar_outputs/ksjt_velocity_filter_01/raw_z8/tiles_manifest.json`
  - `radar_outputs/ksjt_partial_staged_filter_01/sweep_z8/tiles_manifest.json`
  - `radar_outputs/stable_gallery/radar_ksjt_velocity_filter_showcase_20260511.png`
- KSJT staged-network PNG evidence:
  - `radar_outputs/ksjt_dealias_network_01/raw_z6/tiles_manifest.json`
  - `radar_outputs/ksjt_dealias_network_01/staged_z6/tiles_manifest.json`
  - `radar_outputs/stable_gallery/radar_ksjt_staged_network_showcase_20260511.png`
- Runner propagation tests for `--velocity-quality-filter` and `velocity_quality_qc` tilt metadata:
  - `cargo test radar_tile_args_include_despeckle_and_dealias_controls`
- Live runner quality-gated publish:
  - `C:/Users/drew/rustwx-runner/data/radar_tiles/nexrad_level2_ksjt_vel_staged_qgate_smoke/frames/20260511T051237Z/all_tilts_manifest.json`
  - Same frame passed `radar_quality_gate` with max velocity fold fraction `0.005`, severe jumps `200`, and max jump `100 m/s`.
  - `cargo test radar_all_tilts_manifest_builds_tilt_records`
- WxStore propagation and viewer smoke:
  - `cargo test radar_frames_json_preserves_native_resolution_metadata`
  - `cargo check` in `C:/Users/drew/wxstore`
  - Browser smoke against a fresh current-code wxstore instance at `http://127.0.0.1:8898/radar`; the `nexrad_level2_ksjt_vel_filter_smoke` layer loaded its tilt selector and the legend showed `velocity QC 5.0% masked` for the selected low tilt.
- `cargo test` in `C:/Users/drew/rustwx-runner`
- `cargo test` and `cargo check` in `C:/Users/drew/wxstore`
- `radar_benchmark_gate` against KSJT benchmark manifests
- `radar_quality_gate` against KSJT/KTLX tile manifests
- Browser smoke against `http://127.0.0.1:8898/radar`
- Local verification of the CI radar additions:
  - `cargo test -p rustwx-radar --lib`
  - `cargo test -p rustwx-cli --bin radar_quality_gate`
  - `cargo test -p rustwx-cli --bin radar_benchmark_gate`
  - `cargo test -p rustwx-cli --bin radar_dealias_compare`
  - `cargo build --release -p rustwx-cli --bin radar_web_tiles --bin radar_quality_gate --bin radar_benchmark_gate --bin radar_dealias_compare`
- Local verification of the real-fixture radar smoke:
  - `radar_web_tiles` against local `KTLX20130520_201643_V06`, bounded z8, sweep 1, `--dealias-method staged`, `--velocity-quality-filter`, and `--benchmark-supersamples 1,2`.
  - `radar_outputs/ci_smoke_ktlx_moore_vel_01/supersample_benchmark.json`
  - `radar_outputs/ci_smoke_ktlx_moore_vel_01/quality_gate.json`
  - `radar_outputs/ci_smoke_ktlx_moore_vel_01/benchmark_gate.json`
  - The local smoke passed velocity QC with fold-like jump fraction `0.0003945`, severe jumps `23`, max jump `54.24 m/s`, and accepted staged dealiasing reducing severe jumps from `1087` to `23`.
  - `radar_web_tiles` against local `KSJT20260511_043240_V06`, bounded z6, all velocity tilts, `--dealias-method staged`, `--velocity-quality-filter`, and `--benchmark-supersamples 1,2`.
  - `radar_outputs/ci_smoke_ksjt_hard_vel_all_tilts_01/supersample_benchmark.json`
  - `radar_outputs/ci_smoke_ksjt_hard_vel_all_tilts_01/quality_gate.json`
  - `radar_outputs/ci_smoke_ksjt_hard_vel_all_tilts_01/benchmark_gate.json`
  - The local KSJT all-tilt smoke rendered `11` tilts and `26` PNG tiles per supersample run, passed velocity QC for every tilt, and stayed at `2485ms` for sample factor 1 and `2920ms` for sample factor 2.
  - `radar_web_tiles` against local `KSJT20260511_043240_V06`, bounded z7 reflectivity, GR2Analyst palette, `--reflectivity-despeckle`, and `--benchmark-supersamples 1,2`.
  - `radar_outputs/ci_smoke_ksjt_ref_despeckle_01/supersample_benchmark.json`
  - `radar_outputs/ci_smoke_ksjt_ref_despeckle_01/quality_gate.json`
  - `radar_outputs/ci_smoke_ksjt_ref_despeckle_01/benchmark_gate.json`
  - The local KSJT reflectivity smoke rendered `8` PNG tiles per supersample run, removed `4616` of `472545` finite gates (`0.00977`), and stayed at `25ms` for sample factor 1 and `32ms` for sample factor 2.
  - `radar_web_tiles` against local `KSJT20260511_043240_V06`, bounded z7 correlation coefficient, and `--benchmark-supersamples 1,2`.
  - `radar_outputs/ci_smoke_ksjt_cc_01/supersample_benchmark.json`
  - `radar_outputs/ci_smoke_ksjt_cc_01/product_quality_gate.json`
  - `radar_outputs/ci_smoke_ksjt_cc_01/benchmark_gate.json`
  - The local KSJT CC smoke rendered `9` PNG tiles per supersample run, reported `416519` finite CC gates with min `0.2083`, max `1.0517`, mean `0.8386`, passed finite-gate/value range QC, and stayed at `8ms` for sample factor 1 and `19ms` for sample factor 2.
  - `radar_web_tiles` against local `KSJT20260511_043240_V06`, bounded z7 differential reflectivity, and `--benchmark-supersamples 1,2`.
  - `radar_outputs/ci_smoke_ksjt_zdr_01/supersample_benchmark.json`
  - `radar_outputs/ci_smoke_ksjt_zdr_01/product_quality_gate.json`
  - `radar_outputs/ci_smoke_ksjt_zdr_01/benchmark_gate.json`
  - The local KSJT ZDR smoke rendered `9` PNG tiles per supersample run, reported `416519` finite gates with min `-13.0`, max `20.0`, mean `2.0384`, passed decode-range product QC, and stayed at `12ms` for sample factor 1 and `26ms` for sample factor 2.
  - `radar_web_tiles` against local `KSJT20260511_043240_V06`, bounded z7 differential phase, and `--benchmark-supersamples 1,2`.
  - `radar_outputs/ci_smoke_ksjt_phi_01/supersample_benchmark.json`
  - `radar_outputs/ci_smoke_ksjt_phi_01/product_quality_gate.json`
  - `radar_outputs/ci_smoke_ksjt_phi_01/benchmark_gate.json`
  - The local KSJT PHI smoke rendered `9` PNG tiles per supersample run, reported `416519` finite gates with min `0.0`, max `359.6488`, mean `83.1458`, passed decode-range product QC, and stayed at `10ms` for sample factor 1 and `24ms` for sample factor 2.
  - `radar_web_tiles` against local `KSJT20260511_043240_V06`, bounded z7 PHI-derived KDP, and `--benchmark-supersamples 1,2`.
  - `radar_outputs/ci_smoke_ksjt_kdp_01/supersample_benchmark.json`
  - `radar_outputs/ci_smoke_ksjt_kdp_01/product_quality_gate.json`
  - `radar_outputs/ci_smoke_ksjt_kdp_01/benchmark_gate.json`
  - The local KSJT derived-KDP smoke rendered `8` PNG tiles per supersample run, reported `380338` finite gates with min `-44.9561`, max `44.9561`, mean `0.1303`, passed decode-range product QC, and stayed at `11ms` for sample factor 1 and `21ms` for sample factor 2. This is a conservative PHI finite-window derivative, not a raw KDP moment in the fixture; its manifest now records `product_provenance.source=derived`, `inputs=["phi"]`, and `method=centered_phi_range_derivative`, and the quality gate enforces those fields.
- External Py-ART baseline scaffold:
  - `scripts/radar_pyart_baseline.py`
  - `radar_outputs/external_baselines/pyart_ktlx_moore_sweep1_velocity_01.json`
  - The KTLX Moore sweep 1 Py-ART region-based baseline reduced severe jumps from `1110` raw to `18`; the comparable rustwx staged artifact reduced severe jumps from `1087` raw to `23` with a lower max neighbor jump.
  - `radar_outputs/external_baselines/pyart_ksjt_043240_sweep2_velocity_01.json` exists, but the first KSJT attempt produced zero finite gates because Py-ART/xradar sweep/moment alignment does not match rustwx's selected velocity sweep directly. That artifact is a warning, not validation.
  - `radar_outputs/external_baselines/pyart_ksjt_043240_sweep2_resolved_velocity_01.json`
  - The resolved KSJT baseline detects that requested Py-ART sweep 2 is a 0.8789 degree dual-pol/reflectivity sweep with zero velocity gates, selects the matching velocity sweep 3 at the same fixed angle, and compares it to rustwx's 0.8789 degree velocity sweep. Raw severe jumps match exactly at `3058`; Py-ART region-based dealiasing reduces severe jumps to `60`, while rustwx staged reduces severe jumps to `55` in `459ms` and passes the gate.

The next completion pass should rerun the radar crate's focused tests, runner tests, wxstore tests, and both radar gates after any algorithmic change. The latest staged-cleanup change has rerun the radar dealias tests, `radar_dealias_compare`, release radar CLI build, speed-gated release comparisons on KSJT `04:32:40Z` and `05:07:34Z`, full runner/wxstore tests, and a live runner quality-gated KSJT publish.

Final milestone verification after derived KDP/provenance changes:

- `cargo test -p rustwx-radar --lib`: `45` passed.
- `cargo test -p rustwx-cli --bin radar_quality_gate`: `4` passed, including product-provenance gating.
- `cargo test` in `C:/Users/drew/rustwx-runner`: `40` passed.
- `cargo test` in `C:/Users/drew/wxstore`: `13` passed.
- Local KDP product gate passed with `--require-product-source derived --require-product-input phi --require-product-method centered_phi_range_derivative`.
- `.github/workflows/radar-smoke.yml` parses as YAML, and diff hygiene checks are clean except normal Windows LF-to-CRLF notices.

## Missing Before Calling This Goal Complete

- External fixture comparisons against Py-ART and wradlib/xradar output for a small pinned set of public Level-II volumes. KTLX and one resolved KSJT Py-ART comparison exist; wradlib/xradar and broader storm-mode fixtures remain.
- Optimize the stricter staged-network dealiasing path before promoting it as an always-on runner default; current release evidence clears the selected KSJT/KTLX fixtures, with speed-gated hard KSJT all-sweep comparisons staying under `650ms` per staged sweep and `3000ms` staged total, plus a live all-tilt runner publish around `2534ms` for the bounded z6 smoke.
- Reflectivity QC beyond isolated-gate despeckle: texture, echo-area, dual-pol, and/or temporal clutter suppression.
- Broader velocity validation across range folding, weak signal, noisy velocity, non-tornadic high shear, and tornadic/high-shear cases.
- Broader CI or scheduled runner smoke coverage beyond the current KTLX/KSJT velocity, reflectivity, CC, ZDR, PHI, and derived-KDP fixtures: add HCA and raw-KDP product coverage with fixtures that contain those products, more storm modes, and eventually a small per-site sample.
- Per-site smoke coverage for the NEXRAD site table, at least at a tiny bounded zoom range.
- Visual side-by-side comparisons against trusted renderers for the same scans before claiming "best-looking."
