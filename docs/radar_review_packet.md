# Radar Review Packet

Status date: 2026-05-11

This is the review-oriented summary for the current radar milestone. The larger goal is not complete, but this packet identifies the implementation surfaces, artifacts, and verification that are ready to discuss.

## Repos Touched

- `C:/Users/drew/rustwx`
- `C:/Users/drew/rustwx-runner`
- `C:/Users/drew/wxstore`

## Primary Code Surfaces

- `crates/rustwx-radar/src/dealias.rs`
  - Staged velocity dealiasing, velocity quality masking, safe acceptance/rejection, and hard-case cleanup.
- `crates/rustwx-radar/src/tile.rs`
  - XYZ tile rendering, all-tilt manifests, product QC, provenance metadata, velocity QC, and reflectivity despeckle metadata.
- `crates/rustwx-radar/src/nexrad/derived.rs`
  - PHI-derived KDP using a conservative centered finite-window derivative.
- `crates/rustwx-cli/src/bin/radar_web_tiles.rs`
  - Radar tile export CLI with all-tilts, staged dealiasing, quality filters, supersample benchmarks, and derived KDP sampling.
- `crates/rustwx-cli/src/bin/radar_export.rs`
  - Static PNG/AI export path; all-tilts derived KDP now samples PHI sweeps consistently with `radar_web_tiles`.
- `crates/rustwx-cli/src/bin/radar_quality_gate.rs`
  - Reflectivity, velocity, generic product, and product-provenance gates.
- `crates/rustwx-cli/src/bin/radar_benchmark_gate.rs`
  - Render speed gate.
- `crates/rustwx-cli/src/bin/radar_dealias_compare.rs`
  - Raw/radial/sweep/staged dealias comparison harness.
- `C:/Users/drew/rustwx-runner/src/main.rs`
  - Radar publish integration, quality gate invocation, all-tilt frame records, and provenance pass-through.
- `C:/Users/drew/wxstore/src/main.rs`
  - Radar frame/tilt JSON serving and metadata preservation.

## Workflow And Docs

- `.github/workflows/rustwx-ci.yml`
  - Radar crate tests, radar CLI gate tests, and release builds.
- `.github/workflows/radar-smoke.yml`
  - Real-fixture KTLX/KSJT smoke for velocity, reflectivity, CC, ZDR, PHI, and PHI-derived KDP.
- `docs/radar_completion_audit.md`
- `docs/radar_sota_roadmap.md`
- `docs/radar_milestone_wrapup.md`
- `docs/radar_review_packet.md`

## Durable Visual Artifacts

- `C:/Users/drew/Documents/Codex/2026-05-10/i-dont-know-how-codex-the/radar_outputs/stable_gallery/radar_ksjt_dualpol_showcase_20260511.png`
- `C:/Users/drew/Documents/Codex/2026-05-10/i-dont-know-how-codex-the/radar_outputs/stable_gallery/radar_ksjt_staged_network_showcase_20260511.png`
- `C:/Users/drew/rustwx-runner/data/radar_tiles/nexrad_level2_ksjt_vel_staged_qgate_smoke/frames/20260511T051237Z/sweep02_el0p44/6/14/26.png`
- `C:/Users/drew/Documents/Codex/2026-05-10/i-dont-know-how-codex-the/radar_outputs/ksjt_043240_product_inventory_01.txt`

## External Baselines

- `C:/Users/drew/rustwx/scripts/radar_pyart_baseline.py`
- `C:/Users/drew/Documents/Codex/2026-05-10/i-dont-know-how-codex-the/radar_outputs/external_baselines/pyart_ktlx_moore_sweep1_velocity_01.json`
- `C:/Users/drew/Documents/Codex/2026-05-10/i-dont-know-how-codex-the/radar_outputs/external_baselines/pyart_ksjt_043240_sweep2_resolved_velocity_01.json`

## Final Verification

- `cargo test -p rustwx-radar --lib`: 45 passed.
- `cargo test -p rustwx-cli --bin radar_quality_gate`: 4 passed.
- `cargo test -p rustwx-cli --bin radar_export`: 2 passed.
- `cargo test` in `C:/Users/drew/rustwx-runner`: 40 passed.
- `cargo test` in `C:/Users/drew/wxstore`: 13 passed.
- Local derived-KDP gate passed with required provenance:
  - `--require-product-source derived`
  - `--require-product-input phi`
  - `--require-product-method centered_phi_range_derivative`
- `.github/workflows/radar-smoke.yml` parses as YAML.

## Current Git Status Notes

The working tree is intentionally not clean. Radar-related review files include:

- `C:/Users/drew/rustwx/.github/workflows/rustwx-ci.yml`
- `C:/Users/drew/rustwx/.github/workflows/radar-smoke.yml`
- `C:/Users/drew/rustwx/crates/rustwx-radar/src/dealias.rs`
- `C:/Users/drew/rustwx/crates/rustwx-radar/src/nexrad/derived.rs`
- `C:/Users/drew/rustwx/crates/rustwx-radar/src/nexrad/level2.rs`
- `C:/Users/drew/rustwx/crates/rustwx-radar/src/png.rs`
- `C:/Users/drew/rustwx/crates/rustwx-radar/src/tile.rs`
- `C:/Users/drew/rustwx/crates/rustwx-cli/src/bin/radar_benchmark_gate.rs`
- `C:/Users/drew/rustwx/crates/rustwx-cli/src/bin/radar_dealias_compare.rs`
- `C:/Users/drew/rustwx/crates/rustwx-cli/src/bin/radar_export.rs`
- `C:/Users/drew/rustwx/crates/rustwx-cli/src/bin/radar_quality_gate.rs`
- `C:/Users/drew/rustwx/crates/rustwx-cli/src/bin/radar_web_tiles.rs`
- `C:/Users/drew/rustwx/docs/radar_completion_audit.md`
- `C:/Users/drew/rustwx/docs/radar_milestone_wrapup.md`
- `C:/Users/drew/rustwx/docs/radar_review_packet.md`
- `C:/Users/drew/rustwx/docs/radar_sota_roadmap.md`
- `C:/Users/drew/rustwx/scripts/radar_pyart_baseline.py`
- `C:/Users/drew/rustwx-runner/src/main.rs`
- `C:/Users/drew/wxstore/src/main.rs`

There are also broader dirty files in `rustwx` related to GOES/native/satellite work, plus `C:/Users/drew/rustwx-runner/config/runner.toml`. Do not treat those as automatically part of the radar review without a separate pass.

## Suggested Review Split

1. Radar decode and algorithm core:
   - Message 31/RRAD fixes, staged dealiasing, velocity QC, reflectivity despeckle, derived KDP, and product provenance.
2. Radar tile and CLI tools:
   - `radar_web_tiles`, `radar_quality_gate`, `radar_benchmark_gate`, and `radar_dealias_compare`.
3. Operational integration:
   - `rustwx-runner` radar publish changes and `wxstore` radar metadata/viewer changes.
4. CI and smoke coverage:
   - `rustwx-ci.yml`, `radar-smoke.yml`, real-fixture gates, and PNG/JSON artifacts.
5. Research and handoff docs:
   - Audit, roadmap, milestone wrap-up, review packet, and optional Py-ART baseline script.

## Still Not Complete

- Broad external validation against Py-ART/xradar/wradlib across more storm modes.
- HCA and raw-KDP fixture coverage.
- Visual side-by-side comparison against trusted radar renderers.
- More per-site smoke coverage.
- Optimization pass for the connected-region staged dealiaser.
- Commit/review split across the three dirty repos.
