# Radar Milestone Wrap-Up

Status date: 2026-05-11

This is the short discussion version of the longer radar audit. The current milestone is strong enough to wrap as a working Rust-native radar capability slice, but not strong enough to claim the full "world best-looking and fastest" objective is complete.

## What Is Working

- Rust-native radar tile rendering for Level-II reflectivity, velocity, and correlation coefficient.
- All-tilt velocity tile generation through `radar_web_tiles --all-tilts`.
- Native-resolution metadata in tile manifests so clients can avoid fake overzoom claims.
- Staged velocity dealiasing with safe acceptance/rejection metadata.
- Velocity quality filtering using reflectivity and spectrum-width checks.
- Conservative reflectivity despeckle with manifest QC.
- Generic product QC in tile manifests for non-reflectivity/non-velocity products.
- Product provenance metadata in tile manifests, runner frame indexes, and wxstore radar JSON, including quality-gated PHI-derived KDP labeling.
- Runner integration for radar publish jobs, staged dealiasing, velocity QC, reflectivity despeckle, quality gates, and all-tilt output.
- wxstore radar viewing support for frames, tilts, and native/QC metadata.
- CI coverage for radar crate tests and radar CLI gate binaries.
- Scheduled/manual real-fixture smoke coverage for:
  - KTLX Moore 2013 staged velocity.
  - KSJT 2026 hard all-tilt staged velocity.
  - KSJT 2026 GR2Analyst/despeckled reflectivity.
  - KSJT 2026 correlation coefficient with finite-gate/value-range QC.
  - KSJT 2026 ZDR, PHI, and PHI-derived KDP with finite-gate/value-range QC.

## Strong Evidence

- `cargo test -p rustwx-radar --lib`: 42 tests passed after product-QC additions.
- `cargo test -p rustwx-cli --bin radar_quality_gate`: 3 tests passed after generic product gate additions.
- Final verification after KDP/provenance additions:
  - `cargo test -p rustwx-radar --lib`: 45 passed.
  - `cargo test -p rustwx-cli --bin radar_quality_gate`: 4 passed.
  - `cargo test` in `C:/Users/drew/rustwx-runner`: 40 passed.
  - `cargo test` in `C:/Users/drew/wxstore`: 13 passed.
- KSJT hard all-tilt staged velocity local smoke rendered 11 tilts and 26 PNG tiles per supersample run, with every tilt passing the velocity QC gate.
- KSJT reflectivity smoke removed 4616 of 472545 finite gates, a 0.00977 removed fraction against the 0.05 quality cap.
- KSJT CC smoke reported 416519 finite gates, min 0.2083, max 1.0517, mean 0.8386, and passed product-QC gates.
- KSJT ZDR and PHI smokes each reported 416519 finite gates, rendered 9 PNG tiles per supersample run, passed decode-range product QC, and stayed under 26 ms locally.
- KSJT derived-KDP smoke reported 380338 finite gates, rendered 8 PNG tiles per supersample run, passed decode-range product QC, and stayed under 21 ms locally.
- The regenerated KDP manifest reports `source=derived`, `inputs=["phi"]`, and `method=centered_phi_range_derivative`; runner and wxstore tests preserve that metadata, and `radar_quality_gate` now enforces it in the KDP smoke.
- A durable dual-pol contact sheet is available at `C:/Users/drew/Documents/Codex/2026-05-10/i-dont-know-how-codex-the/radar_outputs/stable_gallery/radar_ksjt_dualpol_showcase_20260511.png`.
- KTLX Moore 2013 external Py-ART baseline is now generated locally for sweep 1. Py-ART region-based dealiasing reduced severe jumps to 18; rustwx staged reduced severe jumps to 23 on the same sweep with a lower max jump.
- KSJT external Py-ART baseline now resolves the same-elevation reflectivity/velocity sweep split. On the 0.879 degree velocity sweep, raw severe jumps match between Py-ART and rustwx at 3058; Py-ART region-based reduces them to 60, while rustwx staged reduces them to 55 in 459 ms and passes the gate.

## What Is Still Risky

- External baseline comparison is only started. KTLX Py-ART works and one KSJT sweep is now correctly resolved, but this is still not broad validation across products, sites, and storm modes.
- Staged dealiasing is good on selected hard fixtures, but not broadly proven across storm modes, range folding, weak signal, and non-tornadic high shear.
- Reflectivity QC is conservative and useful, but not a full clutter/AP classifier.
- CC has value-range/product-smoke coverage, but not deeper dual-pol science QC.
- HCA, raw-KDP fixture coverage, and deeper product-specific dual-pol science QC still need suitable fixtures and validation. ZDR, PHI, and PHI-derived KDP now have basic decode-range smoke coverage.
- Per-site smoke coverage is not done.
- The working tree is broad and should be split into reviewable commits before merging.

## Recommended Next Milestone

The next milestone should be "external validation and product breadth":

- Extend the Py-ART/xradar baseline harness beyond the first KTLX and KSJT velocity comparisons.
- Add HCA and raw-KDP real-fixture smokes with generic product QC once suitable volumes are selected.
- Add a small all-site/site-table smoke that renders one bounded tile for a rotating sample of sites.
- Add a speed-focused optimization pass for staged dealiasing once the external baseline agrees the quality target is right.

## Bottom Line

Wrap this pass as: "Rust-native radar tiles, staged velocity dealiasing, runner/wxstore integration, real-fixture smoke gates, and first external baseline evidence."

Do not wrap the larger objective as complete yet.
