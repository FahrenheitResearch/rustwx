# rustwx Radar SOTA Roadmap

Status date: 2026-05-11

This note tracks the concrete path toward first-party Rust radar capabilities that are both visually excellent and fast enough for interactive, agent-first radar analysis.

## Success Criteria

- Decode NEXRAD Level-II moments correctly, including Message 31 scale/offset rules, below-threshold gates, range-folded gates, and moment units.
- Render reflectivity, velocity, spectrum width, dual-pol products, derived products, and all available tilts as transparent Web Mercator tiles.
- Support raw and dealiased velocity, with explicit metadata so clients know which path produced a frame.
- Preserve a fast path for operational updates and a high-quality path for workstation-style display.
- Keep all production decode/render paths Rust-native.
- Verify quality against known research/open-science baselines rather than only internal screenshots.

## Current Rust Path

- `rustwx-radar` owns Level-II decode, product typing, velocity dealiasing, PNG rendering, and XYZ tile rendering.
- `radar_web_tiles` exports single-sweep or all-tilt tile pyramids.
- `rustwx-runner` can publish all-tilt dealiased/despeckled/velocity-filtered radar frames into `data/radar_tiles`, including per-frame and per-tilt QC/native-resolution metadata. Runner can invoke `radar_quality_gate` before publishing a rendered frame.
- `wxstore` can serve both regular radar frame tiles and tilt-specific tiles, preserving QC/native-resolution metadata, exposing tilt selection in the radar viewer, and showing native-resolution plus velocity-filter hints in the radar viewer legend.

## Research Anchors

- NOAA ROC RDA/RPG ICD 2620002 defines Message 31 Digital Radar Data and is the authority for Level-II moment decoding: <https://www.roc.noaa.gov/interface-control-documents.php>
- Py-ART's region-based Doppler dealiasing documents the operationally common region merge approach: <https://arm-doe.github.io/pyart/examples/correct/plot_dealias.html>
- UNRAVEL is the strongest current open modular dealiasing reference for the next production-quality Rust pass. Its value for `rustwx` is not a single trick; it is the staged architecture: local continuity, reference-field constraints, gate filtering, and explicit failure modes before publishing corrected velocity: <https://www.osti.gov/pages/biblio/1660907>
- wradlib is a peer-reviewed open-source reference for radar processing workflows, georeferencing, and hydrometeorological products: <https://hess.copernicus.org/articles/17/863/2013/index.html>
- wradlib's Gabella clutter filter uses spatial continuity and minimum echo-area ideas for single-sweep reflectivity QC: <https://docs.wradlib.org/en/2.2.0/classify.html>
- Py-ART exposes a reflectivity despeckle workflow that removes small connected objects from radar fields: <https://arm-doe.github.io/pyart-docs-travis/API/generated/pyart.correct.despeckle_field.html>
- The ORPG-derived deep-learning dealiasing work is a useful future validation target for learned velocity correction, not the current production path: <https://arxiv.org/abs/2211.13181>

## Implemented Quality Features

- Message 31 velocity and spectrum-width units are normalized to meters per second.
- Message 31 RRAD Nyquist velocity is decoded from the correct signed field at bytes 16-17.
- Raw codes `0` and `1` are masked as below-threshold and range-folded gates instead of plotted as physical values.
- Velocity and spectrum-width gates outside physically reasonable ranges are filtered.
- Sweep-continuity dealiasing now uses gate quality, 2D region growing, neighborhood refinement, physical velocity limits, and a continuity acceptance check so bad corrections fail safely.
- Staged-continuity dealiasing is available as `--dealias-method staged`. It compares radial, sweep, radial-reference-refined sweep, and Py-ART/UNRAVEL-inspired connected-region network candidates before the same safe acceptance gate, then masks unresolved extreme neighbor-pair jumps above the staged output limit. This is now the quality path for hard velocity cases; active-storm dealiasing still dominates runtime.
- Velocity rendering has an explicit `--velocity-quality-filter` path that masks gates failing the same reflectivity/spectrum-width quality checks used by dealiasing. Manifests record `velocity_quality_filter` and `velocity_quality_qc` so this is visible to downstream clients.
- Tile rendering uses polar range/azimuth interpolation and optional supersampling.
- Tile rendering can select built-in palettes including default/NWS, GR2Analyst, NSSL, classic, dark, and colorblind styles.
- Reflectivity rendering has an opt-in polar despeckle filter that removes isolated gates while preserving small contiguous echoes.
- Tile manifests include `sample_factor`, native gate size, native azimuth spacing, max-zoom meters-per-pixel at the radar site, `color_table`, requested `dealias_velocity`, `dealias_method`, actual `dealias_applied`, dealias candidate QC, reflectivity QC, velocity QC, sweep index, elevation angle, tile counts, and timing.
- Tile manifests include `product_provenance`, runner/wxstore preserve it, and `radar_quality_gate` can require source/input/method fields so clients and CI can tell native radar moments from derived products such as PHI-derived KDP.

## Benchmark Evidence

Volume: `KSJT20260511_012134_V06`

Single velocity sweep, bounds `-102,29,-98,33`, zooms `6-8`, release build:

| sample_factor | candidate tiles | output PNGs | total_ms | tiles/sec | bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 29 | 25 | 70 | 414.29 | 1,011,364 |
| 2 | 29 | 26 | 82 | 353.66 | 1,155,546 |

All velocity tilts, same bounds, zooms `6-7`, release build:

| sample_factor | tilts | candidate tiles | output PNGs | total_ms | tiles/sec | bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 16 | 172 | 110 | 625 | 275.20 | 2,300,984 |
| 2 | 16 | 172 | 114 | 714 | 240.90 | 2,615,747 |

Benchmark artifacts:

- `radar_outputs/ksjt_bench_release_z6_8_01/supersample_benchmark.json`
- `radar_outputs/ksjt_bench_release_all_tilts_z6_7_01/supersample_benchmark.json`
- `radar_outputs/radar_benchmark_gate_01.json`
- `radar_outputs/radar_quality_gate_01.json`
- `radar_outputs/ksjt_ref_native_meta_01/tiles_manifest.json`
- `radar_outputs/radar_quality_gate_native_meta_01.json`
- `radar_outputs/stable_gallery/manifest.json`
- `radar_outputs/stable_gallery/radar_showcase_20260511.png`
- `radar_outputs/stable_gallery/staged_manifest.json`
- `radar_outputs/stable_gallery/radar_staged_dealias_showcase_20260511.png`
- `radar_outputs/dealias_compare_ktlx_moore_sweep1_01.json`
- `radar_outputs/dealias_compare_ktlx_current_sweep0_01.json`
- `radar_outputs/dealias_compare_ksjt_sweep0_01.json`
- `radar_outputs/ksjt_velocity_filter_01/raw_z8/tiles_manifest.json`
- `radar_outputs/ksjt_partial_staged_filter_01/sweep_z8/tiles_manifest.json`
- `radar_outputs/stable_gallery/ksjt_velocity_filter_manifest.json`
- `radar_outputs/stable_gallery/radar_ksjt_velocity_filter_showcase_20260511.png`
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
- `radar_outputs/ci_smoke_ktlx_moore_vel_01/supersample_benchmark.json`
- `radar_outputs/ci_smoke_ktlx_moore_vel_01/quality_gate.json`
- `radar_outputs/ci_smoke_ktlx_moore_vel_01/benchmark_gate.json`
- `radar_outputs/ci_smoke_ksjt_hard_vel_all_tilts_01/supersample_benchmark.json`
- `radar_outputs/ci_smoke_ksjt_hard_vel_all_tilts_01/quality_gate.json`
- `radar_outputs/ci_smoke_ksjt_hard_vel_all_tilts_01/benchmark_gate.json`
- `radar_outputs/ci_smoke_ksjt_ref_despeckle_01/supersample_benchmark.json`
- `radar_outputs/ci_smoke_ksjt_ref_despeckle_01/quality_gate.json`
- `radar_outputs/ci_smoke_ksjt_ref_despeckle_01/benchmark_gate.json`
- `radar_outputs/ci_smoke_ksjt_cc_01/supersample_benchmark.json`
- `radar_outputs/ci_smoke_ksjt_cc_01/product_quality_gate.json`
- `radar_outputs/ci_smoke_ksjt_cc_01/benchmark_gate.json`
- `radar_outputs/ci_smoke_ksjt_zdr_01/supersample_benchmark.json`
- `radar_outputs/ci_smoke_ksjt_zdr_01/product_quality_gate.json`
- `radar_outputs/ci_smoke_ksjt_zdr_01/benchmark_gate.json`
- `radar_outputs/ci_smoke_ksjt_phi_01/supersample_benchmark.json`
- `radar_outputs/ci_smoke_ksjt_phi_01/product_quality_gate.json`
- `radar_outputs/ci_smoke_ksjt_phi_01/benchmark_gate.json`
- `radar_outputs/ci_smoke_ksjt_kdp_01/supersample_benchmark.json`
- `radar_outputs/ci_smoke_ksjt_kdp_01/product_quality_gate.json`
- `radar_outputs/ci_smoke_ksjt_kdp_01/benchmark_gate.json`
- `radar_outputs/external_baselines/pyart_ktlx_moore_sweep1_velocity_01.json`
- `radar_outputs/external_baselines/pyart_ksjt_043240_sweep2_velocity_01.json`
- `radar_outputs/external_baselines/pyart_ksjt_043240_sweep2_resolved_velocity_01.json`
- `radar_outputs/stable_gallery/ksjt_staged_network_manifest.json`
- `radar_outputs/stable_gallery/radar_ksjt_staged_network_showcase_20260511.png`
- `C:/Users/drew/rustwx-runner/data/radar_tiles/nexrad_level2_ksjt_vel_staged_qgate_smoke/frames/20260511T051237Z/all_tilts_manifest.json`

The repeatable speed gate is `radar_benchmark_gate`. The current KSJT release artifacts pass with `--min-tiles-per-second 150 --max-ms-per-candidate-tile 6 --require-sample-factor 1 --require-sample-factor 2`.

The repeatable visual/QC gate is `radar_quality_gate`. Current KSJT/KTLX artifacts pass with `--max-reflectivity-removed-fraction 0.05 --max-velocity-fold-fraction 0.005 --max-velocity-severe-jumps 200 --max-velocity-max-jump-ms 100`. A live runner one-shot for KSJT `2026-05-11T05:12:37Z` also passed the same gate before publishing frame `20260511T051237Z`.

CI now builds and tests the radar crate plus radar gate/tile binaries in `.github/workflows/rustwx-ci.yml`. A separate scheduled/manual workflow, `.github/workflows/radar-smoke.yml`, downloads pinned public KTLX Moore 2013 and KSJT 2026 fixtures, renders bounded staged velocity, despeckled reflectivity, and CC/ZDR/PHI/KDP dual-pol tiles with supersample factors 1 and 2, gates velocity QC, gates reflectivity removal fraction, gates dual-pol finite-gate/value range QC, gates KDP provenance, gates render speed, and uploads PNG/JSON artifacts. Local equivalents passed: KTLX staged dealiasing reduced severe jumps from `1087` to `23` and produced `8` PNG tiles; KSJT all-tilt staged rendering covered `11` tilts, produced `26` PNG tiles per supersample run, and stayed under `3s` locally; KSJT reflectivity despeckle removed `0.98%` of finite gates and rendered z7 supersample runs in `25-32ms`; KSJT CC rendered `9` PNG tiles per supersample run in `8-19ms` with `416519` finite gates and CC max `1.0517`; KSJT ZDR rendered `9` PNG tiles per supersample run in `12-26ms` with min/max `-13.0/20.0`; KSJT PHI rendered `9` PNG tiles per supersample run in `10-24ms` with min/max `0.0/359.6488`; KSJT PHI-derived KDP rendered `8` PNG tiles per supersample run in `11-21ms` with `380338` finite gates and min/max `-44.9561/44.9561`, and the gate requires PHI-derived provenance.

`scripts/radar_pyart_baseline.py` is the optional external-baseline hook. It keeps Py-ART out of the Rust operational path while generating comparable raw/Py-ART region-based continuity JSON when `arm_pyart` is installed. The KTLX Moore sweep 1 artifact shows Py-ART reducing severe jumps from `1110` to `18`; the comparable rustwx staged artifact reduces severe jumps from `1087` to `23` with a lower max neighbor jump. The first KSJT Py-ART artifact produced zero finite gates because the requested Py-ART sweep was a same-elevation dual-pol/reflectivity sweep, not the velocity sweep. The resolved KSJT artifact now records that mismatch, selects Py-ART velocity sweep 3 at the same `0.8789` degree fixed angle, and compares it to rustwx's same-angle velocity sweep: raw severe jumps match at `3058`, Py-ART region-based dealiasing reduces severe jumps to `60`, and rustwx staged reduces them to `55` in `459ms`.

## KSJT Velocity QC Snapshot

The first QC pass caught an impossible `616.04 m/s` Nyquist value, traced to the RRAD field offset. After fixing the parser, the same KSJT sweep reports:

| path | nyquist_ms | finite gates | fold-like jumps | severe jumps | max jump |
| --- | ---: | ---: | ---: | ---: | ---: |
| raw | 26.28 | 264,580 | 4,234 | 1,338 | 52.50 |
| dealias-safe | 26.28 | 264,580 | 4,234 | 1,338 | 52.50 |

For this scan, the current sweep dealiaser fails the acceptance test and falls back to raw velocity instead of rendering physically absurd corrected velocities.

With selectable dealias methods, the same z8 GR2 velocity cut renders safely with both radial and sweep methods. Both are rejected by QC on this scan, but radial is the faster fallback path (`55ms` for 9 candidate z8 tiles vs `151ms` for sweep), so runner config currently uses radial while retaining sweep as an override/experimental path.

A current-format KTLX NOMADS volume (`KTLX_20260510_204736.bz2`) parses through the Message 31 path and gives another low-alias baseline: raw velocity reports Nyquist `33.04 m/s`, `247` fold-like jumps, `25` severe jumps, and a `0.000568` fold-like jump fraction. Radial and sweep methods both correctly fail safe on that volume.

The Moore 2013 KTLX fixture used by MetPy/Py-ART examples (`KTLX20130520_201643_V06.gz`) is now a Rust-native validation fixture. The file is an already-unblocked Archive-II stream with Message 31 records, not a legacy Message 1 gap; the parser now preserves already-unblocked `AR2V` streams instead of assuming every post-volume-header body is bzip-blocked.

Moore 2013 KTLX, sweep 1, velocity, bounds `-98.4,34.8,-96.6,36.1`, zoom 9, GR2Analyst palette:

| path | nyquist_ms | finite gates | fold-like jumps | severe jumps | max jump | candidate tiles | total_ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| raw | 26.12 | 155,912 | 1,460 | 1,110 | 52.00 | 12 | 21 |
| radial | 26.12 | 155,912 | 1,460 | 1,110 | 52.00 | 12 | 47 |
| sweep accepted | 26.12 | 130,064 | 154 | 90 | 77.24 | 12 | 99 |

Manifest-level `dealias_qc` now explains why candidates are accepted or rejected:

| method | candidate changes | original fold/severe/max | candidate fold/severe/max | decision |
| --- | ---: | ---: | ---: | --- |
| radial | 29,618 | 1,345 / 1,087 / 52.00 | 2,710 / 2,607 / 167.72 | reject worse continuity |
| sweep | 27,587 | 1,345 / 1,087 / 52.00 | 154 / 90 / 77.24 | accept high-shear improvement |

The high-shear acceptance rule keeps the old no-worse gate, and additionally accepts a candidate only when fold-like and severe jumps both improve by at least half while max-jump regression stays within two Nyquist velocities of the raw max jump. Staged output then masks unresolved extreme neighbor-pair jumps above `min(4 * Nyquist, 100 m/s)`. That accepts the Moore 2013 and hard KSJT candidates, rejects runaway radial candidates, and keeps low-alias validation scans on their raw/fallback path.

The `--force-rejected-dealias` debug flag can still render rejected candidates for research screenshots while keeping production safe by default.

Current validation scans after the high-shear rule:

| volume | method | applied | original fold/severe/max | candidate fold/severe/max | decision |
| --- | --- | --- | ---: | ---: | --- |
| Moore 2013 KTLX | sweep | yes | 1,345 / 1,087 / 52.00 | 154 / 90 / 77.24 | accepted |
| Moore 2013 KTLX | staged | yes | 1,345 / 1,087 / 52.00 | 158 / 94 / 77.24 | accepted |
| Current KTLX | sweep | no | 222 / 23 / 62.50 | n/a | skipped: low alias burden |
| Current KTLX | staged | no | 222 / 23 / 62.50 | n/a | skipped: low alias burden |
| KSJT | sweep | no | 4,164 / 1,313 / 52.50 | 5,039 / 2,896 / 159.18 | rejected: worse continuity |
| KSJT | staged | no | 4,164 / 1,313 / 52.50 | 5,059 / 2,912 / 159.18 | rejected: worse continuity |

`radar_quality_gate` verifies the accepted Moore candidate keeps fold-like jumps down from `1345` to `154` and severe jumps down from `1087` to `90`. It also verifies the current low-alias KTLX scan stays on the skipped path without a forced candidate.

The staged path passes the same quality gate on the Moore and current KTLX validation cuts. The connected-region network candidate now also clears the hard KSJT `2026-05-11T04:32:40Z` all-sweep comparison. On the five formerly failing low/mid tilts, staged reduces severe jumps from `3497/3577/3742/3058/4154` raw to `44/44/47/55/24`, with fold-like jump fractions all below `0.0011`. The stricter cleanup pass also clears KSJT `2026-05-11T05:07:34Z`, where the two previously failing low/mid tilts now pass with staged max jumps of `86.0` and `94.0 m/s`.

`radar_dealias_compare` is the repeatable method-comparison harness. It runs `raw`, `radial`, `sweep`, and `staged` against the same selected sweep, records per-method elapsed time, dealias QC, velocity QC, threshold pass/fail, and the best passing method. It also supports `--require-method-pass staged`, `--expect-best-method`, `--max-method-ms staged=650`, and `--max-total-method-ms staged=3000` so hard fixtures can fail if the intended method regresses in quality or runtime even when another method happens to pass. Current release-mode comparison evidence:

| volume | sweep | best method | raw | radial | sweep | staged | note |
| --- | ---: | --- | ---: | ---: | ---: | ---: | --- |
| Moore 2013 KTLX | 1 | staged | fail, 3ms | fail, 41ms | pass, 79ms | pass, 233ms | staged reduces severe jumps from sweep's `90` to `23` |
| Current KTLX | 0 | raw | pass, 3ms | pass, 12ms | pass, 13ms | pass, 13ms | low-alias skip keeps the fast raw path |
| KSJT 2026-05-11T04:32:40Z | all velocity sweeps | staged/sweep | mixed fail/pass | mixed fail/pass | high tilts pass, 763ms total | all sweeps pass, 2833ms speed-gated total, max 519ms | staged fixes hard low/mid tilts; sweep remains faster where already clean |
| KSJT 2026-05-11T05:07:34Z | all velocity sweeps | staged/sweep | mixed fail/pass | mixed fail/pass | high tilts pass | all sweeps pass, 2665ms speed-gated total, max 518ms | staged cleanup masks unresolved extreme pairs on hard low/mid tilts |

The KSJT velocity-quality filter experiment masked `13,613` of `264,580` finite velocity gates (`5.15%`) and correctly recorded that no dealias candidate was applied when staged was rejected. It did not reduce the strict velocity failure by itself, which correctly pointed the next pass toward connected-region/linking logic. The network staged candidate is the fix that clears that fixture.

## Renderer Speed Notes

Tile rendering now prebinds the selected product moment for each sorted radial once per prepared sweep, avoiding a per-pixel search through each radial's moment list. Release-mode validation cuts:

| volume | before moment prebind | after moment prebind | notes |
| --- | ---: | ---: | --- |
| Moore 2013 KTLX z9 sweep dealias | 108ms | 99ms | accepted high-shear sweep |
| Current KTLX z8 sweep dealias | 145ms | 32ms | skipped by low-alias early exit |
| KSJT z8 sweep dealias | 156ms | 157ms | safe fallback after full candidate |

An azimuth lookup table was tested and removed because it did not improve timings consistently and changed edge-pixel counts slightly; exact binary-search bracketing is retained for now.

Tile manifests now include `resolve_ms`, `prepare_ms`, and `render_ms`. On the current validation cuts, the dealias/resolve phase dominates:

| volume | resolve_ms | prepare_ms | render_ms | total_ms |
| --- | ---: | ---: | ---: | ---: |
| Moore 2013 KTLX | 79 | 1 | 17 | 99 |
| Current KTLX | 17 | 2 | 12 | 32 |
| KSJT | 138 | 1 | 16 | 157 |

This makes the next speed target clear: active-storm sweep dealiasing, not the Web Mercator pixel loop. Low-alias scans now return after raw continuity QC with `skipped_low_alias_burden`.

The staged-network dealiaser has one explicit speed guard: after radial/sweep/refined candidates reach zero fold-like and severe jumps with max jump below Nyquist, it returns before building the connected-region network. On the KSJT all-sweep release comparisons, the speed-gated artifacts keep staged below `650ms` per sweep and below `3000ms` total (`2833ms` for `04:32:40Z`, `2665ms` for `05:07:34Z`). The live runner KSJT `05:12:37Z` z6 all-tilt publish rendered and quality-gated in `2534ms` total, with hard low/mid staged tilts around `352-533ms` and clean high tilts around `9-89ms`.

## Reflectivity QC Snapshot

KSJT `2026-05-11T01:21:34Z`, z8 reflectivity, GR2Analyst palette, sample factor 2:

| path | finite gates | removed gates | removed fraction | resolve_ms | render_ms | total_ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| raw | n/a | 0 | 0.0000 | 0 | 16 | 17 |
| despeckle min-neighbors 2 | 288,674 | 4,224 | 0.0146 | 12 | 17 | 32 |

The current despeckle filter is intentionally conservative and opt-in. It removes isolated polar gates, not broad clutter/AP fields. Heavier reflectivity QC should use texture, echo area, dual-pol moments, and/or multi-volume context before becoming a default path.

`radar_quality_gate` currently caps opt-in despeckle removal at `5%`; the KSJT sample removes `1.46%`.

The regenerated KSJT z8-z9 manifest reports a `250 m` native gate size, `0.4999 deg` native azimuth spacing, and `261.05 m/px` at z9 over KSJT. That makes z9 effectively native-scale for this cut; zooming beyond that should be treated as client overzoom rather than new radar detail.

## Stable PNG Gallery

The thread-local inspection gallery keeps durable PNG copies separate from runner frame directories, which may be regenerated or renamed during publish tests. The current gallery contains:

- KSJT reflectivity at native-scale z9.
- KSJT raw reflectivity z8.
- KSJT opt-in despeckled reflectivity z8.
- KTLX Moore 2013 accepted sweep-dealiased velocity z9.
- KTLX Moore 2013 accepted staged-dealiased velocity z9.
- Current KTLX staged low-alias velocity z8.
- KSJT staged rejected/fallback velocity z8.
- KSJT staged-network accepted velocity before/after z6.
- Current KTLX low-alias velocity z8.
- A contact-sheet PNG combining those tiles for quick visual inspection.
- A KSJT dual-pol contact sheet combining CC, ZDR, PHI, and PHI-derived KDP.
- A live runner-published KSJT staged velocity tile at `C:/Users/drew/rustwx-runner/data/radar_tiles/nexrad_level2_ksjt_vel_staged_qgate_smoke/frames/20260511T051237Z/sweep02_el0p44/6/14/26.png`.

## Remaining Gaps

- Extend fixture-based comparison against Py-ART/wradlib/xradar outputs for selected public Level-II volumes. KTLX and one resolved KSJT Py-ART baseline are generated; wradlib/xradar and broader storm-mode validation remain.
- Broaden dealias validation fixtures across storm modes, range-folding, weak-signal, noisy velocity, and non-tornadic high-shear cases.
- Optimize the connected-region staged dealiaser before making it the runner default for every velocity frame.
- Broaden the pinned-fixture radar smoke beyond the current KTLX/KSJT velocity, reflectivity, CC, ZDR, PHI, and PHI-derived KDP cuts: add HCA/raw-KDP products with suitable fixtures, more storm modes, and eventually bounded per-site samples.
- Add reflectivity QC beyond code masking and opt-in polar despeckle: clutter suppression, texture/echo-area filtering, dual-pol gates, and optional product-specific smoothing.
- Add per-site batch smoke tests for the full NEXRAD site table with bounded zooms.
- Investigate SIMD or LUT-heavy hot paths only after benchmark evidence shows the renderer, not decode or PNG write, is limiting.
