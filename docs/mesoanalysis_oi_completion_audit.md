# Surface OI/Kriging Completion Audit

This audit explains why the first RustWX OI/kriging milestone landed quickly,
and why the full professional system is still larger than the first patch makes
it look.

## Short Answer

RustWX now has a real local full-matrix OI/kriging-style surface correction
engine. That part was tractable because it is a bounded numerical problem:
build local covariance matrices, solve them quickly, apply increments, validate
against held-out observations, and emit packet/grid artifacts.

That is not the same as being a 3D-RTMA replacement. The current product is a
near-surface diagnostic correction layer on top of a model background. It does
not update a dynamically balanced 3D model state, does not assimilate radar and
satellite through a forecast model, and has not yet been calibrated across many
weather regimes, source classes, seasons, and terrain/coastal cases.

## What Is Real Now

Implemented in `rustwx-calc`:

- Barnes and OI method selection.
- Local full covariance matrix solve with Cholesky.
- Per-variable background and observation error settings.
- Source-quality and per-observation error support.
- Time-representativeness weighting for accepted observations relative to the
  model valid time, with age-based quality-weight decay and observation-error
  inflation.
- Exponential, Gaussian, and Matern 3/2 covariance kernels.
- Flow-aligned anisotropy from background 10 m wind.
- Terrain-pressure damping.
- Gross-error rejection.
- Buddy-observation rescue for gross-error rejection, so nearby agreeing
  stations can preserve real mesoscale extremes while isolated spikes remain
  filtered.
- Local innovation envelope limiting to reduce over-correction.
- Confidence grids, neighbor counts, increments, and diagnostics.
- Rayon parallel compute over grid cells.

Implemented in `rustwx-products` and `rustwx-cli`:

- Runner observation loading into mesoanalysis observations.
- Source-family error profiles for METAR, mesonet, RAWS, RWIS, SNOTEL/SCAN,
  marine/coastal, and generic sources.
- Cross-source station de-duplication after source/profile/time filtering, with
  per-source duplicate counts in run reports and packets.
- Same-observation validation.
- Station-hash, spatial-block, and source-provider holdout validation.
- Repeated holdout aggregation.
- Validation strata by source-quality class, representativeness class,
  correction role, terrain-pressure class, and observation-age bucket. These
  strata now flow into calibration matrices so one good whole-domain score does
  not hide a weak source/exposure/terrain bucket.
- Calibration gates can now target named strata directly, so tuning changes can
  be rejected when they damage a specific source-quality, representativeness,
  terrain, or age bucket.
- Run reports and compact agent packets can carry explicit `--case-tag`
  calibration labels such as `regime=dryline`, `hazard=severe`, and
  `domain=ok_plains`; calibration matrices aggregate `case_tag_counts` and can
  require specific tags or minimum unique tag coverage before accepting a sweep.
- Calibration matrices now aggregate compact station-level innovation summaries
  under `aggregate.stations`, keyed by source and station ID, so persistent
  station bias, stale/outlier behavior, and source-local over-correction can be
  audited without reading raw validation samples.
- Calibration gates can now target named stations directly, with thresholds for
  station observation count, station OI-minus-raw MAE, station analysis MAE, and
  station absolute analysis bias. This catches station-local failures that pass
  a domain, source, or stratum mean.
- Calibration can write a compact innovation-history sidecar with schema
  `rustwx.surface_mesoanalysis.innovation_history.v1`, preserving station and
  source/provider time-series entries plus aggregate rollups for agent and
  operator inspection.
- Innovation-history sidecars can now be merged across calibration runs with
  same-case deduplication, newest-N retention per station/source series, and
  recomputed aggregate rollups.
- Innovation histories now include station/source watchlists that rank the worst
  rolling station-field and source-field combinations for agent/operator triage.
- A query-only calibration CLI mode can read an existing innovation history and
  emit `rustwx.surface_mesoanalysis.innovation_query.v1` with station, source,
  variable, minimum-case-count, and top-N filters.
- Calibration can write a WxStore-shaped innovation index directory with
  `rustwx.surface_mesoanalysis.innovation_wxstore_index.v1`, station/source
  JSONL indexes, watchlist JSON files, and a manifest describing query keys and
  sortable fields.
- Barnes baseline comparison.
- WxStore-style grid export.
- Compact LLM-readable `mesoanalysis_agent_packet.json`.

Live smoke evidence from HRRR `20260513 00Z f001` over the CONUS surface fields:

- OI full-CONUS compute was about `1.3-1.4 s` in release mode.
- Spatial-block holdout showed OI beating raw HRRR and Barnes on temperature,
  dewpoint, and wind for the first Oklahoma/Plains-focused smoke case.
- Source-provider holdout was harsher and more mixed: OI still beat Barnes on
  the tested means, but only beat raw HRRR on some folds/variables.

## Why It Felt Easy

The fast part was the numerical spine, not the meteorological truth problem.

Local OI is an elegant algorithm. Once the product is scoped as a surface
diagnostic, the code can be direct:

1. compute model-minus-observation innovations,
2. choose nearby usable observations,
3. build `B H^T (H B H^T + R)^-1`,
4. apply bounded increments,
5. validate with withheld observations.

Rust is also a good fit here. Small dense local matrices, bounded neighbor
counts, cache-friendly loops, and Rayon parallelism make the first version fast.

The hard part is proving when that correction should be trusted.

## What This Is Not Yet

It is not a dynamically balanced 3D data-assimilation system. Changing 2 m
temperature or 10 m wind after the fact does not consistently update the PBL,
soil, pressure, moisture profile, CAPE/CIN, shear, UH, reflectivity, or storm
evolution.

It is not a full RTMA clone. RTMA/URMA are operational NOAA surface analyses
with mature QC, source handling, downscaling, operational monitoring, late-ob
handling for URMA, and long-running verification infrastructure.

It is not yet calibrated enough to use aggressive corrections by default. HRRR,
RAP, and RRFS already assimilate many conventional, radar, satellite, and other
observations. Blindly correcting their fields can double-count data or break
physical consistency. RustWX should default to residuals, confidence, and
bounded diagnostic increments.

## The Remaining Weeks Of Work

The weeks-long work is mostly calibration, source governance, and meteorological
edge cases:

- Multi-case validation across severe events, nocturnal inversions, cold pools,
  drylines, heat bursts, winter fronts, coastal boundaries, complex terrain, and
  quiet regimes.
- Durable WxStore-backed rolling persistence for per-station and per-provider
  innovation statistics across many archived/live cases. A mergeable JSON
  history sidecar now covers retention, same-case dedupe, aggregate rollups, and
  watchlists; a stable query report and WxStore-shaped index artifact now exist,
  but this is not yet wired into the external `wxstore` service/runtime.
- Better source metadata: siting, elevation mismatch, wind height, exposure,
  network maintenance status, and provider reliability.
- Lagrangian advection for moving boundaries.
- Land/water, coastline, terrain-ridge, and front-crossing barriers.
- Regime-aware covariance tuning by variable, source class, terrain class, and
  time of day.
- Posterior error/analysis uncertainty estimates beyond the first confidence
  proxy.
- Independent comparison against RTMA/URMA where available.
- Proper area-focused human maps: raw model, obs dots, residuals, corrected
  analysis, confidence mask, and optional radar/satellite overlays.
- Rustwx-agent packet integration by target, hazard, time, and domain, not just
  a CLI artifact beside the run report.

## Best Current Label

Use this label in packets and demos:

`surface_adjusted_diagnostic`

Recommended wording:

> Surface-adjusted diagnostic analysis. Uses native model fields as the
> dynamically assimilated background, applies bounded source-aware OI increments
> to near-surface variables, and reports validation/confidence. Not a balanced
> 3D model analysis.

## Operational Rule

Correct near-surface fields only when the product is explicitly diagnostic and
the validation/confidence supports it. For 3D soundings, derived severe fields,
model dynamics, storm evolution, ML features, and forecast interpretation,
prefer the native model field plus observation residuals unless a separate
consistency step exists.

## Next Milestone

The next credibility milestone is not a fancier matrix solver. It is a
calibration harness:

- batch over many archived cases,
- compare raw model, Barnes, OI, and RTMA/URMA where available,
- score by source class, terrain class, case tag/regime, valid hour, and
  variable,
- write a stable packet summary for each case,
- fail CI or smoke gates when a tuning change improves one regime by damaging
  another.

That is the work that turns a good first OI engine into a serious meteorology
sidekick.
