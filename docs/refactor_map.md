# RustWx Refactor Map

This map is a guardrail for making RustWx easier to change without changing
weather behavior. It is based on the current workspace after `main` includes
the model maps preview and `rustwx-regrid`.

## Refactor Rules

- Preserve weather science, units, thresholds, selectors, product IDs, product
  titles, palettes, filenames, JSON schemas, and CLI flags.
- Prefer characterization tests and inventories before module moves.
- Prefer mechanical splits over redesigns for the first refactor stack.
- Keep compatibility paths unless a later explicit breaking-change plan says
  otherwise.
- Treat `ARCHITECTURE.md` as the source of truth for crate ownership.

## Workspace Status

`cargo metadata --no-deps --format-version 1` resolves every workspace member.
The previously suspicious members are present:

- `crates/rustwx-model-maps-launcher`
- `crates/rustwx-regrid`

Current workspace members:

| Crate | Primary role |
| --- | --- |
| `rustwx-cli` | User CLI, proof binaries, diagnostics, publication/export tools. |
| `rustwx-calc` | Weather calculation kernels and derived diagnostic math. |
| `rustwx-contour` | Contour topology extraction. |
| `rustwx-core` | Shared model, grid, field, selector, and request types. |
| `rustwx-cross-section` | Cross-section geometry, sampling, vertical axes, style, and lightweight rendering. |
| `rustwx-io` | Source probing, fetching, GRIB extraction, and cache plumbing. |
| `rustwx-models` | Model catalog, source metadata, URL construction, run discovery, and plot recipes. |
| `rustwx-model-maps-launcher` | Small launcher for the standalone model maps surface. |
| `rustwx-products` | Product orchestration: fetch/decode/prep/product assembly/publication. |
| `rustwx-prep` | Prep helpers such as WRF operation preparation. |
| `rustwx-python` | Thin Python bindings and local Studio glue. |
| `rustwx-radar` | Native radar decode, derived radar products, render helpers, and tile/sidecar support. |
| `rustwx-regrid` | Reusable grid-to-grid regridding plans and sparse weights. |
| `rustwx-render` | Projected map request/render/layout/overlay/colorbar/PNG engine. |
| `rustwx-sounding` | Sounding extraction/rendering bridge and SHARPpy-style output. |
| `rustwx-wrf` | WRF NetCDF support and local WRF helpers. |

## Largest Rust Files

These files are not automatically wrong; they are the highest-risk places for
future Codex edits because unrelated concerns are close together.

| Rank | LOC | File | Responsibility summary | First safe move |
| --- | ---: | --- | --- | --- |
| 1 | 7904 | `crates/rustwx-models/src/lib.rs` | Model/source catalog, forecast-hour support, URL plans, latest-run lookup, and plot recipes. | Split catalog/source/recipe/run-discovery modules after snapshotting model catalog output. |
| 2 | 4104 | `crates/rustwx-render/src/render.rs` | Main map renderer, canvas layout, fills, overlays, barbs, labels, and PNG-facing render path. | Continue splitting rendering internals behind the crate-root facade. |
| 3 | 3681 | `crates/rustwx-products/src/mesoanalysis.rs` | Surface objective analysis, observation filtering/QC, background fields, reports, and WxStore export hooks. | Continue mechanical split with observation/report/export helpers behind existing tests. |
| 4 | 3630 | `crates/rustwx-products/src/derived.rs` | Derived product requests, shared loads, native contour mode, compute paths, and batch publication. | Continue mechanical split with render-assembly helpers. |
| 5 | 3446 | `crates/rustwx-python/src/lib.rs` | Python binding surface and Rust/Python request translation. | Split bindings by feature surface only after Rust APIs stabilize. |
| 6 | 2983 | `crates/rustwx-models/src/tests.rs` | Model catalog, recipe, source-policy, URL, blocker, and latest-run regression tests. | Split test helpers by model/source family only if future test edits become hard to localize. |
| 7 | 2574 | `crates/rustwx-cross-section/src/render.rs` | Cross-section raster rendering, terrain, colorbars, wind overlays, and labels. | Split draw primitives after product fixtures exist. |
| 8 | 2463 | `crates/rustwx-cli/src/bin/wrf_ops.rs` | WRF operational planning, namelist generation, launch/bootstrap helpers, and HTML output. | Consider a WRF CLI subcommand module after bin taxonomy settles. |
| 9 | 2351 | `crates/rustwx-cli/src/bin/rustwx_tools_site.rs` | Local tools website serving WXA plots, cross sections, soundings, and store views. | Extract reusable server/job helpers only after Studio/Python boundaries are stable. |
| 10 | 2299 | `crates/rustwx-products/src/windowed.rs` | Windowed product families such as QPF, UH, wind swaths, and temperature extrema. | Continue splitting by product family with catalog snapshot. |
| 11 | 2189 | `crates/rustwx-products/src/wxstore_wxa.rs` | WxStore WXA dense2d import/export/static plotting support. | Continue splitting format/read-write/plot modules after WXA manifest snapshots. |
| 12 | 2126 | `crates/rustwx-products/src/gridded.rs` | Model timestep loading, field decode, pressure/surface field containers, and decode-cache helpers. | Continue splitting load/decode helpers with no behavior changes; crop/domain and fetch helpers are now extracted. |
| 13 | 2057 | `crates/rustwx-cli/src/bin/forecast_now.rs` | One-shot operational orchestrator across products and lanes. | Candidate for main CLI subcommand once shared args are extracted. |
| 14 | 1994 | `crates/rustwx-io/src/lib.rs` | Fetch requests/results, probing, extraction, selector handling, and cache-facing IO helpers. | Continue splitting fetch/probe/grib modules after product characterization tests. |
| 15 | 1964 | `crates/rustwx-products/src/dataset_export.rs` | Native dataset export and training-bundle shaping. | Split plan/materialize/report helpers after manifest snapshots. |
| 16 | 1907 | `crates/rustwx-io/src/earth2_archive.rs` | AIFS/Earth2 archive member/stat selection, archive paths, and validation. | Keep with IO split but preserve archive path behavior exactly. |
| 17 | 1874 | `crates/rustwx-core/src/lib.rs` | Shared core types for grids, products, selectors, models, fields, and bundles. | Continue splitting only if public compatibility is protected by re-exports. |
| 18 | 1841 | `crates/rustwx-products/src/comparison.rs` | Product manifest/report comparison and publication deltas. | Split loader/comparison/report serialization if tests cover JSON shape. |
| 19 | 1818 | `crates/rustwx-products/src/cross_section.rs` | Product-side pressure cross-section construction from decoded model data. | Continue splitting request/support/sampling/render-bridge pieces with fixtures. |
| 20 | 1779 | `crates/rustwx-radar/src/dealias.rs` | Velocity dealiasing algorithms, quality masks, and diagnostics. | Keep semantic changes separate from future module splits. |
| 21 | 1740 | `crates/rustwx-sounding/src/native_table.rs` | Native sounding table extraction, derived columns, and SHARPpy-style handoff. | Split extraction/table-format helpers only after sounding fixtures are stable. |
| 22 | 1734 | `crates/rustwx-calc/src/mesoanalysis.rs` | Numeric mesoanalysis kernels and supporting calculations. | Continue splitting science helpers only with parity tests. |
| 23 | 1721 | `crates/rustwx-products/src/places.rs` | Place registry, geographic aliases, and product-location lookup helpers. | Split registry data/helpers only after place aliases are snapshotted. |
| 24 | 1719 | `crates/rustwx-products/src/mesoanalysis_calibration/tests.rs` | Calibration report and gate regression fixtures. | Split fixture builders only if future calibration test edits become hard to localize. |
| 25 | 1696 | `crates/rustwx-cli/src/bin/production_runner.rs` | Production batch runner and operational product lane orchestration. | Candidate for CLI subcommand/module split after bin taxonomy settles. |

## Public Surface Snapshot

### `rustwx-products`

`rustwx-products/src/lib.rs` currently exports many modules publicly:

`artifact_bundle`, `cache`, `catalog`, `comparison`, `cross_section`,
`custom_poi`, `dataset_export`, `derived`, `direct`, `ecape`, `gallery`,
`grib_ensemble`, `gridded`, `heavy`, `hrrr`, `intelligence`, `lightning`,
`mesoanalysis`, `mesoanalysis_calibration`, `named_geometry`, `native_dataset`,
`native_dataset_hrrr`, `native_dataset_materializer`, `native_dataset_obs`,
`native_dataset_shard_store`, `non_ecape`, `orchestrator`, `places`, `planner`,
`plot_design`, `point_timeseries`, `publication`, `publication_provenance`,
`runtime`, `sampling`, `satellite`, `severe`, `shared_context`, `source`,
`spec`, `thermo_native`, `volume_store`, `windowed`, `windowed_decoder`,
`wxstore_export`, `wxstore_profile`, and `wxstore_wxa`.

It also re-exports domain/shared context types from `named_geometry` and
`shared_context`.

The advisory ownership map for this broad surface now lives in
`docs/rustwx_products_public_surface.md`. That map classifies each module as
stable public, operational public, compatibility public, proof/research public,
internal-candidate public, legacy public, or crate-private. A crate-root
guardrail test checks that every declared module appears in the map and the doc.
`crates/rustwx-products/src/dataset_export/tests.rs` owns dataset-export channel
preset, metadata, NPY header, split-count, and wind-direction regression tests
while retaining parent-private access through the `#[cfg(test)] mod tests;`
child module.
`crates/rustwx-products/src/comparison/tests.rs` owns comparison relation,
input-kind, fetch-key, manifest-normalization, and material-change regression
tests with the same parent-private test-module pattern.
`crates/rustwx-products/src/mesoanalysis_calibration/tests.rs` owns calibration
report, gate, confidence reliability, station/source history, and skipped-report
regression tests with the same parent-private test-module pattern.
`crates/rustwx-products/src/native_dataset_materializer/tests.rs` owns native
dataset materializer manifest shapes, GOES product-family routing, missing-source
NaN shard emission, Level2 time-window lookup, MRMS sentinel sanitization,
validity-mask generation, derived GOES channel dependency expansion, stacked
GOES field assembly, and reflectivity threshold target regression tests with the
same parent-private test-module pattern.

Refactor implication: do not convert `pub mod` to `pub(crate) mod` until the
workspace and Python bindings are checked for each path. The first visibility PR
should classify modules, not aggressively hide them.

### `rustwx-render`

`rustwx-render` keeps most modules private and exposes a facade through `pub use`.
The only public module is `weather`; public facade items come from
`contour_fill`, `error`, `rasterize`, `features`, `panel`, `presentation`,
`projected_map`, `projection`, `render`, `request`, `rustwx_core`, `weather`,
and `colormap`.

Refactor implication: this crate already has a healthier facade pattern. Future
work should split `render.rs` internally while preserving crate-root exports.

### `rustwx-io`

`rustwx-io` has private `cache`, public `earth2_archive`, and many crate-root
public types/functions in `lib.rs`.
`crates/rustwx-io/src/earth2_archive/tests.rs` owns archive path, flat member
file, lead discovery, dewpoint, precipitation-unit, and ensemble-stat
regression tests while retaining parent-private access through the
`#[cfg(test)] mod tests;` child module.

Refactor implication: split `lib.rs` into `fetch`, `probe`, and `grib/*` only
after product fixtures prove selectors, cache paths, projection metadata, and
fallback behavior did not change.

### `rustwx-cli`

The library surface is small: `benchmark`, `cross_section_proof`, and `profile`.
The operational complexity is mostly in `src/bin`, documented separately in
`crates/rustwx-cli/src/bin/README.md`.

### `rustwx-calc`

`rustwx-calc` should stay focused on numeric weather kernels and derived
diagnostic math. `crates/rustwx-calc/src/mesoanalysis/tests.rs` owns the
surface mesoanalysis kernel regression tests while retaining parent-private
access through the `#[cfg(test)] mod tests;` child module.
`crates/rustwx-calc/src/severe/tests.rs` owns the severe diagnostic regression
tests with the same parent-private test-module pattern.

### `rustwx-radar`

`rustwx-radar` owns native radar decoding, velocity dealiasing, derived radar
products, tile generation, and sidecar/report helpers. Keep algorithm changes in
`dealias.rs` separate from tile/export/report refactors.
`crates/rustwx-radar/src/dealias/tests.rs` owns radial continuity, sweep
continuity, staged continuity, low-alias burden, candidate acceptance, velocity
quality masking, and dealiased-sweep report regression tests while retaining
parent-private access through the `#[cfg(test)] mod tests;` child module.
`crates/rustwx-radar/src/tile/tests.rs` owns Web Mercator tile math, radar
coverage bounds, prepared-sweep metadata, tile option validation, velocity and
generic-product QC summaries, product provenance, native/derived KDP/HCA
resolution, and reflectivity despeckle regression tests with the same
parent-private test-module pattern.

## Dependency-Direction Notes

No obvious `rustwx-render` -> `rustwx-products` dependency was found. That is the
most important boundary to preserve: rendering should draw prepared requests,
not fetch/decode/build products.

`rustwx-regrid` currently depends on core types and exposes reusable regrid
plans without depending on products, render, or IO. That is the desired boundary.

`rustwx-products` depends on IO, models, calc, render, cross-section, radar, WRF,
and reuses shared core types. That matches its orchestration role, but it is why
product-side module splits need characterization tests.

`rustwx-sounding` imports render helpers for SHARPpy-style PNG output. That is
reasonable for a rendering-capable sounding crate, but future splits should keep
sounding extraction/data separate from PNG presentation if the file grows.

## Proposed PR Stack

1. **Guardrails and map**
   - Add this file, CLI-bin taxonomy, and workspace check scripts.
   - Validation: `cargo fmt --all -- --check`, `cargo metadata --no-deps --format-version 1`.

2. **Public surface classification**
   - Classify every `rustwx-products` module as stable public, compatibility
     public, internal, research/proof, or legacy.
   - Do not change visibility yet unless usage evidence is clear.
   - Validation: `cargo check --workspace --all-targets`.

3. **Characterization fixtures**
   - Snapshot product catalog keys, representative direct/derived request JSON,
     mesoanalysis report schema, WXA manifest shape, and stable CLI help.
   - Validation: package-level tests that compare snapshots intentionally.
   - Current fixtures: `crates/rustwx-products/tests/fixtures/product_catalog_inventory_v1.json`
     covers product catalog lane slugs, summary counts, and representative
     direct/derived/heavy/windowed sentinel entries. `rustwx-cli` has a
     `bin_inventory` guardrail that keeps `src/bin/README.md` in sync with
     actual `src/bin/*.rs` entrypoints.
     `mesoanalysis_calibration::contract_tests::calibration_contract_serializes_gate_history_and_index_shapes`
     snapshots the calibration matrix/gate schema strings, representative gate
     check JSON, innovation-history watchlists, and WxStore index record shape.

4. **Mechanical `direct.rs` split**
   - Create `direct/` modules for catalog, selectors, request, batch, rendering
     assembly, and tests while preserving `rustwx_products::direct::*`.
   - Validation: `cargo test -p rustwx-products --lib`, `cargo check --workspace --all-targets`.
   - Current split: `crates/rustwx-products/src/direct/types.rs` owns
     request/report/runtime timing structs, output defaults, and internal
     sampled/prepared batch structs. The parent `direct` module re-exports the
     existing public type names, so external paths remain unchanged.
     `crates/rustwx-products/src/direct/planning.rs` owns recipe slug support,
     fetch grouping, selector availability partitioning, canonical fetch-family
     routing, and direct-lane execution-plan construction while preserving
     `rustwx_products::direct::FetchGroup` and
     `rustwx_products::direct::supported_direct_recipe_slugs`.
     `crates/rustwx-products/src/direct/composite.rs` owns direct composite
     panel specs, `crates/rustwx-products/src/direct/titles.rs` owns dataset
     tokens, ensemble/stat title prefixes, and GDEX title suffixing, and
     `crates/rustwx-products/src/direct/domain.rs` owns direct geographic
     bounds, crop padding, periodic longitude crops, and visible-grid span
     helpers.
     `crates/rustwx-products/src/direct/projection.rs` owns projected-map
     construction, inverse-raster projection selection, presentation projection
     variants, model-data domain frames, and projection frame/aspect helpers
     while preserving the public `rustwx_products::direct::build_projected_map`,
     `rustwx_products::direct::build_projected_map_with_projection`, and
     `rustwx_products::direct::model_data_domain_frame_for_projection` paths.
     `crates/rustwx-products/src/direct/rendering.rs` owns render request
     assembly, filled-field unit conversion, Earth2/spread scales, below-ground
     masks, contour/barb/streamline layer prep, raster/source policy, and direct
     visual-mode selection.
     `crates/rustwx-products/src/direct/query.rs` owns direct sampled-field
     fetch requirement discovery, sampled execution-plan construction,
     latest/loaded sampled-field loaders, single sampled-field loading,
     component field extraction, and direct component slug construction while
     preserving the crate-visible `rustwx_products::direct::*` query paths used
     by sampling, intelligence, dataset export, WxStore export, and WXA import.
     `crates/rustwx-products/src/direct/fetch.rs` owns planner-loaded bundle
     lookup, direct selected-field cache/decode, missing-selector reporting,
     runtime fetch timing, and input-fetch identity construction while staying
     parent-private.
     `crates/rustwx-products/src/direct/batch.rs` owns direct run resolution,
     public HRRR/generic batch entrypoints, loaded/prepared batch execution,
     direct-lane blocker assembly, and loaded execution-plan orchestration
     while the parent module re-exports the public and crate-visible entrypoint
     paths. `crates/rustwx-products/src/direct/tests.rs` owns the direct module
     regression tests while retaining parent-private access through the
     `#[cfg(test)] mod tests;` child module. The parent `direct.rs` is now
     below the owned-code hotspot table and mostly holds runtime render/output
     glue that has not yet moved.

5. **Mechanical `derived.rs` split**
   - Split recipe inventory, compute paths, contour mode, request/report, and
     batch publication.
   - Validation: derived catalog snapshot plus `rustwx-products` tests.
   - Current split: `crates/rustwx-products/src/derived/inventory.rs`
     owns supported and blocked derived recipe inventory entries and public
     inventory accessors while preserving
     `rustwx_products::derived::supported_derived_recipe_inventory` and
     `rustwx_products::derived::blocked_derived_recipe_inventory`.
     `crates/rustwx-products/src/derived/types.rs` owns public derived request,
     report, timing, blocker, native-artifact, and `NativeContourRenderMode`
     types while preserving the existing `rustwx_products::derived::*` paths.
     `crates/rustwx-products/src/derived/presentation.rs` owns derived title
     decoration, Earth2/member filename suffixes, output suffix sanitizing,
     local-WRF title detection, and GDEX dataset-token helpers.
     `crates/rustwx-products/src/derived/recipes.rs` owns the derived recipe
     enum, slug/alias parser, display slugs/titles, visual-mode classification,
     heavy-recipe flagging, and derived compute dependency requirements while
     preserving crate-visible `rustwx_products::derived::DerivedRecipe` and
     `rustwx_products::derived::derived_compute_recipes_need_pressure`.
     `crates/rustwx-products/src/derived/planning.rs` owns derived recipe
     de-duplication, native route structs, WRF/GDEX native candidate selection,
     fastest/canonical route partitioning, derived latest-run resolution, and
     derived execution-plan construction while preserving crate-visible
     `rustwx_products::derived::plan_derived_recipes`,
     `rustwx_products::derived::plan_native_thermo_routes_with_surface_product`,
     `rustwx_products::derived::PlannedDerivedSourceRoutes`, and
     `rustwx_products::derived::NativeDerivedRecipe`.
     `crates/rustwx-products/src/derived/compute.rs` owns the derived compute
     field-set traits, computed-field storage, surface-only and pressure-backed
     compute dispatcher, pressure-level slicing/interpolation, height AGL
     assembly, grid-spacing estimate, and haversine helper while preserving
     parent-visible `DerivedComputedFields`,
     `compute_derived_fields_generic`, and
     `compute_surface_only_derived_fields`.
     `crates/rustwx-products/src/derived/query.rs` owns the lightweight derived
     query field, sampled product field/set, required fetch-product helper,
     sampled execution-plan builder, loaded/latest sampled-field loaders, and
     query-field extraction while preserving crate-visible
     `rustwx_products::derived::compute_derived_query_field`,
     `rustwx_products::derived::load_derived_sampled_fields_from_latest`,
     `rustwx_products::derived::load_derived_sampled_fields_from_loaded`,
     `rustwx_products::derived::build_derived_sampled_execution_plan`, and
     `rustwx_products::derived::required_derived_fetch_products`.
     `crates/rustwx-products/src/derived/tests.rs` owns the derived module
     regression tests while retaining parent-private access through the
     `#[cfg(test)] mod tests;` child module.

6. **Mechanical `mesoanalysis_calibration.rs` split**
   - Split schema types, gate evaluation, innovation history/index export,
     parsing, and aggregation while preserving
     `rustwx_products::mesoanalysis_calibration::*` paths and all JSON shapes.
   - Validation: calibration contract snapshot plus `rustwx-products` tests.
   - Current split: `crates/rustwx-products/src/mesoanalysis_calibration/types.rs`
     owns the public calibration report/case/aggregate, innovation history,
     query, WxStore index, and calibration gate schema structs. The parent
     `mesoanalysis_calibration` module re-exports those names so existing
     external paths remain unchanged.
     `crates/rustwx-products/src/mesoanalysis_calibration/gates.rs` owns the
     public calibration gate evaluator, gate metric selectors, threshold-check
     builders, and confidence-reliability gate helper while preserving
     `rustwx_products::mesoanalysis_calibration::evaluate_surface_mesoanalysis_calibration_gate`.
     `crates/rustwx-products/src/mesoanalysis_calibration/history.rs` owns
     innovation history construction/read/merge/write/query, WxStore index
     export, JSONL writing, watchlist builders, history aggregate refresh, and
     history case key/sort helpers while preserving the public
     `rustwx_products::mesoanalysis_calibration::*` history, query, and WxStore
     index paths.
     `crates/rustwx-products/src/mesoanalysis_calibration/parsing.rs` owns
     calibration report construction from run-report JSON values, run schema
     selection, benchmark/external-reference/covariance-ablation parsing,
     validation sample/source/stratum extraction, and parsing-only station and
     stratum key helpers while preserving the public
     `rustwx_products::mesoanalysis_calibration::build_surface_mesoanalysis_calibration_report`
     path and test helper coverage through the parent module.
     `crates/rustwx-products/src/mesoanalysis_calibration/confidence.rs` owns
     confidence case parsing, case-level confidence rollups, ranked-confidence
     reliability status construction, confidence aggregate accumulation, and
     reliability aggregate construction while preserving all confidence and
     calibration gate JSON schema strings.
     `crates/rustwx-products/src/mesoanalysis_calibration/aggregation.rs` owns
     calibration matrix aggregate construction, quality flag derivation,
     diagnostic/source/stratum/station/reference/ablation/domain variable
     aggregate accumulators, and exposes only the small source/station
     aggregate helpers needed by innovation history while preserving
     calibration aggregate JSON shapes.
     `crates/rustwx-products/src/mesoanalysis_calibration/summaries.rs` owns
     validation source/stratum/station summary accumulators, source variable
     summary rollups, and station variable stats accumulation while exposing
     only sibling-visible helper types needed by parsing and aggregation.
     `crates/rustwx-products/src/mesoanalysis_calibration/helpers.rs` owns
     shared JSON path accessors, case-tag normalization, count/key builders,
     finite numeric collection helpers, weighted mean/RMSE helpers, and
     `Option<f64>` delta/comparison helpers while staying sibling-private and
     unre-exported by the parent module.
     `crates/rustwx-products/src/mesoanalysis_calibration/tests.rs` owns the
     calibration report and gate regression tests while retaining
     parent-private access through the `#[cfg(test)] mod tests;` child module.
     The parent `mesoanalysis_calibration.rs` now owns the public file
     discovery/read/write facade, split-module declarations/re-exports, and
     crate-private `run_report.json` directory traversal.

7. **Mechanical `mesoanalysis.rs` split**
   - Split config, observations, background, objective analysis, validation,
     report, and WxStore export hooks.
   - Validation: report schema/default config/representative analysis tests.
   - Current split: `crates/rustwx-products/src/mesoanalysis/tests.rs` owns
     the surface mesoanalysis regression tests while retaining parent-private
     access through the `#[cfg(test)] mod tests;` child module.

8. **`rustwx-models/src/lib.rs` split**
   - Split model catalog, source policy, URL construction, latest-run probing,
     forecast-hour support, and plot recipes.
   - Validation: model catalog and URL plan snapshots.
   - Current split: `crates/rustwx-models/src/tests.rs` owns the model
     catalog, recipe, source-policy, URL, blocker, and latest-run regression
     tests while retaining parent-private access through the
     `#[cfg(test)] mod tests;` child module.

9. **`rustwx-render/src/render.rs` split**
   - Split canvas/layout, fill rendering, contours, barbs, labels, and PNG
     helpers behind the existing facade.
   - Validation: canonical render request fixtures and image/manifest checks
     once fixtures are stable.
   - Current split: `crates/rustwx-render/src/render/tests.rs` owns the
     render regression tests while retaining parent-private access through the
     `#[cfg(test)] mod tests;` child module.

10. **`rustwx-io/src/lib.rs` split**
    - Split fetch/probe/grib selector/extraction/projection/batch modules while
      preserving crate-root public exports.
    - Validation: IO lib tests, product characterization tests, workspace check.
    - Current split: `crates/rustwx-io/src/tests.rs` owns the IO
      fetch/cache/GRIB regression tests while retaining parent-private access
      through the `#[cfg(test)] mod tests;` child module.

11. **`rustwx-core/src/lib.rs` split**
    - Split shared type families only when public compatibility is protected by
      the existing crate-root paths and re-exports.
    - Validation: core lib tests, workspace check.
    - Current split: `crates/rustwx-core/src/tests.rs` owns the core grid,
      selector, metadata, bundle, sampling, and polygon-summary regression
      tests while retaining parent-private access through the
      `#[cfg(test)] mod tests;` child module.

12. **Mechanical `windowed.rs` split**
    - Split windowed QPF, UH, wind-swath, temperature, RH, dewpoint, and VPD
      families while preserving `rustwx_products::windowed::*` public paths.
    - Validation: windowed tests, product characterization tests, workspace
      check.
    - Current split: `crates/rustwx-products/src/windowed/tests.rs` owns the
      windowed planning, fetch-lineage, render-request, and fixed-window label
      regression tests while retaining parent-private access through the
      `#[cfg(test)] mod tests;` child module.

13. **Mechanical `cross_section.rs` split**
    - Split request/support/sampling/render-bridge pieces while preserving
      `rustwx_products::cross_section::*` public paths and cross-section facts.
    - Validation: cross-section tests, product characterization tests,
      workspace check.
    - Current split: `crates/rustwx-products/src/cross_section/tests.rs` owns
      the product-side cross-section fixture builders, pressure-section facts,
      optional-input, product-value, and sample-stencil regression tests while
      retaining parent-private access through the `#[cfg(test)] mod tests;`
      child module.

14. **Mechanical `gridded.rs` split**
    - Split crop/domain helpers, loading, fetch/cache, and decode paths while
      preserving `rustwx_products::gridded::*` public compatibility paths.
    - Validation: gridded, heavy, direct, derived, and windowed product tests.
    - Current split: `crates/rustwx-products/src/gridded/crop.rs` owns
      `GridCrop`, `ProjectedGridIntersection`, `CroppedHeavyDomain`,
      geographic/projected heavy-domain crop classification, lat/lon and
      scalar crop helpers, and parent-visible cropped decode-cache and
      surface/pressure crop helpers while preserving the public
      `rustwx_products::gridded::*` crop paths used by heavy, derived,
      direct, and windowed lanes.
      `crates/rustwx-products/src/gridded/fetch.rs` owns canonical
      surface/pressure bundle resolution, fetch request construction,
      surface/pressure parallel fetch orchestration, shared fetch-pattern
      policy, RAP whole-family safeguards, and HRRR optional pressure-pattern
      selection while preserving crate-visible
      `rustwx_products::gridded::*` fetch helpers used by derived, runtime,
      and windowed lanes.
      `crates/rustwx-products/src/gridded/tests.rs` owns gridded bundle
      routing, fetch-pattern, optional pressure-field policy, decode-cache-name,
      and projected-crop regression tests while retaining parent-private access
      through the `#[cfg(test)] mod tests;` child module.

15. **Mechanical `wxstore_wxa.rs` split**
    - Split WXA dense2d format, geometry, plot-style, read/write, and static
      plotting helpers while preserving WXA metadata, index, and PNG behavior.
    - Validation: WXA tests, product characterization tests, workspace check.
    - Current split: `crates/rustwx-products/src/wxstore_wxa/tests.rs` owns
      WXA regular-latlon geometry, run subtitle, product style, and component
      filtering regression tests while retaining parent-private access through
      the `#[cfg(test)] mod tests;` child module.

16. **Mechanical `non_ecape.rs` split**
    - Split request/report schemas, hour preparation, domain runner, summary,
      publication manifest, and WxStore build reporting while preserving
      `rustwx_products::non_ecape::*` public paths and JSON shapes.
    - Validation: non-ECAPE tests, comparison report tests, product catalog
      inventory, and workspace check.
    - Current split: `crates/rustwx-products/src/non_ecape/types.rs` owns
      HRRR-specific and generic non-ECAPE request/report/timing/summary/build
      report structs, output/default serialization helpers, and the
      compatibility type aliases while the parent module re-exports all public
      names to preserve existing `rustwx_products::non_ecape::*` paths.
      `crates/rustwx-products/src/non_ecape/summary.rs` owns non-ECAPE
      output-summary flattening and static-domain/static-product build timing
      rollups while staying parent-private.
      `crates/rustwx-products/src/non_ecape/manifest.rs` owns non-ECAPE
      publication manifest planning, direct/derived/windowed artifact state
      updates, windowed artifact detail strings, and input-fetch lineage
      collection while staying parent-private.
      `crates/rustwx-products/src/non_ecape/tests.rs` owns non-ECAPE
      validation, execution-plan, manifest-lineage, summary, and report
      serialization regression tests while retaining parent-private access
      through the `#[cfg(test)] mod tests;` child module.

## Standard Validation Commands

Use the scripts added in this guardrail pass for the strict target state:

```powershell
.\scripts\check_workspace.ps1
```

```bash
./scripts/check_workspace.sh
```

The strict script is the desired default. The skip flags are kept only for
temporary diagnosis if a future branch needs to separate build failures from
format drift or package-specific regressions:

```powershell
.\scripts\check_workspace.ps1 -SkipFmt -SkipKnownFailingPackageTests
```

```bash
./scripts/check_workspace.sh --skip-fmt --skip-known-failing-package-tests
```

For smaller module-split PRs, at minimum run:

```powershell
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test -p rustwx-products --lib
cargo test -p rustwx-products --test product_catalog_inventory
cargo test -p rustwx-cli --test bin_inventory
```
