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
| 1 | 10900 | `crates/rustwx-models/src/lib.rs` | Model/source catalog, forecast-hour support, URL plans, latest-run lookup, and plot recipes. | Split catalog/source/recipe/run-discovery modules after snapshotting model catalog output. |
| 2 | 7056 | `crates/rustwx-products/src/mesoanalysis_calibration.rs` | Surface mesoanalysis calibration aggregation, gates, confidence reliability, innovation history, and watchlists. | Mechanical module directory split with schema snapshots first. |
| 3 | 6896 | `crates/rustwx-products/src/derived.rs` | Derived product requests, recipe inventory, shared loads, native contour mode, and batch publication. | Snapshot product keys/selectors, then split catalog/request/batch/render-assembly. |
| 4 | 5307 | `crates/rustwx-render/src/render.rs` | Main map renderer, canvas layout, fills, overlays, barbs, labels, and PNG-facing render path. | Split rendering internals only after render-request fixtures exist. |
| 5 | 4841 | `crates/rustwx-products/src/mesoanalysis.rs` | Surface objective analysis, observation filtering/QC, background fields, reports, and WxStore export hooks. | Mechanical split with report-schema and default-config tests. |
| 6 | 3803 | `crates/rustwx-products/src/direct.rs` | Direct product recipe inventory, fetch/extract planning, projected map assembly, and batch reports. | Continue mechanical split preserving `rustwx_products::direct::*`. |
| 7 | 3451 | `crates/rustwx-python/src/lib.rs` | Python binding surface and Rust/Python request translation. | Split bindings by feature surface only after Rust APIs stabilize. |
| 8 | 3373 | `crates/rustwx-io/src/lib.rs` | Fetch requests/results, probing, extraction, selector handling, and cache-facing IO helpers. | Split fetch/probe/grib modules after product characterization tests. |
| 9 | 3194 | `crates/rustwx-products/src/non_ecape.rs` | Multi-model non-ECAPE orchestration across direct, derived, windowed, and WxStore lanes. | Split request/report/hour runner/publication once direct/derived are clearer. |
| 10 | 3021 | `crates/rustwx-products/src/gridded.rs` | Model timestep loading, field decode, cropping, and pressure/surface field containers. | Extract loading/cropping/pressure containers with no behavior changes. |
| 11 | 2574 | `crates/rustwx-cross-section/src/render.rs` | Cross-section raster rendering, terrain, colorbars, wind overlays, and labels. | Split draw primitives after product fixtures exist. |
| 12 | 2565 | `crates/rustwx-products/src/windowed.rs` | Windowed product families such as QPF, UH, wind swaths, and temperature extrema. | Split by product family with catalog snapshot. |
| 13 | 2557 | `crates/rustwx-core/src/lib.rs` | Shared core types for grids, products, selectors, models, fields, and bundles. | Split only if public compatibility is protected by re-exports. |
| 14 | 2463 | `crates/rustwx-cli/src/bin/wrf_ops.rs` | WRF operational planning, namelist generation, launch/bootstrap helpers, and HTML output. | Consider a WRF CLI subcommand module after bin taxonomy settles. |
| 15 | 2460 | `crates/rustwx-calc/src/mesoanalysis.rs` | Numeric mesoanalysis kernels and supporting calculations. | Defer until product-side mesoanalysis is split and tested. |
| 16 | 2351 | `crates/rustwx-cli/src/bin/rustwx_tools_site.rs` | Local tools website serving WXA plots, cross sections, soundings, and store views. | Extract reusable server/job helpers only after Studio/Python boundaries are stable. |
| 17 | 2347 | `crates/rustwx-products/src/cross_section.rs` | Product-side pressure cross-section construction from decoded model data. | Split request/support/sampling/render-bridge pieces with fixtures. |
| 18 | 2284 | `crates/rustwx-products/src/wxstore_wxa.rs` | WxStore WXA dense2d import/export/static plotting support. | Split format/read-write/plot modules after WXA manifest snapshots. |
| 19 | 2137 | `crates/rustwx-radar/src/dealias.rs` | Velocity dealiasing algorithms, quality masks, and diagnostics. | Keep semantic changes separate from any module split. |
| 20 | 2116 | `crates/rustwx-products/src/dataset_export.rs` | Native dataset export and training-bundle shaping. | Split plan/materialize/report helpers after manifest snapshots. |
| 21 | 2080 | `crates/rustwx-io/src/earth2_archive.rs` | AIFS/Earth2 archive member/stat selection, archive paths, and validation. | Keep with IO split but preserve archive path behavior exactly. |
| 22 | 2057 | `crates/rustwx-cli/src/bin/forecast_now.rs` | One-shot operational orchestrator across products and lanes. | Candidate for main CLI subcommand once shared args are extracted. |
| 23 | 2043 | `crates/rustwx-products/src/comparison.rs` | Product manifest/report comparison and publication deltas. | Split loader/comparison/report serialization if tests cover JSON shape. |
| 24 | 1906 | `crates/rustwx-radar/src/tile.rs` | Radar tile generation and related render/report helpers. | Defer until radar export/tile CLI lanes are classified. |
| 25 | 1853 | `crates/rustwx-calc/src/severe.rs` | Severe-weather diagnostic calculations. | Defer; science kernels should move only with parity tests. |

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

Refactor implication: split `lib.rs` into `fetch`, `probe`, and `grib/*` only
after product fixtures prove selectors, cache paths, projection metadata, and
fallback behavior did not change.

### `rustwx-cli`

The library surface is small: `benchmark`, `cross_section_proof`, and `profile`.
The operational complexity is mostly in `src/bin`, documented separately in
`crates/rustwx-cli/src/bin/README.md`.

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
     helpers while preserving the crate-visible
     `rustwx_products::direct::is_global_scale_domain` path.
     `crates/rustwx-products/src/direct/projection.rs` owns projected-map
     construction, inverse-raster projection selection, presentation projection
     variants, model-data domain frames, and projection frame/aspect helpers
     while preserving the public `rustwx_products::direct::build_projected_map`,
     `rustwx_products::direct::build_projected_map_with_projection`, and
     `rustwx_products::direct::model_data_domain_frame_for_projection` paths.

5. **Mechanical `derived.rs` split**
   - Split recipe inventory, compute paths, contour mode, request/report, and
     batch publication.
   - Validation: derived catalog snapshot plus `rustwx-products` tests.

6. **Mechanical `mesoanalysis.rs` split**
   - Split config, observations, background, objective analysis, validation,
     report, and WxStore export hooks.
   - Validation: report schema/default config/representative analysis tests.

7. **Mechanical `mesoanalysis_calibration.rs` split**
   - Split types, aggregate, gates, confidence reliability, innovation history,
     watchlists, and IO.
   - Validation: schema/gate/history snapshots.

8. **`rustwx-models/src/lib.rs` split**
   - Split model catalog, source policy, URL construction, latest-run probing,
     forecast-hour support, and plot recipes.
   - Validation: model catalog and URL plan snapshots.

9. **`rustwx-render/src/render.rs` split**
   - Split canvas/layout, fill rendering, contours, barbs, labels, and PNG
     helpers behind the existing facade.
   - Validation: canonical render request fixtures and image/manifest checks
     once fixtures are stable.

10. **`rustwx-io/src/lib.rs` split**
    - Split fetch/probe/grib selector/extraction/projection/batch modules while
      preserving crate-root public exports.
    - Validation: IO lib tests, product characterization tests, workspace check.

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
