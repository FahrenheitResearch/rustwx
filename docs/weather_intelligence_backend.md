# Weather Intelligence Backend Notes

This backend pass already has usable typed contracts in the repo. The main
surfaces are split across `rustwx-core`, `rustwx-products`, and a thin HRRR
tool runner in `rustwx-cli` instead of a single monolithic module.

## Current architecture

Primary files:

- `crates/rustwx-core/src/lib.rs`
  - `FieldSelector`: structured weather query identity
  - `ProductKeyMetadata`: human-readable metadata attached to typed products
  - `BundleRequirement`: typed fetch/decode requirement
  - `CanonicalBundleId`: deduped runtime bundle identity
- `crates/rustwx-products/src/planner.rs`
  - `ExecutionPlan`: deduped bundle plan assembled from product requirements
- `crates/rustwx-products/src/runtime.rs`
  - `LoadedBundleSet`: fetched and decoded bundle payloads, keyed by planner identity
- `crates/rustwx-products/src/thermo_native.rs`
  - `compare_native_vs_derived`: backend comparison primitive for native-vs-derived validation
- `crates/rustwx-products/src/intelligence.rs`
  - point samples, area summaries, and bounded field-to-field comparisons over named assets
- `crates/rustwx-products/src/named_geometry.rs`
  - built-in regions, metros, routes, and externally loadable watch-area catalogs
- `crates/rustwx-products/src/artifact_bundle.rs`
  - reusable bundle manifests for PNG + JSON + provenance packaging
- `crates/rustwx-products/src/publication.rs`
  - `RunPublicationManifest`, `PublishedFetchIdentity`, `PublishedArtifactRecord`: artifact bundle and provenance contract
- `crates/rustwx-products/src/shared_context.rs`
  - `Solar07PanelField::artifact_slug`: named-asset override used when an output file name should stay stable even if the display title changes
- `crates/rustwx-cli/src/bin/hrrr_weather_tools.rs`
  - one public HRRR-oriented backend CLI surface for listing assets, querying fields, comparing areas, summarizing routes, and emitting bundle manifests

## Public backend runner

The current operator-facing entry point is:

```text
cargo run -p rustwx-cli --bin hrrr_weather_tools -- <subcommand> ...
```

Current subcommands:

- `list-assets`
- `point-sample`
- `area-summary`
- `compare-area`
- `route-summary`
- `bundle-derived-map`
- `bundle-cross-section`

## Structured weather queries

The selector path is the current typed query surface. A selector gives the
backend a stable field identity, a product key, and native units without
needing product-specific string parsing in downstream code.

```rust
use rustwx_core::{
    BundleRequirement, CanonicalBundleDescriptor, CanonicalField, FieldSelector,
};

let selector = FieldSelector::isobaric(CanonicalField::Temperature, 500);
assert_eq!(selector.key(), "temperature_500hpa");
assert_eq!(selector.display_name(), "Temperature (500hpa)");
assert_eq!(selector.native_units(), "K");

let requirement = BundleRequirement::new(CanonicalBundleDescriptor::PressureAnalysis, 12)
    .with_native_override("pgrb2.0p25");
assert_eq!(requirement.forecast_hour, 12);
assert_eq!(requirement.native_override.as_deref(), Some("pgrb2.0p25"));
```

Operationally, the planner converts `BundleRequirement` values into
`CanonicalBundleId` entries, dedupes them, and hands the runtime a typed
`ExecutionPlan`.

## Comparison primitives

Native-vs-derived verification already has a structured comparison contract.
`compare_native_vs_derived` returns a `NativeDerivedComparisonStats` payload
with domain summaries plus a triage verdict.

```rust
use rustwx_products::thermo_native::{
    NativeComparisonVerdict, compare_native_vs_derived,
};

let native = vec![1.0, 2.0, 3.0, 4.0];
let derived = vec![1.2, 2.2, 3.2, 4.2];
let stats = compare_native_vs_derived("lifted_index", &native, &derived).unwrap();

assert_eq!(stats.verdict, NativeComparisonVerdict::Pass);
assert_eq!(stats.valid_points, 4);
assert!(stats.mean_abs_diff <= 1.0);
```

This is the right seam for backend verification because it exposes:

- summary stats for both fields
- filtered valid-point counts
- correlation and error magnitudes
- a machine-readable verdict (`pass`, `review`, `reject`)
- a human-readable `verdict_reason`

## Artifact bundles

The publication manifest is the current bundle contract between execution and
downstream consumers. Each run records the fetched inputs plus every artifact
that was expected or produced.

```rust
use std::path::PathBuf;

use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, RunPublicationManifest,
};

let mut manifest = RunPublicationManifest::new(
    "weather_intelligence_demo",
    "demo_run",
    PathBuf::from("proof/demo"),
)
.with_artifacts(vec![
    PublishedArtifactRecord::planned("sbcape_native", "sbcape_native.png")
        .with_state(ArtifactPublicationState::Complete)
        .with_input_fetch_keys(vec!["native:gfs:f012".into()]),
]);

manifest.finalize_from_artifact_states();
assert_eq!(manifest.artifacts.len(), 1);
```

Important bundle fields for integration:

- `PublishedFetchIdentity::planned_family_aliases`
  - lets one physical fetch advertise every logical family it served
- `PublishedArtifactRecord::input_fetch_keys`
  - ties each artifact back to its upstream fetch inputs
- `ArtifactContentIdentity`
  - gives downstream storage or catalogs a stable byte identity

## Named assets

`Solar07PanelField` is the current named-asset primitive. It separates
user-facing titles from artifact keys, which keeps bundle naming stable across
copy tweaks or display-title changes.

```rust
use rustwx_products::Solar07PanelField;
use rustwx_render::Solar07Product;

let field = Solar07PanelField::new(Solar07Product::Scp, "dimensionless", vec![1.0])
    .with_title_override("SCP (ML proxy)")
    .with_artifact_slug("scp_ml_proxy");

assert_eq!(field.display_title(), "SCP (ML proxy)");
assert_eq!(field.artifact_slug(), "scp_ml_proxy");
```

## Test coverage added with this pass

Focused tests now cover:

- stable typed query and bundle identity through public APIs
- comparison verdict separation for `pass` vs `review` vs `reject`
- non-finite filtering in comparison stats
- manifest round-tripping for artifact bundle fetch aliases and named assets
- default and overridden named-asset slug behavior
