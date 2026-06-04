# `rustwx-products` Public Surface

This is an advisory ownership map for the crate-root modules in
`crates/rustwx-products/src/lib.rs`. It does not change visibility. Its job is
to make future refactors safer by saying which exports are product facades,
which are compatibility surfaces, and which are candidates for internalization.

## Review Rule

Before changing any `pub mod` to `pub(crate) mod`, audit workspace usage, CLI
usage, Python bindings, and any downstream integrations. Then update this file,
the crate-root public-surface guardrail tests, and the validation evidence for
the branch.

## Surface Kinds

| Kind | Meaning |
| --- | --- |
| Stable public | Intended facade or shared domain API. Keep paths stable unless there is an explicit migration plan. |
| Operational public | Public because operational product workflows call through it. Splits should preserve existing paths first. |
| Compatibility public | Broad export kept for existing callers while consumers are audited. Candidate for a facade later. |
| Proof/research public | Useful for experiments, diagnostics, or research workflows. Stabilize only after product direction is clear. |
| Internal-candidate public | Currently public, but likely an implementation detail once usage is proven. |
| Legacy public | Public compatibility for older or model-specific paths. Avoid expanding this surface. |
| Crate-private | Already private to `rustwx-products`. Keep private unless a facade needs to expose it. |

## Module Map

| Module | Kind | Notes |
| --- | --- | --- |
| `agent_backend` | Stable public | Agent-facing preflight/orchestration contract for product capability, status, lane, and cost hints. |
| `artifact_bundle` | Compatibility public | Bundle helper surface kept public for current artifact workflows. |
| `cache` | Compatibility public | Cache plumbing is exported broadly; future work should prefer narrower facades. |
| `catalog` | Stable public | Product catalog facade. |
| `comparison` | Operational public | Publication and manifest comparison workflow. |
| `cross_section` | Stable public | Product-side cross-section construction facade. |
| `custom_poi` | Internal-candidate public | Point-of-interest support that should be audited before remaining public. |
| `dataset_export` | Operational public | Native dataset and training-bundle export workflow. |
| `derived` | Stable public | Main derived-product facade. Preserve `rustwx_products::derived::*` during splits. |
| `direct` | Stable public | Main direct-product facade. Preserve `rustwx_products::direct::*` during splits. |
| `ecape` | Operational public | Operational ECAPE product lane. |
| `gallery` | Proof/research public | Gallery/proof output support. |
| `grib_ensemble` | Operational public | Ensemble GRIB product workflow. |
| `gridded` | Compatibility public | Shared decoded-grid containers and loaders exported for existing callers. |
| `heavy` | Operational public | Heavy precipitation product lane. |
| `hrrr` | Legacy public | HRRR-specific compatibility surface. Prefer model-generic APIs for new work. |
| `intelligence` | Proof/research public | Experimental intelligence/reporting lane. |
| `lightning` | Proof/research public | Lightning-oriented experimental lane. |
| `mesoanalysis` | Proof/research public | Surface analysis workflow with operational value but still high-risk and research-heavy. |
| `mesoanalysis_calibration` | Proof/research public | Calibration and reliability workflow; split only with schema guards. |
| `named_geometry` | Stable public | Named domain/point/geometry facade and crate-root re-export source. |
| `native_dataset` | Operational public | Native dataset product lane. |
| `native_dataset_hrrr` | Internal-candidate public | HRRR-specific native dataset implementation detail candidate. |
| `native_dataset_materializer` | Internal-candidate public | Materialization implementation detail candidate. |
| `native_dataset_obs` | Operational public | Observation-backed native dataset lane. |
| `native_dataset_shard_store` | Operational public | Shard store support for native datasets. |
| `non_ecape` | Operational public | Multi-model non-ECAPE orchestration lane. |
| `orchestrator` | Operational public | Product workflow orchestration. |
| `places` | Compatibility public | Place-label catalog and overlay support exported for current callers. |
| `planner` | Compatibility public | Planning surface kept public for existing workflows. |
| `plot_design` | Internal-candidate public | Design/layout support likely belongs behind product facades. |
| `point_timeseries` | Operational public | Point time-series workflow. |
| `publication` | Operational public | Publication manifest/report/output workflow. |
| `publication_provenance` | Compatibility public | Provenance helper surface for current publication callers. |
| `qpf` | Crate-private | Internal QPF helper module. |
| `runtime` | Compatibility public | Runtime helper surface kept public while consumers are audited. |
| `sampling` | Stable public | Shared sampling facade. |
| `satellite` | Operational public | Satellite product lane. |
| `severe` | Operational public | Severe-weather product lane. |
| `shared_context` | Stable public | Shared projected context/panel facade and crate-root re-export source. |
| `source` | Compatibility public | Source helper surface exported for current product callers. |
| `spec` | Compatibility public | Product specification helpers. |
| `thermo_native` | Proof/research public | Thermodynamic native workflow that needs characterization before stabilization. |
| `volume_store` | Stable public | Volume-store facade. Keep distinct from operational `wxstore_*` lanes. |
| `windowed` | Operational public | Windowed product families such as QPF, UH, wind swaths, and extrema. |
| `windowed_decoder` | Internal-candidate public | Decode implementation support that should sit behind product facades later. |
| `wxstore_export` | Operational public | WxStore export workflow. |
| `wxstore_profile` | Operational public | WxStore profile workflow. |
| `wxstore_wxa` | Operational public | WxStore/WXA import, export, and plotting workflow. |

## Refactor Implications

Treat `agent_backend`, `direct`, `derived`, `cross_section`, `catalog`,
`sampling`, `named_geometry`, `shared_context`, and `volume_store` as the first
crate-root paths to preserve during mechanical splits.

Treat `wxstore_export`, `wxstore_profile`, and `wxstore_wxa` as operational
lanes, not replacements for `volume_store`. They can use or publish store-shaped
artifacts without owning the core volume-store API.

When splitting large files, prefer directory modules that re-export the same
crate paths first. Visibility tightening should be a later branch with usage
evidence and migration notes.
