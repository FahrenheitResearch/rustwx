# rustwx-cli Bin Taxonomy

This directory contains stable commands, proof lanes, diagnostics, export tools,
and legacy aliases. The first refactor rule is: do not delete, rename, or change
flags in this directory during mechanical cleanup.

## Lane Meanings

| Lane | Meaning |
| --- | --- |
| Stable user command | Intended for regular users or agent-facing workflows. |
| Proof / research | Exercises product/render/science lanes or explores new behavior. |
| Diagnostic / debug | Inspectors, probes, benchmarks, profilers, and one-off troubleshooting. |
| CI smoke gate | Validation commands that can fail a repeatable gate. |
| Publication / export | Writes artifacts, datasets, tiles, stores, manifests, or publication companions. |
| Legacy alias | Compatibility entrypoint kept for old scripts or muscle memory. |
| Candidate subcommand | Should probably become a main CLI subcommand later, but is kept as-is for now. |

## Inventory

| Binary | Lane | Notes |
| --- | --- | --- |
| `agent_preflight` | Stable user command | Emits product capability, support/blocker, execution-lane, and cost-hint JSON for agent apps. |
| `cache_warm` | Candidate subcommand | Warms planner/runtime fetch and decode caches without rendering. |
| `cross_section_proof` | Proof / research | Generates projected cross-section proof artifacts. |
| `debug_selector_grid` | Diagnostic / debug | Selector/grid inspection helper. |
| `derived_batch` | Stable user command | Model-agnostic derived product batch lane. |
| `direct_batch` | Stable user command | Model-agnostic direct/native product batch lane. |
| `forecast_now` | Candidate subcommand | One-shot operational multi-model/multi-hour orchestrator. |
| `goes_native_sequence` | Publication / export | Discovers, caches, and renders native-grid GOES ABI crops. |
| `goes_satellite_batch` | Publication / export | Discovers, caches, and renders NOAA GOES/GLM products. |
| `goes_satellite_preview` | Proof / research | Renders one GOES ABI file through the map pipeline. |
| `goes_satellite_rgb_preview` | Proof / research | Renders raw GOES ABI RGB composites. |
| `goes_web_tiles` | Publication / export | Renders GOES GeoColor Web Mercator tiles. |
| `grib_ensemble_reduce` | Publication / export | Fetches member GRIB files, reduces a direct recipe, and renders ensemble-stat output. |
| `grib_field_probe` | Diagnostic / debug | Probes GRIB fields/selectors. |
| `heavy_panel_hour` | Proof / research | Exercises severe and ECAPE heavy-product panel generation. |
| `hrrr_batch` | Legacy alias | HRRR-specific heavy map batch lane. |
| `hrrr_capability_inventory` | Diagnostic / debug | Inventories HRRR map/cross-section capability. |
| `hrrr_dataset_export` | Publication / export | Exports wxtrain-compatible HRRR dataset bundles. |
| `hrrr_derived_batch` | Legacy alias | HRRR-specific derived batch alias for older scripts. |
| `hrrr_direct_batch` | Legacy alias | HRRR-specific direct batch alias for older scripts. |
| `hrrr_ecape_grid_research` | Proof / research | Full-grid ECAPE research statistics. |
| `hrrr_ecape_profile_probe` | Diagnostic / debug | Extracts a column and computes ECAPE layer diagnostics. |
| `hrrr_ecape_ratio_display` | Proof / research | ECAPE/CAPE display comparison plots. |
| `hrrr_major_city_hour` | Proof / research | CONUS plus major-city crop proof lane. |
| `hrrr_mdt_archive_ingest` | Publication / export | Builds event-scoped archive metadata and pressure stores for MDT/HIGH days. |
| `hrrr_mesoanalysis` | Legacy alias | Legacy alias for `surface_mesoanalysis`. |
| `hrrr_native_proof` | Proof / research | Main HRRR weather-native proof suite. |
| `hrrr_non_ecape_hour` | Legacy alias | HRRR-specific non-ECAPE hour lane. |
| `hrrr_place_label_density_compare` | Proof / research | Place-label density comparison proof. |
| `hrrr_place_label_proof` | Proof / research | Region/metro place-label proof set. |
| `hrrr_pressure_volume_store` | Publication / export | Builds cropped pressure VolumeStore artifacts. |
| `hrrr_region_city_gallery` | Proof / research | Region and city crop validation galleries. |
| `hrrr_severe_proof` | Proof / research | HRRR severe fixed-depth diagnostic proof maps. |
| `hrrr_temperature_xsection` | Proof / research | HRRR cross-section proof runner. |
| `hrrr_us_region_hours` | Proof / research | Multi-hour HRRR region proof output. |
| `hrrr_weather_tools` | Candidate subcommand | Structured backend tools for named assets, field queries, comparisons, and bundles. |
| `hrrr_windowed_batch` | Proof / research | HRRR windowed QPF/UH/wind/temperature product batch lane. |
| `model_wxprofile_store` | Publication / export | Builds native WxStore `.wxp` profile stores. |
| `named_geometry` | Stable user command | Lists and queries named geometry assets. |
| `native_dataset_plan` | Publication / export | Writes native dataset orchestration plans. |
| `native_dataset_runner` | Publication / export | Runs native dataset plans in dry-run or materialization mode. |
| `native_dataset_shard_export` | Publication / export | Initializes native training shard stores. |
| `native_obs_preview` | Proof / research | Quicklook PNGs from GOES, MRMS, or NEXRAD files. |
| `non_ecape_hour` | Stable user command | All-model non-ECAPE hour pass. |
| `plot_recipe_proof` | Proof / research | Selector-backed atmospheric proof plot. |
| `png_web_tiles` | Publication / export | Cuts geographic PNGs into transparent web-map tiles. |
| `product_catalog` | Stable user command | Emits supported product catalog JSON. |
| `product_compare` | Stable user command | Compares product manifests/reports. |
| `product_sampling` | Stable user command | Machine-readable point/area product sampling. |
| `production_runner` | Candidate subcommand | Operational scheduler skeleton. |
| `proof_gallery` | Publication / export | Builds static proof gallery from manifests/catalog. |
| `radar_benchmark_gate` | CI smoke gate | Validates radar tile benchmark manifests against speed gates. |
| `radar_coverage_map` | Proof / research | First-order NEXRAD coverage map. |
| `radar_dealias_compare` | Diagnostic / debug | Compares velocity dealiasing methods. |
| `radar_export` | Publication / export | Renders NEXRAD Level-II PNG and feature JSON. |
| `radar_export_batch` | Publication / export | Plans batched NEXRAD tensor exports. |
| `radar_quality_gate` | CI smoke gate | Validates radar tile manifests against quality gates. |
| `radar_sidecar_sample` | Diagnostic / debug | Samples NEXRAD polar sidecars at lat/lon. |
| `radar_web_tiles` | Publication / export | Renders NEXRAD Level-II web-map tiles. |
| `rustwx_grid_export` | Publication / export | Exports product grids as WxStore-importable manifests. |
| `rustwx_tools_site` | Candidate subcommand | Serves local RustWx tools website. |
| `severe_batch` | Stable user command | Shared severe map product batch lane. |
| `sounding_plot` | Stable user command | Renders native Rust SHARPpy-style model soundings. |
| `static_plot_webp_publish` | Publication / export | Generates WebP companions and manifest updates. |
| `style_proof` | Proof / research | Generates real-map style comparison sets. |
| `surface_mesoanalysis` | Stable user command | Model-agnostic surface objective-analysis lane. |
| `surface_mesoanalysis_calibration` | Stable user command | Aggregates mesoanalysis reports into calibration matrices/gates. |
| `volume_store_cross_section_render` | Stable user command | Renders cross-section PNGs from pressure VolumeStore data. |
| `volume_store_dashboard` | Candidate subcommand | Serves a local dashboard for VolumeStore inspection. |
| `volume_store_profile` | Diagnostic / debug | Profiles synthetic VolumeStore read/write path. |
| `volume_store_sounding_render` | Stable user command | Renders sounding PNGs from pressure VolumeStore data. |
| `weather_native_bench` | Diagnostic / debug | Benchmarks native contour weather maps. |
| `weather_native_profile` | Diagnostic / debug | Profiles map and cross-section render components. |
| `wrf_local_pressure_volume_store` | Publication / export | Exports local WRF pressure columns into VolumeStore artifacts. |
| `wrf_local_showcase` | Proof / research | Renders local WRF direct recipe showcase plots. |
| `wrf_ops` | Candidate subcommand | WRF operational planning and bootstrap helpers. |
| `wx_mrms_summarize` | Publication / export | Per-case MRMS QPE polygon summaries. |
| `wx_obs_extract` | Publication / export | Per-case extraction of patches, points, transects, and thresholds. |
| `wxprofile_ecape_probe` | Diagnostic / debug | Computes ECAPE numbers from WxProfile store columns. |
| `wxprofile_sounding_render` | Stable user command | Renders sounding PNGs from native WxProfile stores. |
| `wxstore_wxa_showcase` | Proof / research | Renders static plots from WxStore `.wxa` dense2d files. |

`rustwx_tools_site.html` is an HTML asset stored beside `rustwx_tools_site.rs`,
not a Rust binary.

## Refactor Notes

- Shared argument structs should be extracted only after this inventory is used
  to choose a target family.
- HRRR-specific binaries should stay available until their model-agnostic
  replacements are proven and old scripts are migrated.
- Gate binaries should remain small and deterministic.
- Publication/export tools should keep output filenames and JSON schemas stable.
