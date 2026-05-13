//! Rustwx-owned radar ingest, rendering, and analysis.
//!
//! The crate intentionally keeps the radar engine inside `crates/` rather than
//! depending on a vendored radar crate. RustDar and ptx-radar are reference
//! implementations for algorithms and visual style; this crate owns the API.

pub mod ai;
pub mod aws;
pub mod batch;
pub mod cells;
pub mod dealias;
pub mod nexrad;
pub mod png;
pub mod render;
pub mod sidecar;
pub mod tile;

pub use ai::{AiExportOptions, RadarAiFrame, build_ai_frame};
pub use batch::{
    CartesianGridSpec, Level2CartesianTensorBuildOptions, Level2DedupeKey, Level2Tensor,
    Level2TensorMetadata, Level2TensorOptions, Level2TensorProduct, RadarBatchGroup,
    RadarBatchManifest, RadarBatchRequest, RadarBatchResolvedRequest, ResolvedLevel2Volume,
    build_level2_cartesian_tensors_with_options, build_level2_tensors_stub, dedupe_key_for,
    group_resolved_requests, manifest_from_resolved_requests, parse_level2_object_name_scan_time,
    parse_level2_object_scan_time, plan_batch_requests, resolve_nearest_volume,
    select_nearest_volume,
};
pub use dealias::{
    DealiasAcceptancePolicy, DealiasContinuityScore, DealiasDecision, DealiasMethod, DealiasReport,
    VelocityQualityMaskReport, dealias_velocity_file, dealias_velocity_sweep,
    dealias_velocity_sweep_with_policy, dealias_velocity_sweep_with_report,
    mask_velocity_sweep_quality,
};
pub use nexrad::{Level2File, Level2SiteMetadata, Level2Sweep, RadarProduct, RadarSite};
pub use png::{
    RadarFrameRender, RadarSweepSelection, render_product_frame, render_product_png,
    select_sweep_with_hca_inputs, select_sweep_with_product, sweeps_with_hca_inputs,
    sweeps_with_product,
};
pub use render::ColorTablePreset;
pub use sidecar::{
    GATE_FLAG_DEALIASED, GATE_FLAG_DERIVED, GATE_FLAG_FILTERED, GATE_FLAG_MISSING,
    GATE_FLAG_RANGE_FOLDED, GATE_FLAG_VALID, RADAR_POLAR_SIDECAR_SCHEMA, RadarPolarGateFlagMeaning,
    RadarPolarSample, RadarPolarSampleMethod, RadarPolarSidecar, RadarPolarSidecarManifest,
    RadarPolarSidecarOptions, RadarPolarSidecarRecord, RadarRelativePolar, radar_lat_lon_to_polar,
    radar_polar_to_lat_lon, write_polar_sidecar,
};
pub use tile::{
    RadarTileManifest, RadarTileOptions, RadarTilePngCompression, RadarTileRecord,
    RadarTileSiteRecord, RadarVelocityQcSummary, radar_velocity_qc_summary,
    render_product_web_tiles,
};
