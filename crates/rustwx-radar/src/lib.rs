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
pub use dealias::{DealiasMethod, dealias_velocity_file, dealias_velocity_sweep};
pub use nexrad::{Level2File, Level2Sweep, RadarProduct, RadarSite};
pub use png::{RadarFrameRender, render_product_frame, render_product_png};
