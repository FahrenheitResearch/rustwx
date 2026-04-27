//! Rustwx-owned radar ingest, rendering, and analysis.
//!
//! The crate intentionally keeps the radar engine inside `crates/` rather than
//! depending on a vendored radar crate. RustDar and ptx-radar are reference
//! implementations for algorithms and visual style; this crate owns the API.

pub mod ai;
pub mod aws;
pub mod cells;
pub mod dealias;
pub mod nexrad;
pub mod png;
pub mod render;

pub use ai::{AiExportOptions, RadarAiFrame, build_ai_frame};
pub use dealias::{DealiasMethod, dealias_velocity_file, dealias_velocity_sweep};
pub use nexrad::{Level2File, Level2Sweep, RadarProduct, RadarSite};
pub use png::{RadarFrameRender, render_product_frame, render_product_png};
