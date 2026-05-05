//! Severe-weather composite CUDA kernels (STP, SCP, SHIP, EHI, …).

mod sources;

pub mod boyden_index;
pub mod brn;
pub mod cross_totals;
pub mod ehi;
pub mod ffwi;
pub mod haines_index;
pub mod hot_dry_windy;
pub mod k_index;
pub mod scp;
pub mod ship;
pub mod stp;
pub mod sweat_index;
pub mod total_totals;
pub mod vertical_totals;

pub use rustwx_cuda_core as core;
pub use rustwx_cuda_core::{Context, ContextHandle, DeviceVec, Error, Result};
