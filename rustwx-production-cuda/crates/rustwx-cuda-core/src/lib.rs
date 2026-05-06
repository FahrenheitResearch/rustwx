//! Shared CUDA infrastructure for rustwx-production-cuda.
//!
//! Mirrors the met-cu approach: kernels live as `.cu` source, get compiled
//! to PTX via NVRTC at runtime, and are cached on disk so subsequent runs
//! pay no compile cost. No `--use_fast_math` — strict IEEE to keep
//! agreement with the metrust CPU reference at ~1e-10.

mod buffer;
mod context;
mod error;
mod kernel;

pub mod constants;

pub use buffer::{require_eq, DeviceVec, HostVec};
pub use context::{global, Context, ContextHandle};
pub use error::{Error, Result};
pub use kernel::{
    compile_or_load_ptx, function, launch_cfg_1d, launch_cfg_2d, KernelModule, LaunchCfg,
};

pub use cudarc;
