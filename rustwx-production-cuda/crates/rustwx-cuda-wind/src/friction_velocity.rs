//! Friction velocity `u* = sqrt(|cov(u, w)|)` — port of met-cu's
//! `friction_velocity_kernel`. Single-thread reduction, returns a scalar.
//!
//! DEFER: actual_max_diff = unknown (not benchmarked yet). The CUDA kernel uses
//! the two-pass `sum((u-mu)*(w-mw)) / n` form, while
//! `metrust::calc::wind::friction_velocity` uses the algebraic identity
//! `mean_uw - mean_u * mean_w`. The two forms are mathematically equivalent
//! but differ in floating-point rounding; running the verification test will
//! quantify the actual divergence. This wrapper is built but its test is left
//! `#[ignore]`d and not promoted to the suite until the divergence is measured.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{ContextHandle, DeviceVec, Error, KernelModule, LaunchCfg, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/friction_velocity.cu");
const MODULE_KEY: &str = "wind_friction_velocity";
const FUNCTION: &str = "friction_velocity_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Friction velocity `u*` from a u and w time series. Returns a single-element
/// `Vec<f64>` for API symmetry with the elementwise wrappers.
pub fn host(ctx: &ContextHandle, u: &[f64], w: &[f64]) -> Result<Vec<f64>> {
    if u.len() != w.len() {
        return Err(Error::LengthMismatch {
            what: "u vs w",
            expected: u.len(),
            got: w.len(),
        });
    }
    let n = u.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let u_d = DeviceVec::from_host(ctx, u)?;
    let w_d = DeviceVec::from_host(ctx, w)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, 1)?;

    // Single thread — kernel guards on tid == 0.
    let cfg = LaunchCfg {
        grid_dim: (1, 1, 1),
        block_dim: (1, 1, 1),
        shared_mem_bytes: 0,
    };
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_d.slice())
        .arg(w_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
