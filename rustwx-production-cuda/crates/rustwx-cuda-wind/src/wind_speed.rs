//! `speed = sqrt(u^2 + v^2)` — port of met-cu's `wind_speed_kernel`.
//! Matches `wx_math::dynamics::wind_speed`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/wind_speed.cu");
const MODULE_KEY: &str = "wind_wind_speed";
const FUNCTION: &str = "wind_speed_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    u: &CudaSlice<f64>,
    v: &CudaSlice<f64>,
    out: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(u).arg(v).arg(out).arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Compute wind speed from u, v components elementwise.
pub fn host(ctx: &ContextHandle, u: &[f64], v: &[f64]) -> Result<Vec<f64>> {
    if u.len() != v.len() {
        return Err(Error::LengthMismatch {
            what: "u vs v",
            expected: u.len(),
            got: v.len(),
        });
    }
    let n = u.len();
    let u_d = DeviceVec::from_host(ctx, u)?;
    let v_d = DeviceVec::from_host(ctx, v)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(ctx, u_d.slice(), v_d.slice(), out_d.slice_mut(), n)?;
    out_d.copy_to_host(ctx)
}
