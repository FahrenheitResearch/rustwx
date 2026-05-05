//! Meteorological wind direction (degrees) from u, v — port of met-cu's
//! `wind_direction_kernel`.
//!
//! Note: the CUDA kernel returns 0.0 only on exact-zero u and v (`u == 0 && v == 0`),
//! while `wx_math::dynamics::wind_direction` uses a magnitude threshold of `1e-10`.
//! For inputs away from the calm-wind boundary the two agree to ~1e-10.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/wind_direction.cu");
const MODULE_KEY: &str = "wind_wind_direction";
const FUNCTION: &str = "wind_direction_kernel";

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

/// Compute meteorological wind direction (deg, [0, 360)) from u, v components.
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
