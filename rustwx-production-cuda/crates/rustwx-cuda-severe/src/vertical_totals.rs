//! Vertical Totals — port of met-cu's `vertical_totals_kernel`.
//! Matches `wx_math::composite::vertical_totals`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/vertical_totals.cu");
const MODULE_KEY: &str = "severe_vertical_totals";
const FUNCTION: &str = "vertical_totals_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    t850: &CudaSlice<f64>,
    t500: &CudaSlice<f64>,
    vt: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(t850).arg(t500).arg(vt).arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// VT = T850 - T500. Inputs in deg C.
pub fn host(ctx: &ContextHandle, t850: &[f64], t500: &[f64]) -> Result<Vec<f64>> {
    if t500.len() != t850.len() {
        return Err(Error::LengthMismatch {
            what: "t850 vs t500",
            expected: t850.len(),
            got: t500.len(),
        });
    }
    let n = t850.len();
    let t850_d = DeviceVec::from_host(ctx, t850)?;
    let t500_d = DeviceVec::from_host(ctx, t500)?;
    let mut vt_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(ctx, t850_d.slice(), t500_d.slice(), vt_d.slice_mut(), n)?;
    vt_d.copy_to_host(ctx)
}
