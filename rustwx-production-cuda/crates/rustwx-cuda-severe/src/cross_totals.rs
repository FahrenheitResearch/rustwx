//! Cross Totals — port of met-cu's `cross_totals_kernel`.
//! Matches `wx_math::composite::cross_totals`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/cross_totals.cu");
const MODULE_KEY: &str = "severe_cross_totals";
const FUNCTION: &str = "cross_totals_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    td850: &CudaSlice<f64>,
    t500: &CudaSlice<f64>,
    ct: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(td850).arg(t500).arg(ct).arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// CT = Td850 - T500. Inputs in deg C.
pub fn host(
    ctx: &ContextHandle,
    td850: &[f64],
    t500: &[f64],
) -> Result<Vec<f64>> {
    if t500.len() != td850.len() {
        return Err(Error::LengthMismatch {
            what: "td850 vs t500",
            expected: td850.len(),
            got: t500.len(),
        });
    }
    let n = td850.len();
    let td850_d = DeviceVec::from_host(ctx, td850)?;
    let t500_d = DeviceVec::from_host(ctx, t500)?;
    let mut ct_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(ctx, td850_d.slice(), t500_d.slice(), ct_d.slice_mut(), n)?;
    ct_d.copy_to_host(ctx)
}
