//! Total Totals — port of met-cu's `total_totals_kernel`.
//! Matches `wx_math::composite::total_totals`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/total_totals.cu");
const MODULE_KEY: &str = "severe_total_totals";
const FUNCTION: &str = "total_totals_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    t850: &CudaSlice<f64>,
    t500: &CudaSlice<f64>,
    td850: &CudaSlice<f64>,
    tt: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(t850).arg(t500).arg(td850).arg(tt).arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// TT = (T850 - T500) + (Td850 - T500). Inputs in deg C.
pub fn host(ctx: &ContextHandle, t850: &[f64], t500: &[f64], td850: &[f64]) -> Result<Vec<f64>> {
    let n = t850.len();
    if t500.len() != n {
        return Err(Error::LengthMismatch {
            what: "t500",
            expected: n,
            got: t500.len(),
        });
    }
    if td850.len() != n {
        return Err(Error::LengthMismatch {
            what: "td850",
            expected: n,
            got: td850.len(),
        });
    }
    let t850_d = DeviceVec::from_host(ctx, t850)?;
    let t500_d = DeviceVec::from_host(ctx, t500)?;
    let td850_d = DeviceVec::from_host(ctx, td850)?;
    let mut tt_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(
        ctx,
        t850_d.slice(),
        t500_d.slice(),
        td850_d.slice(),
        tt_d.slice_mut(),
        n,
    )?;
    tt_d.copy_to_host(ctx)
}
