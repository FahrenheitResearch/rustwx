//! K-Index — port of met-cu's `k_index_kernel`.
//! Matches `wx_math::composite::k_index`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/k_index.cu");
const MODULE_KEY: &str = "severe_k_index";
const FUNCTION: &str = "k_index_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    t850: &CudaSlice<f64>,
    t700: &CudaSlice<f64>,
    t500: &CudaSlice<f64>,
    td850: &CudaSlice<f64>,
    td700: &CudaSlice<f64>,
    ki: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(t850)
        .arg(t700)
        .arg(t500)
        .arg(td850)
        .arg(td700)
        .arg(ki)
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// KI = (T850 - T500) + Td850 - (T700 - Td700). All inputs in deg C.
pub fn host(
    ctx: &ContextHandle,
    t850: &[f64],
    t700: &[f64],
    t500: &[f64],
    td850: &[f64],
    td700: &[f64],
) -> Result<Vec<f64>> {
    let n = t850.len();
    for (name, len) in [
        ("t700", t700.len()),
        ("t500", t500.len()),
        ("td850", td850.len()),
        ("td700", td700.len()),
    ] {
        if len != n {
            return Err(Error::LengthMismatch {
                what: name,
                expected: n,
                got: len,
            });
        }
    }
    let t850_d = DeviceVec::from_host(ctx, t850)?;
    let t700_d = DeviceVec::from_host(ctx, t700)?;
    let t500_d = DeviceVec::from_host(ctx, t500)?;
    let td850_d = DeviceVec::from_host(ctx, td850)?;
    let td700_d = DeviceVec::from_host(ctx, td700)?;
    let mut ki_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(
        ctx,
        t850_d.slice(),
        t700_d.slice(),
        t500_d.slice(),
        td850_d.slice(),
        td700_d.slice(),
        ki_d.slice_mut(),
        n,
    )?;
    ki_d.copy_to_host(ctx)
}
