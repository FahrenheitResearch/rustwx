//! Boyden Index — port of met-cu's `boyden_index_kernel`.
//! Matches `wx_math::composite::boyden_index`.
//!
//! Note: argument order matches the metrust/wx-math signature
//! `(z1000, z700, t700)`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/boyden_index.cu");
const MODULE_KEY: &str = "severe_boyden_index";
const FUNCTION: &str = "boyden_index_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    z1000: &CudaSlice<f64>,
    z700: &CudaSlice<f64>,
    t700: &CudaSlice<f64>,
    bi: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(z1000)
        .arg(z700)
        .arg(t700)
        .arg(bi)
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// BI = (Z700 - Z1000)/10 - T700 - 200. z in m, t in deg C.
pub fn host(
    ctx: &ContextHandle,
    z1000: &[f64],
    z700: &[f64],
    t700: &[f64],
) -> Result<Vec<f64>> {
    let n = z1000.len();
    if z700.len() != n {
        return Err(Error::LengthMismatch {
            what: "z700",
            expected: n,
            got: z700.len(),
        });
    }
    if t700.len() != n {
        return Err(Error::LengthMismatch {
            what: "t700",
            expected: n,
            got: t700.len(),
        });
    }
    let z1000_d = DeviceVec::from_host(ctx, z1000)?;
    let z700_d = DeviceVec::from_host(ctx, z700)?;
    let t700_d = DeviceVec::from_host(ctx, t700)?;
    let mut bi_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(
        ctx,
        z1000_d.slice(),
        z700_d.slice(),
        t700_d.slice(),
        bi_d.slice_mut(),
        n,
    )?;
    bi_d.copy_to_host(ctx)
}
