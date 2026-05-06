//! `Pi = (p/1000)^kappa` — port of met-cu's `exner_function_kernel`.
//! Matches `wx_math::thermo::exner_function`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/exner_function.cu");
const MODULE_KEY: &str = "thermo_exner_function";
const FUNCTION: &str = "exner_function_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    pressure: &CudaSlice<f64>,
    exner: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(pressure).arg(exner).arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Exner function from pressure (hPa).
pub fn host(ctx: &ContextHandle, pressure: &[f64]) -> Result<Vec<f64>> {
    let n = pressure.len();
    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let mut e_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(ctx, p_d.slice(), e_d.slice_mut(), n)?;
    e_d.copy_to_host(ctx)
}
