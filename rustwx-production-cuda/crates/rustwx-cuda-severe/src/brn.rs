//! Bulk Richardson Number — port of met-cu's `bulk_richardson_number_kernel`.
//! Matches `wx_math::composite::bulk_richardson_number` (NaN when shear is
//! too small).

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/brn.cu");
const MODULE_KEY: &str = "severe_brn";
const FUNCTION: &str = "brn_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// BRN = CAPE / (0.5 * shear^2). Returns NaN when 0.5*shear^2 < 0.1.
pub fn host(
    ctx: &ContextHandle,
    cape: &[f64],
    shear: &[f64],
) -> Result<Vec<f64>> {
    if shear.len() != cape.len() {
        return Err(Error::LengthMismatch {
            what: "cape vs shear",
            expected: cape.len(),
            got: shear.len(),
        });
    }
    let n = cape.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let cape_d = DeviceVec::from_host(ctx, cape)?;
    let shear_d = DeviceVec::from_host(ctx, shear)?;
    let mut brn_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(cape_d.slice())
        .arg(shear_d.slice())
        .arg(brn_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    brn_d.copy_to_host(ctx)
}
