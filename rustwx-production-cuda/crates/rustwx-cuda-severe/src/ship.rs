//! Significant Hail Parameter (SHIP) — port of met-cu's `compute_ship_kernel`.
//! Matches `wx_math::composite::significant_hail_parameter`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/ship.cu");
const MODULE_KEY: &str = "severe_ship";
const FUNCTION: &str = "ship_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Compute SHIP elementwise.
/// `cape` MUCAPE (J/kg), `shear` 0-6 km bulk shear (m/s), `t500` (degC),
/// `lr` 700-500 hPa lapse rate (degC/km), `mr` mixing ratio (g/kg).
pub fn host(
    ctx: &ContextHandle,
    cape: &[f64],
    shear: &[f64],
    t500: &[f64],
    lr: &[f64],
    mr: &[f64],
) -> Result<Vec<f64>> {
    let n = cape.len();
    for (name, len) in [
        ("shear", shear.len()),
        ("t500", t500.len()),
        ("lr", lr.len()),
        ("mr", mr.len()),
    ] {
        if len != n {
            return Err(Error::LengthMismatch {
                what: name,
                expected: n,
                got: len,
            });
        }
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let cape_d = DeviceVec::from_host(ctx, cape)?;
    let shear_d = DeviceVec::from_host(ctx, shear)?;
    let t500_d = DeviceVec::from_host(ctx, t500)?;
    let lr_d = DeviceVec::from_host(ctx, lr)?;
    let mr_d = DeviceVec::from_host(ctx, mr)?;
    let mut ship_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(cape_d.slice())
        .arg(shear_d.slice())
        .arg(t500_d.slice())
        .arg(lr_d.slice())
        .arg(mr_d.slice())
        .arg(ship_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    ship_d.copy_to_host(ctx)
}
