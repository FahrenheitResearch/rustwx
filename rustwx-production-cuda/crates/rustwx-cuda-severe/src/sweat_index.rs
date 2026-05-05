//! SWEAT Index — port of met-cu's `sweat_index_kernel`.
//! Matches `wx_math::composite::sweat_index`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/sweat_index.cu");
const MODULE_KEY: &str = "severe_sweat_index";
const FUNCTION: &str = "sweat_index_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// SWEAT Index. `tt` Total Totals, `td850` (deg C),
/// `wspd850`, `wspd500` (knots), `wdir850`, `wdir500` (degrees).
pub fn host(
    ctx: &ContextHandle,
    tt: &[f64],
    td850: &[f64],
    wspd850: &[f64],
    wdir850: &[f64],
    wspd500: &[f64],
    wdir500: &[f64],
) -> Result<Vec<f64>> {
    let n = tt.len();
    for (name, len) in [
        ("td850", td850.len()),
        ("wspd850", wspd850.len()),
        ("wdir850", wdir850.len()),
        ("wspd500", wspd500.len()),
        ("wdir500", wdir500.len()),
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

    let tt_d = DeviceVec::from_host(ctx, tt)?;
    let td_d = DeviceVec::from_host(ctx, td850)?;
    let wspd850_d = DeviceVec::from_host(ctx, wspd850)?;
    let wdir850_d = DeviceVec::from_host(ctx, wdir850)?;
    let wspd500_d = DeviceVec::from_host(ctx, wspd500)?;
    let wdir500_d = DeviceVec::from_host(ctx, wdir500)?;
    let mut sw_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(tt_d.slice())
        .arg(td_d.slice())
        .arg(wspd850_d.slice())
        .arg(wdir850_d.slice())
        .arg(wspd500_d.slice())
        .arg(wdir500_d.slice())
        .arg(sw_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    sw_d.copy_to_host(ctx)
}
