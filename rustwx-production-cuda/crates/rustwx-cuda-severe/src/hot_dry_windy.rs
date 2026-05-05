//! Hot-Dry-Windy Index — port of met-cu's `hot_dry_windy_kernel`.
//! Matches `wx_math::composite::hot_dry_windy`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/hot_dry_windy.cu");
const MODULE_KEY: &str = "severe_hot_dry_windy";
const FUNCTION: &str = "hot_dry_windy_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// HDW = VPD * wind. `t_c` (deg C), `rh` (%), `wspd_ms` (m/s),
/// `vpd` (hPa); when `vpd[i] <= 0` the GPU computes VPD internally.
pub fn host(
    ctx: &ContextHandle,
    t_c: &[f64],
    rh: &[f64],
    wspd_ms: &[f64],
    vpd: &[f64],
) -> Result<Vec<f64>> {
    let n = t_c.len();
    for (name, len) in [
        ("rh", rh.len()),
        ("wspd_ms", wspd_ms.len()),
        ("vpd", vpd.len()),
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

    let t_d = DeviceVec::from_host(ctx, t_c)?;
    let rh_d = DeviceVec::from_host(ctx, rh)?;
    let w_d = DeviceVec::from_host(ctx, wspd_ms)?;
    let v_d = DeviceVec::from_host(ctx, vpd)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(t_d.slice())
        .arg(rh_d.slice())
        .arg(w_d.slice())
        .arg(v_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
