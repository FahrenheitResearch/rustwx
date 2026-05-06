//! Corfidi MCS upwind / downwind storm motion — port of met-cu's
//! `corfidi_storm_motion_kernel`. One thread per column.
//!
//! Returns four per-column outputs: `(upwind_u, upwind_v, downwind_u,
//! downwind_v)`.
//!
//! DEFER: the 0-6 km mean wind here uses centered height-weights, while
//! `metrust::calc::wind::corfidi_storm_motion` delegates to
//! `metrust::calc::wind::mean_wind`, which is trapezoidal with interpolated
//! endpoints. The two answers are close but not bit-equal — see
//! `DIVERGENT_KERNELS.md`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/corfidi_storm_motion.cu");
const MODULE_KEY: &str = "wind_corfidi_storm_motion";
const FUNCTION: &str = "corfidi_storm_motion_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Corfidi MCS storm motion per column. Returns
/// `(upwind_u, upwind_v, downwind_u, downwind_v)`, each of length `ncols`.
/// `u_llj_ms` / `v_llj_ms` are scalar low-level-jet (e.g. 850-hPa wind)
/// components shared across all columns.
pub fn host(
    ctx: &ContextHandle,
    u: &[f64],
    v: &[f64],
    heights: &[f64],
    ncols: usize,
    nlevels: usize,
    u_llj_ms: f64,
    v_llj_ms: f64,
) -> Result<(Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>)> {
    let total = ncols.checked_mul(nlevels).ok_or(Error::LengthMismatch {
        what: "ncols * nlevels overflow",
        expected: usize::MAX,
        got: 0,
    })?;
    if u.len() != total {
        return Err(Error::LengthMismatch {
            what: "u vs ncols*nlevels",
            expected: total,
            got: u.len(),
        });
    }
    if v.len() != total {
        return Err(Error::LengthMismatch {
            what: "v vs ncols*nlevels",
            expected: total,
            got: v.len(),
        });
    }
    if heights.len() != total {
        return Err(Error::LengthMismatch {
            what: "heights vs ncols*nlevels",
            expected: total,
            got: heights.len(),
        });
    }

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let u_d = DeviceVec::from_host(ctx, u)?;
    let v_d = DeviceVec::from_host(ctx, v)?;
    let h_d = DeviceVec::from_host(ctx, heights)?;
    let mut up_u_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut up_v_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut dn_u_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut dn_v_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;

    let cfg = launch_cfg_1d(ncols, 256);
    let ncols_i32: i32 = ncols as i32;
    let nlevels_i32: i32 = nlevels as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_d.slice())
        .arg(v_d.slice())
        .arg(h_d.slice())
        .arg(&u_llj_ms)
        .arg(&v_llj_ms)
        .arg(up_u_d.slice_mut())
        .arg(up_v_d.slice_mut())
        .arg(dn_u_d.slice_mut())
        .arg(dn_v_d.slice_mut())
        .arg(&ncols_i32)
        .arg(&nlevels_i32);
    unsafe { builder.launch(cfg)? };

    let up_u = up_u_d.copy_to_host(ctx)?;
    let up_v = up_v_d.copy_to_host(ctx)?;
    let dn_u = dn_u_d.copy_to_host(ctx)?;
    let dn_v = dn_v_d.copy_to_host(ctx)?;
    Ok((up_u, up_v, dn_u, dn_v))
}
