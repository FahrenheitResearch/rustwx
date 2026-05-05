//! Bunkers right/left mover storm motion — port of met-cu's
//! `bunkers_storm_motion_kernel`. One thread per column.
//!
//! Returns six per-column outputs: `(rm_u, rm_v, lm_u, lm_v, mw_u, mw_v)`,
//! i.e. (right mover u/v, left mover u/v, mean wind u/v).
//!
//! DEFER: the kernel uses 0-6 km height-weighted mean wind and (top - bottom)
//! bulk shear, while `metrust::calc::wind::bunkers_storm_motion` uses a
//! pressure-weighted mean wind plus a `(5.5-6 km layer mean) -
//! (0-0.5 km layer mean)` shear vector. The two storm-motion vectors disagree
//! by O(m/s), so a 1e-10 parity test is not feasible — see
//! `DIVERGENT_KERNELS.md`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/wind/bunkers_storm_motion.cu");
const MODULE_KEY: &str = "wind_bunkers_storm_motion";
const FUNCTION: &str = "bunkers_storm_motion_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Bunkers storm motion per column. Returns
/// `(rm_u, rm_v, lm_u, lm_v, mw_u, mw_v)` — right mover, left mover, and
/// mean-wind components — each as a `Vec<f64>` of length `ncols`.
#[allow(clippy::type_complexity)]
pub fn host(
    ctx: &ContextHandle,
    u: &[f64],
    v: &[f64],
    heights: &[f64],
    ncols: usize,
    nlevels: usize,
) -> Result<(Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>)> {
    let total = ncols
        .checked_mul(nlevels)
        .ok_or(Error::LengthMismatch {
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
    let mut rm_u_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut rm_v_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut lm_u_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut lm_v_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut mw_u_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut mw_v_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;

    let cfg = launch_cfg_1d(ncols, 256);
    let ncols_i32: i32 = ncols as i32;
    let nlevels_i32: i32 = nlevels as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_d.slice())
        .arg(v_d.slice())
        .arg(h_d.slice())
        .arg(rm_u_d.slice_mut())
        .arg(rm_v_d.slice_mut())
        .arg(lm_u_d.slice_mut())
        .arg(lm_v_d.slice_mut())
        .arg(mw_u_d.slice_mut())
        .arg(mw_v_d.slice_mut())
        .arg(&ncols_i32)
        .arg(&nlevels_i32);
    unsafe { builder.launch(cfg)? };

    let rm_u = rm_u_d.copy_to_host(ctx)?;
    let rm_v = rm_v_d.copy_to_host(ctx)?;
    let lm_u = lm_u_d.copy_to_host(ctx)?;
    let lm_v = lm_v_d.copy_to_host(ctx)?;
    let mw_u = mw_u_d.copy_to_host(ctx)?;
    let mw_v = mw_v_d.copy_to_host(ctx)?;
    Ok((rm_u, rm_v, lm_u, lm_v, mw_u, mw_v))
}
