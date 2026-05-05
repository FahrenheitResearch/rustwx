//! Storm-relative helicity (positive, negative, total) integrated from the
//! surface to a fixed depth — port of met-cu's `srh_kernel`.
//! One thread per column. Matches `metrust::calc::wind::storm_relative_helicity`
//! for monotonically increasing height profiles whose surface level is `h[0]`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/wind/storm_relative_helicity.cu");
const MODULE_KEY: &str = "wind_storm_relative_helicity";
const FUNCTION: &str = "storm_relative_helicity_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Storm-relative helicity per column. Returns `(srh_pos, srh_neg, srh_total)`
/// each of length `ncols`. `u`, `v`, `heights` are row-major `(ncols, nlevels)`
/// slices.
pub fn host(
    ctx: &ContextHandle,
    u: &[f64],
    v: &[f64],
    heights: &[f64],
    ncols: usize,
    nlevels: usize,
    depth_m: f64,
    storm_u: f64,
    storm_v: f64,
) -> Result<(Vec<f64>, Vec<f64>, Vec<f64>)> {
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
    let mut pos_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut neg_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut tot_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;

    let cfg = launch_cfg_1d(ncols, 256);
    let ncols_i32: i32 = ncols as i32;
    let nlevels_i32: i32 = nlevels as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_d.slice())
        .arg(v_d.slice())
        .arg(h_d.slice())
        .arg(&storm_u)
        .arg(&storm_v)
        .arg(&depth_m)
        .arg(pos_d.slice_mut())
        .arg(neg_d.slice_mut())
        .arg(tot_d.slice_mut())
        .arg(&ncols_i32)
        .arg(&nlevels_i32);
    unsafe { builder.launch(cfg)? };

    let pos = pos_d.copy_to_host(ctx)?;
    let neg = neg_d.copy_to_host(ctx)?;
    let tot = tot_d.copy_to_host(ctx)?;
    Ok((pos, neg, tot))
}
