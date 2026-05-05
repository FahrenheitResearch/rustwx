//! Height-weighted mean wind in a layer — port of met-cu's `mean_wind_kernel`.
//! One thread per column.
//!
//! DEFER: the kernel uses centered-box weights `dh = (h[k+1] - h[k-1]) / 2`
//! while `metrust::calc::wind::mean_wind` is a trapezoidal integration with
//! interpolated layer endpoints. The two outputs are close but not bit-equal;
//! a parity test against the metrust reference is omitted (see
//! `DIVERGENT_KERNELS.md`).

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/mean_wind.cu");
const MODULE_KEY: &str = "wind_mean_wind";
const FUNCTION: &str = "mean_wind_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Mean wind in a height layer. Returns `(mean_u, mean_v)` per column.
/// `u`, `v`, `heights` are row-major `(ncols, nlevels)` slices.
pub fn host(
    ctx: &ContextHandle,
    u: &[f64],
    v: &[f64],
    heights: &[f64],
    ncols: usize,
    nlevels: usize,
    bottom_m: f64,
    top_m: f64,
) -> Result<(Vec<f64>, Vec<f64>)> {
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
    let mut mu_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut mv_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;

    let cfg = launch_cfg_1d(ncols, 256);
    let ncols_i32: i32 = ncols as i32;
    let nlevels_i32: i32 = nlevels as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_d.slice())
        .arg(v_d.slice())
        .arg(h_d.slice())
        .arg(&bottom_m)
        .arg(&top_m)
        .arg(mu_d.slice_mut())
        .arg(mv_d.slice_mut())
        .arg(&ncols_i32)
        .arg(&nlevels_i32);
    unsafe { builder.launch(cfg)? };

    let mu = mu_d.copy_to_host(ctx)?;
    let mv = mv_d.copy_to_host(ctx)?;
    Ok((mu, mv))
}
