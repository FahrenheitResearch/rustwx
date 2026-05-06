//! Bulk wind shear `(delta_u, delta_v)` over a height layer — port of
//! met-cu's `bulk_shear_kernel`. One thread per column.
//!
//! Profile inputs are C-contiguous row-major `(ncols, nlevels)` slices: the
//! element at column `c`, level `k` lives at index `c * nlevels + k`.
//! Matches `metrust::calc::wind::bulk_shear` for monotonically increasing
//! height profiles whose surface level coincides with `bottom_m`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/bulk_shear.cu");
const MODULE_KEY: &str = "wind_bulk_shear";
const FUNCTION: &str = "bulk_shear_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Bulk wind shear over a height layer. Returns `(shear_u, shear_v)` per
/// column, each of length `ncols`. `u`, `v`, and `heights` must be of length
/// `ncols * nlevels` (row-major, column-major-of-columns: `[c*nlevels + k]`).
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
    let mut su_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;
    let mut sv_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;

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
        .arg(su_d.slice_mut())
        .arg(sv_d.slice_mut())
        .arg(&ncols_i32)
        .arg(&nlevels_i32);
    unsafe { builder.launch(cfg)? };

    let su = su_d.copy_to_host(ctx)?;
    let sv = sv_d.copy_to_host(ctx)?;
    Ok((su, sv))
}
