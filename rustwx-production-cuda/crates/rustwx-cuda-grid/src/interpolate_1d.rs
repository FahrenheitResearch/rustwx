//! Per-column 1-D linear interpolation to new vertical levels — port of
//! met-cu's `interpolate_1d_kernel`. Mirrors `metrust::interpolate::interpolate_1d`
//! applied per column, with target levels broadcast across the (ny, nx) slab.
//!
//! `field` and `levels_in` are row-major `[nz_in][ny][nx]`. `levels_out` is
//! a 1-D `[nz_out]` array of target coordinates. Returns a flattened
//! `[nz_out][ny][nx]` volume; columns where the target is outside the input
//! range come back as `NaN`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_2d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/interpolate_1d.cu");
const MODULE_KEY: &str = "grid_interpolate_1d";
const FUNCTION: &str = "interpolate_1d_kernel";

/// `ascending = true` if `levels_in` increases with the level index along z.
pub fn host(
    ctx: &ContextHandle,
    field: &[f64],
    levels_in: &[f64],
    levels_out: &[f64],
    nx: usize,
    ny: usize,
    nz_in: usize,
    ascending: bool,
) -> Result<Vec<f64>> {
    let slab = ny * nx;
    let n_in = nz_in * slab;
    let nz_out = levels_out.len();
    let n_out = nz_out * slab;
    if field.len() != n_in {
        return Err(Error::LengthMismatch { what: "field vs nz*ny*nx", expected: n_in, got: field.len() });
    }
    if levels_in.len() != n_in {
        return Err(Error::LengthMismatch { what: "levels_in vs nz*ny*nx", expected: n_in, got: levels_in.len() });
    }

    let m = KernelModule::load(ctx, MODULE_KEY, &with_constants(KERNEL_SRC))?;
    let func = m.function(FUNCTION)?;

    let f_d = DeviceVec::from_host(ctx, field)?;
    let li_d = DeviceVec::from_host(ctx, levels_in)?;
    let lo_d = DeviceVec::from_host(ctx, levels_out)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n_out)?;

    let cfg = launch_cfg_2d(nx as u32, ny as u32, 16, 16);
    let nz_in_i: i32 = nz_in as i32;
    let nz_out_i: i32 = nz_out as i32;
    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let asc: i32 = if ascending { 1 } else { 0 };

    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(f_d.slice())
        .arg(li_d.slice())
        .arg(lo_d.slice())
        .arg(out_d.slice_mut())
        .arg(&nz_in_i)
        .arg(&nz_out_i)
        .arg(&ny_i)
        .arg(&nx_i)
        .arg(&asc);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
