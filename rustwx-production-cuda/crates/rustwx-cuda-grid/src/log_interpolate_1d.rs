//! Per-column log-pressure interpolation — port of met-cu's
//! `log_interpolate_1d_kernel`. Mirrors `metrust::interpolate::log_interpolate_1d`
//! applied per column of a 3-D `[nz][ny][nx]` field.
//!
//! `field` and `pressure` are row-major level-major. `p_target` is a 1-D
//! array of target pressures. Returns `[nz_out][ny][nx]`; columns where the
//! target pressure does not bracket within the input range come back as
//! `NaN`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_2d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/log_interpolate_1d.cu");
const MODULE_KEY: &str = "grid_log_interpolate_1d";
const FUNCTION: &str = "log_interpolate_1d_kernel";

pub fn host(
    ctx: &ContextHandle,
    field: &[f64],
    pressure: &[f64],
    p_target: &[f64],
    nx: usize,
    ny: usize,
    nz_in: usize,
) -> Result<Vec<f64>> {
    let slab = ny * nx;
    let n_in = nz_in * slab;
    let nz_out = p_target.len();
    let n_out = nz_out * slab;
    if field.len() != n_in {
        return Err(Error::LengthMismatch {
            what: "field vs nz*ny*nx",
            expected: n_in,
            got: field.len(),
        });
    }
    if pressure.len() != n_in {
        return Err(Error::LengthMismatch {
            what: "pressure vs nz*ny*nx",
            expected: n_in,
            got: pressure.len(),
        });
    }

    let m = KernelModule::load(ctx, MODULE_KEY, &with_constants(KERNEL_SRC))?;
    let func = m.function(FUNCTION)?;

    let f_d = DeviceVec::from_host(ctx, field)?;
    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let pt_d = DeviceVec::from_host(ctx, p_target)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n_out)?;

    let cfg = launch_cfg_2d(nx as u32, ny as u32, 16, 16);
    let nz_in_i: i32 = nz_in as i32;
    let nz_out_i: i32 = nz_out as i32;
    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;

    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(f_d.slice())
        .arg(p_d.slice())
        .arg(pt_d.slice())
        .arg(out_d.slice_mut())
        .arg(&nz_in_i)
        .arg(&nz_out_i)
        .arg(&ny_i)
        .arg(&nx_i);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
