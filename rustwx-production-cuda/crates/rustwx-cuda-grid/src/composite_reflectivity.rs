//! Composite reflectivity (column-max in dBZ) — port of met-cu's
//! `composite_reflectivity_kernel`. Mirrors
//! `wx_math::composite::composite_reflectivity_from_refl` with one caveat: the
//! CPU reference clamps the output at `-30.0 dBZ`. The kernel returns the raw
//! column maximum, so callers must ensure inputs include at least one value
//! `> -30 dBZ` per column for the two implementations to agree.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/composite_reflectivity.cu");
const MODULE_KEY: &str = "grid_composite_reflectivity";
const FUNCTION: &str = "composite_reflectivity_kernel";

/// `field_3d` is row-major `[nz][ny][nx]`. Returns a flattened `[ny][nx]`
/// slab of column-max values.
pub fn host(
    ctx: &ContextHandle,
    field_3d: &[f64],
    nx: usize,
    ny: usize,
    nz: usize,
) -> Result<Vec<f64>> {
    let ncols = ny * nx;
    let n_in = nz * ncols;
    if field_3d.len() != n_in {
        return Err(Error::LengthMismatch {
            what: "field vs nz*ny*nx",
            expected: n_in,
            got: field_3d.len(),
        });
    }

    let m = KernelModule::load(ctx, MODULE_KEY, &with_constants(KERNEL_SRC))?;
    let func = m.function(FUNCTION)?;

    let f_d = DeviceVec::from_host(ctx, field_3d)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, ncols)?;

    let cfg = launch_cfg_1d(ncols, 256);
    let ncols_i: i32 = ncols as i32;
    let nz_i: i32 = nz as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(f_d.slice())
        .arg(out_d.slice_mut())
        .arg(&ncols_i)
        .arg(&nz_i);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
