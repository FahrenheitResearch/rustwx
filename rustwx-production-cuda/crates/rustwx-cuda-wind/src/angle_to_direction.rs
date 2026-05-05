//! Angle-to-cardinal-direction code — port of met-cu's
//! `angle_to_direction_kernel`. Returns a float in `[0, n_directions)` per
//! element (`0 = N`, `1 = NNE`, …).
//!
//! `metrust::calc::utils::angle_to_direction` returns a `&'static str` label,
//! not a numeric code, so this kernel does not have a 1:1 metrust counterpart.
//! DEFER: no automated agreement test is added to the suite. The wrapper is
//! useful in its own right and the CUDA logic mirrors met-cu's kernel exactly.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/angle_to_direction.cu");
const MODULE_KEY: &str = "wind_angle_to_direction";
const FUNCTION: &str = "angle_to_direction_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Compute the cardinal-direction code per element. `n_directions` is provided
/// per-element to mirror met-cu's `ElementwiseKernel` signature; pass a vector
/// of e.g. all `16.0` for a standard 16-point compass.
pub fn host(
    ctx: &ContextHandle,
    angle_deg: &[f64],
    n_directions: &[f64],
) -> Result<Vec<f64>> {
    if angle_deg.len() != n_directions.len() {
        return Err(Error::LengthMismatch {
            what: "angle_deg vs n_directions",
            expected: angle_deg.len(),
            got: n_directions.len(),
        });
    }
    let n = angle_deg.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let a_d = DeviceVec::from_host(ctx, angle_deg)?;
    let nd_d = DeviceVec::from_host(ctx, n_directions)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(a_d.slice())
        .arg(nd_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
