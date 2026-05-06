//! `df/dy` (boundary-aware centered finite differences) — port of met-cu's
//! `first_derivative_y_kernel`. Matches `wx_math::dynamics::gradient_y`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_2d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_grid_helpers;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/first_derivative_y.cu");
const MODULE_KEY: &str = "grid_first_derivative_y";
const FUNCTION: &str = "first_derivative_y_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_grid_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

pub fn host(ctx: &ContextHandle, f: &[f64], dy: &[f64], nx: usize, ny: usize) -> Result<Vec<f64>> {
    let n = nx * ny;
    if f.len() != n {
        return Err(Error::LengthMismatch {
            what: "f vs nx*ny",
            expected: n,
            got: f.len(),
        });
    }
    if dy.len() != n {
        return Err(Error::LengthMismatch {
            what: "dy vs nx*ny",
            expected: n,
            got: dy.len(),
        });
    }

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let f_d = DeviceVec::from_host(ctx, f)?;
    let dy_d = DeviceVec::from_host(ctx, dy)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_2d(nx as u32, ny as u32, 16, 16);
    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(f_d.slice())
        .arg(dy_d.slice())
        .arg(out_d.slice_mut())
        .arg(&ny_i)
        .arg(&nx_i);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
