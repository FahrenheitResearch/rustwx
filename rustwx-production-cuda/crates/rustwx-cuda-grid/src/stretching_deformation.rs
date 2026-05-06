//! Stretching deformation `du/dx - dv/dy` — port of met-cu's
//! `stretching_deformation_kernel`. Matches `wx_math::dynamics::stretching_deformation`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_2d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_grid_helpers;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/stretching_deformation.cu");
const MODULE_KEY: &str = "grid_stretching_deformation";
const FUNCTION: &str = "stretching_deformation_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_grid_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

pub fn host(
    ctx: &ContextHandle,
    u: &[f64],
    v: &[f64],
    dx: &[f64],
    dy: &[f64],
    nx: usize,
    ny: usize,
) -> Result<Vec<f64>> {
    let n = nx * ny;
    if u.len() != n {
        return Err(Error::LengthMismatch {
            what: "u vs nx*ny",
            expected: n,
            got: u.len(),
        });
    }
    if v.len() != n {
        return Err(Error::LengthMismatch {
            what: "v vs nx*ny",
            expected: n,
            got: v.len(),
        });
    }
    if dx.len() != n {
        return Err(Error::LengthMismatch {
            what: "dx vs nx*ny",
            expected: n,
            got: dx.len(),
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

    let u_d = DeviceVec::from_host(ctx, u)?;
    let v_d = DeviceVec::from_host(ctx, v)?;
    let dx_d = DeviceVec::from_host(ctx, dx)?;
    let dy_d = DeviceVec::from_host(ctx, dy)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_2d(nx as u32, ny as u32, 16, 16);
    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_d.slice())
        .arg(v_d.slice())
        .arg(dx_d.slice())
        .arg(dy_d.slice())
        .arg(out_d.slice_mut())
        .arg(&ny_i)
        .arg(&nx_i);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
