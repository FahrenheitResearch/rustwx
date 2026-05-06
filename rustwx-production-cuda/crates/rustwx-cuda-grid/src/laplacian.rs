//! Laplacian `d2f/dx2 + d2f/dy2` — port of met-cu's `laplacian_kernel`.
//! Matches `wx_math::dynamics::laplacian` (when `dx`/`dy` are constant fields).

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{launch_cfg_2d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_grid_helpers;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/laplacian.cu");
const MODULE_KEY: &str = "grid_laplacian";
const FUNCTION: &str = "laplacian_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_grid_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    f: &CudaSlice<f64>,
    dx: &CudaSlice<f64>,
    dy: &CudaSlice<f64>,
    out: &mut CudaSlice<f64>,
    nx: usize,
    ny: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_2d(nx as u32, ny as u32, 16, 16);
    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(f)
        .arg(dx)
        .arg(dy)
        .arg(out)
        .arg(&ny_i)
        .arg(&nx_i);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

pub fn host(
    ctx: &ContextHandle,
    f: &[f64],
    dx: &[f64],
    dy: &[f64],
    nx: usize,
    ny: usize,
) -> Result<Vec<f64>> {
    let n = nx * ny;
    if f.len() != n {
        return Err(Error::LengthMismatch {
            what: "f vs nx*ny",
            expected: n,
            got: f.len(),
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

    let f_d = DeviceVec::from_host(ctx, f)?;
    let dx_d = DeviceVec::from_host(ctx, dx)?;
    let dy_d = DeviceVec::from_host(ctx, dy)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(
        ctx,
        f_d.slice(),
        dx_d.slice(),
        dy_d.slice(),
        out_d.slice_mut(),
        nx,
        ny,
    )?;
    out_d.copy_to_host(ctx)
}
