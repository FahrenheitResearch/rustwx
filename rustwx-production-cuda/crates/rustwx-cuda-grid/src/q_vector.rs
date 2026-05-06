//! Q-vector components (Q1, Q2) — port of met-cu's `q_vector_kernel`.
//! Matches `wx_math::dynamics::q_vector` (when `dx`/`dy` are constant fields).
//!
//! `p_hpa` is the pressure level in hPa; converted to Pa internally to match
//! the CPU reference. Caller passes geostrophic wind for `u_geo`/`v_geo`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{launch_cfg_2d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_grid_helpers;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/q_vector.cu");
const MODULE_KEY: &str = "grid_q_vector";
const FUNCTION: &str = "q_vector_kernel";

/// `Rd` matches `wx_math::dynamics` (which uses 287.058, not the more precise
/// 287.04749… used elsewhere in `wx-math`). Passed as a runtime scalar so that
/// the `.cu` source remains independent of compile-time constants.
const RD: f64 = 287.058;

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_grid_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    t: &CudaSlice<f64>,
    u_geo: &CudaSlice<f64>,
    v_geo: &CudaSlice<f64>,
    p_hpa: f64,
    dx: &CudaSlice<f64>,
    dy: &CudaSlice<f64>,
    q1: &mut CudaSlice<f64>,
    q2: &mut CudaSlice<f64>,
    nx: usize,
    ny: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_2d(nx as u32, ny as u32, 16, 16);
    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let p_pa = p_hpa * 100.0;
    let rd = RD;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_geo)
        .arg(v_geo)
        .arg(t)
        .arg(dx)
        .arg(dy)
        .arg(&p_pa)
        .arg(&rd)
        .arg(q1)
        .arg(q2)
        .arg(&ny_i)
        .arg(&nx_i);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Returns `(q1, q2)` flattened in the same row-major layout as `t`.
pub fn host(
    ctx: &ContextHandle,
    t: &[f64],
    u_geo: &[f64],
    v_geo: &[f64],
    p_hpa: f64,
    dx: &[f64],
    dy: &[f64],
    nx: usize,
    ny: usize,
) -> Result<(Vec<f64>, Vec<f64>)> {
    let n = nx * ny;
    if t.len() != n {
        return Err(Error::LengthMismatch {
            what: "t vs nx*ny",
            expected: n,
            got: t.len(),
        });
    }
    if u_geo.len() != n {
        return Err(Error::LengthMismatch {
            what: "u_geo vs nx*ny",
            expected: n,
            got: u_geo.len(),
        });
    }
    if v_geo.len() != n {
        return Err(Error::LengthMismatch {
            what: "v_geo vs nx*ny",
            expected: n,
            got: v_geo.len(),
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

    let t_d = DeviceVec::from_host(ctx, t)?;
    let u_d = DeviceVec::from_host(ctx, u_geo)?;
    let v_d = DeviceVec::from_host(ctx, v_geo)?;
    let dx_d = DeviceVec::from_host(ctx, dx)?;
    let dy_d = DeviceVec::from_host(ctx, dy)?;
    let mut q1_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    let mut q2_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    launch_device(
        ctx,
        t_d.slice(),
        u_d.slice(),
        v_d.slice(),
        p_hpa,
        dx_d.slice(),
        dy_d.slice(),
        q1_d.slice_mut(),
        q2_d.slice_mut(),
        nx,
        ny,
    )?;

    let q1 = q1_d.copy_to_host(ctx)?;
    let q2 = q2_d.copy_to_host(ctx)?;
    Ok((q1, q2))
}
