//! Ageostrophic wind `(u - u_g, v - v_g)` — port of met-cu's
//! `ageostrophic_wind_kernel`. Matches `wx_math::dynamics::ageostrophic_wind`
//! when paired with `wx_math::dynamics::geostrophic_wind` on the CPU.
//!
//! The kernel computes the geostrophic wind internally from the height field
//! `Z` and the Coriolis parameter `f`, then subtracts it from the observed
//! `(u, v)`. Matches the CPU near-equator behaviour by zeroing `f` host-side
//! where `|f| < 1e-10`, which makes the kernel return `(u, v)` directly (and
//! the CPU reference yields `(u - 0, v - 0)` because its geostrophic wind is
//! zero there).

use std::f64::consts::PI;

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_2d,
};

use crate::sources::with_grid_helpers;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/ageostrophic_wind.cu");
const MODULE_KEY: &str = "grid_ageostrophic_wind";
const FUNCTION: &str = "ageostrophic_wind_kernel";

const G: f64 = 9.80665;
const OMEGA: f64 = 7.2921159e-5;
const F_CUTOFF: f64 = 1e-10;

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_grid_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

fn coriolis_parameter(lat_deg: f64) -> f64 {
    2.0 * OMEGA * (lat_deg * PI / 180.0).sin()
}

/// Returns `(ua, va)` flattened in the same row-major layout as `u`.
pub fn host(
    ctx: &ContextHandle,
    u: &[f64],
    v: &[f64],
    height: &[f64],
    lats: &[f64],
    dx: &[f64],
    dy: &[f64],
    nx: usize,
    ny: usize,
) -> Result<(Vec<f64>, Vec<f64>)> {
    let n = nx * ny;
    if u.len() != n {
        return Err(Error::LengthMismatch { what: "u vs nx*ny", expected: n, got: u.len() });
    }
    if v.len() != n {
        return Err(Error::LengthMismatch { what: "v vs nx*ny", expected: n, got: v.len() });
    }
    if height.len() != n {
        return Err(Error::LengthMismatch { what: "height vs nx*ny", expected: n, got: height.len() });
    }
    if lats.len() != n {
        return Err(Error::LengthMismatch { what: "lats vs nx*ny", expected: n, got: lats.len() });
    }
    if dx.len() != n {
        return Err(Error::LengthMismatch { what: "dx vs nx*ny", expected: n, got: dx.len() });
    }
    if dy.len() != n {
        return Err(Error::LengthMismatch { what: "dy vs nx*ny", expected: n, got: dy.len() });
    }

    let f: Vec<f64> = lats
        .iter()
        .map(|&lat| {
            let fc = coriolis_parameter(lat);
            if fc.abs() < F_CUTOFF { 0.0 } else { fc }
        })
        .collect();

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let u_d = DeviceVec::from_host(ctx, u)?;
    let v_d = DeviceVec::from_host(ctx, v)?;
    let z_d = DeviceVec::from_host(ctx, height)?;
    let f_d = DeviceVec::from_host(ctx, &f)?;
    let dx_d = DeviceVec::from_host(ctx, dx)?;
    let dy_d = DeviceVec::from_host(ctx, dy)?;
    let mut ua_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    let mut va_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_2d(nx as u32, ny as u32, 16, 16);
    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let grav = G;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_d.slice())
        .arg(v_d.slice())
        .arg(z_d.slice())
        .arg(f_d.slice())
        .arg(dx_d.slice())
        .arg(dy_d.slice())
        .arg(&grav)
        .arg(ua_d.slice_mut())
        .arg(va_d.slice_mut())
        .arg(&ny_i)
        .arg(&nx_i);
    unsafe { builder.launch(cfg)? };

    let ua = ua_d.copy_to_host(ctx)?;
    let va = va_d.copy_to_host(ctx)?;
    Ok((ua, va))
}
