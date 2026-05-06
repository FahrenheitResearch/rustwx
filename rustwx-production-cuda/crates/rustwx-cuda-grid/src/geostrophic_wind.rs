//! Geostrophic wind from geopotential height — port of met-cu's
//! `geostrophic_wind_kernel`. Matches `wx_math::dynamics::geostrophic_wind`
//! (when `dx`/`dy` are constant fields and the Coriolis parameter is computed
//! consistently).
//!
//! The CPU reference takes `lats` and computes `f` via `coriolis_parameter`;
//! this wrapper exposes the same API and computes `f` host-side before
//! launching the kernel. A small zero-out prefilter (matching the CPU
//! `|f| < 1e-10` cutoff) is applied so behaviour matches near the equator.

use std::f64::consts::PI;

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_2d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_grid_helpers;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/geostrophic_wind.cu");
const MODULE_KEY: &str = "grid_geostrophic_wind";
const FUNCTION: &str = "geostrophic_wind_kernel";

const G: f64 = 9.80665;
const OMEGA: f64 = 7.2921159e-5;
/// Match the CPU near-equator cutoff exactly.
const F_CUTOFF: f64 = 1e-10;

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_grid_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

fn coriolis_parameter(lat_deg: f64) -> f64 {
    2.0 * OMEGA * (lat_deg * PI / 180.0).sin()
}

/// Returns `(u_geo, v_geo)` flattened in the same row-major layout as `height`.
pub fn host(
    ctx: &ContextHandle,
    height: &[f64],
    lats: &[f64],
    dx: &[f64],
    dy: &[f64],
    nx: usize,
    ny: usize,
) -> Result<(Vec<f64>, Vec<f64>)> {
    let n = nx * ny;
    if height.len() != n {
        return Err(Error::LengthMismatch {
            what: "height vs nx*ny",
            expected: n,
            got: height.len(),
        });
    }
    if lats.len() != n {
        return Err(Error::LengthMismatch {
            what: "lats vs nx*ny",
            expected: n,
            got: lats.len(),
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

    // Match the CPU `|f| < 1e-10` near-equator cutoff by zeroing host-side
    // before the kernel runs. The kernel itself uses a 1e-20 cutoff; once the
    // value is exactly zero, both branches produce (0, 0).
    let f: Vec<f64> = lats
        .iter()
        .map(|&lat| {
            let fc = coriolis_parameter(lat);
            if fc.abs() < F_CUTOFF {
                0.0
            } else {
                fc
            }
        })
        .collect();

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let z_d = DeviceVec::from_host(ctx, height)?;
    let f_d = DeviceVec::from_host(ctx, &f)?;
    let dx_d = DeviceVec::from_host(ctx, dx)?;
    let dy_d = DeviceVec::from_host(ctx, dy)?;
    let mut ug_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    let mut vg_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_2d(nx as u32, ny as u32, 16, 16);
    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let grav = G;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(z_d.slice())
        .arg(f_d.slice())
        .arg(dx_d.slice())
        .arg(dy_d.slice())
        .arg(&grav)
        .arg(ug_d.slice_mut())
        .arg(vg_d.slice_mut())
        .arg(&ny_i)
        .arg(&nx_i);
    unsafe { builder.launch(cfg)? };

    let ug = ug_d.copy_to_host(ctx)?;
    let vg = vg_d.copy_to_host(ctx)?;
    Ok((ug, vg))
}
