//! Vertical interpolation from a 3D volume to a 2D slab.
//!
//! Mirrors `wx-math::regrid::interpolate_vertical`. Bracketing + weight are
//! computed host-side (single thread does it once); the kernel is a clean
//! elementwise pass over `ny*nx` cells. That keeps the kernel simple and
//! avoids per-thread divergent branches for the bracket search.
//!
//! This is the kernel that sits upstream of every per-cell compute; making
//! it cheap on GPU amortizes PCIe across whatever follows.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/interpolate_vertical.cu");
const MODULE_KEY: &str = "grid_interpolate_vertical";
const FUNCTION: &str = "interpolate_vertical_kernel";

/// Device-resident launch — caller owns the buffers. Bracket and weight are
/// computed host-side (cheap, runs once); pass them in as scalars along with
/// the precomputed `slab_size`, `offset0`, and `offset1` byte indices into
/// `values_3d`.
pub fn launch_device(
    ctx: &ContextHandle,
    values_3d: &CudaSlice<f64>,
    result: &mut CudaSlice<f64>,
    slab_size: usize,
    offset0: usize,
    offset1: usize,
    weight: f64,
) -> Result<()> {
    let m = KernelModule::load(ctx, MODULE_KEY, &with_constants(KERNEL_SRC))?;
    let func = m.function(FUNCTION)?;

    let cfg = launch_cfg_1d(slab_size, 256);
    let slab_i32 = slab_size as i32;
    let off0_i32 = offset0 as i32;
    let off1_i32 = offset1 as i32;

    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(values_3d)
        .arg(result)
        .arg(&slab_i32)
        .arg(&off0_i32)
        .arg(&off1_i32)
        .arg(&weight);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Linear or log-vertical interpolation of a 3D volume to a target level.
///
/// `values_3d` is row-major `[nz][ny][nx]`. `levels` is the vertical
/// coordinate (size `nz`). Returns a `ny*nx` slab; cells outside the
/// vertical range come back as `NaN`.
pub fn host(
    ctx: &ContextHandle,
    values_3d: &[f64],
    levels: &[f64],
    target_level: f64,
    nx: usize,
    ny: usize,
    nz: usize,
    log_interp: bool,
) -> Result<Vec<f64>> {
    if values_3d.len() != nz * ny * nx {
        return Err(Error::LengthMismatch {
            what: "values_3d",
            expected: nz * ny * nx,
            got: values_3d.len(),
        });
    }
    if levels.len() != nz {
        return Err(Error::LengthMismatch {
            what: "levels",
            expected: nz,
            got: levels.len(),
        });
    }

    let slab_size = ny * nx;

    let bracket = if nz >= 2 && levels[nz - 1] > levels[0] {
        find_bracket_ascending(levels, target_level)
    } else {
        find_bracket_descending(levels, target_level)
    };

    let (k0, k1) = match bracket {
        Some(b) => b,
        None => return Ok(vec![f64::NAN; slab_size]),
    };

    let l0 = levels[k0];
    let l1 = levels[k1];
    let weight = if log_interp && l0 > 0.0 && l1 > 0.0 && target_level > 0.0 {
        (target_level.ln() - l0.ln()) / (l1.ln() - l0.ln())
    } else {
        (target_level - l0) / (l1 - l0)
    };

    let v_d = DeviceVec::from_host(ctx, values_3d)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, slab_size)?;

    launch_device(
        ctx,
        v_d.slice(),
        out_d.slice_mut(),
        slab_size,
        k0 * slab_size,
        k1 * slab_size,
        weight,
    )?;

    out_d.copy_to_host(ctx)
}

fn find_bracket_ascending(levels: &[f64], t: f64) -> Option<(usize, usize)> {
    if t < levels[0] || t > *levels.last().unwrap() {
        return None;
    }
    for k in 1..levels.len() {
        if levels[k] >= t {
            return Some((k - 1, k));
        }
    }
    None
}

fn find_bracket_descending(levels: &[f64], t: f64) -> Option<(usize, usize)> {
    if t > levels[0] || t < *levels.last().unwrap() {
        return None;
    }
    for k in 1..levels.len() {
        if levels[k] <= t {
            return Some((k - 1, k));
        }
    }
    None
}
