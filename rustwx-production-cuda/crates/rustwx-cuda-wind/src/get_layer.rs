//! Pressure-layer extraction — port of met-cu's `get_layer_kernel`.
//! One thread per column. Inputs and outputs are row-major
//! `(ncols, nlevels)` slices.
//!
//! Output layout: `p_out[c*nlevels + i]` for `i in 0..count[c]` are the
//! pressure values inside `[p_top, p_bottom]`; remaining slots are filled
//! with NaN. `count[c]` is the number of valid levels.
//!
//! DEFER: the kernel performs pure index selection. `wx_math::thermo::get_layer`
//! additionally interpolates new endpoints in log-pressure at the layer
//! boundaries, so the two implementations differ at the layer edges. See
//! `DIVERGENT_KERNELS.md`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/get_layer.cu");
const MODULE_KEY: &str = "wind_get_layer";
const FUNCTION: &str = "get_layer_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Extract values inside a pressure layer. Returns
/// `(p_layer, v_layer, count)` where `p_layer` and `v_layer` are
/// `ncols * nlevels` long with NaN padding past `count[c]` valid entries.
pub fn host(
    ctx: &ContextHandle,
    pressure: &[f64],
    values: &[f64],
    ncols: usize,
    nlevels: usize,
    p_bottom: f64,
    p_top: f64,
) -> Result<(Vec<f64>, Vec<f64>, Vec<i32>)> {
    let total = ncols
        .checked_mul(nlevels)
        .ok_or(Error::LengthMismatch {
            what: "ncols * nlevels overflow",
            expected: usize::MAX,
            got: 0,
        })?;
    if pressure.len() != total {
        return Err(Error::LengthMismatch {
            what: "pressure vs ncols*nlevels",
            expected: total,
            got: pressure.len(),
        });
    }
    if values.len() != total {
        return Err(Error::LengthMismatch {
            what: "values vs ncols*nlevels",
            expected: total,
            got: values.len(),
        });
    }

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let v_d = DeviceVec::from_host(ctx, values)?;
    let mut p_out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, total)?;
    let mut v_out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, total)?;
    let mut cnt_d: DeviceVec<i32> = DeviceVec::zeros(ctx, ncols)?;

    let cfg = launch_cfg_1d(ncols, 256);
    let ncols_i32: i32 = ncols as i32;
    let nlevels_i32: i32 = nlevels as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(v_d.slice())
        .arg(&p_bottom)
        .arg(&p_top)
        .arg(p_out_d.slice_mut())
        .arg(v_out_d.slice_mut())
        .arg(cnt_d.slice_mut())
        .arg(&ncols_i32)
        .arg(&nlevels_i32);
    unsafe { builder.launch(cfg)? };

    let p_out = p_out_d.copy_to_host(ctx)?;
    let v_out = v_out_d.copy_to_host(ctx)?;
    let cnt = cnt_d.copy_to_host(ctx)?;
    Ok((p_out, v_out, cnt))
}
