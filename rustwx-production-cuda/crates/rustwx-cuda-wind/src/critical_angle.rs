//! Critical angle (degrees) — port of met-cu's `critical_angle_kernel`.
//!
//! Wrapper exposes the six-input form (storm motion, surface wind, 500 m wind)
//! exactly as the kernel takes them. The metrust scalar reference at
//! `metrust::calc::severe::critical_angle` differs in the sign of the
//! storm-relative inflow vector (`u_sfc - storm_u` vs the kernel's
//! `storm_u - u_sfc`), which flips the cosine sign and yields
//! `180 - kernel_angle`. wx-math's vectorised `critical_angle` in
//! `composite.rs` uses `inflow = -storm` and shear (already differenced) as
//! inputs — also a different convention from the per-grid-point kernel here.
//!
//! DEFER: a metrust-vs-CUDA agreement test for this kernel is non-trivial
//! given the API mismatch and is omitted from the test suite for now.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/critical_angle.cu");
const MODULE_KEY: &str = "wind_critical_angle";
const FUNCTION: &str = "critical_angle_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Critical angle (degrees) between low-level shear and storm-relative inflow.
/// Inputs are six vectors of equal length: storm motion, surface wind, 500 m wind.
pub fn host(
    ctx: &ContextHandle,
    storm_u: &[f64],
    storm_v: &[f64],
    u_sfc: &[f64],
    v_sfc: &[f64],
    u_500: &[f64],
    v_500: &[f64],
) -> Result<Vec<f64>> {
    let n = storm_u.len();
    let check = |name: &'static str, len: usize| -> Result<()> {
        if len != n {
            return Err(Error::LengthMismatch {
                what: name,
                expected: n,
                got: len,
            });
        }
        Ok(())
    };
    check("storm_u vs storm_v", storm_v.len())?;
    check("storm_u vs u_sfc", u_sfc.len())?;
    check("storm_u vs v_sfc", v_sfc.len())?;
    check("storm_u vs u_500", u_500.len())?;
    check("storm_u vs v_500", v_500.len())?;

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let su_d = DeviceVec::from_host(ctx, storm_u)?;
    let sv_d = DeviceVec::from_host(ctx, storm_v)?;
    let usf_d = DeviceVec::from_host(ctx, u_sfc)?;
    let vsf_d = DeviceVec::from_host(ctx, v_sfc)?;
    let u5_d = DeviceVec::from_host(ctx, u_500)?;
    let v5_d = DeviceVec::from_host(ctx, v_500)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(su_d.slice())
        .arg(sv_d.slice())
        .arg(usf_d.slice())
        .arg(vsf_d.slice())
        .arg(u5_d.slice())
        .arg(v5_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
