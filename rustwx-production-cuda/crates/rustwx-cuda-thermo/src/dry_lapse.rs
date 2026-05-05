//! `T = (T_sfc + 273.15) * (p/p_ref)^kappa - 273.15` — port of met-cu's
//! `dry_lapse_kernel` (elementwise; one target pressure per element).
//! `wx_math::thermo::dry_lapse` operates on a profile slice; the kernel is
//! the per-level form.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/dry_lapse.cu");
const MODULE_KEY: &str = "thermo_dry_lapse";
const FUNCTION: &str = "dry_lapse_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Temperature (Celsius) at `pressure` along a dry adiabat starting from
/// `t_surface` (Celsius) at `reference_pressure`.
pub fn host(
    ctx: &ContextHandle,
    pressure: &[f64],
    reference_pressure: &[f64],
    t_surface: &[f64],
) -> Result<Vec<f64>> {
    let n = pressure.len();
    if reference_pressure.len() != n {
        return Err(Error::LengthMismatch {
            what: "pressure vs reference_pressure",
            expected: n,
            got: reference_pressure.len(),
        });
    }
    if t_surface.len() != n {
        return Err(Error::LengthMismatch {
            what: "pressure vs t_surface",
            expected: n,
            got: t_surface.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let pr_d = DeviceVec::from_host(ctx, reference_pressure)?;
    let ts_d = DeviceVec::from_host(ctx, t_surface)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(pr_d.slice())
        .arg(ts_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
