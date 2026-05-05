//! `e_s = svp_hpa(T_C)` (Ambaum 2020) — port of met-cu's
//! `saturation_vapor_pressure_kernel`.
//! Matches `wx_math::thermo::saturation_vapor_pressure`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/saturation_vapor_pressure.cu");
const MODULE_KEY: &str = "thermo_saturation_vapor_pressure";
const FUNCTION: &str = "saturation_vapor_pressure_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Saturation vapor pressure (hPa) from temperature (Celsius).
pub fn host(ctx: &ContextHandle, temperature: &[f64]) -> Result<Vec<f64>> {
    let n = temperature.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let mut es_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(t_d.slice())
        .arg(es_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    es_d.copy_to_host(ctx)
}
