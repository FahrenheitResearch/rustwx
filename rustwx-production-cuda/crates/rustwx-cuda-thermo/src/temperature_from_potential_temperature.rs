//! `T = theta * (p/1000)^(Rd/Cp)` — port of met-cu's
//! `temperature_from_potential_temperature_kernel`.
//! Matches `wx_math::thermo::temperature_from_potential_temperature`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!(
    "../../../kernels/thermo/temperature_from_potential_temperature.cu"
);
const MODULE_KEY: &str = "thermo_temperature_from_potential_temperature";
const FUNCTION: &str = "temperature_from_potential_temperature_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Inverse Poisson: temperature (K) from pressure (hPa) and theta (K).
pub fn host(
    ctx: &ContextHandle,
    pressure: &[f64],
    theta: &[f64],
) -> Result<Vec<f64>> {
    if pressure.len() != theta.len() {
        return Err(Error::LengthMismatch {
            what: "pressure vs theta",
            expected: pressure.len(),
            got: theta.len(),
        });
    }
    let n = pressure.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let th_d = DeviceVec::from_host(ctx, theta)?;
    let mut t_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(th_d.slice())
        .arg(t_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    t_d.copy_to_host(ctx)
}
