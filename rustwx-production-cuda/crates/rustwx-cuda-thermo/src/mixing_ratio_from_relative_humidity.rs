//! Mixing ratio (kg/kg) from p, T, RH — port of met-cu's
//! `mixing_ratio_from_relative_humidity_kernel`.
//!
//! NOTE: kernel returns kg/kg, while
//! `metrust::calc::thermo::mixing_ratio_from_relative_humidity` returns g/kg.
//! The verification test divides the metrust value by 1000.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/mixing_ratio_from_relative_humidity.cu");
const MODULE_KEY: &str = "thermo_mixing_ratio_from_relative_humidity";
const FUNCTION: &str = "mixing_ratio_from_relative_humidity_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Mixing ratio (kg/kg) from pressure (hPa), temperature (C), RH (%).
pub fn host(
    ctx: &ContextHandle,
    pressure: &[f64],
    temperature: &[f64],
    relative_humidity: &[f64],
) -> Result<Vec<f64>> {
    let n = pressure.len();
    if temperature.len() != n {
        return Err(Error::LengthMismatch {
            what: "pressure vs temperature",
            expected: n,
            got: temperature.len(),
        });
    }
    if relative_humidity.len() != n {
        return Err(Error::LengthMismatch {
            what: "pressure vs relative_humidity",
            expected: n,
            got: relative_humidity.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let rh_d = DeviceVec::from_host(ctx, relative_humidity)?;
    let mut w_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(t_d.slice())
        .arg(rh_d.slice())
        .arg(w_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    w_d.copy_to_host(ctx)
}
