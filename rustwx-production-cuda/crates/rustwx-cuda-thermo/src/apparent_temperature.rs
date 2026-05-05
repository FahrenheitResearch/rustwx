//! Apparent temperature (Celsius) — port of met-cu's `apparent_temperature_kernel`.
//!
//! DEFER: the heat-index branch uses raw Rothfusz with no Steadman/T_F average,
//! so it diverges from `metrust::calc::atmo::apparent_temperature` (which calls
//! `metrust::calc::atmo::heat_index`) by a few tenths of K for `t_f` in roughly
//! `78..82 F`. Same root cause as `heat_index`. See DIVERGENT_KERNELS.md.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/apparent_temperature.cu");
const MODULE_KEY: &str = "thermo_apparent_temperature";
const FUNCTION: &str = "apparent_temperature_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Apparent temperature (C) from temperature (C), RH (%), wind speed (m/s).
pub fn host(
    ctx: &ContextHandle,
    temperature: &[f64],
    relative_humidity: &[f64],
    wind_speed: &[f64],
) -> Result<Vec<f64>> {
    let n = temperature.len();
    if relative_humidity.len() != n {
        return Err(Error::LengthMismatch {
            what: "temperature vs relative_humidity",
            expected: n,
            got: relative_humidity.len(),
        });
    }
    if wind_speed.len() != n {
        return Err(Error::LengthMismatch {
            what: "temperature vs wind_speed",
            expected: n,
            got: wind_speed.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let rh_d = DeviceVec::from_host(ctx, relative_humidity)?;
    let w_d = DeviceVec::from_host(ctx, wind_speed)?;
    let mut at_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(t_d.slice())
        .arg(rh_d.slice())
        .arg(w_d.slice())
        .arg(at_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    at_d.copy_to_host(ctx)
}
