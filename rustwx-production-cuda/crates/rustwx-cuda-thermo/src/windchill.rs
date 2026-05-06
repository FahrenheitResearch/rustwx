//! NWS wind chill (FCM 2003) — port of met-cu's `windchill_kernel`.
//! Matches `metrust::calc::atmo::windchill`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/windchill.cu");
const MODULE_KEY: &str = "thermo_windchill";
const FUNCTION: &str = "windchill_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Wind chill (Celsius) from temperature (C) and wind speed (m/s).
pub fn host(ctx: &ContextHandle, temperature: &[f64], wind_speed: &[f64]) -> Result<Vec<f64>> {
    if temperature.len() != wind_speed.len() {
        return Err(Error::LengthMismatch {
            what: "temperature vs wind_speed",
            expected: temperature.len(),
            got: wind_speed.len(),
        });
    }
    let n = temperature.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let w_d = DeviceVec::from_host(ctx, wind_speed)?;
    let mut wc_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(t_d.slice())
        .arg(w_d.slice())
        .arg(wc_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    wc_d.copy_to_host(ctx)
}
