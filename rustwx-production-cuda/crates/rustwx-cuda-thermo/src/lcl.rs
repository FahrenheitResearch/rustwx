//! Lifting Condensation Level (LCL) via dry adiabatic ascent — port of met-cu's
//! `lcl_kernel`. Matches `metrust::calc::thermo::lcl` (which wraps
//! `wx_math::thermo::drylift`).

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/lcl.cu");
const MODULE_KEY: &str = "thermo_lcl";
const FUNCTION: &str = "lcl_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// LCL pressure (hPa) and LCL temperature (Celsius) for each input column point.
/// Returns `(p_lcl, t_lcl)` aligned with the input order.
pub fn host(
    ctx: &ContextHandle,
    pressure: &[f64],
    temperature: &[f64],
    dewpoint: &[f64],
) -> Result<(Vec<f64>, Vec<f64>)> {
    let n = pressure.len();
    if temperature.len() != n {
        return Err(Error::LengthMismatch {
            what: "pressure vs temperature",
            expected: n,
            got: temperature.len(),
        });
    }
    if dewpoint.len() != n {
        return Err(Error::LengthMismatch {
            what: "pressure vs dewpoint",
            expected: n,
            got: dewpoint.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let td_d = DeviceVec::from_host(ctx, dewpoint)?;
    let mut p_out: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    let mut t_out: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(t_d.slice())
        .arg(td_d.slice())
        .arg(p_out.slice_mut())
        .arg(t_out.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    Ok((p_out.copy_to_host(ctx)?, t_out.copy_to_host(ctx)?))
}
