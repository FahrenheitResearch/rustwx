//! Omega (Pa/s) from w (m/s), p (hPa), T (C) — port of met-cu's
//! `vertical_velocity_pressure_kernel`. Matches
//! `metrust::calc::thermo::vertical_velocity_pressure`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/vertical_velocity_pressure.cu");
const MODULE_KEY: &str = "thermo_vertical_velocity_pressure";
const FUNCTION: &str = "vertical_velocity_pressure_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Omega (Pa/s) from w (m/s), pressure (hPa), temperature (C).
pub fn host(
    ctx: &ContextHandle,
    w: &[f64],
    pressure: &[f64],
    temperature: &[f64],
) -> Result<Vec<f64>> {
    let n = w.len();
    if pressure.len() != n {
        return Err(Error::LengthMismatch {
            what: "w vs pressure",
            expected: n,
            got: pressure.len(),
        });
    }
    if temperature.len() != n {
        return Err(Error::LengthMismatch {
            what: "w vs temperature",
            expected: n,
            got: temperature.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let w_d = DeviceVec::from_host(ctx, w)?;
    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let mut o_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(w_d.slice())
        .arg(p_d.slice())
        .arg(t_d.slice())
        .arg(o_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    o_d.copy_to_host(ctx)
}
