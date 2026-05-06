//! Vertical velocity w (m/s) from omega (Pa/s), p (hPa), T (C) — port of
//! met-cu's `vertical_velocity_kernel`. Matches `metrust::calc::thermo::vertical_velocity`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/vertical_velocity.cu");
const MODULE_KEY: &str = "thermo_vertical_velocity";
const FUNCTION: &str = "vertical_velocity_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Vertical velocity (m/s) from omega (Pa/s), pressure (hPa), temperature (C).
pub fn host(
    ctx: &ContextHandle,
    omega: &[f64],
    pressure: &[f64],
    temperature: &[f64],
) -> Result<Vec<f64>> {
    let n = omega.len();
    if pressure.len() != n {
        return Err(Error::LengthMismatch {
            what: "omega vs pressure",
            expected: n,
            got: pressure.len(),
        });
    }
    if temperature.len() != n {
        return Err(Error::LengthMismatch {
            what: "omega vs temperature",
            expected: n,
            got: temperature.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let o_d = DeviceVec::from_host(ctx, omega)?;
    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let mut w_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(o_d.slice())
        .arg(p_d.slice())
        .arg(t_d.slice())
        .arg(w_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    w_d.copy_to_host(ctx)
}
