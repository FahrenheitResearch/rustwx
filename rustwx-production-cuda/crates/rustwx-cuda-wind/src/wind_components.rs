//! `(u, v) = (-speed*sin(d), -speed*cos(d))` — port of met-cu's
//! `wind_components_kernel`. Matches `wx_math::dynamics::wind_components`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/wind_components.cu");
const MODULE_KEY: &str = "wind_wind_components";
const FUNCTION: &str = "wind_components_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Compute (u, v) wind components from speed (m/s) and meteorological direction (deg).
pub fn host(ctx: &ContextHandle, speed: &[f64], direction: &[f64]) -> Result<(Vec<f64>, Vec<f64>)> {
    if speed.len() != direction.len() {
        return Err(Error::LengthMismatch {
            what: "speed vs direction",
            expected: speed.len(),
            got: direction.len(),
        });
    }
    let n = speed.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let s_d = DeviceVec::from_host(ctx, speed)?;
    let d_d = DeviceVec::from_host(ctx, direction)?;
    let mut u_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    let mut v_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(s_d.slice())
        .arg(d_d.slice())
        .arg(u_d.slice_mut())
        .arg(v_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    let u_out = u_d.copy_to_host(ctx)?;
    let v_out = v_d.copy_to_host(ctx)?;
    Ok((u_out, v_out))
}
