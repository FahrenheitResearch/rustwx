//! Coriolis parameter `f = 2 Ω sin(lat)` — port of met-cu's
//! `coriolis_parameter_kernel`. Matches `wx_math::dynamics::coriolis_parameter`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/coriolis_parameter.cu");
const MODULE_KEY: &str = "wind_coriolis_parameter";
const FUNCTION: &str = "coriolis_parameter_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Compute the Coriolis parameter f (1/s) from latitude (degrees).
pub fn host(ctx: &ContextHandle, latitude_deg: &[f64]) -> Result<Vec<f64>> {
    let n = latitude_deg.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let lat_d = DeviceVec::from_host(ctx, latitude_deg)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(lat_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
