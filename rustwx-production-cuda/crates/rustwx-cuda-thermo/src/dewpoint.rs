//! Dewpoint (Celsius) from vapor pressure (hPa) — port of met-cu's
//! `dewpoint_kernel`. Matches `wx_math::thermo::dewpoint_from_vapor_pressure`
//! for `e > 0`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/dewpoint.cu");
const MODULE_KEY: &str = "thermo_dewpoint";
const FUNCTION: &str = "dewpoint_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Dewpoint (Celsius) from vapor pressure (hPa).
pub fn host(ctx: &ContextHandle, vapor_pressure: &[f64]) -> Result<Vec<f64>> {
    let n = vapor_pressure.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let e_d = DeviceVec::from_host(ctx, vapor_pressure)?;
    let mut td_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(e_d.slice())
        .arg(td_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    td_d.copy_to_host(ctx)
}
