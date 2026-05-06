//! `e = SVP(Td)` — port of met-cu's `vapor_pressure_from_dewpoint_kernel`.
//! Matches `wx_math::thermo::vapor_pressure_from_dewpoint`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, KernelModule, Result};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/vapor_pressure_from_dewpoint.cu");
const MODULE_KEY: &str = "thermo_vapor_pressure_from_dewpoint";
const FUNCTION: &str = "vapor_pressure_from_dewpoint_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Vapor pressure (hPa) from dewpoint (Celsius).
pub fn host(ctx: &ContextHandle, dewpoint: &[f64]) -> Result<Vec<f64>> {
    let n = dewpoint.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let td_d = DeviceVec::from_host(ctx, dewpoint)?;
    let mut e_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(td_d.slice()).arg(e_d.slice_mut()).arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    e_d.copy_to_host(ctx)
}
