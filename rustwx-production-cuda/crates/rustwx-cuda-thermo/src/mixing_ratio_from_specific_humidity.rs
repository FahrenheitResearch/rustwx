//! `w = q / (1 - q)` (kg/kg) — port of met-cu's
//! `mixing_ratio_from_specific_humidity_kernel`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!(
    "../../../kernels/thermo/mixing_ratio_from_specific_humidity.cu"
);
const MODULE_KEY: &str = "thermo_mixing_ratio_from_specific_humidity";
const FUNCTION: &str = "mixing_ratio_from_specific_humidity_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Mixing ratio (kg/kg) from specific humidity (kg/kg).
pub fn host(
    ctx: &ContextHandle,
    specific_humidity: &[f64],
) -> Result<Vec<f64>> {
    let n = specific_humidity.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let q_d = DeviceVec::from_host(ctx, specific_humidity)?;
    let mut w_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(q_d.slice())
        .arg(w_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    w_d.copy_to_host(ctx)
}
