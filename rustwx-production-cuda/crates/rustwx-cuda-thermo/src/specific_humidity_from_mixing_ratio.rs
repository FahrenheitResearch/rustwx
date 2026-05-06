//! `q = w / (1 + w)` — port of met-cu's
//! `specific_humidity_from_mixing_ratio_kernel`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/specific_humidity_from_mixing_ratio.cu");
const MODULE_KEY: &str = "thermo_specific_humidity_from_mixing_ratio";
const FUNCTION: &str = "specific_humidity_from_mixing_ratio_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Specific humidity (kg/kg) from mixing ratio (kg/kg).
pub fn host(ctx: &ContextHandle, mixing_ratio: &[f64]) -> Result<Vec<f64>> {
    let n = mixing_ratio.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let w_d = DeviceVec::from_host(ctx, mixing_ratio)?;
    let mut q_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(w_d.slice()).arg(q_d.slice_mut()).arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    q_d.copy_to_host(ctx)
}
