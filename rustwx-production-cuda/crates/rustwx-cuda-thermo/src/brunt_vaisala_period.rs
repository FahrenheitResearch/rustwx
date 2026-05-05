//! Brunt-Vaisala period (s) from BVF (s^-1) — port of met-cu's
//! `brunt_vaisala_period_kernel`. Matches `wx_math::thermo::brunt_vaisala_period`
//! for `bvf > 0` (the only physically meaningful regime). For `bvf <= 0` the
//! kernel returns `1e30` while wx_math returns `f64::INFINITY`; the test
//! restricts inputs to the positive branch.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/brunt_vaisala_period.cu");
const MODULE_KEY: &str = "thermo_brunt_vaisala_period";
const FUNCTION: &str = "brunt_vaisala_period_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Brunt-Vaisala period (s) from BVF (s^-1).
pub fn host(ctx: &ContextHandle, bvf: &[f64]) -> Result<Vec<f64>> {
    let n = bvf.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let b_d = DeviceVec::from_host(ctx, bvf)?;
    let mut p_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(b_d.slice())
        .arg(p_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    p_d.copy_to_host(ctx)
}
