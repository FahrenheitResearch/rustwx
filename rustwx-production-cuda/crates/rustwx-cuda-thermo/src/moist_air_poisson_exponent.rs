//! Moist-air Poisson exponent kappa = R_moist / Cp_moist — port of met-cu's
//! `moist_air_poisson_exponent_kernel`. Matches
//! `metrust::calc::thermo::moist_air_poisson_exponent`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/moist_air_poisson_exponent.cu");
const MODULE_KEY: &str = "thermo_moist_air_poisson_exponent";
const FUNCTION: &str = "moist_air_poisson_exponent_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Moist-air Poisson exponent kappa from mixing ratio (kg/kg).
pub fn host(ctx: &ContextHandle, mixing_ratio: &[f64]) -> Result<Vec<f64>> {
    let n = mixing_ratio.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let w_d = DeviceVec::from_host(ctx, mixing_ratio)?;
    let mut k_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(w_d.slice())
        .arg(k_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    k_d.copy_to_host(ctx)
}
