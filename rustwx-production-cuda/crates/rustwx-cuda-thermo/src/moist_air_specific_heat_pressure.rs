//! Moist-air Cp (J/(kg*K)) — port of met-cu's
//! `moist_air_specific_heat_pressure_kernel`. Matches
//! `metrust::calc::thermo::moist_air_specific_heat_pressure`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/moist_air_specific_heat_pressure.cu");
const MODULE_KEY: &str = "thermo_moist_air_specific_heat_pressure";
const FUNCTION: &str = "moist_air_specific_heat_pressure_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Moist-air Cp (J/(kg*K)) from mixing ratio (kg/kg).
pub fn host(ctx: &ContextHandle, mixing_ratio: &[f64]) -> Result<Vec<f64>> {
    let n = mixing_ratio.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let w_d = DeviceVec::from_host(ctx, mixing_ratio)?;
    let mut cp_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(w_d.slice()).arg(cp_d.slice_mut()).arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    cp_d.copy_to_host(ctx)
}
