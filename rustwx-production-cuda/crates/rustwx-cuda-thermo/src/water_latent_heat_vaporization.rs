//! Latent heat of vaporization Lv(T) (J/kg) — port of met-cu's
//! `water_latent_heat_vaporization_kernel`. Matches
//! `metrust::calc::thermo::water_latent_heat_vaporization`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/water_latent_heat_vaporization.cu");
const MODULE_KEY: &str = "thermo_water_latent_heat_vaporization";
const FUNCTION: &str = "water_latent_heat_vaporization_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Latent heat of vaporization (J/kg) at temperature `t_c` (Celsius).
pub fn host(ctx: &ContextHandle, temperature: &[f64]) -> Result<Vec<f64>> {
    let n = temperature.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let mut lv_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(t_d.slice())
        .arg(lv_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    lv_d.copy_to_host(ctx)
}
