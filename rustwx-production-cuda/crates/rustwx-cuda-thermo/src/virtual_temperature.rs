//! `Tv = T_K * (1 + w/eps) / (1 + w) - 273.15` — port of met-cu's
//! `virtual_temperature_kernel` (MetPy formulation).

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/virtual_temperature.cu");
const MODULE_KEY: &str = "thermo_virtual_temperature";
const FUNCTION: &str = "virtual_temperature_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Virtual temperature (Celsius) from temperature (Celsius) and mixing ratio
/// in kg/kg.
pub fn host(
    ctx: &ContextHandle,
    temperature: &[f64],
    mixing_ratio: &[f64],
) -> Result<Vec<f64>> {
    if temperature.len() != mixing_ratio.len() {
        return Err(Error::LengthMismatch {
            what: "temperature vs mixing_ratio",
            expected: temperature.len(),
            got: mixing_ratio.len(),
        });
    }
    let n = temperature.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let w_d = DeviceVec::from_host(ctx, mixing_ratio)?;
    let mut tv_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(t_d.slice())
        .arg(w_d.slice())
        .arg(tv_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    tv_d.copy_to_host(ctx)
}
