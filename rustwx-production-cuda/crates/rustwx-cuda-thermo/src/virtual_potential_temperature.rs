//! `theta_v = theta * (1 + 0.61 * w)` (kg/kg) — port of met-cu's
//! `virtual_potential_temperature_kernel`.
//! Matches `wx_math::thermo::virtual_potential_temperature` when the
//! mixing ratio is converted to g/kg on the CPU side.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/virtual_potential_temperature.cu");
const MODULE_KEY: &str = "thermo_virtual_potential_temperature";
const FUNCTION: &str = "virtual_potential_temperature_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Virtual potential temperature (K) from pressure (hPa), temperature (C),
/// and mixing ratio in kg/kg.
pub fn host(
    ctx: &ContextHandle,
    pressure: &[f64],
    temperature: &[f64],
    mixing_ratio: &[f64],
) -> Result<Vec<f64>> {
    let n = pressure.len();
    if temperature.len() != n {
        return Err(Error::LengthMismatch {
            what: "pressure vs temperature",
            expected: n,
            got: temperature.len(),
        });
    }
    if mixing_ratio.len() != n {
        return Err(Error::LengthMismatch {
            what: "pressure vs mixing_ratio",
            expected: n,
            got: mixing_ratio.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let w_d = DeviceVec::from_host(ctx, mixing_ratio)?;
    let mut th_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(t_d.slice())
        .arg(w_d.slice())
        .arg(th_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    th_d.copy_to_host(ctx)
}
