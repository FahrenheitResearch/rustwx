//! `DSE = Cp_d * T + g * z` — port of met-cu's `dry_static_energy_kernel`.
//! Pass `temperature` in Kelvin to match `wx_math::thermo::dry_static_energy`.
//! Argument order mirrors the kernel: `(height, temperature)`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/dry_static_energy.cu");
const MODULE_KEY: &str = "thermo_dry_static_energy";
const FUNCTION: &str = "dry_static_energy_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Dry static energy (J/kg). `height` (m), `temperature` (K).
pub fn host(
    ctx: &ContextHandle,
    height: &[f64],
    temperature: &[f64],
) -> Result<Vec<f64>> {
    if height.len() != temperature.len() {
        return Err(Error::LengthMismatch {
            what: "height vs temperature",
            expected: height.len(),
            got: temperature.len(),
        });
    }
    let n = height.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let h_d = DeviceVec::from_host(ctx, height)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let mut dse_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(h_d.slice())
        .arg(t_d.slice())
        .arg(dse_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    dse_d.copy_to_host(ctx)
}
