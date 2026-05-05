//! `w = eps * e / (p - e)` — port of met-cu's `mixing_ratio_kernel`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/mixing_ratio.cu");
const MODULE_KEY: &str = "thermo_mixing_ratio";
const FUNCTION: &str = "mixing_ratio_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Mixing ratio (kg/kg) from vapor pressure (hPa) and total pressure (hPa).
pub fn host(
    ctx: &ContextHandle,
    vapor_pressure: &[f64],
    pressure: &[f64],
) -> Result<Vec<f64>> {
    if vapor_pressure.len() != pressure.len() {
        return Err(Error::LengthMismatch {
            what: "vapor_pressure vs pressure",
            expected: vapor_pressure.len(),
            got: pressure.len(),
        });
    }
    let n = vapor_pressure.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let e_d = DeviceVec::from_host(ctx, vapor_pressure)?;
    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let mut w_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(e_d.slice())
        .arg(p_d.slice())
        .arg(w_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    w_d.copy_to_host(ctx)
}
