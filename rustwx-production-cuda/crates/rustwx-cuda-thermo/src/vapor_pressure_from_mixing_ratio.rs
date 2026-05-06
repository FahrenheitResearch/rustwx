//! `e = w * p / (eps + w)` — port of met-cu's
//! `vapor_pressure_from_mixing_ratio_kernel`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/vapor_pressure_from_mixing_ratio.cu");
const MODULE_KEY: &str = "thermo_vapor_pressure_from_mixing_ratio";
const FUNCTION: &str = "vapor_pressure_from_mixing_ratio_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Vapor pressure (hPa) from mixing ratio (kg/kg) and total pressure (hPa).
pub fn host(ctx: &ContextHandle, mixing_ratio: &[f64], pressure: &[f64]) -> Result<Vec<f64>> {
    if mixing_ratio.len() != pressure.len() {
        return Err(Error::LengthMismatch {
            what: "mixing_ratio vs pressure",
            expected: mixing_ratio.len(),
            got: pressure.len(),
        });
    }
    let n = mixing_ratio.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let w_d = DeviceVec::from_host(ctx, mixing_ratio)?;
    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let mut e_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(w_d.slice())
        .arg(p_d.slice())
        .arg(e_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    e_d.copy_to_host(ctx)
}
