//! Relative humidity (%) from p (hPa), T (C), and mixing ratio (kg/kg) —
//! port of met-cu's `relative_humidity_from_mixing_ratio_kernel`.
//!
//! NOTE: kernel takes mixing ratio in kg/kg; metrust's reference takes g/kg.
//! The verification test multiplies the host input by 1000 before calling the
//! CPU function.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/relative_humidity_from_mixing_ratio.cu");
const MODULE_KEY: &str = "thermo_relative_humidity_from_mixing_ratio";
const FUNCTION: &str = "relative_humidity_from_mixing_ratio_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Relative humidity (%) from pressure (hPa), temperature (C), mixing ratio (kg/kg).
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
    let mut rh_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(t_d.slice())
        .arg(w_d.slice())
        .arg(rh_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    rh_d.copy_to_host(ctx)
}
