//! `w_s = eps * e_s / (p - e_s)` (kg/kg) — port of met-cu's
//! `saturation_mixing_ratio_kernel`. Matches
//! `wx_math::thermo::saturation_mixing_ratio` after a kg/kg <-> g/kg scaling.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/saturation_mixing_ratio.cu");
const MODULE_KEY: &str = "thermo_saturation_mixing_ratio";
const FUNCTION: &str = "saturation_mixing_ratio_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    pressure: &CudaSlice<f64>,
    temperature: &CudaSlice<f64>,
    ws: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(pressure).arg(temperature).arg(ws).arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Saturation mixing ratio (kg/kg) from pressure (hPa) and temperature (C).
pub fn host(
    ctx: &ContextHandle,
    pressure: &[f64],
    temperature: &[f64],
) -> Result<Vec<f64>> {
    if pressure.len() != temperature.len() {
        return Err(Error::LengthMismatch {
            what: "pressure vs temperature",
            expected: pressure.len(),
            got: temperature.len(),
        });
    }
    let n = pressure.len();
    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let mut ws_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(ctx, p_d.slice(), t_d.slice(), ws_d.slice_mut(), n)?;
    ws_d.copy_to_host(ctx)
}
