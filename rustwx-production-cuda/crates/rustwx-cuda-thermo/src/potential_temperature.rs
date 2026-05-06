//! `theta = T_K * (1000/p)^(Rd/Cp)` — port of met-cu's
//! `potential_temperature_kernel`. Matches `metrust::calc::thermo::potential_temperature`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/potential_temperature.cu");
const MODULE_KEY: &str = "thermo_potential_temperature";
const FUNCTION: &str = "potential_temperature_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers. Use this when running
/// multiple kernels back-to-back to avoid PCIe per call.
pub fn launch_device(
    ctx: &ContextHandle,
    pressure: &CudaSlice<f64>,
    temperature: &CudaSlice<f64>,
    theta: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(pressure)
        .arg(temperature)
        .arg(theta)
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Compute potential temperature elementwise on host vectors.
/// `pressure` in hPa, `temperature` in Celsius. Returns theta in Kelvin.
pub fn host(ctx: &ContextHandle, pressure: &[f64], temperature: &[f64]) -> Result<Vec<f64>> {
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
    let mut theta_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(ctx, p_d.slice(), t_d.slice(), theta_d.slice_mut(), n)?;
    theta_d.copy_to_host(ctx)
}
