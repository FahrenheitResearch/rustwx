//! Air density (kg/m^3) with virtual temperature — port of met-cu's
//! `density_kernel`. Matches `wx_math::thermo::density` after converting
//! mixing ratio between kg/kg (kernel) and g/kg (CPU).

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/density.cu");
const MODULE_KEY: &str = "thermo_density";
const FUNCTION: &str = "density_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    pressure: &CudaSlice<f64>,
    temperature: &CudaSlice<f64>,
    mixing_ratio: &CudaSlice<f64>,
    rho: &mut CudaSlice<f64>,
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
        .arg(mixing_ratio)
        .arg(rho)
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Air density (kg/m^3). `pressure` (hPa), `temperature` (C),
/// `mixing_ratio` in kg/kg.
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
    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let w_d = DeviceVec::from_host(ctx, mixing_ratio)?;
    let mut rho_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(
        ctx,
        p_d.slice(),
        t_d.slice(),
        w_d.slice(),
        rho_d.slice_mut(),
        n,
    )?;
    rho_d.copy_to_host(ctx)
}
