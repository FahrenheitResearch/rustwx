//! `RH = SVP(Td) / SVP(T) * 100` — port of met-cu's
//! `relative_humidity_from_dewpoint_kernel`.
//! Matches `wx_math::thermo::rh_from_dewpoint`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str = include_str!(
    "../../../kernels/thermo/relative_humidity_from_dewpoint.cu"
);
const MODULE_KEY: &str = "thermo_relative_humidity_from_dewpoint";
const FUNCTION: &str = "relative_humidity_from_dewpoint_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    temperature: &CudaSlice<f64>,
    dewpoint: &CudaSlice<f64>,
    rh: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(temperature).arg(dewpoint).arg(rh).arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Relative humidity (%) from temperature and dewpoint (both Celsius).
pub fn host(
    ctx: &ContextHandle,
    temperature: &[f64],
    dewpoint: &[f64],
) -> Result<Vec<f64>> {
    if temperature.len() != dewpoint.len() {
        return Err(Error::LengthMismatch {
            what: "temperature vs dewpoint",
            expected: temperature.len(),
            got: dewpoint.len(),
        });
    }
    let n = temperature.len();
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let td_d = DeviceVec::from_host(ctx, dewpoint)?;
    let mut rh_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(ctx, t_d.slice(), td_d.slice(), rh_d.slice_mut(), n)?;
    rh_d.copy_to_host(ctx)
}
