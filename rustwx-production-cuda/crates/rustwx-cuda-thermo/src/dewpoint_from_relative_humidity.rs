//! Dewpoint (Celsius) from temperature and relative humidity — port of
//! met-cu's `dewpoint_from_relative_humidity_kernel`. Matches
//! `metrust::calc::thermo::dewpoint_from_relative_humidity`
//! (which wraps `wx_math::thermo::dewpoint_from_rh`).

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/dewpoint_from_relative_humidity.cu");
const MODULE_KEY: &str = "thermo_dewpoint_from_relative_humidity";
const FUNCTION: &str = "dewpoint_from_relative_humidity_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Dewpoint (Celsius) from temperature (Celsius) and RH (%).
pub fn host(
    ctx: &ContextHandle,
    temperature: &[f64],
    relative_humidity: &[f64],
) -> Result<Vec<f64>> {
    if temperature.len() != relative_humidity.len() {
        return Err(Error::LengthMismatch {
            what: "temperature vs relative_humidity",
            expected: temperature.len(),
            got: relative_humidity.len(),
        });
    }
    let n = temperature.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let rh_d = DeviceVec::from_host(ctx, relative_humidity)?;
    let mut td_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(t_d.slice())
        .arg(rh_d.slice())
        .arg(td_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    td_d.copy_to_host(ctx)
}
