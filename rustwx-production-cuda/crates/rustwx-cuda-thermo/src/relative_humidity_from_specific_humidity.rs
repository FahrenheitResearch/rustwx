//! Relative humidity (%) from p (hPa), T (C), specific humidity (kg/kg) —
//! port of met-cu's `relative_humidity_from_specific_humidity_kernel`. Matches
//! `metrust::calc::thermo::relative_humidity_from_specific_humidity`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/relative_humidity_from_specific_humidity.cu");
const MODULE_KEY: &str = "thermo_relative_humidity_from_specific_humidity";
const FUNCTION: &str = "relative_humidity_from_specific_humidity_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Relative humidity (%) from pressure (hPa), temperature (C), specific humidity (kg/kg).
pub fn host(
    ctx: &ContextHandle,
    pressure: &[f64],
    temperature: &[f64],
    specific_humidity: &[f64],
) -> Result<Vec<f64>> {
    let n = pressure.len();
    if temperature.len() != n {
        return Err(Error::LengthMismatch {
            what: "pressure vs temperature",
            expected: n,
            got: temperature.len(),
        });
    }
    if specific_humidity.len() != n {
        return Err(Error::LengthMismatch {
            what: "pressure vs specific_humidity",
            expected: n,
            got: specific_humidity.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let q_d = DeviceVec::from_host(ctx, specific_humidity)?;
    let mut rh_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(t_d.slice())
        .arg(q_d.slice())
        .arg(rh_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    rh_d.copy_to_host(ctx)
}
