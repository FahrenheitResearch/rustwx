//! Dewpoint (Celsius) from pressure (hPa) and specific humidity (kg/kg) —
//! port of met-cu's `dewpoint_from_specific_humidity_kernel`. Matches
//! `metrust::calc::thermo::dewpoint_from_specific_humidity`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/dewpoint_from_specific_humidity.cu");
const MODULE_KEY: &str = "thermo_dewpoint_from_specific_humidity";
const FUNCTION: &str = "dewpoint_from_specific_humidity_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Dewpoint (Celsius) from pressure (hPa) and specific humidity (kg/kg).
pub fn host(
    ctx: &ContextHandle,
    pressure: &[f64],
    specific_humidity: &[f64],
) -> Result<Vec<f64>> {
    if pressure.len() != specific_humidity.len() {
        return Err(Error::LengthMismatch {
            what: "pressure vs specific_humidity",
            expected: pressure.len(),
            got: specific_humidity.len(),
        });
    }
    let n = pressure.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let q_d = DeviceVec::from_host(ctx, specific_humidity)?;
    let mut td_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(q_d.slice())
        .arg(td_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    td_d.copy_to_host(ctx)
}
