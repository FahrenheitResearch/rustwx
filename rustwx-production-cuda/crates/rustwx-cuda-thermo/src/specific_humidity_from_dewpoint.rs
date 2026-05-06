//! Specific humidity (kg/kg) from pressure and dewpoint — port of met-cu's
//! `specific_humidity_from_dewpoint_kernel`. Matches
//! `metrust::calc::thermo::specific_humidity_from_dewpoint`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_thermo_helpers;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/specific_humidity_from_dewpoint.cu");
const MODULE_KEY: &str = "thermo_specific_humidity_from_dewpoint";
const FUNCTION: &str = "specific_humidity_from_dewpoint_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_thermo_helpers(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Specific humidity (kg/kg) from pressure (hPa) and dewpoint (C).
pub fn host(ctx: &ContextHandle, pressure: &[f64], dewpoint: &[f64]) -> Result<Vec<f64>> {
    if pressure.len() != dewpoint.len() {
        return Err(Error::LengthMismatch {
            what: "pressure vs dewpoint",
            expected: pressure.len(),
            got: dewpoint.len(),
        });
    }
    let n = pressure.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let td_d = DeviceVec::from_host(ctx, dewpoint)?;
    let mut q_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(td_d.slice())
        .arg(q_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    q_d.copy_to_host(ctx)
}
