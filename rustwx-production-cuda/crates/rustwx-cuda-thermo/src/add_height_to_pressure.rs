//! Add a height increment to a pressure via the standard atmosphere — port of
//! met-cu's `add_height_to_pressure_kernel`. Matches
//! `metrust::calc::thermo::add_height_to_pressure` (which composes
//! wx_math std-atm functions; the kernel uses the same formulas).

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/add_height_to_pressure.cu");
const MODULE_KEY: &str = "thermo_add_height_to_pressure";
const FUNCTION: &str = "add_height_to_pressure_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// New pressure (hPa) after a height increment (m).
pub fn host(ctx: &ContextHandle, pressure: &[f64], delta_height: &[f64]) -> Result<Vec<f64>> {
    if pressure.len() != delta_height.len() {
        return Err(Error::LengthMismatch {
            what: "pressure vs delta_height",
            expected: pressure.len(),
            got: delta_height.len(),
        });
    }
    let n = pressure.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let dh_d = DeviceVec::from_host(ctx, delta_height)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(dh_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
