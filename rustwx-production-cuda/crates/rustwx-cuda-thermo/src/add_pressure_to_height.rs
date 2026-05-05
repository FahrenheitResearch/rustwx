//! Add a pressure increment to a height via the standard atmosphere — port of
//! met-cu's `add_pressure_to_height_kernel`. Matches
//! `metrust::calc::thermo::add_pressure_to_height`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/add_pressure_to_height.cu");
const MODULE_KEY: &str = "thermo_add_pressure_to_height";
const FUNCTION: &str = "add_pressure_to_height_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// New height (m) after a pressure increment (hPa).
pub fn host(
    ctx: &ContextHandle,
    height: &[f64],
    delta_pressure: &[f64],
) -> Result<Vec<f64>> {
    if height.len() != delta_pressure.len() {
        return Err(Error::LengthMismatch {
            what: "height vs delta_pressure",
            expected: height.len(),
            got: delta_pressure.len(),
        });
    }
    let n = height.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let h_d = DeviceVec::from_host(ctx, height)?;
    let dp_d = DeviceVec::from_host(ctx, delta_pressure)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(h_d.slice())
        .arg(dp_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
