//! Standard-atmosphere height (m) from pressure (hPa) — port of met-cu's
//! `pressure_to_height_std_kernel`. Matches `wx_math::thermo::pressure_to_height_std`.
//!
//! NOTE: `metrust::calc::atmo::pressure_to_height_std` uses a different
//! constant set and will diverge slightly — we validate against the wx_math
//! reference which the kernel mirrors verbatim.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/pressure_to_height_std.cu");
const MODULE_KEY: &str = "thermo_pressure_to_height_std";
const FUNCTION: &str = "pressure_to_height_std_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Height (meters) at a given pressure (hPa) in the standard atmosphere.
pub fn host(ctx: &ContextHandle, pressure: &[f64]) -> Result<Vec<f64>> {
    let n = pressure.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, pressure)?;
    let mut h_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(h_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    h_d.copy_to_host(ctx)
}
