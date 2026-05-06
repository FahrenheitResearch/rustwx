//! Standard-atmosphere pressure (hPa) from height (m) — port of met-cu's
//! `height_to_pressure_std_kernel`. Matches `wx_math::thermo::height_to_pressure_std`.
//!
//! NOTE: `metrust::calc::atmo::height_to_pressure_std` uses a different
//! constant set (T0=288.0, BARO_EXP via M_air/R_star) and will diverge — we
//! validate against the wx_math reference, which the kernel mirrors verbatim.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/height_to_pressure_std.cu");
const MODULE_KEY: &str = "thermo_height_to_pressure_std";
const FUNCTION: &str = "height_to_pressure_std_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Pressure (hPa) at a given height (meters) in the standard atmosphere.
pub fn host(ctx: &ContextHandle, height: &[f64]) -> Result<Vec<f64>> {
    let n = height.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let h_d = DeviceVec::from_host(ctx, height)?;
    let mut p_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(h_d.slice()).arg(p_d.slice_mut()).arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    p_d.copy_to_host(ctx)
}
