//! Geopotential (m^2/s^2) -> height (m) — port of met-cu's
//! `geopotential_to_height_kernel`. Matches `metrust::calc::thermo::geopotential_to_height`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/geopotential_to_height.cu");
const MODULE_KEY: &str = "thermo_geopotential_to_height";
const FUNCTION: &str = "geopotential_to_height_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Geopotential height (m) from geopotential (m^2/s^2).
pub fn host(ctx: &ContextHandle, geopotential: &[f64]) -> Result<Vec<f64>> {
    let n = geopotential.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let g_d = DeviceVec::from_host(ctx, geopotential)?;
    let mut h_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(g_d.slice())
        .arg(h_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    h_d.copy_to_host(ctx)
}
