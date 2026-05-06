//! Height (m) -> geopotential (m^2/s^2) — port of met-cu's
//! `height_to_geopotential_kernel`. Matches `metrust::calc::thermo::height_to_geopotential`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/height_to_geopotential.cu");
const MODULE_KEY: &str = "thermo_height_to_geopotential";
const FUNCTION: &str = "height_to_geopotential_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Geopotential (m^2/s^2) from geopotential height (m).
pub fn host(ctx: &ContextHandle, height: &[f64]) -> Result<Vec<f64>> {
    let n = height.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let h_d = DeviceVec::from_host(ctx, height)?;
    let mut g_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(h_d.slice()).arg(g_d.slice_mut()).arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    g_d.copy_to_host(ctx)
}
