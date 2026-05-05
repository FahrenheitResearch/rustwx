//! Turbulent kinetic energy `TKE = 0.5 * (var(u) + var(v) + var(w))` — port of
//! met-cu's `tke_kernel`. Single-thread reduction, returns a scalar.
//! Matches `metrust::calc::wind::tke`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, LaunchCfg,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/tke.cu");
const MODULE_KEY: &str = "wind_tke";
const FUNCTION: &str = "tke_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// TKE from u, v, w time series. Returns a single-element `Vec<f64>`.
pub fn host(ctx: &ContextHandle, u: &[f64], v: &[f64], w: &[f64]) -> Result<Vec<f64>> {
    if u.len() != v.len() {
        return Err(Error::LengthMismatch {
            what: "u vs v",
            expected: u.len(),
            got: v.len(),
        });
    }
    if u.len() != w.len() {
        return Err(Error::LengthMismatch {
            what: "u vs w",
            expected: u.len(),
            got: w.len(),
        });
    }
    let n = u.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let u_d = DeviceVec::from_host(ctx, u)?;
    let v_d = DeviceVec::from_host(ctx, v)?;
    let w_d = DeviceVec::from_host(ctx, w)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, 1)?;

    let cfg = LaunchCfg {
        grid_dim: (1, 1, 1),
        block_dim: (1, 1, 1),
        shared_mem_bytes: 0,
    };
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_d.slice())
        .arg(v_d.slice())
        .arg(w_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
