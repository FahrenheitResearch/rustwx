//! `comp = u * nx + v * ny` — port of met-cu's `normal_component_kernel`.
//!
//! The wrapper takes per-element unit-normal components `(nx, ny)` directly,
//! matching the in-kernel signature. Compute `(nx, ny)` upstream from a
//! cross-section if you need the metrust API surface.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/normal_component.cu");
const MODULE_KEY: &str = "wind_normal_component";
const FUNCTION: &str = "normal_component_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Compute the normal component of (u, v) onto the unit normal (nx, ny).
pub fn host(ctx: &ContextHandle, u: &[f64], v: &[f64], nx: &[f64], ny: &[f64]) -> Result<Vec<f64>> {
    let n = u.len();
    if v.len() != n {
        return Err(Error::LengthMismatch {
            what: "u vs v",
            expected: n,
            got: v.len(),
        });
    }
    if nx.len() != n {
        return Err(Error::LengthMismatch {
            what: "u vs nx",
            expected: n,
            got: nx.len(),
        });
    }
    if ny.len() != n {
        return Err(Error::LengthMismatch {
            what: "u vs ny",
            expected: n,
            got: ny.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let u_d = DeviceVec::from_host(ctx, u)?;
    let v_d = DeviceVec::from_host(ctx, v)?;
    let nx_d = DeviceVec::from_host(ctx, nx)?;
    let ny_d = DeviceVec::from_host(ctx, ny)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_d.slice())
        .arg(v_d.slice())
        .arg(nx_d.slice())
        .arg(ny_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
