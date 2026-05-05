//! `comp = u * tx + v * ty` — port of met-cu's `tangential_component_kernel`.
//!
//! Wrapper takes per-element unit-tangent components `(tx, ty)` directly.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/wind/tangential_component.cu");
const MODULE_KEY: &str = "wind_tangential_component";
const FUNCTION: &str = "tangential_component_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Compute the tangential component of (u, v) along the unit tangent (tx, ty).
pub fn host(
    ctx: &ContextHandle,
    u: &[f64],
    v: &[f64],
    tx: &[f64],
    ty: &[f64],
) -> Result<Vec<f64>> {
    let n = u.len();
    if v.len() != n {
        return Err(Error::LengthMismatch {
            what: "u vs v",
            expected: n,
            got: v.len(),
        });
    }
    if tx.len() != n {
        return Err(Error::LengthMismatch {
            what: "u vs tx",
            expected: n,
            got: tx.len(),
        });
    }
    if ty.len() != n {
        return Err(Error::LengthMismatch {
            what: "u vs ty",
            expected: n,
            got: ty.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let u_d = DeviceVec::from_host(ctx, u)?;
    let v_d = DeviceVec::from_host(ctx, v)?;
    let tx_d = DeviceVec::from_host(ctx, tx)?;
    let ty_d = DeviceVec::from_host(ctx, ty)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(u_d.slice())
        .arg(v_d.slice())
        .arg(tx_d.slice())
        .arg(ty_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
