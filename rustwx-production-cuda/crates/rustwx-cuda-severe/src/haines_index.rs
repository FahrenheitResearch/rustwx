//! Haines Index (Low Elevation) — port of met-cu's `haines_index_kernel`.
//! Matches `wx_math::composite::haines_index` (returned as f64).

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/haines_index.cu");
const MODULE_KEY: &str = "severe_haines_index";
const FUNCTION: &str = "haines_index_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Haines Index 2..=6 returned as f64 (always integer-valued).
/// Inputs are 950 hPa and 850 hPa T (deg C) and 850 hPa Td (deg C).
pub fn host(ctx: &ContextHandle, t950: &[f64], t850: &[f64], td850: &[f64]) -> Result<Vec<f64>> {
    let n = t950.len();
    if t850.len() != n {
        return Err(Error::LengthMismatch {
            what: "t850",
            expected: n,
            got: t850.len(),
        });
    }
    if td850.len() != n {
        return Err(Error::LengthMismatch {
            what: "td850",
            expected: n,
            got: td850.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let t950_d = DeviceVec::from_host(ctx, t950)?;
    let t850_d = DeviceVec::from_host(ctx, t850)?;
    let td_d = DeviceVec::from_host(ctx, td850)?;
    let mut h_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(t950_d.slice())
        .arg(t850_d.slice())
        .arg(td_d.slice())
        .arg(h_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    h_d.copy_to_host(ctx)
}
