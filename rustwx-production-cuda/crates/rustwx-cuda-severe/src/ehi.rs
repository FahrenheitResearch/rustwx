//! Energy-Helicity Index — port of met-cu's `compute_ehi_kernel`.
//! Matches `wx_math::composite::compute_ehi`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/ehi.cu");
const MODULE_KEY: &str = "severe_ehi";
const FUNCTION: &str = "ehi_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    cape: &CudaSlice<f64>,
    srh: &CudaSlice<f64>,
    ehi: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder.arg(cape).arg(srh).arg(ehi).arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Compute EHI = (CAPE * SRH) / 160000 elementwise.
pub fn host(
    ctx: &ContextHandle,
    cape: &[f64],
    srh: &[f64],
) -> Result<Vec<f64>> {
    if srh.len() != cape.len() {
        return Err(Error::LengthMismatch {
            what: "cape vs srh",
            expected: cape.len(),
            got: srh.len(),
        });
    }
    let n = cape.len();
    let cape_d = DeviceVec::from_host(ctx, cape)?;
    let srh_d = DeviceVec::from_host(ctx, srh)?;
    let mut ehi_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(ctx, cape_d.slice(), srh_d.slice(), ehi_d.slice_mut(), n)?;
    ehi_d.copy_to_host(ctx)
}
