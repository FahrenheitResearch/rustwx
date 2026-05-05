//! Significant Tornado Parameter (fixed-layer STP) — port of met-cu's
//! `significant_tornado_parameter_kernel`. Matches
//! `metrust::calc::severe::significant_tornado_parameter`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/stp.cu");
const MODULE_KEY: &str = "severe_stp";
const FUNCTION: &str = "stp_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    cape: &CudaSlice<f64>,
    lcl: &CudaSlice<f64>,
    srh: &CudaSlice<f64>,
    shear: &CudaSlice<f64>,
    stp: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(cape)
        .arg(lcl)
        .arg(srh)
        .arg(shear)
        .arg(stp)
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Compute STP elementwise.
/// `cape` (J/kg), `lcl` height AGL (m), `srh` 0-1 km (m^2/s^2),
/// `shear` 0-6 km bulk shear (m/s).
pub fn host(
    ctx: &ContextHandle,
    cape: &[f64],
    lcl: &[f64],
    srh: &[f64],
    shear: &[f64],
) -> Result<Vec<f64>> {
    if lcl.len() != cape.len() {
        return Err(Error::LengthMismatch {
            what: "cape vs lcl",
            expected: cape.len(),
            got: lcl.len(),
        });
    }
    if srh.len() != cape.len() {
        return Err(Error::LengthMismatch {
            what: "cape vs srh",
            expected: cape.len(),
            got: srh.len(),
        });
    }
    if shear.len() != cape.len() {
        return Err(Error::LengthMismatch {
            what: "cape vs shear",
            expected: cape.len(),
            got: shear.len(),
        });
    }
    let n = cape.len();
    let cape_d = DeviceVec::from_host(ctx, cape)?;
    let lcl_d = DeviceVec::from_host(ctx, lcl)?;
    let srh_d = DeviceVec::from_host(ctx, srh)?;
    let shear_d = DeviceVec::from_host(ctx, shear)?;
    let mut stp_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(
        ctx,
        cape_d.slice(),
        lcl_d.slice(),
        srh_d.slice(),
        shear_d.slice(),
        stp_d.slice_mut(),
        n,
    )?;
    stp_d.copy_to_host(ctx)
}
