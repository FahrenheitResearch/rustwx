//! Supercell Composite Parameter (SCP) — port of met-cu's
//! `supercell_composite_parameter_kernel`. Matches
//! `metrust::calc::severe::supercell_composite_parameter`.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/scp.cu");
const MODULE_KEY: &str = "severe_scp";
const FUNCTION: &str = "scp_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Device-resident launch — caller owns the buffers.
pub fn launch_device(
    ctx: &ContextHandle,
    mucape: &CudaSlice<f64>,
    srh: &CudaSlice<f64>,
    shear: &CudaSlice<f64>,
    scp: &mut CudaSlice<f64>,
    n: usize,
) -> Result<()> {
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(mucape)
        .arg(srh)
        .arg(shear)
        .arg(scp)
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };
    Ok(())
}

/// Compute SCP elementwise.
/// `mucape` (J/kg), `srh` effective layer (m^2/s^2), `shear` effective bulk shear (m/s).
pub fn host(
    ctx: &ContextHandle,
    mucape: &[f64],
    srh: &[f64],
    shear: &[f64],
) -> Result<Vec<f64>> {
    if srh.len() != mucape.len() {
        return Err(Error::LengthMismatch {
            what: "mucape vs srh",
            expected: mucape.len(),
            got: srh.len(),
        });
    }
    if shear.len() != mucape.len() {
        return Err(Error::LengthMismatch {
            what: "mucape vs shear",
            expected: mucape.len(),
            got: shear.len(),
        });
    }
    let n = mucape.len();
    let mucape_d = DeviceVec::from_host(ctx, mucape)?;
    let srh_d = DeviceVec::from_host(ctx, srh)?;
    let shear_d = DeviceVec::from_host(ctx, shear)?;
    let mut scp_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;
    launch_device(
        ctx,
        mucape_d.slice(),
        srh_d.slice(),
        shear_d.slice(),
        scp_d.slice_mut(),
        n,
    )?;
    scp_d.copy_to_host(ctx)
}
