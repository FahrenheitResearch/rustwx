//! Sigma coordinate -> pressure (hPa) — port of met-cu's
//! `sigma_to_pressure_kernel`. Matches `metrust::calc::atmo::sigma_to_pressure`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/sigma_to_pressure.cu");
const MODULE_KEY: &str = "thermo_sigma_to_pressure";
const FUNCTION: &str = "sigma_to_pressure_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Pressure (hPa) from sigma, surface pressure (hPa), top pressure (hPa).
pub fn host(
    ctx: &ContextHandle,
    sigma: &[f64],
    psfc: &[f64],
    ptop: &[f64],
) -> Result<Vec<f64>> {
    let n = sigma.len();
    if psfc.len() != n {
        return Err(Error::LengthMismatch {
            what: "sigma vs psfc",
            expected: n,
            got: psfc.len(),
        });
    }
    if ptop.len() != n {
        return Err(Error::LengthMismatch {
            what: "sigma vs ptop",
            expected: n,
            got: ptop.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let s_d = DeviceVec::from_host(ctx, sigma)?;
    let psfc_d = DeviceVec::from_host(ctx, psfc)?;
    let ptop_d = DeviceVec::from_host(ctx, ptop)?;
    let mut p_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(s_d.slice())
        .arg(psfc_d.slice())
        .arg(ptop_d.slice())
        .arg(p_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    p_d.copy_to_host(ctx)
}
