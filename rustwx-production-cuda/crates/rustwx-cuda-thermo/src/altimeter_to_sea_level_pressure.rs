//! Altimeter setting (hPa) -> sea level pressure (hPa) — port of met-cu's
//! `altimeter_to_sea_level_pressure_kernel`.
//!
//! DEFER: numerically diverges from `metrust::calc::atmo::altimeter_to_sea_level_pressure`.
//! met-cu's step-1 uses the simple ratio formula (`alt * ratio^(1/ROCP) + 0.3`)
//! while metrust uses the full Smithsonian inverse
//! (`(alt^n - p0^n*L*H/T0)^(1/n) + 0.3`). The two forms agree at sea level but
//! diverge at non-zero elevation by tens of Pa. See DIVERGENT_KERNELS.md.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/altimeter_to_sea_level_pressure.cu");
const MODULE_KEY: &str = "thermo_altimeter_to_sea_level_pressure";
const FUNCTION: &str = "altimeter_to_sea_level_pressure_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Sea-level pressure (hPa) from altimeter (hPa), elevation (m) and temperature (Celsius).
pub fn host(
    ctx: &ContextHandle,
    altimeter: &[f64],
    elevation: &[f64],
    temperature: &[f64],
) -> Result<Vec<f64>> {
    let n = altimeter.len();
    if elevation.len() != n {
        return Err(Error::LengthMismatch {
            what: "altimeter vs elevation",
            expected: n,
            got: elevation.len(),
        });
    }
    if temperature.len() != n {
        return Err(Error::LengthMismatch {
            what: "altimeter vs temperature",
            expected: n,
            got: temperature.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let alt_d = DeviceVec::from_host(ctx, altimeter)?;
    let elev_d = DeviceVec::from_host(ctx, elevation)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let mut slp_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(alt_d.slice())
        .arg(elev_d.slice())
        .arg(t_d.slice())
        .arg(slp_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    slp_d.copy_to_host(ctx)
}
