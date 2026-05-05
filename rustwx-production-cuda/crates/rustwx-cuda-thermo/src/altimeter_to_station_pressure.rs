//! Altimeter setting (hPa) -> station pressure (hPa) — port of met-cu's
//! `altimeter_to_station_pressure_kernel`. Matches
//! `wx_math::thermo::altimeter_to_station_pressure` (simple ratio form).
//!
//! DEFER vs metrust: `metrust::calc::atmo::altimeter_to_station_pressure` uses
//! the Smithsonian +0.3 inverse instead. We validate against the wx_math
//! reference. See DIVERGENT_KERNELS.md.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/altimeter_to_station_pressure.cu");
const MODULE_KEY: &str = "thermo_altimeter_to_station_pressure";
const FUNCTION: &str = "altimeter_to_station_pressure_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Station pressure (hPa) from altimeter (hPa) and elevation (m).
pub fn host(
    ctx: &ContextHandle,
    altimeter: &[f64],
    elevation: &[f64],
) -> Result<Vec<f64>> {
    if altimeter.len() != elevation.len() {
        return Err(Error::LengthMismatch {
            what: "altimeter vs elevation",
            expected: altimeter.len(),
            got: elevation.len(),
        });
    }
    let n = altimeter.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let alt_d = DeviceVec::from_host(ctx, altimeter)?;
    let elev_d = DeviceVec::from_host(ctx, elevation)?;
    let mut p_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(alt_d.slice())
        .arg(elev_d.slice())
        .arg(p_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    p_d.copy_to_host(ctx)
}
