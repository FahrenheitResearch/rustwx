//! Station pressure (hPa) -> altimeter setting (hPa) — port of met-cu's
//! `station_to_altimeter_pressure_kernel`.
//!
//! DEFER: kernel uses the literal `BARO_EXP = 0.190284` whereas
//! `metrust::calc::atmo::station_to_altimeter_pressure` recomputes
//! `BARO_EXP = G * M_air / (R_star * L)` (~0.19026308...). Disagreement
//! is in the ~1e-5 relative range, well above 1e-10. See DIVERGENT_KERNELS.md.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/station_to_altimeter_pressure.cu");
const MODULE_KEY: &str = "thermo_station_to_altimeter_pressure";
const FUNCTION: &str = "station_to_altimeter_pressure_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Altimeter setting (hPa) from station pressure (hPa) and elevation (m).
pub fn host(ctx: &ContextHandle, station_pressure: &[f64], elevation: &[f64]) -> Result<Vec<f64>> {
    if station_pressure.len() != elevation.len() {
        return Err(Error::LengthMismatch {
            what: "station_pressure vs elevation",
            expected: station_pressure.len(),
            got: elevation.len(),
        });
    }
    let n = station_pressure.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let p_d = DeviceVec::from_host(ctx, station_pressure)?;
    let elev_d = DeviceVec::from_host(ctx, elevation)?;
    let mut alt_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(p_d.slice())
        .arg(elev_d.slice())
        .arg(alt_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    alt_d.copy_to_host(ctx)
}
