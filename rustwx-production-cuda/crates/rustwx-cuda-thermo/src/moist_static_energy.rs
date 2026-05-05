//! `MSE = Cp_d*T + g*z + Lv0*q` — port of met-cu's
//! `moist_static_energy_kernel`. Pass `temperature` in Kelvin and
//! `specific_humidity` in kg/kg to match `wx_math::thermo::moist_static_energy`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/moist_static_energy.cu");
const MODULE_KEY: &str = "thermo_moist_static_energy";
const FUNCTION: &str = "moist_static_energy_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Moist static energy (J/kg). `height` (m), `temperature` (K),
/// `specific_humidity` (kg/kg).
pub fn host(
    ctx: &ContextHandle,
    height: &[f64],
    temperature: &[f64],
    specific_humidity: &[f64],
) -> Result<Vec<f64>> {
    let n = height.len();
    if temperature.len() != n {
        return Err(Error::LengthMismatch {
            what: "height vs temperature",
            expected: n,
            got: temperature.len(),
        });
    }
    if specific_humidity.len() != n {
        return Err(Error::LengthMismatch {
            what: "height vs specific_humidity",
            expected: n,
            got: specific_humidity.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let h_d = DeviceVec::from_host(ctx, height)?;
    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let q_d = DeviceVec::from_host(ctx, specific_humidity)?;
    let mut mse_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(h_d.slice())
        .arg(t_d.slice())
        .arg(q_d.slice())
        .arg(mse_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    mse_d.copy_to_host(ctx)
}
