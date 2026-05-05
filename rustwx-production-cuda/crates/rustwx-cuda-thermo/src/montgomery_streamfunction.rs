//! Montgomery streamfunction `psi = Cp_d * T + g * z` — port of met-cu's
//! `montgomery_streamfunction_kernel`. Matches `metrust::calc::thermo::montgomery_streamfunction`
//! when `temperature` is supplied in **Kelvin** (the CPU reference takes T_K).
//!
//! NOTE: met-cu's Python kernel labelled the input `temperature` (ambiguous).
//! We adopt Kelvin to match the wx_math/metrust signature.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str =
    include_str!("../../../kernels/thermo/montgomery_streamfunction.cu");
const MODULE_KEY: &str = "thermo_montgomery_streamfunction";
const FUNCTION: &str = "montgomery_streamfunction_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Montgomery streamfunction (J/kg) from height (m) and temperature (Kelvin).
pub fn host(
    ctx: &ContextHandle,
    height: &[f64],
    temperature_k: &[f64],
) -> Result<Vec<f64>> {
    if height.len() != temperature_k.len() {
        return Err(Error::LengthMismatch {
            what: "height vs temperature_k",
            expected: height.len(),
            got: temperature_k.len(),
        });
    }
    let n = height.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let h_d = DeviceVec::from_host(ctx, height)?;
    let t_d = DeviceVec::from_host(ctx, temperature_k)?;
    let mut psi_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(h_d.slice())
        .arg(t_d.slice())
        .arg(psi_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    psi_d.copy_to_host(ctx)
}
