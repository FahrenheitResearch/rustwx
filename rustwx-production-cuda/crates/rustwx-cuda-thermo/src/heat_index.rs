//! NWS heat index (Rothfusz regression) — port of met-cu's
//! `heat_index_kernel`. Input: temperature (Celsius), RH (%). Output:
//! HI (Celsius).
//!
//! DEFER: actual_max_diff != 0 vs metrust::calc::atmo::heat_index near
//! the t_f≈80 boundary; see DIVERGENT_KERNELS.md. Met-cu's threshold
//! check (`t_f < 80`) differs from metrust, which averages Steadman with
//! T_F before deciding. We retain met-cu's logic verbatim and document
//! the divergence rather than masking it with a looser tolerance.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{launch_cfg_1d, ContextHandle, DeviceVec, Error, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/thermo/heat_index.cu");
const MODULE_KEY: &str = "thermo_heat_index";
const FUNCTION: &str = "heat_index_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Heat index (Celsius) from temperature (Celsius) and relative humidity (%).
pub fn host(
    ctx: &ContextHandle,
    temperature: &[f64],
    relative_humidity: &[f64],
) -> Result<Vec<f64>> {
    if temperature.len() != relative_humidity.len() {
        return Err(Error::LengthMismatch {
            what: "temperature vs relative_humidity",
            expected: temperature.len(),
            got: relative_humidity.len(),
        });
    }
    let n = temperature.len();
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let t_d = DeviceVec::from_host(ctx, temperature)?;
    let rh_d = DeviceVec::from_host(ctx, relative_humidity)?;
    let mut hi_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(t_d.slice())
        .arg(rh_d.slice())
        .arg(hi_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    hi_d.copy_to_host(ctx)
}
