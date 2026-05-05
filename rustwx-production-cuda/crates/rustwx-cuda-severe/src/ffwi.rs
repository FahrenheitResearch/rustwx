//! Fosberg Fire Weather Index (FFWI) — port of met-cu's
//! `fosberg_fire_weather_index_kernel`. Matches
//! `wx_math::composite::fosberg_fire_weather_index`.

use cudarc::driver::PushKernelArg;
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, Error, KernelModule, Result, launch_cfg_1d,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/severe/ffwi.cu");
const MODULE_KEY: &str = "severe_ffwi";
const FUNCTION: &str = "ffwi_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// FFWI. `t_f` (deg F), `rh` (%, 0-100), `wspd_mph` (mph). Returns [0, 100].
pub fn host(
    ctx: &ContextHandle,
    t_f: &[f64],
    rh: &[f64],
    wspd_mph: &[f64],
) -> Result<Vec<f64>> {
    let n = t_f.len();
    if rh.len() != n {
        return Err(Error::LengthMismatch {
            what: "rh",
            expected: n,
            got: rh.len(),
        });
    }
    if wspd_mph.len() != n {
        return Err(Error::LengthMismatch {
            what: "wspd_mph",
            expected: n,
            got: wspd_mph.len(),
        });
    }
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let tf_d = DeviceVec::from_host(ctx, t_f)?;
    let rh_d = DeviceVec::from_host(ctx, rh)?;
    let w_d = DeviceVec::from_host(ctx, wspd_mph)?;
    let mut out_d: DeviceVec<f64> = DeviceVec::zeros(ctx, n)?;

    let cfg = launch_cfg_1d(n, 256);
    let n_i32: i32 = n as i32;
    let mut builder = ctx.stream().launch_builder(&func);
    builder
        .arg(tf_d.slice())
        .arg(rh_d.slice())
        .arg(w_d.slice())
        .arg(out_d.slice_mut())
        .arg(&n_i32);
    unsafe { builder.launch(cfg)? };

    out_d.copy_to_host(ctx)
}
