//! Device-to-device 2D rectangular crop. Used to take a sub-grid out of a
//! larger device-resident field without round-tripping through host memory.
//!
//! Pipeline use: upload one large grid (e.g. CONUS) once, then call
//! `launch_device` repeatedly to extract regional crops on the GPU. d2d
//! bandwidth (~500 GB/s on HBM) is far higher than PCIe (~25 GB/s), so this
//! turns multiple per-region uploads into a single big upload + cheap crops.
//!
//! For 3D volumes, call `launch_device_3d`, which loops over levels.

use cudarc::driver::{CudaSlice, PushKernelArg};
use rustwx_cuda_core::{launch_cfg_2d, ContextHandle, KernelModule, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/grid/crop_2d.cu");
const MODULE_KEY: &str = "grid_crop_2d";
const FUNCTION: &str = "crop_2d_kernel";

/// Crop a `dst_nx × dst_ny` rectangle out of an `src_nx × src_ny` source,
/// starting at `(off_x, off_y)`.
pub fn launch_device(
    ctx: &ContextHandle,
    src: &CudaSlice<f64>,
    dst: &mut CudaSlice<f64>,
    src_nx: usize,
    dst_nx: usize,
    dst_ny: usize,
    off_x: usize,
    off_y: usize,
) -> Result<()> {
    let m = KernelModule::load(ctx, MODULE_KEY, &with_constants(KERNEL_SRC))?;
    let func = m.function(FUNCTION)?;

    let cfg = launch_cfg_2d(dst_nx as u32, dst_ny as u32, 16, 16);
    let src_nx_i = src_nx as i32;
    let dst_nx_i = dst_nx as i32;
    let dst_ny_i = dst_ny as i32;
    let off_x_i = off_x as i32;
    let off_y_i = off_y as i32;

    let mut b = ctx.stream().launch_builder(&func);
    b.arg(src)
        .arg(dst)
        .arg(&src_nx_i)
        .arg(&dst_nx_i)
        .arg(&dst_ny_i)
        .arg(&off_x_i)
        .arg(&off_y_i);
    unsafe { b.launch(cfg)? };
    Ok(())
}

/// Crop `nz` slabs sequentially. `src` is `[nz][src_ny][src_nx]`, `dst` is
/// `[nz][dst_ny][dst_nx]`. Each level is a separate kernel launch but the
/// kernels are tiny so launch overhead is negligible at HRRR scale.
pub fn launch_device_3d(
    ctx: &ContextHandle,
    src: &CudaSlice<f64>,
    dst: &mut CudaSlice<f64>,
    src_nx: usize,
    src_ny: usize,
    dst_nx: usize,
    dst_ny: usize,
    nz: usize,
    off_x: usize,
    off_y: usize,
) -> Result<()> {
    let m = KernelModule::load(ctx, MODULE_KEY, &with_constants(KERNEL_SRC))?;
    let func = m.function(FUNCTION)?;

    let src_slab = src_ny * src_nx;
    let dst_slab = dst_ny * dst_nx;
    let cfg = launch_cfg_2d(dst_nx as u32, dst_ny as u32, 16, 16);
    let src_nx_i = src_nx as i32;
    let dst_nx_i = dst_nx as i32;
    let dst_ny_i = dst_ny as i32;
    let off_x_i = off_x as i32;
    let off_y_i = off_y as i32;

    for k in 0..nz {
        let src_view = src.slice(k * src_slab..(k + 1) * src_slab);
        let mut dst_view = dst.slice_mut(k * dst_slab..(k + 1) * dst_slab);
        let mut b = ctx.stream().launch_builder(&func);
        b.arg(&src_view)
            .arg(&mut dst_view)
            .arg(&src_nx_i)
            .arg(&dst_nx_i)
            .arg(&dst_ny_i)
            .arg(&off_x_i)
            .arg(&off_y_i);
        unsafe { b.launch(cfg)? };
    }
    Ok(())
}
