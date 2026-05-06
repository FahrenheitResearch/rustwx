//! Kernel-source helpers for map-rasterization kernels. Mirrors the
//! pattern in rustwx-cuda-{thermo,wind,severe,grid}.

pub const CONSTANTS_CUH: &str = include_str!("../../../kernels/common/constants.cuh");

pub fn with_constants(kernel_src: &str) -> String {
    let mut s = String::with_capacity(CONSTANTS_CUH.len() + kernel_src.len() + 8);
    s.push_str(CONSTANTS_CUH);
    s.push('\n');
    s.push_str(kernel_src);
    s
}
