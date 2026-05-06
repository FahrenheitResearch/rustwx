//! Kernel-source helpers for grid stencil kernels.

pub const CONSTANTS_CUH: &str = include_str!("../../../kernels/common/constants.cuh");

pub const GRID_HELPERS_CUH: &str = include_str!("../../../kernels/common/grid_helpers.cuh");

#[allow(dead_code)]
pub fn with_constants(kernel_src: &str) -> String {
    let mut s = String::with_capacity(CONSTANTS_CUH.len() + kernel_src.len() + 8);
    s.push_str(CONSTANTS_CUH);
    s.push('\n');
    s.push_str(kernel_src);
    s
}

pub fn with_grid_helpers(kernel_src: &str) -> String {
    let mut s =
        String::with_capacity(CONSTANTS_CUH.len() + GRID_HELPERS_CUH.len() + kernel_src.len() + 16);
    s.push_str(CONSTANTS_CUH);
    s.push('\n');
    s.push_str(GRID_HELPERS_CUH);
    s.push('\n');
    s.push_str(kernel_src);
    s
}
