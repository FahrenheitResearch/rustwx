//! Kernel-source helpers — same pattern as `rustwx-cuda-thermo/src/sources.rs`.

pub const CONSTANTS_CUH: &str =
    include_str!("../../../kernels/common/constants.cuh");

#[allow(dead_code)]
pub const THERMO_HELPERS_CUH: &str =
    include_str!("../../../kernels/common/thermo_helpers.cuh");

pub fn with_constants(kernel_src: &str) -> String {
    let mut s = String::with_capacity(CONSTANTS_CUH.len() + kernel_src.len() + 8);
    s.push_str(CONSTANTS_CUH);
    s.push('\n');
    s.push_str(kernel_src);
    s
}

#[allow(dead_code)]
pub fn with_thermo_helpers(kernel_src: &str) -> String {
    let mut s = String::with_capacity(
        CONSTANTS_CUH.len() + THERMO_HELPERS_CUH.len() + kernel_src.len() + 16,
    );
    s.push_str(CONSTANTS_CUH);
    s.push('\n');
    s.push_str(THERMO_HELPERS_CUH);
    s.push('\n');
    s.push_str(kernel_src);
    s
}
