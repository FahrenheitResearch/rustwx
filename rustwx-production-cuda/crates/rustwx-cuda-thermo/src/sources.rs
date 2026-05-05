//! Kernel source strings, embedded at build time.
//!
//! NVRTC doesn't resolve filesystem `#include`s in the runtime path we use,
//! so each kernel module concatenates the shared headers + its own `.cu`
//! source into one string and feeds that to the compiler. The cache key
//! includes a hash of the full concatenated source, so changing any header
//! invalidates every dependent kernel automatically.

pub const CONSTANTS_CUH: &str =
    include_str!("../../../kernels/common/constants.cuh");

#[allow(dead_code)]
pub const THERMO_HELPERS_CUH: &str =
    include_str!("../../../kernels/common/thermo_helpers.cuh");

/// `kernels/thermo/<file>.cu` prefixed with the constants header.
pub fn with_constants(kernel_src: &str) -> String {
    let mut s = String::with_capacity(CONSTANTS_CUH.len() + kernel_src.len() + 8);
    s.push_str(CONSTANTS_CUH);
    s.push('\n');
    s.push_str(kernel_src);
    s
}

/// Constants + thermo helpers + kernel source.
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
