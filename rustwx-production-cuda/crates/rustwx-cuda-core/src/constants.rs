//! Physical constants used across kernels — the Rust-side mirror of
//! `kernels/common/constants.cuh`. Keep these in sync; verification tests
//! fail loudly if a kernel and its CPU reference disagree.

pub const RD: f64 = 287.04749097718457; // J/(kg*K) — dry air gas constant
pub const RV: f64 = 461.52311572606084; // J/(kg*K) — water vapor gas constant
pub const CP_D: f64 = 1004.6662184201462;
pub const EPS: f64 = 0.6219569100577033; // Rd/Rv
pub const G: f64 = 9.80665;
pub const ROCP: f64 = 0.2857142857142857; // Rd/Cp = 2/7
pub const ZEROCNK: f64 = 273.15;
pub const LV0: f64 = 2_500_840.0;
pub const LS0: f64 = 2_834_540.0;
pub const LAPSE_STD: f64 = 0.0065;
pub const P0_STD: f64 = 1013.25;
pub const T0_STD: f64 = 288.15;
pub const OMEGA: f64 = 7.2921159e-5; // rad/s — Earth angular velocity (matches wx-math)
