// Shared physical constants. Kept identical to crates/rustwx-cuda-core/src/constants.rs.
// Verified against metrust wx-math at ~1e-10 tolerance.

#pragma once

__device__ const double RD   = 287.04749097718457;
__device__ const double RV   = 461.52311572606084;
__device__ const double CP_D = 1004.6662184201462;
__device__ const double G0   = 9.80665;
__device__ const double ROCP = 0.2857142857142857;
__device__ const double ZEROCNK = 273.15;
__device__ const double EPS  = 0.6219569100577033;
__device__ const double LV0  = 2500840.0;
__device__ const double LS0  = 2834540.0;
__device__ const double LAPSE_STD = 0.0065;
__device__ const double P0_STD   = 1013.25;
__device__ const double T0_STD   = 288.15;

__device__ const double SAT_PRESSURE_0C = 611.2;
__device__ const double T0_TRIP = 273.16;
__device__ const double OMEGA   = 7.2921159e-5; // rad/s — Earth angular velocity (matches wx-math)
__device__ const double CP_L = 4219.4;
__device__ const double CP_V = 1860.078011865639;
__device__ const double CP_I = 2090.0;
__device__ const double RV_METPY = 461.52311572606084;

// NVRTC doesn't define <math.h> macros — provide M_PI explicitly so kernels
// using it (coriolis_parameter, wind_components, wind_direction, ...) compile.
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif
