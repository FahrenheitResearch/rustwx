// Significant Hail Parameter (SHIP).
//
// Mirrors wx_math::composite::significant_hail_parameter and met-cu's
// compute_ship_kernel.
//
// SHIP = (MUCAPE * MR * LR * (-T500) * SHEAR) / 42000000
//   then scaled by mucape/1300 when mucape < 1300.
// All component inputs are floored at 0; t500 is negated then floored at 0.
//
// Inputs:  cape (J/kg), shear (m/s), t500 (deg C, expected negative),
//          lr_700_500 (deg C/km), mr (g/kg).
// Output:  SHIP (dimensionless).

extern "C" __global__
void ship_kernel(
    const double* __restrict__ cape,
    const double* __restrict__ shear,
    const double* __restrict__ t500,
    const double* __restrict__ lr,
    const double* __restrict__ mr,
    double* __restrict__ ship,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;

    double mucape = cape[idx];
    if (mucape < 0.0) mucape = 0.0;

    double mr_v = mr[idx];
    if (mr_v < 0.0) mr_v = 0.0;

    double lr_v = lr[idx];
    if (lr_v < 0.0) lr_v = 0.0;

    double t5 = -t500[idx];
    if (t5 < 0.0) t5 = 0.0;

    double s06 = shear[idx];
    if (s06 < 0.0) s06 = 0.0;

    double s = (mucape * mr_v * lr_v * t5 * s06) / 42000000.0;
    if (mucape < 1300.0) {
        s = s * (mucape / 1300.0);
    }
    ship[idx] = s;
}
