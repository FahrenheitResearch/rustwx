// SWEAT Index (Severe Weather Threat Index).
//
// SWEAT = 12*Td850 + 20*(TT-49) + 2*f850 + f500 + 125*(sin(d500-d850) + 0.2)
//   - term1 = max(0, 12*Td850)
//   - term2 = max(0, 20*(TT-49))
//   - term3 = 2*wspd850
//   - term4 = wspd500
//   - term5 = 125*(sin(d_diff)+0.2) only when wind direction/speed criteria met,
//             else 0
//   - sweat = max(0, sum)
//
// Mirrors wx_math::composite::sweat_index.
//
// Inputs:  tt, td850 (deg C), wspd850, wdir850, wspd500, wdir500.

extern "C" __global__
void sweat_index_kernel(
    const double* __restrict__ tt,
    const double* __restrict__ td850,
    const double* __restrict__ wspd850,
    const double* __restrict__ wdir850,
    const double* __restrict__ wspd500,
    const double* __restrict__ wdir500,
    double* __restrict__ sweat,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;

    double td = td850[idx];
    double tt_v = tt[idx];
    double f850 = wspd850[idx];
    double d850 = wdir850[idx];
    double f500 = wspd500[idx];
    double d500 = wdir500[idx];

    double term1 = (td > 0.0) ? 12.0 * td : 0.0;
    double term2 = (tt_v > 49.0) ? 20.0 * (tt_v - 49.0) : 0.0;
    double term3 = 2.0 * f850;
    double term4 = f500;

    double d_diff = d500 - d850;
    double term5 = 0.0;
    if (d850 >= 130.0 && d850 <= 250.0 &&
        d500 >= 210.0 && d500 <= 310.0 &&
        d_diff > 0.0 &&
        f850 >= 15.0 && f500 >= 15.0) {
        // d_diff in degrees -> radians, matching Rust's f64::to_radians
        // which is `x * (std::f64::consts::PI / 180.0)`.
        const double PI_OVER_180 = 3.141592653589793 / 180.0;
        double rad = d_diff * PI_OVER_180;
        term5 = 125.0 * (sin(rad) + 0.2);
    }

    double s = term1 + term2 + term3 + term4 + term5;
    if (s < 0.0) s = 0.0;
    sweat[idx] = s;
}
