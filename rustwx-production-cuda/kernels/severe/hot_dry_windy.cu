// Hot-Dry-Windy Index = VPD * wind.
//
// Mirrors wx_math::composite::hot_dry_windy and met-cu's hot_dry_windy_kernel.
// When vpd_in > 0 the supplied VPD is used; otherwise it is computed from
// (T, RH) using the SHARPpy/Wexler 8th-order polynomial (matching
// wx_math::thermo::vappres).
//
// Inputs:  t_c (deg C), rh (%), wspd_ms (m/s), vpd_in (hPa, 0 = compute).
// Output:  HDW.

extern "C" __global__
void hot_dry_windy_kernel(
    const double* __restrict__ t_c,
    const double* __restrict__ rh,
    const double* __restrict__ wspd_ms,
    const double* __restrict__ vpd_in,
    double* __restrict__ hdw,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;

    double t = t_c[idx];
    double r = rh[idx];
    double w = wspd_ms[idx];
    double vpd_user = vpd_in[idx];

    double vpd;
    if (vpd_user > 0.0) {
        vpd = vpd_user;
    } else {
        // SHARPpy/Wexler vapor-pressure polynomial. Bit-identical to
        // wx_math::thermo::vappres apart from pow vs powi(8).
        double pol = t * (1.1112018e-17 + (t * -3.0994571e-20));
        pol = t * (2.1874425e-13 + (t * (-1.789232e-15 + pol)));
        pol = t * (4.3884180e-09 + (t * (-2.988388e-11 + pol)));
        pol = t * (7.8736169e-05 + (t * (-6.111796e-07 + pol)));
        pol = 0.99999683 + (t * (-9.082695e-03 + pol));
        // Match Rust's `pol.powi(8)` via repeated squaring for bit-identical
        // results.
        double p2 = pol * pol;
        double p4 = p2 * p2;
        double p8 = p4 * p4;
        double es = 6.1078 / p8;
        double ea = es * (r / 100.0);
        vpd = es - ea;
        if (vpd < 0.0) vpd = 0.0;
    }

    hdw[idx] = vpd * w;
}
