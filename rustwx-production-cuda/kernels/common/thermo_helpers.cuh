// Thermodynamic device helpers ported verbatim from met-cu
// (python/metcu/kernels/thermo.py, _CUDA_CONSTANTS block).
// Each helper has been spot-verified against metrust wx-math.
//
// Usage: include `constants.cuh` first, then this header.

#pragma once

// Saturation vapor pressure over liquid water (Pa) -- Ambaum (2020)
__device__ inline double svp_liquid_pa(double t_k) {
    double latent = LV0 - (CP_L - CP_V) * (t_k - T0_TRIP);
    double heat_pow = (CP_L - CP_V) / RV_METPY;
    double exp_term = (LV0 / T0_TRIP - latent / t_k) / RV_METPY;
    return SAT_PRESSURE_0C * pow(T0_TRIP / t_k, heat_pow) * exp(exp_term);
}

// SVP in hPa from Celsius
__device__ inline double svp_hpa(double t_c) {
    return svp_liquid_pa(t_c + ZEROCNK) / 100.0;
}

// Saturation mixing ratio (kg/kg) from p (hPa) and T (Celsius)
__device__ inline double sat_mixing_ratio(double p_hpa, double t_c) {
    double es = svp_hpa(t_c);
    double ws = EPS * es / (p_hpa - es);
    return ws > 0.0 ? ws : 0.0;
}

// SHARPpy vapor pressure (hPa) from Celsius (Wexler enhancement polynomial)
__device__ inline double vappres_sharppy(double t) {
    double pol = t * (1.1112018e-17 + (t * -3.0994571e-20));
    pol = t * (2.1874425e-13 + (t * (-1.789232e-15 + pol)));
    pol = t * (4.3884180e-09 + (t * (-2.988388e-11 + pol)));
    pol = t * (7.8736169e-05 + (t * (-6.111796e-07 + pol)));
    pol = 0.99999683 + (t * (-9.082695e-03 + pol));
    double p8 = pol * pol; p8 *= p8; p8 *= p8;
    return 6.1078 / p8;
}

// SHARPpy mixing ratio (g/kg) from p (hPa) and T (Celsius)
__device__ inline double mixratio_gkg(double p, double t) {
    double x = 0.02 * (t - 12.5 + (7500.0 / p));
    double wfw = 1.0 + (0.0000045 * p) + (0.0014 * x * x);
    double fwesw = wfw * vappres_sharppy(t);
    return 621.97 * (fwesw / (p - fwesw));
}

// Virtual temperature in Celsius
__device__ inline double virtual_temp(double t, double p, double td) {
    double w = mixratio_gkg(p, td) / 1000.0;
    double tk = t + ZEROCNK;
    return tk * (1.0 + 0.61 * w) - ZEROCNK;
}

// Wobus function for moist-adiabat iteration
__device__ inline double wobf(double t) {
    double tc = t - 20.0;
    if (tc <= 0.0) {
        double npol = 1.0
            + tc * (-8.841660499999999e-3
                + tc * (1.4714143e-4
                    + tc * (-9.671989000000001e-7
                        + tc * (-3.2607217e-8 + tc * (-3.8598073e-10)))));
        double n2 = npol * npol;
        return 15.13 / (n2 * n2);
    } else {
        double ppol = tc
            * (4.9618922e-07
                + tc * (-6.1059365e-09
                    + tc * (3.9401551e-11
                        + tc * (-1.2588129e-13 + tc * (1.6688280e-16)))));
        ppol = 1.0 + tc * (3.6182989e-03 + tc * (-1.3603273e-05 + ppol));
        double p2 = ppol * ppol;
        return (29.93 / (p2 * p2)) + (0.96 * tc) - 14.8;
    }
}

// Saturated lift -- Newton-Raphson, max 7 iters
__device__ inline double satlift(double p, double thetam) {
    if (p >= 1000.0) return thetam;
    double pwrp = pow(p / 1000.0, ROCP);
    double t1 = (thetam + ZEROCNK) * pwrp - ZEROCNK;
    double e1 = wobf(t1) - wobf(thetam);
    double rate = 1.0;
    for (int iter = 0; iter < 7; iter++) {
        if (fabs(e1) < 0.001) break;
        double t2 = t1 - (e1 * rate);
        double e2 = (t2 + ZEROCNK) / pwrp - ZEROCNK;
        e2 += wobf(t2) - wobf(e2) - thetam;
        rate = (t2 - t1) / (e2 - e1);
        t1 = t2;
        e1 = e2;
    }
    return t1 - e1 * rate;
}

// LCL temperature from T, Td (both Celsius)
__device__ inline double lcltemp(double t, double td) {
    double s = t - td;
    double dlt = s * (1.2185 + 0.001278 * t + s * (-0.00219 + 1.173e-5 * s - 0.0000052 * t));
    return t - dlt;
}

// Dry lift to LCL: outputs p_lcl (hPa), t_lcl (Celsius)
__device__ inline void drylift(double p, double t, double td,
                               double *p_lcl, double *t_lcl) {
    *t_lcl = lcltemp(t, td);
    *p_lcl = 1000.0 * pow((*t_lcl + ZEROCNK) /
                          ((t + ZEROCNK) * pow(1000.0 / p, ROCP)),
                          1.0 / ROCP);
}

// Dewpoint (Celsius) from vapor pressure (hPa) -- inverse Bolton
__device__ inline double dewpoint_from_vp(double e_hpa) {
    if (e_hpa <= 0.0) return -ZEROCNK;
    double ln_ratio = log(e_hpa / 6.112);
    return 243.5 * ln_ratio / (17.67 - ln_ratio);
}

// Moist lapse rate dT/dp (K/hPa)
__device__ inline double moist_lapse_rate(double p_hpa, double t_c) {
    double t_k = t_c + ZEROCNK;
    double es = svp_hpa(t_c);
    double rs = EPS * es / (p_hpa - es);
    if (rs < 0.0) rs = 0.0;
    double num = (RD * t_k + LV0 * rs) / p_hpa;
    double den = CP_D + (LV0 * LV0 * rs * EPS) / (RD * t_k * t_k);
    return num / den;
}

// Single RK4 step for moist adiabat
__device__ inline double moist_rk4_step(double p, double t, double dp) {
    double k1 = dp * moist_lapse_rate(p, t);
    double k2 = dp * moist_lapse_rate(p + dp / 2.0, t + k1 / 2.0);
    double k3 = dp * moist_lapse_rate(p + dp / 2.0, t + k2 / 2.0);
    double k4 = dp * moist_lapse_rate(p + dp,       t + k3);
    return t + (k1 + 2.0 * k2 + 2.0 * k3 + k4) / 6.0;
}
