// Turbulent kinetic energy from u, v, w time series.
// TKE = 0.5 * (var(u) + var(v) + var(w)) using population variance.
// Single-block, single-thread reduction (port of met-cu's tke_kernel).

extern "C" __global__
void tke_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    const double* __restrict__ w,
    double* __restrict__ tke_out,
    int n
) {
    int tid = blockDim.x * blockIdx.x + threadIdx.x;
    if (tid != 0) return;

    double mu = 0.0, mv = 0.0, mw = 0.0;
    for (int i = 0; i < n; i++) {
        mu += u[i]; mv += v[i]; mw += w[i];
    }
    mu /= (double)n; mv /= (double)n; mw /= (double)n;

    double var_u = 0.0, var_v = 0.0, var_w = 0.0;
    for (int i = 0; i < n; i++) {
        double du = u[i] - mu;
        double dv = v[i] - mv;
        double dw = w[i] - mw;
        var_u += du * du;
        var_v += dv * dv;
        var_w += dw * dw;
    }
    var_u /= (double)n; var_v /= (double)n; var_w /= (double)n;
    tke_out[0] = 0.5 * (var_u + var_v + var_w);
}
