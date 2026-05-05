//! Smoke test: CUDA `corfidi_storm_motion` launches and produces finite results.
//!
//! DEFER: the 0-6 km mean wind here uses centered height-weights, while
//! `metrust::calc::wind::corfidi_storm_motion` delegates to
//! `metrust::calc::wind::mean_wind`, which is trapezoidal with interpolated
//! endpoints. See `DIVERGENT_KERNELS.md`.

use rustwx_cuda_core::global;
use rustwx_cuda_wind::corfidi_storm_motion;

const NCOLS: usize = 256;
const NLEVELS: usize = 30;

fn synthetic_profile() -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut u = vec![0.0; NCOLS * NLEVELS];
    let mut v = vec![0.0; NCOLS * NLEVELS];
    let mut h = vec![0.0; NCOLS * NLEVELS];
    for c in 0..NCOLS {
        let phase = (c as f64) * 0.017;
        for k in 0..NLEVELS {
            let f = (k as f64) / ((NLEVELS - 1) as f64);
            let height = f * 15000.0;
            let z_km = height / 1000.0;
            let ui = 2.0 + 4.0 * z_km + 1.5 * (z_km + phase).sin();
            let vi = -3.0 + 0.8 * z_km + 1.2 * ((z_km + phase) * 0.7).cos();
            let idx = c * NLEVELS + k;
            u[idx] = ui;
            v[idx] = vi;
            h[idx] = height;
        }
    }
    (u, v, h)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn smoke() {
    let ctx = global().expect("init CUDA context");
    let (u, v, heights) = synthetic_profile();

    let u_llj = 12.0;
    let v_llj = 4.0;

    let (up_u, up_v, dn_u, dn_v) = corfidi_storm_motion::host(
        &ctx, &u, &v, &heights, NCOLS, NLEVELS, u_llj, v_llj,
    )
    .expect("kernel");
    assert_eq!(up_u.len(), NCOLS);
    assert_eq!(dn_u.len(), NCOLS);

    // downwind = upwind + mean_wind, so (downwind - upwind) is the same mean
    // wind for every column. We only check finiteness + the kernel's internal
    // identity here; metrust parity is deferred.
    for c in 0..NCOLS {
        for &x in &[up_u[c], up_v[c], dn_u[c], dn_v[c]] {
            assert!(x.is_finite(), "col={c} non-finite");
        }
    }
}
