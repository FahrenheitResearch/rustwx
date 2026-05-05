//! Smoke test: CUDA `mean_wind` launches and produces finite results.
//!
//! DEFER: the kernel uses centered-box weights, while
//! `metrust::calc::wind::mean_wind` is a trapezoidal integration with
//! interpolated layer endpoints. The two outputs are close but not bit-equal,
//! so we only check shape + finiteness here. See `DIVERGENT_KERNELS.md`.

use rustwx_cuda_core::global;
use rustwx_cuda_wind::mean_wind;

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
    let (mu, mv) =
        mean_wind::host(&ctx, &u, &v, &heights, NCOLS, NLEVELS, 0.0, 6000.0)
            .expect("kernel");
    assert_eq!(mu.len(), NCOLS);
    assert_eq!(mv.len(), NCOLS);
    for c in 0..NCOLS {
        assert!(mu[c].is_finite() && mv[c].is_finite(), "col={c} non-finite");
        assert!(
            mu[c].abs() < 100.0 && mv[c].abs() < 100.0,
            "col={c} mu={} mv={} out of plausible range",
            mu[c],
            mv[c]
        );
    }
}
