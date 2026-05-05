//! Numerical agreement test: CUDA `storm_relative_helicity` vs
//! `metrust::calc::wind::storm_relative_helicity`. The kernel and CPU
//! reference should agree exactly for monotonically increasing height
//! profiles, since both linearly interpolate at the upper boundary and use
//! the same `sru[i+1]*srv[i] - sru[i]*srv[i+1]` cross-product.
//!
//! `#[ignore]`d by default so CI without a GPU stays green.

use metrust::calc::wind::storm_relative_helicity as cpu_srh;
use rustwx_cuda_core::global;
use rustwx_cuda_wind::storm_relative_helicity;

const TOL: f64 = 1e-10;

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
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (u, v, heights) = synthetic_profile();

    let depth = 3000.0;
    let storm_u = 8.0;
    let storm_v = 2.5;

    let (pos_gpu, neg_gpu, tot_gpu) = storm_relative_helicity::host(
        &ctx, &u, &v, &heights, NCOLS, NLEVELS, depth, storm_u, storm_v,
    )
    .expect("kernel");
    assert_eq!(pos_gpu.len(), NCOLS);

    let mut max_abs = 0.0f64;
    for c in 0..NCOLS {
        let s = c * NLEVELS;
        let e = s + NLEVELS;
        let (pos_cpu, neg_cpu, tot_cpu) = cpu_srh(
            &u[s..e],
            &v[s..e],
            &heights[s..e],
            depth,
            storm_u,
            storm_v,
        );
        let dp = (pos_gpu[c] - pos_cpu).abs();
        let dn = (neg_gpu[c] - neg_cpu).abs();
        let dt = (tot_gpu[c] - tot_cpu).abs();
        let m = dp.max(dn).max(dt);
        if m > max_abs {
            max_abs = m;
        }
        assert!(
            dp < TOL && dn < TOL && dt < TOL,
            "col={c} gpu=({},{},{}) cpu=({},{},{}) dp={dp:e} dn={dn:e} dt={dt:e}",
            pos_gpu[c],
            neg_gpu[c],
            tot_gpu[c],
            pos_cpu,
            neg_cpu,
            tot_cpu
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
