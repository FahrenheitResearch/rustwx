//! Numerical agreement test: CUDA `dry_static_energy` vs
//! `wx_math::thermo::dry_static_energy`. Both consume T in Kelvin.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::dry_static_energy;
use wx_math::thermo::dry_static_energy as cpu_dse;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut h = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        h.push(0.0 + f * 12_000.0);
        t.push(220.0 + f * 90.0); // 220 .. 310 K
    }
    (h, t)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (h, t) = synthetic_profile(8192);
    let gpu = dry_static_energy::host(&ctx, &h, &t).expect("kernel");
    assert_eq!(gpu.len(), h.len());

    let mut max_abs = 0.0;
    for i in 0..h.len() {
        let cpu = cpu_dse(h[i], t[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} diff={:e} at i={i}",
            gpu[i],
            cpu,
            abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
