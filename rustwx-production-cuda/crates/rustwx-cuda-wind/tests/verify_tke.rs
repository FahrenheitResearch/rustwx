//! CUDA `tke` vs metrust's `wind::tke`. Both use the two-pass population
//! variance form, so they should agree to ~1e-10 on synthetic data.

use metrust::calc::wind::tke as cpu_tke;
use rustwx_cuda_core::global;
use rustwx_cuda_wind::tke;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut u = Vec::with_capacity(n);
    let mut v = Vec::with_capacity(n);
    let mut w = Vec::with_capacity(n);
    for i in 0..n {
        let t = (i as f64) * 0.01;
        u.push(5.0 + 1.5 * t.sin());
        v.push(-2.0 + 0.8 * (2.0 * t).cos());
        w.push(0.3 * (3.0 * t).sin());
    }
    (u, v, w)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (u, v, w) = synthetic(4096);
    let gpu = tke::host(&ctx, &u, &v, &w).expect("kernel");
    assert_eq!(gpu.len(), 1);
    let cpu = cpu_tke(&u, &v, &w);
    let abs = (gpu[0] - cpu).abs();
    eprintln!("gpu={} cpu={cpu} abs={abs:e}", gpu[0]);
    assert!(abs < TOL, "abs={abs:e} gpu={} cpu={cpu}", gpu[0]);
}
