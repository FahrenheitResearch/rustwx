//! Numerical agreement test: CUDA `density` vs `wx_math::thermo::density`
//! (after converting kg/kg -> g/kg).

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::density;
use wx_math::thermo::density as cpu_rho;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    let mut w = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(100.0 + f * 950.0);
        t.push(-50.0 + f * 90.0);
        w.push(0.0001 + f * 0.024);
    }
    (p, t, w)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, t, w) = synthetic_profile(8192);
    let gpu = density::host(&ctx, &p, &t, &w).expect("kernel");
    assert_eq!(gpu.len(), p.len());

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_rho(p[i], t[i], w[i] * 1000.0);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
