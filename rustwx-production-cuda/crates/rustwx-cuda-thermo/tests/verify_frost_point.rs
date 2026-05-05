//! Numerical agreement: CUDA `frost_point` vs metrust.

use metrust::calc::thermo::frost_point as cpu_fp;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::frost_point;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut t = Vec::with_capacity(n);
    let mut rh = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        t.push(-30.0 + f * 50.0); // -30..20 C
        rh.push(10.0 + f * 80.0); // 10..90 %
    }
    (t, rh)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t, rh) = synthetic(8192);
    let gpu = frost_point::host(&ctx, &t, &rh).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..t.len() {
        let cpu = cpu_fp(t[i], rh[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} t={} rh={}", gpu[i], cpu, abs, t[i], rh[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
