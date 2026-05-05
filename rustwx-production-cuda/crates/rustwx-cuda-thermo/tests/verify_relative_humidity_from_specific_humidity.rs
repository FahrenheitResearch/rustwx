//! Numerical agreement: CUDA `relative_humidity_from_specific_humidity` vs metrust.

use metrust::calc::thermo::relative_humidity_from_specific_humidity as cpu_rh;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::relative_humidity_from_specific_humidity;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    let mut q = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(400.0 + f * 600.0);
        t.push(-20.0 + f * 50.0);
        q.push(0.0001 + f * 0.018);
    }
    (p, t, q)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, t, q) = synthetic(8192);
    let gpu = relative_humidity_from_specific_humidity::host(&ctx, &p, &t, &q).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_rh(p[i], t[i], q[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} p={} t={} q={}", gpu[i], cpu, abs, p[i], t[i], q[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
