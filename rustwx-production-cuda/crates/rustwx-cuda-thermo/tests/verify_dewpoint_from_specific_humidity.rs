//! Numerical agreement: CUDA `dewpoint_from_specific_humidity` vs metrust.

use metrust::calc::thermo::dewpoint_from_specific_humidity as cpu_td;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::dewpoint_from_specific_humidity;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut q = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(300.0 + f * 700.0);     // 300..1000 hPa
        q.push(0.0005 + f * 0.0195);   // 0.5..20 g/kg in kg/kg
    }
    (p, q)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, q) = synthetic(8192);
    let gpu = dewpoint_from_specific_humidity::host(&ctx, &p, &q).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_td(p[i], q[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} p={} q={}", gpu[i], cpu, abs, p[i], q[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
