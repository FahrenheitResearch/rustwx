//! Numerical agreement: CUDA `mixing_ratio_from_relative_humidity` (kg/kg) vs
//! `metrust::calc::thermo::mixing_ratio_from_relative_humidity` (g/kg).

use metrust::calc::thermo::mixing_ratio_from_relative_humidity as cpu_w;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::mixing_ratio_from_relative_humidity;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    let mut rh = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(300.0 + f * 700.0);
        t.push(-30.0 + f * 70.0);
        rh.push(5.0 + f * 90.0);
    }
    (p, t, rh)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, t, rh) = synthetic(8192);
    let gpu = mixing_ratio_from_relative_humidity::host(&ctx, &p, &t, &rh).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        // metrust returns g/kg; kernel returns kg/kg
        let cpu = cpu_w(p[i], t[i], rh[i]) / 1000.0;
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} p={} t={} rh={}", gpu[i], cpu, abs, p[i], t[i], rh[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
