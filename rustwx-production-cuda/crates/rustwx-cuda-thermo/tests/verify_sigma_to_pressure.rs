//! Numerical agreement test: CUDA `sigma_to_pressure` vs `metrust::calc::atmo`.

use metrust::calc::atmo::sigma_to_pressure as cpu_s2p;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::sigma_to_pressure;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut s = Vec::with_capacity(n);
    let mut psfc = Vec::with_capacity(n);
    let mut ptop = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        s.push(f);
        psfc.push(950.0 + f * 70.0);
        ptop.push(50.0 + f * 50.0);
    }
    (s, psfc, ptop)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (s, psfc, ptop) = synthetic(8192);
    let gpu = sigma_to_pressure::host(&ctx, &s, &psfc, &ptop).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..s.len() {
        let cpu = cpu_s2p(s[i], psfc[i], ptop[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
