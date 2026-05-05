//! Numerical agreement test: CUDA `virtual_potential_temperature` vs the
//! wx-math CPU reference.
//!
//! wx-math accepts mixing ratio in **g/kg**; the kernel uses kg/kg.
//! The test feeds the same physical value to both sides in the appropriate
//! unit.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::virtual_potential_temperature;
use wx_math::thermo::virtual_potential_temperature as cpu_thv;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    let mut w = Vec::with_capacity(n); // kg/kg
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(100.0 + f * 950.0);
        t.push(-60.0 + f * 95.0);
        w.push(0.0001 + f * 0.024);
    }
    (p, t, w)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, t, w) = synthetic_profile(8192);
    let gpu = virtual_potential_temperature::host(&ctx, &p, &t, &w)
        .expect("kernel");
    assert_eq!(gpu.len(), p.len());

    let mut max_abs = 0.0;
    let mut max_rel = 0.0;
    for i in 0..p.len() {
        let w_gkg = w[i] * 1000.0;
        let cpu = cpu_thv(p[i], t[i], w_gkg);
        let abs = (gpu[i] - cpu).abs();
        let rel = abs / cpu.abs().max(1.0);
        if abs > max_abs { max_abs = abs; }
        if rel > max_rel { max_rel = rel; }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} rel_diff={:e} at i={i}",
            gpu[i], cpu, abs, rel
        );
    }
    eprintln!("max_abs={max_abs:e} max_rel={max_rel:e}");
}
