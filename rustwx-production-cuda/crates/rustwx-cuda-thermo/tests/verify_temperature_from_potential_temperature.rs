//! Numerical agreement test: CUDA `temperature_from_potential_temperature`
//! vs the wx-math CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::temperature_from_potential_temperature;
use wx_math::thermo::temperature_from_potential_temperature as cpu_t;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut th = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(100.0 + f * 950.0);
        th.push(280.0 + f * 120.0); // 280 K -> 400 K
    }
    (p, th)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, th) = synthetic_profile(8192);
    let gpu = temperature_from_potential_temperature::host(&ctx, &p, &th).expect("kernel");
    assert_eq!(gpu.len(), p.len());

    let mut max_abs = 0.0;
    let mut max_rel = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_t(p[i], th[i]);
        let abs = (gpu[i] - cpu).abs();
        let rel = abs / cpu.abs().max(1.0);
        if abs > max_abs {
            max_abs = abs;
        }
        if rel > max_rel {
            max_rel = rel;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} rel_diff={:e} at i={i}",
            gpu[i],
            cpu,
            abs,
            rel
        );
    }
    eprintln!("max_abs={max_abs:e} max_rel={max_rel:e}");
}
