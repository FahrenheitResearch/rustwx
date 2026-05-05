//! Numerical agreement test: CUDA `mixing_ratio_from_specific_humidity` (kg/kg)
//! vs the wx-math CPU reference (g/kg).

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::mixing_ratio_from_specific_humidity;
use wx_math::thermo::mixing_ratio_from_specific_humidity as cpu_w_gkg;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> Vec<f64> {
    let mut q = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        q.push(0.0001 + f * 0.025); // 0.0001 .. 0.025 kg/kg
    }
    q
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let q = synthetic_profile(8192);
    let gpu = mixing_ratio_from_specific_humidity::host(&ctx, &q).expect("kernel");
    assert_eq!(gpu.len(), q.len());

    let mut max_abs = 0.0;
    for i in 0..q.len() {
        // wx-math returns g/kg; convert to kg/kg for comparison.
        let cpu = cpu_w_gkg(q[i]) / 1000.0;
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
