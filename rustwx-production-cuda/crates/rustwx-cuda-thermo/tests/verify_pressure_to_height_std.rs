//! Numerical agreement test: CUDA `pressure_to_height_std` vs `wx_math::thermo`.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::pressure_to_height_std;
use wx_math::thermo::pressure_to_height_std as cpu_p_to_h;

const TOL: f64 = 1e-10;

fn synthetic_pressures(n: usize) -> Vec<f64> {
    (0..n)
        .map(|i| {
            let f = (i as f64) / ((n.max(2) - 1) as f64);
            200.0 + f * 850.0 // 200 hPa to 1050 hPa
        })
        .collect()
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let p = synthetic_pressures(8192);
    let gpu = pressure_to_height_std::host(&ctx, &p).expect("kernel");
    assert_eq!(gpu.len(), p.len());

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_p_to_h(p[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} p={}", gpu[i], cpu, abs, p[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
