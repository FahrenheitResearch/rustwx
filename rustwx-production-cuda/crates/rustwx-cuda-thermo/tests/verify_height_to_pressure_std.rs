//! Numerical agreement test: CUDA `height_to_pressure_std` vs `wx_math::thermo`.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::height_to_pressure_std;
use wx_math::thermo::height_to_pressure_std as cpu_h_to_p;

const TOL: f64 = 1e-10;

fn synthetic_heights(n: usize) -> Vec<f64> {
    (0..n)
        .map(|i| {
            let f = (i as f64) / ((n.max(2) - 1) as f64);
            -200.0 + f * 10_200.0 // -200 m to ~10 km
        })
        .collect()
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let h = synthetic_heights(8192);
    let gpu = height_to_pressure_std::host(&ctx, &h).expect("kernel");
    assert_eq!(gpu.len(), h.len());

    let mut max_abs = 0.0;
    for i in 0..h.len() {
        let cpu = cpu_h_to_p(h[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} diff={:e} at i={i} h={}",
            gpu[i],
            cpu,
            abs,
            h[i]
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
