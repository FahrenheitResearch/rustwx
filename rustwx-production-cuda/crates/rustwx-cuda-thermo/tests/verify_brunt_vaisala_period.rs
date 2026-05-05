//! Numerical agreement: CUDA `brunt_vaisala_period` vs `wx_math::thermo`.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::brunt_vaisala_period;
use wx_math::thermo::brunt_vaisala_period as cpu_period;

const TOL: f64 = 1e-10;

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let n = 8192;
    // Range over realistic positive BVF values: ~1e-4 .. 0.05 s^-1
    let bvf: Vec<f64> = (0..n)
        .map(|i| {
            let f = (i as f64) / (n as f64 - 1.0);
            1e-4 + f * 0.05
        })
        .collect();
    let gpu = brunt_vaisala_period::host(&ctx, &bvf).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..n {
        let cpu = cpu_period(bvf[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} bvf={}", gpu[i], cpu, abs, bvf[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
