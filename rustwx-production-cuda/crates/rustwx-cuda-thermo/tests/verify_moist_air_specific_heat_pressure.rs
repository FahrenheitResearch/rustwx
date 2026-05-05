//! Numerical agreement: CUDA `moist_air_specific_heat_pressure` vs metrust.

use metrust::calc::thermo::moist_air_specific_heat_pressure as cpu_cp;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::moist_air_specific_heat_pressure;

const TOL: f64 = 1e-10;

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let n = 8192;
    let w: Vec<f64> = (0..n).map(|i| (i as f64) * 0.03 / (n as f64 - 1.0)).collect();
    let gpu = moist_air_specific_heat_pressure::host(&ctx, &w).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..n {
        let cpu = cpu_cp(w[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} w={}", gpu[i], cpu, abs, w[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
