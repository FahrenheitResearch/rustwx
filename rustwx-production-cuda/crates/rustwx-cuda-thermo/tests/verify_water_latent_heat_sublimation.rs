//! Numerical agreement: CUDA `water_latent_heat_sublimation` vs metrust.

use metrust::calc::thermo::water_latent_heat_sublimation as cpu_ls;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::water_latent_heat_sublimation;

const TOL: f64 = 1e-10;

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let n = 8192;
    let t: Vec<f64> = (0..n).map(|i| -50.0 + (i as f64) * 100.0 / (n as f64 - 1.0)).collect();
    let gpu = water_latent_heat_sublimation::host(&ctx, &t).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..n {
        let cpu = cpu_ls(t[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
