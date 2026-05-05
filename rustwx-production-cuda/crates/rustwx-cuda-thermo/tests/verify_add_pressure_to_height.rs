//! Numerical agreement test: CUDA `add_pressure_to_height` vs `metrust::calc::thermo`.

use metrust::calc::thermo::add_pressure_to_height as cpu_addp;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::add_pressure_to_height;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut h = Vec::with_capacity(n);
    let mut dp = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        h.push(0.0 + f * 8000.0); // 0..8 km
        dp.push(-200.0 + f * 400.0); // -200..+200 hPa
    }
    (h, dp)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (h, dp) = synthetic(8192);
    let gpu = add_pressure_to_height::host(&ctx, &h, &dp).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..h.len() {
        let cpu = cpu_addp(h[i], dp[i]);
        if !cpu.is_finite() || !gpu[i].is_finite() { continue; }
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} h={} dp={}", gpu[i], cpu, abs, h[i], dp[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
