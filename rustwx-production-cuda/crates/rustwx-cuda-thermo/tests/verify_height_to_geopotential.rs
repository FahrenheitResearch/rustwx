//! Numerical agreement test: CUDA `height_to_geopotential` vs `metrust::calc::thermo`.

use metrust::calc::thermo::height_to_geopotential as cpu_h2g;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::height_to_geopotential;

const TOL: f64 = 1e-10;

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let n = 8192;
    let h: Vec<f64> = (0..n).map(|i| (i as f64) * 1.5).collect();
    let gpu = height_to_geopotential::host(&ctx, &h).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..n {
        let cpu = cpu_h2g(h[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
