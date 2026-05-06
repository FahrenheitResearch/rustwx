//! Numerical agreement test: CUDA `geopotential_to_height` vs `metrust::calc::thermo`.

use metrust::calc::thermo::geopotential_to_height as cpu_g2h;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::geopotential_to_height;

const TOL: f64 = 1e-10;

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let n = 8192;
    let geopot: Vec<f64> = (0..n).map(|i| (i as f64) * 12.0 - 50_000.0).collect();
    let gpu = geopotential_to_height::host(&ctx, &geopot).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..n {
        let cpu = cpu_g2h(geopot[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} diff={:e} at i={i}",
            gpu[i],
            cpu,
            abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
