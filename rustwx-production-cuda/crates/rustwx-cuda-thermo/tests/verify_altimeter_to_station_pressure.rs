//! Numerical agreement test: CUDA `altimeter_to_station_pressure` vs `wx_math::thermo`.
//! metrust uses Smithsonian +0.3 — see DIVERGENT_KERNELS.md.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::altimeter_to_station_pressure;
use wx_math::thermo::altimeter_to_station_pressure as cpu_a2p;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut a = Vec::with_capacity(n);
    let mut e = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        a.push(950.0 + f * 100.0);
        e.push(f * 3000.0);
    }
    (a, e)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (a, e) = synthetic(8192);
    let gpu = altimeter_to_station_pressure::host(&ctx, &a, &e).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..a.len() {
        let cpu = cpu_a2p(a[i], e[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
