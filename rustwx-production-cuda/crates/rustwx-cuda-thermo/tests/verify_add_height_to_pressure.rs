//! Numerical agreement test: CUDA `add_height_to_pressure` vs `metrust::calc::thermo`.

use metrust::calc::thermo::add_height_to_pressure as cpu_addh;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::add_height_to_pressure;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut dh = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(300.0 + f * 700.0); // 300..1000 hPa
        dh.push(-2000.0 + f * 4000.0); // -2 km .. +2 km
    }
    (p, dh)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, dh) = synthetic(8192);
    let gpu = add_height_to_pressure::host(&ctx, &p, &dh).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_addh(p[i], dh[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} diff={:e} at i={i} p={} dh={}",
            gpu[i],
            cpu,
            abs,
            p[i],
            dh[i]
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
