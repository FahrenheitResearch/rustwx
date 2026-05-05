//! Numerical agreement test: CUDA `virtual_temperature` vs an inline CPU
//! reference (MetPy formula). wx-math doesn't expose this exact form
//! (it has SHARPpy-style `virtual_temp(t, p, td)` instead), so the
//! reference is reimplemented here verbatim from met-cu.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::virtual_temperature;

const ZEROCNK: f64 = 273.15;
const EPS: f64 = 0.6219569100577033;
const TOL: f64 = 1e-10;

fn cpu_tv(t_c: f64, w_kgkg: f64) -> f64 {
    let t_k = t_c + ZEROCNK;
    t_k * (1.0 + w_kgkg / EPS) / (1.0 + w_kgkg) - ZEROCNK
}

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut t = Vec::with_capacity(n);
    let mut w = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        t.push(-40.0 + f * 80.0); // -40 .. 40 C
        w.push(0.0001 + f * 0.024); // 0.1 .. 24 g/kg
    }
    (t, w)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t, w) = synthetic_profile(8192);
    let gpu = virtual_temperature::host(&ctx, &t, &w).expect("kernel");
    assert_eq!(gpu.len(), t.len());

    let mut max_abs = 0.0;
    let mut max_rel = 0.0;
    for i in 0..t.len() {
        let cpu = cpu_tv(t[i], w[i]);
        let abs = (gpu[i] - cpu).abs();
        let rel = abs / cpu.abs().max(1.0);
        if abs > max_abs {
            max_abs = abs;
        }
        if rel > max_rel {
            max_rel = rel;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} rel_diff={:e} at i={i}",
            gpu[i], cpu, abs, rel
        );
    }
    eprintln!("max_abs={max_abs:e} max_rel={max_rel:e}");
}
