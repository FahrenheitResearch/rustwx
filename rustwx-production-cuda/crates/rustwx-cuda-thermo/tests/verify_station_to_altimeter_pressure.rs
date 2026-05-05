//! Numerical agreement test for the met-cu station -> altimeter kernel.
//!
//! Compared against an inline reference that uses the same literal
//! `BARO_EXP = 0.190284` as the kernel; metrust uses a computed BARO_EXP
//! that differs in the ~1e-5 range. See DIVERGENT_KERNELS.md.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::station_to_altimeter_pressure;

const TOL: f64 = 1e-10;

fn cpu_alt(p_stn: f64, elev: f64) -> f64 {
    const BARO_EXP: f64 = 0.190284;
    const P0: f64 = 1013.25;
    const LAPSE: f64 = 0.0065;
    const T0: f64 = 288.15;
    let n = 1.0 / BARO_EXP;
    let term = (p_stn - 0.3).powf(n) + P0.powf(n) * LAPSE * elev / T0;
    term.powf(1.0 / n)
}

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut e = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(700.0 + f * 320.0);
        e.push(f * 3000.0);
    }
    (p, e)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, e) = synthetic(8192);
    let gpu = station_to_altimeter_pressure::host(&ctx, &p, &e).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_alt(p[i], e[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
