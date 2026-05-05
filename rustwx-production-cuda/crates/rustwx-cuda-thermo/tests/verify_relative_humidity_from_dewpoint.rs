//! Numerical agreement test: CUDA `relative_humidity_from_dewpoint` vs
//! `wx_math::thermo::rh_from_dewpoint`.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::relative_humidity_from_dewpoint;
use wx_math::thermo::rh_from_dewpoint as cpu_rh;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut t = Vec::with_capacity(n);
    let mut td = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        let ti = -40.0 + f * 80.0;
        t.push(ti);
        td.push(ti - 0.5 - f * 25.0); // Td below T
    }
    (t, td)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t, td) = synthetic_profile(8192);
    let gpu = relative_humidity_from_dewpoint::host(&ctx, &t, &td).expect("kernel");
    assert_eq!(gpu.len(), t.len());

    let mut max_abs = 0.0;
    for i in 0..t.len() {
        let cpu = cpu_rh(t[i], td[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
