//! Numerical agreement: CUDA `specific_humidity_from_dewpoint` vs metrust.

use metrust::calc::thermo::specific_humidity_from_dewpoint as cpu_q;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::specific_humidity_from_dewpoint;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut td = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(300.0 + f * 700.0);
        td.push(-40.0 + f * 75.0);
    }
    (p, td)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, td) = synthetic(8192);
    let gpu = specific_humidity_from_dewpoint::host(&ctx, &p, &td).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_q(p[i], td[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} diff={:e} at i={i} p={} td={}",
            gpu[i],
            cpu,
            abs,
            p[i],
            td[i]
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
