//! Numerical agreement test: CUDA total_totals vs wx_math CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_severe::total_totals;
use wx_math::composite::total_totals as cpu_tt;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut t850 = Vec::with_capacity(n);
    let mut t500 = Vec::with_capacity(n);
    let mut td850 = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        t850.push(10.0 + f * 20.0);
        t500.push(-25.0 + f * 15.0);
        td850.push(0.0 + f * 20.0);
    }
    (t850, t500, td850)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t850, t500, td850) = synthetic(8192);
    let gpu = total_totals::host(&ctx, &t850, &t500, &td850).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..t850.len() {
        let cpu = cpu_tt(t850[i], t500[i], td850[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} at i={i}",
            gpu[i],
            cpu,
            abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
