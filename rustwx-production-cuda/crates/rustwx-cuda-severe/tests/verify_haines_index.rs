//! Numerical agreement test: CUDA haines_index vs wx_math CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_severe::haines_index;
use wx_math::composite::haines_index as cpu_haines;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut t950 = Vec::with_capacity(n);
    let mut t850 = Vec::with_capacity(n);
    let mut td850 = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        // Make stability span all three A buckets:
        //   dt = t950 - t850 in roughly [0, 12]
        let t850_v = 10.0 + f * 5.0;        // 10..15
        let dt = f * 12.0;                  // 0..12
        t950.push(t850_v + dt);
        t850.push(t850_v);
        // Moisture span all three B buckets:
        //   td = t850 - dd, dd in [0, 14]
        let dd = f * 14.0;
        td850.push(t850_v - dd);
    }
    (t950, t850, td850)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t950, t850, td850) = synthetic(8192);
    let gpu = haines_index::host(&ctx, &t950, &t850, &td850).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..t950.len() {
        let cpu = cpu_haines(t950[i], t850[i], td850[i]) as f64;
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} at i={i}",
            gpu[i], cpu, abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
