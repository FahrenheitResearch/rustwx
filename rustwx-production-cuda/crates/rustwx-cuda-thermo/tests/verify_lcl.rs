//! Numerical agreement: CUDA `lcl` vs metrust.

use metrust::calc::thermo::lcl as cpu_lcl;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::lcl;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    let mut td = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(900.0 + f * 130.0);
        let ti = 5.0 + f * 30.0;
        t.push(ti);
        td.push(ti - (2.0 + f * 18.0)); // Td = T - dewpoint depression in [2,20] C
    }
    (p, t, td)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, t, td) = synthetic(8192);
    let (gpu_p, gpu_t) = lcl::host(&ctx, &p, &t, &td).expect("kernel");

    let mut max_abs_p = 0.0_f64;
    let mut max_abs_t = 0.0_f64;
    for i in 0..p.len() {
        let (cpu_p_lcl, cpu_t_lcl) = cpu_lcl(p[i], t[i], td[i]);
        let dp = (gpu_p[i] - cpu_p_lcl).abs();
        let dt = (gpu_t[i] - cpu_t_lcl).abs();
        if dp > max_abs_p {
            max_abs_p = dp;
        }
        if dt > max_abs_t {
            max_abs_t = dt;
        }
        assert!(
            dp < TOL,
            "p_lcl: gpu={} cpu={} diff={:e} at i={i}",
            gpu_p[i],
            cpu_p_lcl,
            dp
        );
        assert!(
            dt < TOL,
            "t_lcl: gpu={} cpu={} diff={:e} at i={i}",
            gpu_t[i],
            cpu_t_lcl,
            dt
        );
    }
    eprintln!("max_abs_p={max_abs_p:e} max_abs_t={max_abs_t:e}");
}
