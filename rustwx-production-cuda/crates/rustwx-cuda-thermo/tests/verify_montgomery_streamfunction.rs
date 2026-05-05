//! Numerical agreement: CUDA `montgomery_streamfunction` vs metrust.
//! Both take T in Kelvin.

use metrust::calc::thermo::montgomery_streamfunction as cpu_psi;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::montgomery_streamfunction;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut z = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        z.push(f * 12_000.0);
        t.push(220.0 + f * 90.0); // Kelvin
    }
    (z, t)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (z, t) = synthetic(8192);
    let gpu = montgomery_streamfunction::host(&ctx, &z, &t).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..z.len() {
        // metrust signature: (theta_k, p_hpa, t_k, z_m); only t_k & z_m are used
        let cpu = cpu_psi(0.0, 0.0, t[i], z[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
