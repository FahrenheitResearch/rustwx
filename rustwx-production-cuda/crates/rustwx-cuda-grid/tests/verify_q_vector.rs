//! Numerical agreement test: CUDA `q_vector` vs metrust CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::q_vector;
use wx_math::dynamics::q_vector as cpu_qv;

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const DX: f64 = 1000.0;
const DY: f64 = 1000.0;
const P_HPA: f64 = 850.0;

fn synthetic_fields() -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = NX * NY;
    let mut t = vec![0.0; n];
    let mut ug = vec![0.0; n];
    let mut vg = vec![0.0; n];
    for j in 0..NY {
        for i in 0..NX {
            let x = i as f64 * DX;
            let y = j as f64 * DY;
            // Smooth temperature pattern (K) and geostrophic wind (m/s).
            t[j * NX + i] = 280.0 + 5.0 * (x * 1e-5).sin() + 3.0 * (y * 1e-5).cos();
            ug[j * NX + i] = 10.0 + 2.0 * (y * 1e-5).cos();
            vg[j * NX + i] = -3.0 + 1.5 * (x * 1e-5).sin();
        }
    }
    (t, ug, vg)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t, ug, vg) = synthetic_fields();
    let n = NX * NY;
    let dx = vec![DX; n];
    let dy = vec![DY; n];

    let (gpu_q1, gpu_q2) =
        q_vector::host(&ctx, &t, &ug, &vg, P_HPA, &dx, &dy, NX, NY).expect("kernel");
    let (cpu_q1, cpu_q2) = cpu_qv(&t, &ug, &vg, P_HPA, NX, NY, DX, DY);
    assert_eq!(gpu_q1.len(), cpu_q1.len());

    let mut max1 = 0.0_f64;
    let mut max2 = 0.0_f64;
    for k in 0..n {
        let a1 = (gpu_q1[k] - cpu_q1[k]).abs();
        let a2 = (gpu_q2[k] - cpu_q2[k]).abs();
        if a1 > max1 {
            max1 = a1;
        }
        if a2 > max2 {
            max2 = a2;
        }
        assert!(
            a1 < TOL,
            "q1 k={k} gpu={} cpu={} abs={:e}",
            gpu_q1[k],
            cpu_q1[k],
            a1
        );
        assert!(
            a2 < TOL,
            "q2 k={k} gpu={} cpu={} abs={:e}",
            gpu_q2[k],
            cpu_q2[k],
            a2
        );
    }
    eprintln!("q_vector max_abs_q1={max1:e} max_abs_q2={max2:e}");
}
