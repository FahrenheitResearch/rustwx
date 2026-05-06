//! Numerical agreement test: CUDA `frontogenesis` vs the metrust CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::frontogenesis;
use wx_math::dynamics::frontogenesis_2d as cpu_ref;

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const DX: f64 = 1000.0;
const DY: f64 = 1000.0;

fn synthetic() -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = NX * NY;
    let mut theta = vec![0.0; n];
    let mut u = vec![0.0; n];
    let mut v = vec![0.0; n];
    for j in 0..NY {
        for i in 0..NX {
            let x = i as f64 * DX;
            let y = j as f64 * DY;
            // Smooth potential temperature field with a horizontal gradient.
            theta[j * NX + i] = 280.0 + 5.0 * (x * 1e-5).sin() + 3.0 * (y * 1e-5).cos();
            u[j * NX + i] = 5.0 + (y * 1e-5).sin();
            v[j * NX + i] = -3.0 + (x * 1e-5).cos();
        }
    }
    (theta, u, v)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (theta, u, v) = synthetic();
    let n = NX * NY;
    let dx = vec![DX; n];
    let dy = vec![DY; n];

    let gpu = frontogenesis::host(&ctx, &theta, &u, &v, &dx, &dy, NX, NY).expect("kernel");
    let cpu = cpu_ref(&theta, &u, &v, NX, NY, DX, DY);

    let mut max_abs = 0.0;
    for k in 0..n {
        let abs = (gpu[k] - cpu[k]).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "k={k} gpu={} cpu={} abs={:e}",
            gpu[k],
            cpu[k],
            abs
        );
    }
    eprintln!("frontogenesis max_abs={max_abs:e}");
}
