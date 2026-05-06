//! Numerical agreement test: CUDA `advection` vs the metrust CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::advection;
use wx_math::dynamics::advection as cpu_ref;

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const DX: f64 = 1000.0;
const DY: f64 = 1000.0;

fn synthetic() -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = NX * NY;
    let mut field = vec![0.0; n];
    let mut u = vec![0.0; n];
    let mut v = vec![0.0; n];
    for j in 0..NY {
        for i in 0..NX {
            let x = i as f64 * DX;
            let y = j as f64 * DY;
            field[j * NX + i] = (x * 1e-5).sin() + 0.5 * (y * 1e-5).cos();
            u[j * NX + i] = 5.0 + (y * 1e-5).sin();
            v[j * NX + i] = -3.0 + (x * 1e-5).cos();
        }
    }
    (field, u, v)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (field, u, v) = synthetic();
    let n = NX * NY;
    let dx = vec![DX; n];
    let dy = vec![DY; n];

    let gpu = advection::host(&ctx, &field, &u, &v, &dx, &dy, NX, NY).expect("kernel");
    let cpu = cpu_ref(&field, &u, &v, NX, NY, DX, DY);

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
    eprintln!("advection max_abs={max_abs:e}");
}
