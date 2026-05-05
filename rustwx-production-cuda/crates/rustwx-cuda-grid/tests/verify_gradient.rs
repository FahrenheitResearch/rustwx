//! Numerical agreement test: CUDA `gradient` vs the metrust CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::gradient;
use wx_math::dynamics::{gradient_x as cpu_dx, gradient_y as cpu_dy};

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const DX: f64 = 1000.0;
const DY: f64 = 1000.0;

fn synthetic_field() -> Vec<f64> {
    let n = NX * NY;
    let mut f = vec![0.0; n];
    for j in 0..NY {
        for i in 0..NX {
            let x = i as f64 * DX;
            let y = j as f64 * DY;
            f[j * NX + i] = (x * 1e-5).sin() + 0.5 * (y * 1e-5).cos();
        }
    }
    f
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let f = synthetic_field();
    let n = NX * NY;
    let dx = vec![DX; n];
    let dy = vec![DY; n];

    let (gpu_dx, gpu_dy) = gradient::host(&ctx, &f, &dx, &dy, NX, NY).expect("kernel");
    let cpu_x = cpu_dx(&f, NX, NY, DX);
    let cpu_y = cpu_dy(&f, NX, NY, DY);

    let mut max_abs_x = 0.0;
    let mut max_abs_y = 0.0;
    for k in 0..n {
        let ax = (gpu_dx[k] - cpu_x[k]).abs();
        let ay = (gpu_dy[k] - cpu_y[k]).abs();
        if ax > max_abs_x { max_abs_x = ax; }
        if ay > max_abs_y { max_abs_y = ay; }
        assert!(ax < TOL, "x k={k} gpu={} cpu={} abs={:e}", gpu_dx[k], cpu_x[k], ax);
        assert!(ay < TOL, "y k={k} gpu={} cpu={} abs={:e}", gpu_dy[k], cpu_y[k], ay);
    }
    eprintln!("gradient max_abs_x={max_abs_x:e} max_abs_y={max_abs_y:e}");
}
