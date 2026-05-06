//! Numerical agreement test: CUDA `second_derivative_x` vs metrust CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::second_derivative_x;
use wx_math::gridmath::second_derivative as cpu_d2;

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const DX: f64 = 1000.0;

fn synthetic_field() -> Vec<f64> {
    let n = NX * NY;
    let mut f = vec![0.0; n];
    for j in 0..NY {
        for i in 0..NX {
            let x = i as f64 * DX;
            let y = j as f64 * 1000.0;
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

    let gpu = second_derivative_x::host(&ctx, &f, &dx, NX, NY).expect("kernel");
    let cpu = cpu_d2(&f, DX, 0, NX, NY);
    assert_eq!(gpu.len(), cpu.len());

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
    eprintln!("second_derivative_x max_abs={max_abs:e}");
}
