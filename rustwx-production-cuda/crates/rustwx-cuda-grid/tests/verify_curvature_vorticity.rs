//! Numerical agreement test: CUDA `curvature_vorticity` vs metrust CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::curvature_vorticity;
use wx_math::dynamics::curvature_vorticity as cpu_curv;

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const DX: f64 = 1000.0;
const DY: f64 = 1000.0;

fn synthetic_uv() -> (Vec<f64>, Vec<f64>) {
    let n = NX * NY;
    let mut u = vec![0.0; n];
    let mut v = vec![0.0; n];
    for j in 0..NY {
        for i in 0..NX {
            let x = i as f64 * DX;
            let y = j as f64 * DY;
            // Rotational-ish field with non-zero magnitude everywhere.
            u[j * NX + i] = 5.0 + (x * 1e-5).sin() + 0.3 * (y * 1e-5);
            v[j * NX + i] = 3.0 + (y * 1e-5).cos() - 0.2 * (x * 1e-5);
        }
    }
    (u, v)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (u, v) = synthetic_uv();
    let n = NX * NY;
    let dx = vec![DX; n];
    let dy = vec![DY; n];

    let gpu = curvature_vorticity::host(&ctx, &u, &v, &dx, &dy, NX, NY).expect("kernel");
    let cpu = cpu_curv(&u, &v, NX, NY, DX, DY);
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
    eprintln!("curvature_vorticity max_abs={max_abs:e}");
}
