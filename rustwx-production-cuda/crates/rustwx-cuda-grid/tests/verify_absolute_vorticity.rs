//! Numerical agreement test: CUDA `absolute_vorticity` vs the metrust CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::absolute_vorticity;
use wx_math::dynamics::absolute_vorticity as cpu_avort;

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const DX: f64 = 1000.0;
const DY: f64 = 1000.0;

fn synthetic_uv() -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = NX * NY;
    let mut u = vec![0.0; n];
    let mut v = vec![0.0; n];
    let mut lat = vec![0.0; n];
    for j in 0..NY {
        for i in 0..NX {
            let x = i as f64 * DX;
            let y = j as f64 * DY;
            u[j * NX + i] = (x * 1e-5).sin() + 0.5 * y * 1e-5;
            v[j * NX + i] = (y * 1e-5).cos() - 0.3 * x * 1e-5;
            // Latitudes spanning 30N..50N.
            lat[j * NX + i] = 30.0 + 20.0 * (j as f64) / ((NY - 1) as f64);
        }
    }
    (u, v, lat)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (u, v, lat) = synthetic_uv();
    let n = NX * NY;
    let dx = vec![DX; n];
    let dy = vec![DY; n];

    let gpu = absolute_vorticity::host(&ctx, &u, &v, &dx, &dy, &lat, NX, NY).expect("kernel");
    let cpu = cpu_avort(&u, &v, &lat, NX, NY, DX, DY);
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
    eprintln!("absolute_vorticity max_abs={max_abs:e}");
}
