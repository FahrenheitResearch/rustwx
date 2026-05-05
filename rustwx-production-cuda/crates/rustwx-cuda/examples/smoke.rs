//! End-to-end smoke test. Run with:
//!     cargo run -p rustwx-cuda --example smoke --release
//!
//! Initializes the global CUDA context, compiles the
//! `potential_temperature` kernel via NVRTC (cached on disk after first run),
//! and prints the GPU output alongside the metrust CPU reference.

use rustwx_cuda::core::global;
use rustwx_cuda::thermo::potential_temperature;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = global()?;
    println!(
        "device: {} (CUDA {:?})",
        ctx.cuda().name()?,
        ctx.cuda().compute_capability()?
    );

    let p = vec![1000.0, 850.0, 700.0, 500.0, 300.0, 200.0, 100.0];
    let t = vec![25.0, 12.0, 0.0, -20.0, -45.0, -55.0, -75.0];

    let theta = potential_temperature::host(&ctx, &p, &t)?;

    println!(" p (hPa) | T (C)  |  theta (K) gpu | theta (K) cpu");
    println!("---------+--------+----------------+--------------");
    for i in 0..p.len() {
        let cpu = wx_math::thermo::potential_temperature(p[i], t[i]);
        println!(
            " {:>7.1} | {:>6.1} | {:>14.6} | {:>13.6}",
            p[i], t[i], theta[i], cpu
        );
    }
    Ok(())
}
