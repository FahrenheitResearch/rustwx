//! Grid stencil CUDA kernels (vorticity, divergence, frontogenesis, …).

mod sources;

pub mod absolute_vorticity;
pub mod advection;
pub mod ageostrophic_wind;
pub mod composite_reflectivity;
pub mod crop_2d;
pub mod curvature_vorticity;
pub mod divergence;
pub mod first_derivative_x;
pub mod first_derivative_y;
pub mod frontogenesis;
pub mod geostrophic_wind;
pub mod gradient;
pub mod interpolate_1d;
pub mod interpolate_vertical;
pub mod laplacian;
pub mod log_interpolate_1d;
pub mod q_vector;
pub mod second_derivative_x;
pub mod second_derivative_y;
pub mod shearing_deformation;
pub mod stretching_deformation;
pub mod total_deformation;
pub mod vorticity;

pub use rustwx_cuda_core as core;
pub use rustwx_cuda_core::{Context, ContextHandle, DeviceVec, Error, Result};
