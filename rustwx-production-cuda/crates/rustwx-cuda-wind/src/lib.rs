//! Wind / kinematics CUDA kernels.

mod sources;

pub mod angle_to_direction;
pub mod bulk_shear;
pub mod bunkers_storm_motion;
pub mod coriolis_parameter;
pub mod corfidi_storm_motion;
pub mod critical_angle;
pub mod friction_velocity;
pub mod get_layer;
pub mod mean_wind;
pub mod normal_component;
pub mod storm_relative_helicity;
pub mod tangential_component;
pub mod tke;
pub mod wind_components;
pub mod wind_direction;
pub mod wind_speed;

pub use rustwx_cuda_core as core;
pub use rustwx_cuda_core::{Context, ContextHandle, DeviceVec, Error, Result};
