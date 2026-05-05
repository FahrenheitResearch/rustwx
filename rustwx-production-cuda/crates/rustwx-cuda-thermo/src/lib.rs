//! Thermodynamic CUDA kernels.
//!
//! Each module wraps one met-cu kernel: the `.cu` source is in
//! `kernels/thermo/<name>.cu`, prepended at compile time with the shared
//! constants/helpers from `kernels/common/*.cuh`. Numerical agreement with
//! `metrust::calc::thermo::*` is asserted in the integration tests.

mod sources;

pub mod add_height_to_pressure;
pub mod add_pressure_to_height;
pub mod altimeter_to_sea_level_pressure;
pub mod altimeter_to_station_pressure;
pub mod apparent_temperature;
pub mod brunt_vaisala_period;
pub mod density;
pub mod dewpoint;
pub mod dewpoint_from_relative_humidity;
pub mod dewpoint_from_specific_humidity;
pub mod dry_lapse;
pub mod dry_static_energy;
pub mod exner_function;
pub mod frost_point;
pub mod geopotential_to_height;
pub mod heat_index;
pub mod height_to_geopotential;
pub mod height_to_pressure_std;
pub mod lcl;
pub mod mixing_ratio;
pub mod mixing_ratio_from_relative_humidity;
pub mod mixing_ratio_from_specific_humidity;
pub mod moist_air_gas_constant;
pub mod moist_air_poisson_exponent;
pub mod moist_air_specific_heat_pressure;
pub mod moist_static_energy;
pub mod montgomery_streamfunction;
pub mod potential_temperature;
pub mod pressure_to_height_std;
pub mod relative_humidity_from_dewpoint;
pub mod relative_humidity_from_mixing_ratio;
pub mod relative_humidity_from_specific_humidity;
pub mod saturation_mixing_ratio;
pub mod saturation_vapor_pressure;
pub mod scale_height;
pub mod sigma_to_pressure;
pub mod specific_humidity_from_dewpoint;
pub mod specific_humidity_from_mixing_ratio;
pub mod station_to_altimeter_pressure;
pub mod temperature_from_potential_temperature;
pub mod vapor_pressure_from_dewpoint;
pub mod vapor_pressure_from_mixing_ratio;
pub mod vertical_velocity;
pub mod vertical_velocity_pressure;
pub mod virtual_potential_temperature;
pub mod virtual_temperature;
pub mod water_latent_heat_melting;
pub mod water_latent_heat_sublimation;
pub mod water_latent_heat_vaporization;
pub mod windchill;

pub use rustwx_cuda_core as core;
pub use rustwx_cuda_core::{Context, ContextHandle, DeviceVec, Error, Result};
