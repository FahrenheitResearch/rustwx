pub mod abi;
pub mod batch;
pub mod geostationary;
pub mod goes;
pub mod native_sequence;
pub mod netcdf;
pub mod render;
pub mod rgb;

pub use abi::{
    AbiFixedGrid, AbiSector, GoesAbiField, GoesAbiScene, GoesImagerProjection, read_goes_abi_field,
    read_goes_abi_field_window, read_goes_abi_scene,
};
pub use batch::{
    GoesSatelliteArtifact, GoesSatelliteBatchReport, GoesSatelliteBatchRequest,
    GoesSatelliteProduct, run_goes_satellite_batch,
};
pub use geostationary::{
    SweepAngleAxis, lat_lon_to_scan_angles, lat_lon_to_scan_angles_fast, scan_angles_to_lat_lon,
};
pub use goes::{GoesAbiFilename, GoesSatellite, parse_goes_abi_filename};
pub use native_sequence::{
    GoesNativeSequenceFrame, GoesNativeSequenceReport, GoesNativeSequenceRequest,
    GoesNativeSequenceTiming, run_goes_native_sequence,
};
pub use netcdf::{ScaledVariable, open_goes_netcdf_lossy, read_scaled_f32, read_scaled_f32_window};
pub use render::{
    GoesAbiBandMapRequest, GoesAbiLayerStyle, GoesAbiMapRequest,
    build_goes_abi_band_render_request, build_goes_abi_map_render_request,
};
pub use rgb::{
    GoesAbiRgbCompositeRequest, GoesAbiRgbCompositeStyle,
    build_goes_abi_rgb_composite_render_request, compose_goes_abi_rgb_pixel,
};
