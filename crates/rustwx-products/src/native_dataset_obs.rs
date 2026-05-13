//! Hour-once decode and tile remap scaffolding for native observation sources.
//!
//! These types keep expensive GOES/MRMS source reads separate from per-tile
//! fanout. Scheduler code can decode one source hour, cache/precompute weights
//! per tile, then reuse the decoded arrays across many tile outputs.

use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use grib_core::grib2::{Grib2File, GridDefinition, ProductDefinition, unpack_message_normalized};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use crate::native_dataset::NativeDatasetBounds;
use crate::satellite::{
    GoesAbiScene, lat_lon_to_scan_angles_fast, open_goes_netcdf_lossy, read_goes_abi_scene,
    read_scaled_f32,
};

pub const GOES_MCMIPC_CHANNELS: &[GoesAbiChannelSpec] = &[
    GoesAbiChannelSpec::new("C01", 1, "CMI_C01"),
    GoesAbiChannelSpec::new("C02", 2, "CMI_C02"),
    GoesAbiChannelSpec::new("C03", 3, "CMI_C03"),
    GoesAbiChannelSpec::new("C04", 4, "CMI_C04"),
    GoesAbiChannelSpec::new("C05", 5, "CMI_C05"),
    GoesAbiChannelSpec::new("C06", 6, "CMI_C06"),
    GoesAbiChannelSpec::new("C07", 7, "CMI_C07"),
    GoesAbiChannelSpec::new("C08", 8, "CMI_C08"),
    GoesAbiChannelSpec::new("C09", 9, "CMI_C09"),
    GoesAbiChannelSpec::new("C10", 10, "CMI_C10"),
    GoesAbiChannelSpec::new("C11", 11, "CMI_C11"),
    GoesAbiChannelSpec::new("C12", 12, "CMI_C12"),
    GoesAbiChannelSpec::new("C13", 13, "CMI_C13"),
    GoesAbiChannelSpec::new("C14", 14, "CMI_C14"),
    GoesAbiChannelSpec::new("C15", 15, "CMI_C15"),
    GoesAbiChannelSpec::new("C16", 16, "CMI_C16"),
];

pub const MRMS_NATIVE_PRODUCTS: &[MrmsProductSpec] = &[
    MrmsProductSpec::new("MergedReflectivityQCComposite", "reflectivity"),
    MrmsProductSpec::new("MESH", "mesh"),
    MrmsProductSpec::new("RotationTrackML30min", "rotation"),
    MrmsProductSpec::new("MergedAzShear_0-2kmAGL", "rotation"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GoesAbiChannelSpec {
    pub id: &'static str,
    pub channel: u8,
    pub variable_name: &'static str,
}

impl GoesAbiChannelSpec {
    pub const fn new(id: &'static str, channel: u8, variable_name: &'static str) -> Self {
        Self {
            id,
            channel,
            variable_name,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MrmsProductSpec {
    pub product_id: &'static str,
    pub family: &'static str,
}

impl MrmsProductSpec {
    pub const fn new(product_id: &'static str, family: &'static str) -> Self {
        Self { product_id, family }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedGoesHour {
    pub path: PathBuf,
    pub scene: GoesAbiScene,
    pub bands: Vec<DecodedGoesBand>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedGoesBand {
    pub channel_id: String,
    pub channel: u8,
    pub variable_name: String,
    pub units: Option<String>,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedMrmsHour {
    pub path: PathBuf,
    pub product_id: String,
    pub valid_time_utc: DateTime<Utc>,
    pub parameter: MrmsGribParameter,
    pub grid: RegularLatLonGrid,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MrmsGribParameter {
    pub discipline: u8,
    pub parameter_category: u8,
    pub parameter_number: u8,
    pub forecast_time: u32,
    pub level_type: u8,
    pub level_value: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RegularLatLonGrid {
    pub nx: usize,
    pub ny: usize,
    pub first_lat_deg: f64,
    pub first_lon_deg: f64,
    pub dx_deg: f64,
    pub dy_deg: f64,
    pub west_deg: f64,
    pub east_deg: f64,
    pub south_deg: f64,
    pub north_deg: f64,
    pub west_to_east: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NativeObsTileGrid {
    pub bounds: NativeDatasetBounds,
    pub nx: usize,
    pub ny: usize,
}

impl NativeObsTileGrid {
    pub fn new(bounds: NativeDatasetBounds, nx: usize, ny: usize) -> Result<Self, Box<dyn Error>> {
        if nx == 0 || ny == 0 {
            return Err(boxed_error("tile grid dimensions must be non-zero"));
        }
        if !(bounds.west.is_finite()
            && bounds.east.is_finite()
            && bounds.south.is_finite()
            && bounds.north.is_finite()
            && bounds.west < bounds.east
            && bounds.south < bounds.north)
        {
            return Err(boxed_error(
                "tile bounds must be finite west/east/south/north",
            ));
        }
        Ok(Self { bounds, nx, ny })
    }

    pub fn lat_lon_at(&self, row: usize, col: usize) -> (f64, f64) {
        let x_den = self.nx.saturating_sub(1).max(1) as f64;
        let y_den = self.ny.saturating_sub(1).max(1) as f64;
        let lon = self.bounds.west + (self.bounds.east - self.bounds.west) * (col as f64 / x_den);
        let lat =
            self.bounds.north - (self.bounds.north - self.bounds.south) * (row as f64 / y_den);
        (lat, lon)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemappedGoesTile {
    pub tile: NativeObsTileGrid,
    pub bands: Vec<RemappedObsBand>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemappedMrmsTile {
    pub tile: NativeObsTileGrid,
    pub product_id: String,
    pub units: Option<String>,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemappedObsBand {
    pub field_id: String,
    pub units: Option<String>,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BilinearTileWeights {
    pub target_nx: usize,
    pub target_ny: usize,
    pub weights: Vec<Option<BilinearWeight>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BilinearWeight {
    pub i00: usize,
    pub i10: usize,
    pub i01: usize,
    pub i11: usize,
    pub w00: f32,
    pub w10: f32,
    pub w01: f32,
    pub w11: f32,
}

pub fn read_goes_multiband_hour(
    path: impl AsRef<Path>,
    channels: &[GoesAbiChannelSpec],
) -> Result<DecodedGoesHour, Box<dyn Error>> {
    let path = path.as_ref();
    let scene = read_goes_abi_scene(path)?;
    let file = open_goes_netcdf_lossy(path)?;
    let mut bands = Vec::with_capacity(channels.len());
    for channel in channels {
        let (variable_name, variable) = match read_scaled_f32(&file, channel.variable_name) {
            Ok(variable) => (channel.variable_name, variable),
            Err(err) if scene.channel == Some(channel.channel) => {
                ("CMI", read_scaled_f32(&file, "CMI").map_err(|_| err)?)
            }
            Err(err) => return Err(err),
        };
        validate_goes_shape(&scene, &variable.values, variable_name)?;
        bands.push(DecodedGoesBand {
            channel_id: channel.id.to_string(),
            channel: channel.channel,
            variable_name: variable_name.to_string(),
            units: variable.units,
            values: variable.values,
        });
    }
    Ok(DecodedGoesHour {
        path: path.to_path_buf(),
        scene,
        bands,
    })
}

pub fn read_mrms_product_hour(path: impl AsRef<Path>) -> Result<DecodedMrmsHour, Box<dyn Error>> {
    let path = path.as_ref();
    let bytes = read_maybe_gz(path)?;
    let grib = Grib2File::from_bytes(&bytes)?;
    let message = grib
        .messages
        .first()
        .ok_or_else(|| boxed_error("MRMS GRIB2 file has no messages"))?;
    let values_f64 = unpack_message_normalized(message)?;
    let grid = RegularLatLonGrid::from_grib_grid(&message.grid)?;
    let expected_len = grid.nx.saturating_mul(grid.ny);
    if values_f64.len() != expected_len {
        return Err(boxed_error(format!(
            "MRMS value count {} does not match grid {}x{}",
            values_f64.len(),
            grid.nx,
            grid.ny
        )));
    }
    Ok(DecodedMrmsHour {
        path: path.to_path_buf(),
        product_id: infer_mrms_product_id(path),
        valid_time_utc: DateTime::<Utc>::from_naive_utc_and_offset(message.reference_time, Utc),
        parameter: MrmsGribParameter::from_product(message.discipline, &message.product),
        grid,
        values: values_f64.into_iter().map(|value| value as f32).collect(),
    })
}

pub fn precompute_mrms_tile_weights(
    source: &RegularLatLonGrid,
    tile: NativeObsTileGrid,
) -> BilinearTileWeights {
    let mut weights = Vec::with_capacity(tile.nx.saturating_mul(tile.ny));
    for row in 0..tile.ny {
        for col in 0..tile.nx {
            let (lat, lon) = tile.lat_lon_at(row, col);
            weights.push(source.bilinear_weight(lat, lon));
        }
    }
    BilinearTileWeights {
        target_nx: tile.nx,
        target_ny: tile.ny,
        weights,
    }
}

pub fn precompute_goes_tile_weights(
    scene: &GoesAbiScene,
    tile: NativeObsTileGrid,
) -> BilinearTileWeights {
    let mut weights = Vec::with_capacity(tile.nx.saturating_mul(tile.ny));
    for row in 0..tile.ny {
        for col in 0..tile.nx {
            let (lat, lon) = tile.lat_lon_at(row, col);
            let weight = lat_lon_to_scan_angles_fast(
                scene.projection.perspective_point_height_m,
                scene.projection.semi_major_axis_m,
                scene.projection.semi_minor_axis_m,
                scene.projection.longitude_of_projection_origin_deg,
                scene.projection.sweep_angle_axis,
                lat,
                lon,
            )
            .and_then(|(x, y)| {
                fixed_grid_bilinear_weight(
                    scene.fixed_grid.nx,
                    scene.fixed_grid.ny,
                    &scene.fixed_grid.x_scan_rad,
                    &scene.fixed_grid.y_scan_rad,
                    x,
                    y,
                )
            });
            weights.push(weight);
        }
    }
    BilinearTileWeights {
        target_nx: tile.nx,
        target_ny: tile.ny,
        weights,
    }
}

pub fn remap_mrms_hour_to_tile(
    hour: &DecodedMrmsHour,
    tile: NativeObsTileGrid,
) -> Result<RemappedMrmsTile, Box<dyn Error>> {
    let weights = precompute_mrms_tile_weights(&hour.grid, tile);
    Ok(RemappedMrmsTileBuilder::new(hour, tile)
        .with_values(apply_bilinear_weights(&hour.values, &weights)?))
}

pub fn remap_goes_hour_to_tile(
    hour: &DecodedGoesHour,
    tile: NativeObsTileGrid,
) -> Result<RemappedGoesTile, Box<dyn Error>> {
    let weights = precompute_goes_tile_weights(&hour.scene, tile);
    let mut bands = Vec::with_capacity(hour.bands.len());
    for band in &hour.bands {
        bands.push(RemappedObsBand {
            field_id: band.channel_id.clone(),
            units: band.units.clone(),
            values: apply_bilinear_weights(&band.values, &weights)?,
        });
    }
    Ok(RemappedGoesTile { tile, bands })
}

pub fn apply_bilinear_weights(
    values: &[f32],
    weights: &BilinearTileWeights,
) -> Result<Vec<f32>, Box<dyn Error>> {
    let mut out = Vec::with_capacity(weights.weights.len());
    for weight in &weights.weights {
        let Some(weight) = weight else {
            out.push(f32::NAN);
            continue;
        };
        let Some((&v00, &v10, &v01, &v11)) = values
            .get(weight.i00)
            .zip(values.get(weight.i10))
            .zip(values.get(weight.i01))
            .zip(values.get(weight.i11))
            .map(|(((v00, v10), v01), v11)| (v00, v10, v01, v11))
        else {
            return Err(boxed_error("bilinear weight index exceeds source values"));
        };
        if v00.is_finite() && v10.is_finite() && v01.is_finite() && v11.is_finite() {
            out.push(v00 * weight.w00 + v10 * weight.w10 + v01 * weight.w01 + v11 * weight.w11);
        } else {
            out.push(f32::NAN);
        }
    }
    Ok(out)
}

impl RegularLatLonGrid {
    pub fn new(
        nx: usize,
        ny: usize,
        first_lat_deg: f64,
        first_lon_deg: f64,
        dx_deg: f64,
        dy_deg: f64,
        west_to_east: bool,
    ) -> Result<Self, Box<dyn Error>> {
        if nx == 0 || ny == 0 {
            return Err(boxed_error("regular grid dimensions must be non-zero"));
        }
        if !(first_lat_deg.is_finite()
            && first_lon_deg.is_finite()
            && dx_deg.is_finite()
            && dy_deg.is_finite()
            && dx_deg > 0.0
            && dy_deg > 0.0)
        {
            return Err(boxed_error(
                "regular grid origin and spacing must be finite",
            ));
        }
        let last_lat = first_lat_deg - dy_deg * ny.saturating_sub(1) as f64;
        let last_lon = if west_to_east {
            first_lon_deg + dx_deg * nx.saturating_sub(1) as f64
        } else {
            first_lon_deg - dx_deg * nx.saturating_sub(1) as f64
        };
        Ok(Self {
            nx,
            ny,
            first_lat_deg,
            first_lon_deg,
            dx_deg,
            dy_deg,
            west_deg: first_lon_deg.min(last_lon),
            east_deg: first_lon_deg.max(last_lon),
            south_deg: first_lat_deg.min(last_lat),
            north_deg: first_lat_deg.max(last_lat),
            west_to_east,
        })
    }

    fn from_grib_grid(grid: &GridDefinition) -> Result<Self, Box<dyn Error>> {
        if grid.template != 0 {
            return Err(boxed_error(format!(
                "MRMS regular-grid scaffold expects GRIB2 template 3.0, got {}",
                grid.template
            )));
        }
        if grid.scan_mode & 0x20 != 0 {
            return Err(boxed_error(format!(
                "MRMS regular-grid scaffold expects row-major GRIB scan mode, got {:#04x}",
                grid.scan_mode
            )));
        }
        let nx = grid.nx as usize;
        let ny = grid.ny as usize;
        let lon1 = normalize_longitude_deg(grid.lon1);
        let lon2 = normalize_longitude_deg(grid.lon2);
        let first_lat = grid.lat1.max(grid.lat2);
        let west_to_east = grid.scan_mode & 0x80 == 0;
        let first_lon = lon1;
        let dx = if grid.dx > 0.0 {
            grid.dx.abs()
        } else {
            (lon2 - lon1).abs() / nx.saturating_sub(1).max(1) as f64
        };
        let dy = if grid.dy > 0.0 {
            grid.dy.abs()
        } else {
            (grid.lat2 - grid.lat1).abs() / ny.saturating_sub(1).max(1) as f64
        };
        Self::new(nx, ny, first_lat, first_lon, dx, dy, west_to_east)
    }

    pub fn bilinear_weight(&self, lat_deg: f64, lon_deg: f64) -> Option<BilinearWeight> {
        if self.nx < 2 || self.ny < 2 || !lat_deg.is_finite() || !lon_deg.is_finite() {
            return None;
        }
        let lon = normalize_longitude_deg(lon_deg);
        if lat_deg > self.north_deg
            || lat_deg < self.south_deg
            || lon < self.west_deg
            || lon > self.east_deg
        {
            return None;
        }
        let x = if self.west_to_east {
            (lon - self.first_lon_deg) / self.dx_deg
        } else {
            (self.first_lon_deg - lon) / self.dx_deg
        };
        let y = (self.first_lat_deg - lat_deg) / self.dy_deg;
        bilinear_weight_from_fractional_xy(self.nx, self.ny, x, y)
    }
}

impl MrmsGribParameter {
    fn from_product(discipline: u8, product: &ProductDefinition) -> Self {
        Self {
            discipline,
            parameter_category: product.parameter_category,
            parameter_number: product.parameter_number,
            forecast_time: product.forecast_time,
            level_type: product.level_type,
            level_value: product.level_value,
        }
    }
}

struct RemappedMrmsTileBuilder<'a> {
    hour: &'a DecodedMrmsHour,
    tile: NativeObsTileGrid,
}

impl<'a> RemappedMrmsTileBuilder<'a> {
    fn new(hour: &'a DecodedMrmsHour, tile: NativeObsTileGrid) -> Self {
        Self { hour, tile }
    }

    fn with_values(self, values: Vec<f32>) -> RemappedMrmsTile {
        RemappedMrmsTile {
            tile: self.tile,
            product_id: self.hour.product_id.clone(),
            units: None,
            values,
        }
    }
}

fn fixed_grid_bilinear_weight(
    nx: usize,
    ny: usize,
    x_axis: &[f64],
    y_axis: &[f64],
    x: f64,
    y: f64,
) -> Option<BilinearWeight> {
    if x_axis.len() != nx || y_axis.len() != ny {
        return None;
    }
    let x_fraction = fractional_axis_index(x_axis, x)?;
    let y_fraction = fractional_axis_index(y_axis, y)?;
    bilinear_weight_from_fractional_xy(nx, ny, x_fraction, y_fraction)
}

fn fractional_axis_index(axis: &[f64], value: f64) -> Option<f64> {
    if axis.len() < 2 || !value.is_finite() {
        return None;
    }
    let ascending = axis[0] <= *axis.last()?;
    let first = axis[0];
    let last = *axis.last()?;
    if ascending {
        if value < first || value > last {
            return None;
        }
    } else if value > first || value < last {
        return None;
    }

    let mut lo = 0usize;
    let mut hi = axis.len() - 1;
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        let mid_value = axis[mid];
        if (ascending && mid_value <= value) || (!ascending && mid_value >= value) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    let denom = axis[hi] - axis[lo];
    if denom == 0.0 {
        return None;
    }
    Some(lo as f64 + (value - axis[lo]) / denom)
}

fn bilinear_weight_from_fractional_xy(
    nx: usize,
    ny: usize,
    x: f64,
    y: f64,
) -> Option<BilinearWeight> {
    if nx < 2 || ny < 2 || x < 0.0 || y < 0.0 || x > (nx - 1) as f64 || y > (ny - 1) as f64 {
        return None;
    }
    let x0 = x.floor().min((nx - 2) as f64) as usize;
    let y0 = y.floor().min((ny - 2) as f64) as usize;
    let fx = (x - x0 as f64) as f32;
    let fy = (y - y0 as f64) as f32;
    let i00 = y0 * nx + x0;
    let i10 = i00 + 1;
    let i01 = i00 + nx;
    let i11 = i01 + 1;
    Some(BilinearWeight {
        i00,
        i10,
        i01,
        i11,
        w00: (1.0 - fx) * (1.0 - fy),
        w10: fx * (1.0 - fy),
        w01: (1.0 - fx) * fy,
        w11: fx * fy,
    })
}

fn validate_goes_shape(
    scene: &GoesAbiScene,
    values: &[f32],
    variable_name: &str,
) -> Result<(), Box<dyn Error>> {
    let expected = scene.fixed_grid.nx.saturating_mul(scene.fixed_grid.ny);
    if values.len() != expected {
        return Err(boxed_error(format!(
            "GOES variable {variable_name} length {} does not match fixed grid {}x{}",
            values.len(),
            scene.fixed_grid.nx,
            scene.fixed_grid.ny
        )));
    }
    Ok(())
}

fn read_maybe_gz(path: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(&bytes[..]);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out)?;
        Ok(out)
    } else {
        Ok(bytes)
    }
}

fn infer_mrms_product_id(path: &Path) -> String {
    let name = path
        .file_name()
        .map(|value| value.to_string_lossy())
        .unwrap_or_default();
    for spec in MRMS_NATIVE_PRODUCTS {
        if name.contains(spec.product_id) {
            return spec.product_id.to_string();
        }
    }
    name.split(['_', '.'])
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("mrms")
        .to_string()
}

fn normalize_longitude_deg(lon: f64) -> f64 {
    let mut value = (lon + 180.0).rem_euclid(360.0) - 180.0;
    if value == -180.0 {
        value = 180.0;
    }
    value
}

fn boxed_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_obs_plan_contains_expected_goes_channels_and_mrms_products() {
        let channel_ids: Vec<_> = GOES_MCMIPC_CHANNELS
            .iter()
            .map(|channel| channel.id)
            .collect();
        assert_eq!(
            channel_ids,
            vec![
                "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12",
                "C13", "C14", "C15", "C16"
            ]
        );
        assert!(
            MRMS_NATIVE_PRODUCTS
                .iter()
                .any(|product| product.family == "reflectivity")
        );
        assert!(
            MRMS_NATIVE_PRODUCTS
                .iter()
                .any(|product| product.family == "mesh")
        );
        assert!(
            MRMS_NATIVE_PRODUCTS
                .iter()
                .any(|product| product.family == "rotation")
        );
    }

    #[test]
    fn obs_regular_bilinear_remap_samples_synthetic_mrms_grid() {
        let grid = RegularLatLonGrid::new(3, 3, 2.0, 0.0, 1.0, 1.0, true).unwrap();
        let tile =
            NativeObsTileGrid::new(NativeDatasetBounds::new(0.5, 1.5, 0.5, 1.5), 2, 2).unwrap();
        let weights = precompute_mrms_tile_weights(&grid, tile);
        let values = vec![
            20.0, 21.0, 22.0, //
            10.0, 11.0, 12.0, //
            0.0, 1.0, 2.0,
        ];
        let sampled = apply_bilinear_weights(&values, &weights).unwrap();
        assert_eq!(sampled.len(), 4);
        assert!((sampled[0] - 15.5).abs() < 1.0e-6, "{sampled:?}");
        assert!((sampled[1] - 16.5).abs() < 1.0e-6, "{sampled:?}");
        assert!((sampled[2] - 5.5).abs() < 1.0e-6, "{sampled:?}");
        assert!((sampled[3] - 6.5).abs() < 1.0e-6, "{sampled:?}");
    }

    #[test]
    fn obs_fixed_grid_axis_weight_handles_descending_y() {
        let weight = fixed_grid_bilinear_weight(3, 3, &[0.0, 1.0, 2.0], &[2.0, 1.0, 0.0], 0.5, 1.5)
            .expect("point should be inside fixed grid");
        assert_eq!(weight.i00, 0);
        assert_eq!(weight.i10, 1);
        assert_eq!(weight.i01, 3);
        assert_eq!(weight.i11, 4);
        assert!((weight.w00 - 0.25).abs() < 1.0e-6);
    }
}
