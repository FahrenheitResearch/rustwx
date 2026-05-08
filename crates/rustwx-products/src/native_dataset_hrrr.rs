//! HRRR hour-once decode and tile remap scaffolding for native datasets.
//!
//! The scheduler owns when an hour should be fetched. This module owns the
//! narrower mechanics of planning the HRRR fields, decoding one GRIB byte
//! payload into an hour cache, precomputing reusable tile remap weights, and
//! fanning the decoded hour out to tile grids without decoding again.

use chrono::{DateTime, Utc};
use rustwx_core::{
    CanonicalField, FieldSelector, GridProjection, GridShape, LatLonGrid, ModelId, SelectedField2D,
};
use std::collections::{BTreeMap, HashSet};
use std::error::Error;

use crate::thermo_native::{NativeThermoRecipe, extract_native_thermo_field};

const REGULAR_GRID_EPS: f64 = 1.0e-5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HrrrDecodeRoute {
    FieldSelector(FieldSelector),
    NativeThermo(NativeThermoRecipe),
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HrrrDatasetFieldSpec {
    pub id: &'static str,
    pub label: &'static str,
    pub route: HrrrDecodeRoute,
}

pub const HRRR_DATASET_FIELD_SPECS: &[HrrrDatasetFieldSpec] = &[
    HrrrDatasetFieldSpec {
        id: "t2m",
        label: "2 m temperature",
        route: HrrrDecodeRoute::FieldSelector(FieldSelector::height_agl(
            CanonicalField::Temperature,
            2,
        )),
    },
    HrrrDatasetFieldSpec {
        id: "d2m",
        label: "2 m dewpoint",
        route: HrrrDecodeRoute::FieldSelector(FieldSelector::height_agl(
            CanonicalField::Dewpoint,
            2,
        )),
    },
    HrrrDatasetFieldSpec {
        id: "u10",
        label: "10 m U wind",
        route: HrrrDecodeRoute::FieldSelector(FieldSelector::height_agl(CanonicalField::UWind, 10)),
    },
    HrrrDatasetFieldSpec {
        id: "v10",
        label: "10 m V wind",
        route: HrrrDecodeRoute::FieldSelector(FieldSelector::height_agl(CanonicalField::VWind, 10)),
    },
    HrrrDatasetFieldSpec {
        id: "refc",
        label: "composite reflectivity",
        route: HrrrDecodeRoute::FieldSelector(FieldSelector::entire_atmosphere(
            CanonicalField::CompositeReflectivity,
        )),
    },
    HrrrDatasetFieldSpec {
        id: "mslp",
        label: "mean sea-level pressure",
        route: HrrrDecodeRoute::FieldSelector(FieldSelector::mean_sea_level(
            CanonicalField::PressureReducedToMeanSeaLevel,
        )),
    },
    HrrrDatasetFieldSpec {
        id: "terrain",
        label: "terrain/orography",
        route: HrrrDecodeRoute::FieldSelector(FieldSelector::surface(
            CanonicalField::GeopotentialHeight,
        )),
    },
    HrrrDatasetFieldSpec {
        id: "pwat",
        label: "precipitable water",
        route: HrrrDecodeRoute::FieldSelector(FieldSelector::entire_atmosphere(
            CanonicalField::PrecipitableWater,
        )),
    },
    HrrrDatasetFieldSpec {
        id: "cape",
        label: "surface CAPE",
        route: HrrrDecodeRoute::NativeThermo(NativeThermoRecipe::Sbcape),
    },
    HrrrDatasetFieldSpec {
        id: "cin",
        label: "surface CIN",
        route: HrrrDecodeRoute::NativeThermo(NativeThermoRecipe::Sbcin),
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HrrrHourFieldPlan {
    pub fields: Vec<HrrrDatasetFieldSpec>,
    pub selectors: Vec<(String, FieldSelector)>,
    pub native_thermo: Vec<(String, NativeThermoRecipe)>,
    pub unsupported_field_ids: Vec<String>,
}

pub fn hrrr_dataset_field_specs() -> &'static [HrrrDatasetFieldSpec] {
    HRRR_DATASET_FIELD_SPECS
}

pub fn hrrr_dataset_field_spec(id: &str) -> Option<&'static HrrrDatasetFieldSpec> {
    HRRR_DATASET_FIELD_SPECS.iter().find(|field| field.id == id)
}

pub fn plan_hrrr_hour_fields<I, S>(field_ids: I) -> Result<HrrrHourFieldPlan, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut seen = HashSet::<String>::new();
    let mut fields = Vec::new();
    let mut selectors = Vec::new();
    let mut native_thermo = Vec::new();
    let mut unsupported_field_ids = Vec::new();

    for id in field_ids {
        let id = id.as_ref();
        if !seen.insert(id.to_string()) {
            continue;
        }
        let spec = *hrrr_dataset_field_spec(id)
            .ok_or_else(|| format!("unknown HRRR native dataset field '{id}'"))?;
        match spec.route {
            HrrrDecodeRoute::FieldSelector(selector) => {
                selectors.push((spec.id.to_string(), selector));
            }
            HrrrDecodeRoute::NativeThermo(recipe) => {
                native_thermo.push((spec.id.to_string(), recipe));
            }
            HrrrDecodeRoute::Unsupported => {
                unsupported_field_ids.push(spec.id.to_string());
            }
        }
        fields.push(spec);
    }

    Ok(HrrrHourFieldPlan {
        fields,
        selectors,
        native_thermo,
        unsupported_field_ids,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct HrrrCachedField {
    pub field_id: String,
    pub selector: Option<FieldSelector>,
    pub native_thermo: Option<NativeThermoRecipe>,
    pub units: String,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HrrrHourCache {
    pub valid_time: DateTime<Utc>,
    pub source_id: String,
    pub grid: Option<LatLonGrid>,
    pub projection: Option<GridProjection>,
    pub fields: BTreeMap<String, HrrrCachedField>,
}

impl HrrrHourCache {
    pub fn new(valid_time: DateTime<Utc>, source_id: impl Into<String>) -> Self {
        Self {
            valid_time,
            source_id: source_id.into(),
            grid: None,
            projection: None,
            fields: BTreeMap::new(),
        }
    }

    pub fn add_selected_field(
        &mut self,
        field_id: impl Into<String>,
        field: SelectedField2D,
    ) -> Result<(), String> {
        let field_id = field_id.into();
        self.ensure_grid_matches(&field.grid)?;
        if self.projection.is_none() {
            self.projection = field.projection.clone();
        }
        self.fields.insert(
            field_id.clone(),
            HrrrCachedField {
                field_id,
                selector: Some(field.selector),
                native_thermo: None,
                units: field.units,
                values: field.values,
            },
        );
        Ok(())
    }

    pub fn add_native_thermo_field(
        &mut self,
        field_id: impl Into<String>,
        recipe: NativeThermoRecipe,
        units: impl Into<String>,
        grid: LatLonGrid,
        values: Vec<f32>,
    ) -> Result<(), String> {
        let field_id = field_id.into();
        self.ensure_grid_matches(&grid)?;
        self.fields.insert(
            field_id.clone(),
            HrrrCachedField {
                field_id,
                selector: None,
                native_thermo: Some(recipe),
                units: units.into(),
                values,
            },
        );
        Ok(())
    }

    fn ensure_grid_matches(&mut self, grid: &LatLonGrid) -> Result<(), String> {
        match &self.grid {
            Some(existing) if existing != grid => Err(format!(
                "decoded HRRR fields do not share one grid: existing {}x{}, new {}x{}",
                existing.shape.nx, existing.shape.ny, grid.shape.nx, grid.shape.ny
            )),
            Some(_) => Ok(()),
            None => {
                self.grid = Some(grid.clone());
                Ok(())
            }
        }
    }
}

pub fn decode_hrrr_hour_once<I, S>(
    valid_time: DateTime<Utc>,
    source_id: impl Into<String>,
    bytes: &[u8],
    field_ids: I,
) -> Result<HrrrHourCache, Box<dyn Error>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let plan = plan_hrrr_hour_fields(field_ids)?;
    let mut cache = HrrrHourCache::new(valid_time, source_id);

    if !plan.selectors.is_empty() {
        let selectors = plan
            .selectors
            .iter()
            .map(|(_, selector)| *selector)
            .collect::<Vec<_>>();
        let decoded = rustwx_io::extract_fields_from_bytes(bytes, &selectors)?;
        for ((field_id, _), field) in plan.selectors.iter().zip(decoded.into_iter()) {
            cache.add_selected_field(field_id.clone(), field)?;
        }
    }

    for (field_id, recipe) in &plan.native_thermo {
        let Some(field) = extract_native_thermo_field(ModelId::Hrrr, *recipe, bytes)? else {
            return Err(format!("HRRR native thermo field '{}' was not found", field_id).into());
        };
        cache.add_native_thermo_field(
            field_id.clone(),
            *recipe,
            field.units,
            field.grid,
            field.values.into_iter().map(|value| value as f32).collect(),
        )?;
    }

    Ok(cache)
}

pub fn load_hrrr_hour_once_from_bytes<I, S>(
    valid_time: DateTime<Utc>,
    source_id: impl Into<String>,
    bytes: &[u8],
    field_ids: I,
) -> Result<HrrrHourCache, Box<dyn Error>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    decode_hrrr_hour_once(valid_time, source_id, bytes, field_ids)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemapMethod {
    Nearest,
    Bilinear,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NativeDatasetTileGrid {
    pub tile_id: String,
    pub grid: LatLonGrid,
}

impl NativeDatasetTileGrid {
    pub fn new(tile_id: impl Into<String>, grid: LatLonGrid) -> Self {
        Self {
            tile_id: tile_id.into(),
            grid,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemapCellWeights {
    pub source_indices: [usize; 4],
    pub weights: [f32; 4],
}

#[derive(Debug, Clone, PartialEq)]
pub struct TileRemap {
    pub tile_id: String,
    pub source_shape: GridShape,
    pub target_grid: LatLonGrid,
    pub method: RemapMethod,
    pub cells: Vec<RemapCellWeights>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HrrrTileHour {
    pub valid_time: DateTime<Utc>,
    pub source_id: String,
    pub tile_id: String,
    pub grid: LatLonGrid,
    pub projection: Option<GridProjection>,
    pub fields: BTreeMap<String, HrrrCachedField>,
}

pub fn precompute_tile_remap(
    source_grid: &LatLonGrid,
    tile_grid: &NativeDatasetTileGrid,
    method: RemapMethod,
) -> Result<TileRemap, String> {
    precompute_tile_remap_with_projection(source_grid, None, tile_grid, method)
}

pub fn precompute_tile_remap_with_projection(
    source_grid: &LatLonGrid,
    source_projection: Option<&GridProjection>,
    tile_grid: &NativeDatasetTileGrid,
    method: RemapMethod,
) -> Result<TileRemap, String> {
    let cells = match method {
        RemapMethod::Nearest => build_nearest_weights(source_grid, &tile_grid.grid),
        RemapMethod::Bilinear => build_bilinear_weights(source_grid, &tile_grid.grid)
            .or_else(|| {
                build_projected_bilinear_weights(source_grid, source_projection, &tile_grid.grid)
            })
            .unwrap_or_else(|| build_nearest_weights(source_grid, &tile_grid.grid)),
    };

    Ok(TileRemap {
        tile_id: tile_grid.tile_id.clone(),
        source_shape: source_grid.shape,
        target_grid: tile_grid.grid.clone(),
        method,
        cells,
    })
}

pub fn remap_hrrr_hour_to_tile(
    hour: &HrrrHourCache,
    remap: &TileRemap,
) -> Result<HrrrTileHour, String> {
    let source_grid = hour
        .grid
        .as_ref()
        .ok_or_else(|| "HRRR hour cache has no decoded grid".to_string())?;
    if source_grid.shape != remap.source_shape {
        return Err(format!(
            "remap source shape {}x{} does not match hour grid {}x{}",
            remap.source_shape.nx,
            remap.source_shape.ny,
            source_grid.shape.nx,
            source_grid.shape.ny
        ));
    }
    if remap.cells.len() != remap.target_grid.shape.len() {
        return Err("remap weight count does not match target grid".to_string());
    }

    let mut fields = BTreeMap::new();
    for (field_id, field) in &hour.fields {
        if field.values.len() != source_grid.shape.len() {
            return Err(format!(
                "field '{field_id}' length {} does not match hour grid length {}",
                field.values.len(),
                source_grid.shape.len()
            ));
        }
        fields.insert(
            field_id.clone(),
            HrrrCachedField {
                field_id: field.field_id.clone(),
                selector: field.selector,
                native_thermo: field.native_thermo,
                units: field.units.clone(),
                values: apply_remap(&field.values, &remap.cells),
            },
        );
    }

    Ok(HrrrTileHour {
        valid_time: hour.valid_time,
        source_id: hour.source_id.clone(),
        tile_id: remap.tile_id.clone(),
        grid: remap.target_grid.clone(),
        projection: hour.projection.clone(),
        fields,
    })
}

fn apply_remap(values: &[f32], cells: &[RemapCellWeights]) -> Vec<f32> {
    cells
        .iter()
        .map(|cell| {
            cell.source_indices
                .iter()
                .zip(cell.weights.iter())
                .map(|(source_index, weight)| values[*source_index] * *weight)
                .sum()
        })
        .collect()
}

fn build_nearest_weights(source: &LatLonGrid, target: &LatLonGrid) -> Vec<RemapCellWeights> {
    target
        .lat_deg
        .iter()
        .copied()
        .zip(target.lon_deg.iter().copied())
        .map(|(lat, lon)| {
            let index = nearest_source_index(source, lat, lon);
            RemapCellWeights {
                source_indices: [index, index, index, index],
                weights: [1.0, 0.0, 0.0, 0.0],
            }
        })
        .collect()
}

fn build_bilinear_weights(
    source: &LatLonGrid,
    target: &LatLonGrid,
) -> Option<Vec<RemapCellWeights>> {
    let x_axis = regular_lon_axis(source)?;
    let y_axis = regular_lat_axis(source)?;
    if x_axis.len() < 2 || y_axis.len() < 2 {
        return None;
    }

    let nx = source.shape.nx;
    let ny = source.shape.ny;
    Some(
        target
            .lat_deg
            .iter()
            .copied()
            .zip(target.lon_deg.iter().copied())
            .map(|(lat, lon)| {
                let x = fractional_axis_index(&x_axis, f64::from(lon));
                let y = fractional_axis_index(&y_axis, f64::from(lat));
                let x0 = x.floor().clamp(0.0, (nx - 2) as f64) as usize;
                let y0 = y.floor().clamp(0.0, (ny - 2) as f64) as usize;
                let x1 = x0 + 1;
                let y1 = y0 + 1;
                let tx = (x - x0 as f64).clamp(0.0, 1.0) as f32;
                let ty = (y - y0 as f64).clamp(0.0, 1.0) as f32;

                RemapCellWeights {
                    source_indices: [y0 * nx + x0, y0 * nx + x1, y1 * nx + x0, y1 * nx + x1],
                    weights: [
                        (1.0 - tx) * (1.0 - ty),
                        tx * (1.0 - ty),
                        (1.0 - tx) * ty,
                        tx * ty,
                    ],
                }
            })
            .collect(),
    )
}

fn build_projected_bilinear_weights(
    source: &LatLonGrid,
    source_projection: Option<&GridProjection>,
    target: &LatLonGrid,
) -> Option<Vec<RemapCellWeights>> {
    let projection = source_projection?;
    let nx = source.shape.nx;
    let ny = source.shape.ny;
    if nx < 2 || ny < 2 {
        return None;
    }

    let source_x_axis = (0..nx)
        .map(|x| {
            let idx = x;
            project_lat_lon_for_remap(projection, source.lat_deg[idx], source.lon_deg[idx])
                .map(|(projected_x, _)| projected_x)
        })
        .collect::<Option<Vec<_>>>()?;
    let source_y_axis = (0..ny)
        .map(|y| {
            let idx = y * nx;
            project_lat_lon_for_remap(projection, source.lat_deg[idx], source.lon_deg[idx])
                .map(|(_, projected_y)| projected_y)
        })
        .collect::<Option<Vec<_>>>()?;
    if !axis_is_monotonic(&source_x_axis) || !axis_is_monotonic(&source_y_axis) {
        return None;
    }

    target
        .lat_deg
        .iter()
        .copied()
        .zip(target.lon_deg.iter().copied())
        .map(|(lat, lon)| {
            let (projected_x, projected_y) = project_lat_lon_for_remap(projection, lat, lon)?;
            let x = fractional_monotonic_axis_index(&source_x_axis, projected_x);
            let y = fractional_monotonic_axis_index(&source_y_axis, projected_y);
            let x0 = x.floor().clamp(0.0, (nx - 2) as f64) as usize;
            let y0 = y.floor().clamp(0.0, (ny - 2) as f64) as usize;
            let x1 = x0 + 1;
            let y1 = y0 + 1;
            let tx = (x - x0 as f64).clamp(0.0, 1.0) as f32;
            let ty = (y - y0 as f64).clamp(0.0, 1.0) as f32;

            Some(RemapCellWeights {
                source_indices: [y0 * nx + x0, y0 * nx + x1, y1 * nx + x0, y1 * nx + x1],
                weights: [
                    (1.0 - tx) * (1.0 - ty),
                    tx * (1.0 - ty),
                    (1.0 - tx) * ty,
                    tx * ty,
                ],
            })
        })
        .collect()
}

fn nearest_source_index(source: &LatLonGrid, target_lat: f32, target_lon: f32) -> usize {
    let mut best_index = 0usize;
    let mut best_distance = f64::INFINITY;
    for index in 0..source.shape.len() {
        let dlat = f64::from(source.lat_deg[index] - target_lat);
        let dlon = longitude_delta_deg(source.lon_deg[index], target_lon);
        let distance = dlat * dlat + dlon * dlon;
        if distance < best_distance {
            best_distance = distance;
            best_index = index;
        }
    }
    best_index
}

fn longitude_delta_deg(a: f32, b: f32) -> f64 {
    (f64::from(a) - f64::from(b) + 540.0).rem_euclid(360.0) - 180.0
}

fn regular_lon_axis(grid: &LatLonGrid) -> Option<Vec<f64>> {
    let nx = grid.shape.nx;
    let ny = grid.shape.ny;
    let axis = grid.lon_deg[0..nx]
        .iter()
        .map(|value| f64::from(*value))
        .collect::<Vec<_>>();
    if !axis_is_regular(&axis) {
        return None;
    }
    for y in 1..ny {
        let row = &grid.lon_deg[y * nx..(y + 1) * nx];
        if row
            .iter()
            .zip(axis.iter())
            .any(|(actual, expected)| (f64::from(*actual) - *expected).abs() > REGULAR_GRID_EPS)
        {
            return None;
        }
    }
    Some(axis)
}

fn regular_lat_axis(grid: &LatLonGrid) -> Option<Vec<f64>> {
    let nx = grid.shape.nx;
    let ny = grid.shape.ny;
    let axis = (0..ny)
        .map(|y| f64::from(grid.lat_deg[y * nx]))
        .collect::<Vec<_>>();
    if !axis_is_regular(&axis) {
        return None;
    }
    for y in 0..ny {
        let expected = axis[y];
        for x in 1..nx {
            if (f64::from(grid.lat_deg[y * nx + x]) - expected).abs() > REGULAR_GRID_EPS {
                return None;
            }
        }
    }
    Some(axis)
}

fn axis_is_regular(axis: &[f64]) -> bool {
    if axis.len() < 2 || axis.iter().any(|value| !value.is_finite()) {
        return false;
    }
    let step = axis[1] - axis[0];
    if step.abs() <= REGULAR_GRID_EPS {
        return false;
    }
    axis.windows(2)
        .all(|pair| ((pair[1] - pair[0]) - step).abs() <= REGULAR_GRID_EPS)
}

fn fractional_axis_index(axis: &[f64], value: f64) -> f64 {
    let step = axis[1] - axis[0];
    ((value - axis[0]) / step).clamp(0.0, (axis.len() - 1) as f64)
}

fn axis_is_monotonic(axis: &[f64]) -> bool {
    if axis.len() < 2 || axis.iter().any(|value| !value.is_finite()) {
        return false;
    }
    let ascending = axis[axis.len() - 1] >= axis[0];
    axis.windows(2).all(|pair| {
        if ascending {
            pair[1] > pair[0]
        } else {
            pair[1] < pair[0]
        }
    })
}

fn fractional_monotonic_axis_index(axis: &[f64], value: f64) -> f64 {
    if axis.len() < 2 || !value.is_finite() {
        return 0.0;
    }
    let ascending = axis[axis.len() - 1] >= axis[0];
    if ascending {
        if value <= axis[0] {
            return 0.0;
        }
        if value >= axis[axis.len() - 1] {
            return (axis.len() - 1) as f64;
        }
    } else {
        if value >= axis[0] {
            return 0.0;
        }
        if value <= axis[axis.len() - 1] {
            return (axis.len() - 1) as f64;
        }
    }

    let mut lo = 0usize;
    let mut hi = axis.len() - 1;
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        let is_before = if ascending {
            axis[mid] <= value
        } else {
            axis[mid] >= value
        };
        if is_before {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    let span = axis[hi] - axis[lo];
    if span.abs() <= f64::EPSILON {
        return lo as f64;
    }
    lo as f64 + ((value - axis[lo]) / span).clamp(0.0, 1.0)
}

fn project_lat_lon_for_remap(
    projection: &GridProjection,
    lat_deg: f32,
    lon_deg: f32,
) -> Option<(f64, f64)> {
    match *projection {
        GridProjection::LambertConformal {
            standard_parallel_1_deg,
            standard_parallel_2_deg,
            central_meridian_deg,
        } => lambert_project_for_remap(
            f64::from(lat_deg),
            f64::from(lon_deg),
            standard_parallel_1_deg,
            standard_parallel_2_deg,
            central_meridian_deg,
        ),
        _ => None,
    }
}

fn lambert_project_for_remap(
    lat_deg: f64,
    lon_deg: f64,
    standard_parallel_1_deg: f64,
    standard_parallel_2_deg: f64,
    central_meridian_deg: f64,
) -> Option<(f64, f64)> {
    if !(lat_deg.is_finite()
        && lon_deg.is_finite()
        && standard_parallel_1_deg.is_finite()
        && standard_parallel_2_deg.is_finite()
        && central_meridian_deg.is_finite())
    {
        return None;
    }
    let phi = lat_deg.to_radians();
    let phi1 = standard_parallel_1_deg.to_radians();
    let phi2 = standard_parallel_2_deg.to_radians();
    if phi.abs() >= std::f64::consts::FRAC_PI_2
        || phi1.abs() >= std::f64::consts::FRAC_PI_2
        || phi2.abs() >= std::f64::consts::FRAC_PI_2
    {
        return None;
    }

    let t = (std::f64::consts::FRAC_PI_4 + phi / 2.0).tan();
    let t1 = (std::f64::consts::FRAC_PI_4 + phi1 / 2.0).tan();
    let t2 = (std::f64::consts::FRAC_PI_4 + phi2 / 2.0).tan();
    if t <= 0.0 || t1 <= 0.0 || t2 <= 0.0 {
        return None;
    }
    let n = if (phi1 - phi2).abs() <= 1.0e-12 {
        phi1.sin()
    } else {
        (phi1.cos() / phi2.cos()).ln() / (t2 / t1).ln()
    };
    if !n.is_finite() || n.abs() <= 1.0e-12 {
        return None;
    }
    let f = phi1.cos() * t1.powf(n) / n;
    let rho = f / t.powf(n);
    if !rho.is_finite() {
        return None;
    }
    let theta = n * longitude_delta_deg(lon_deg as f32, central_meridian_deg as f32).to_radians();
    Some((rho * theta.sin(), -rho * theta.cos()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn regular_grid(
        nx: usize,
        ny: usize,
        lat0: f32,
        lon0: f32,
        dlat: f32,
        dlon: f32,
    ) -> LatLonGrid {
        let mut lat = Vec::with_capacity(nx * ny);
        let mut lon = Vec::with_capacity(nx * ny);
        for y in 0..ny {
            for x in 0..nx {
                lat.push(lat0 + y as f32 * dlat);
                lon.push(lon0 + x as f32 * dlon);
            }
        }
        LatLonGrid::new(GridShape::new(nx, ny).unwrap(), lat, lon).unwrap()
    }

    fn synthetic_hour(values: Vec<f32>) -> HrrrHourCache {
        let mut hour = HrrrHourCache::new(
            Utc.with_ymd_and_hms(2026, 5, 7, 18, 0, 0).unwrap(),
            "mock-hrrr",
        );
        hour.add_native_thermo_field(
            "cape",
            NativeThermoRecipe::Sbcape,
            "J/kg",
            regular_grid(3, 3, 0.0, 0.0, 1.0, 1.0),
            values,
        )
        .unwrap();
        hour
    }

    #[test]
    fn hrrr_selector_plan_contains_expected_field_ids() {
        let plan = plan_hrrr_hour_fields([
            "t2m", "d2m", "u10", "v10", "refc", "mslp", "terrain", "pwat", "cape", "cin",
        ])
        .unwrap();
        let ids = plan.fields.iter().map(|field| field.id).collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "t2m", "d2m", "u10", "v10", "refc", "mslp", "terrain", "pwat", "cape", "cin"
            ]
        );
        assert_eq!(plan.selectors.len(), 8);
        assert!(plan.unsupported_field_ids.is_empty());
        assert!(
            plan.selectors.iter().any(|(_, selector)| *selector
                == FieldSelector::surface(CanonicalField::GeopotentialHeight))
        );
        assert!(
            plan.selectors.iter().any(|(_, selector)| *selector
                == FieldSelector::height_agl(CanonicalField::Temperature, 2))
        );
        assert!(plan.selectors.iter().any(|(_, selector)| *selector
            == FieldSelector::entire_atmosphere(CanonicalField::PrecipitableWater)));
        assert!(
            plan.native_thermo
                .iter()
                .any(|(_, recipe)| *recipe == NativeThermoRecipe::Sbcape)
        );
        assert!(
            plan.native_thermo
                .iter()
                .any(|(_, recipe)| *recipe == NativeThermoRecipe::Sbcin)
        );
    }

    #[test]
    fn synthetic_nearest_remap_maps_known_grid_values() {
        let hour = synthetic_hour(vec![0.0, 1.0, 2.0, 10.0, 11.0, 12.0, 20.0, 21.0, 22.0]);
        let target = NativeDatasetTileGrid::new(
            "nearest",
            LatLonGrid::new(
                GridShape::new(2, 2).unwrap(),
                vec![0.1, 1.9, 1.1, 0.0],
                vec![0.1, 1.9, 0.9, 2.0],
            )
            .unwrap(),
        );
        let remap =
            precompute_tile_remap(hour.grid.as_ref().unwrap(), &target, RemapMethod::Nearest)
                .unwrap();
        let tile = remap_hrrr_hour_to_tile(&hour, &remap).unwrap();
        assert_eq!(
            tile.fields.get("cape").unwrap().values,
            vec![0.0, 22.0, 11.0, 2.0]
        );
    }

    #[test]
    fn synthetic_bilinear_remap_interpolates_regular_grid() {
        let hour = synthetic_hour(vec![
            0.0, 10.0, 20.0, 100.0, 110.0, 120.0, 200.0, 210.0, 220.0,
        ]);
        let target = NativeDatasetTileGrid::new(
            "bilinear",
            LatLonGrid::new(
                GridShape::new(2, 1).unwrap(),
                vec![0.5, 1.5],
                vec![0.5, 1.5],
            )
            .unwrap(),
        );
        let remap =
            precompute_tile_remap(hour.grid.as_ref().unwrap(), &target, RemapMethod::Bilinear)
                .unwrap();
        let tile = remap_hrrr_hour_to_tile(&hour, &remap).unwrap();
        let values = &tile.fields.get("cape").unwrap().values;
        assert!((values[0] - 55.0).abs() < 1.0e-4);
        assert!((values[1] - 165.0).abs() < 1.0e-4);
    }

    #[test]
    fn decoded_hour_cache_fans_out_to_multiple_tiles_without_redecode() {
        let hour = synthetic_hour(vec![0.0, 1.0, 2.0, 10.0, 11.0, 12.0, 20.0, 21.0, 22.0]);
        let source_grid = hour.grid.as_ref().unwrap();
        let tile_a = NativeDatasetTileGrid::new("a", regular_grid(1, 1, 0.0, 0.0, 1.0, 1.0));
        let tile_b = NativeDatasetTileGrid::new("b", regular_grid(1, 1, 2.0, 2.0, 1.0, 1.0));

        let remap_a = precompute_tile_remap(source_grid, &tile_a, RemapMethod::Nearest).unwrap();
        let remap_b = precompute_tile_remap(source_grid, &tile_b, RemapMethod::Nearest).unwrap();
        let out_a = remap_hrrr_hour_to_tile(&hour, &remap_a).unwrap();
        let out_b = remap_hrrr_hour_to_tile(&hour, &remap_b).unwrap();

        assert_eq!(hour.fields.len(), 1);
        assert_eq!(out_a.fields.get("cape").unwrap().values, vec![0.0]);
        assert_eq!(out_b.fields.get("cape").unwrap().values, vec![22.0]);
    }
}
