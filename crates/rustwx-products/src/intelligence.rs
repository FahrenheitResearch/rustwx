use crate::derived::compute_derived_query_field;
use crate::direct::load_direct_sampled_fields_from_latest;
use crate::gridded::{load_model_timestep_from_parts, resolve_model_run};
use crate::named_geometry::{NamedGeoBounds, NamedGeometryAsset, NamedGeometryKind};
use crate::publication::fetch_key;
use rustwx_core::{
    Field2D, FieldAreaSummary, FieldPointSample, FieldPointSampleMethod, GeoPoint, GeoPolygon,
    ModelId, ProductKey, SourceId,
};
use rustwx_models::LatestRun;
use rustwx_models::plot_recipe;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryFieldKind {
    DirectRecipe,
    DerivedRecipe,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRunDescriptor {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
}

impl QueryRunDescriptor {
    pub fn from_latest(latest: &LatestRun, forecast_hour: u16) -> Self {
        Self {
            model: latest.model,
            date_yyyymmdd: latest.cycle.date_yyyymmdd.clone(),
            cycle_utc: latest.cycle.hour_utc,
            forecast_hour,
            source: latest.source,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedQueryFieldMetadata {
    pub kind: QueryFieldKind,
    pub recipe_slug: String,
    pub title: String,
    pub units: String,
    pub run: QueryRunDescriptor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_selector: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_fetch_keys: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedQueryField {
    pub metadata: ResolvedQueryFieldMetadata,
    pub field: Field2D,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PointQueryResult {
    pub metadata: ResolvedQueryFieldMetadata,
    pub point: GeoPoint,
    pub sample: FieldPointSample,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AreaQueryResult {
    pub metadata: ResolvedQueryFieldMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub area: Option<NamedGeometryAsset>,
    pub bounds: NamedGeoBounds,
    pub summary: FieldAreaSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldComparisonSummary {
    pub compared_cell_count: usize,
    pub changed_cell_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_diff: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_diff: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_signed_diff: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_abs_diff: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rmse: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AreaComparisonResult {
    pub left: ResolvedQueryFieldMetadata,
    pub right: ResolvedQueryFieldMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub area: Option<NamedGeometryAsset>,
    pub bounds: NamedGeoBounds,
    pub left_summary: FieldAreaSummary,
    pub right_summary: FieldAreaSummary,
    pub delta: FieldComparisonSummary,
}

pub fn resolve_query_field(
    model: ModelId,
    date_yyyymmdd: &str,
    cycle_override_utc: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
    recipe_slug: &str,
    cache_root: &Path,
    use_cache: bool,
) -> Result<ResolvedQueryField, Box<dyn std::error::Error>> {
    if plot_recipe(recipe_slug).is_some() {
        return resolve_direct_query_field(
            model,
            date_yyyymmdd,
            cycle_override_utc,
            forecast_hour,
            source,
            recipe_slug,
            cache_root,
            use_cache,
        );
    }

    resolve_derived_query_field(
        model,
        date_yyyymmdd,
        cycle_override_utc,
        forecast_hour,
        source,
        recipe_slug,
        cache_root,
        use_cache,
    )
}

pub fn sample_query_field_point(
    field: &ResolvedQueryField,
    point: GeoPoint,
    method: FieldPointSampleMethod,
) -> PointQueryResult {
    PointQueryResult {
        metadata: field.metadata.clone(),
        point,
        sample: field.field.sample_point(point, method),
    }
}

pub fn summarize_query_field_bounds(
    field: &ResolvedQueryField,
    bounds: NamedGeoBounds,
    area: Option<NamedGeometryAsset>,
) -> AreaQueryResult {
    let polygon = bounds_polygon(bounds);
    AreaQueryResult {
        metadata: field.metadata.clone(),
        area,
        bounds,
        summary: field.field.summarize_polygon(&polygon),
    }
}

pub fn compare_query_fields_over_bounds(
    left: &ResolvedQueryField,
    right: &ResolvedQueryField,
    bounds: NamedGeoBounds,
    area: Option<NamedGeometryAsset>,
) -> Result<AreaComparisonResult, Box<dyn std::error::Error>> {
    if left.field.grid.shape != right.field.grid.shape {
        return Err("field comparison requires matching grid shapes".into());
    }

    let polygon = bounds_polygon(bounds);
    let delta = compare_field_values_within_polygon(&left.field, &right.field, &polygon);
    Ok(AreaComparisonResult {
        left: left.metadata.clone(),
        right: right.metadata.clone(),
        area,
        bounds,
        left_summary: left.field.summarize_polygon(&polygon),
        right_summary: right.field.summarize_polygon(&polygon),
        delta,
    })
}

pub fn bounds_from_named_asset(
    asset: &NamedGeometryAsset,
) -> Result<NamedGeoBounds, Box<dyn std::error::Error>> {
    match asset.kind {
        NamedGeometryKind::Country
        | NamedGeometryKind::Region
        | NamedGeometryKind::Metro
        | NamedGeometryKind::WatchArea => {}
        NamedGeometryKind::Route | NamedGeometryKind::Other => {
            return Err(format!("named asset '{}' does not carry bounds", asset.slug).into());
        }
    }
    asset
        .bounds_geometry()
        .ok_or_else(|| format!("named asset '{}' does not carry bounds", asset.slug).into())
}

pub fn bounds_polygon(bounds: NamedGeoBounds) -> GeoPolygon {
    GeoPolygon::new(
        vec![
            GeoPoint::new(bounds.south_deg, bounds.west_deg),
            GeoPoint::new(bounds.south_deg, bounds.east_deg),
            GeoPoint::new(bounds.north_deg, bounds.east_deg),
            GeoPoint::new(bounds.north_deg, bounds.west_deg),
            GeoPoint::new(bounds.south_deg, bounds.west_deg),
        ],
        Vec::new(),
    )
}

fn resolve_direct_query_field(
    model: ModelId,
    date_yyyymmdd: &str,
    cycle_override_utc: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
    recipe_slug: &str,
    cache_root: &Path,
    use_cache: bool,
) -> Result<ResolvedQueryField, Box<dyn std::error::Error>> {
    let latest = resolve_model_run(
        model,
        date_yyyymmdd,
        cycle_override_utc,
        forecast_hour,
        source,
    )?;
    let sampled = load_direct_sampled_fields_from_latest(
        &latest,
        forecast_hour,
        cache_root,
        use_cache,
        &[recipe_slug.to_string()],
    )?;
    let run = QueryRunDescriptor::from_latest(&latest, forecast_hour);
    if let Some(blocker) = sampled
        .blockers
        .iter()
        .find(|blocker| blocker.recipe_slug == recipe_slug)
    {
        return Err(format!(
            "direct query field '{}' is blocked: {}",
            recipe_slug, blocker.reason
        )
        .into());
    }
    let sampled_field = sampled
        .fields
        .into_iter()
        .find(|field| field.recipe_slug == recipe_slug)
        .ok_or_else(|| format!("direct query field '{}' did not resolve", recipe_slug))?;
    let title = plot_recipe(&sampled_field.recipe_slug)
        .map(|recipe| recipe.title.to_string())
        .unwrap_or_else(|| sampled_field.recipe_slug.clone());

    Ok(ResolvedQueryField {
        metadata: ResolvedQueryFieldMetadata {
            kind: QueryFieldKind::DirectRecipe,
            recipe_slug: sampled_field.recipe_slug.clone(),
            title,
            units: sampled_field.field.units.clone(),
            run,
            field_selector: sampled_field
                .field_selector
                .map(|selector| selector.to_string()),
            input_fetch_keys: sampled_field
                .input_fetches
                .iter()
                .map(|fetch| fetch.fetch_key.clone())
                .collect(),
        },
        field: sampled_field.field,
    })
}

fn resolve_derived_query_field(
    model: ModelId,
    date_yyyymmdd: &str,
    cycle_override_utc: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
    recipe_slug: &str,
    cache_root: &Path,
    use_cache: bool,
) -> Result<ResolvedQueryField, Box<dyn std::error::Error>> {
    let loaded = load_model_timestep_from_parts(
        model,
        date_yyyymmdd,
        cycle_override_utc,
        forecast_hour,
        source,
        None,
        None,
        cache_root,
        use_cache,
    )?;
    let query = compute_derived_query_field(
        &loaded.surface_decode.value,
        &loaded.pressure_decode.value,
        recipe_slug,
    )?;
    let grid = loaded.surface_decode.value.core_grid()?;
    let values = query.values.iter().map(|&value| value as f32).collect();
    let field = Field2D::new(
        ProductKey::named(query.recipe_slug.clone()),
        query.units.clone(),
        grid,
        values,
    )?;

    Ok(ResolvedQueryField {
        metadata: ResolvedQueryFieldMetadata {
            kind: QueryFieldKind::DerivedRecipe,
            recipe_slug: query.recipe_slug.clone(),
            title: query.title,
            units: query.units,
            run: QueryRunDescriptor::from_latest(&loaded.latest, forecast_hour),
            field_selector: None,
            input_fetch_keys: vec![
                fetch_key("surface", &loaded.surface_file.request.request),
                fetch_key("pressure", &loaded.pressure_file.request.request),
            ],
        },
        field,
    })
}

fn compare_field_values_within_polygon(
    left: &Field2D,
    right: &Field2D,
    polygon: &GeoPolygon,
) -> FieldComparisonSummary {
    let mut compared_cell_count = 0usize;
    let mut changed_cell_count = 0usize;
    let mut min_diff = f64::INFINITY;
    let mut max_diff = f64::NEG_INFINITY;
    let mut signed_sum = 0.0f64;
    let mut abs_sum = 0.0f64;
    let mut squared_sum = 0.0f64;

    for idx in 0..left.grid.shape.len() {
        let point = GeoPoint::new(left.grid.lat_deg[idx] as f64, left.grid.lon_deg[idx] as f64);
        if !polygon.contains(point) {
            continue;
        }
        let left_value = left.values[idx];
        let right_value = right.values[idx];
        if !left_value.is_finite() || !right_value.is_finite() {
            continue;
        }
        let diff = right_value as f64 - left_value as f64;
        compared_cell_count += 1;
        if diff.abs() > 1.0e-9 {
            changed_cell_count += 1;
        }
        min_diff = min_diff.min(diff);
        max_diff = max_diff.max(diff);
        signed_sum += diff;
        abs_sum += diff.abs();
        squared_sum += diff * diff;
    }

    if compared_cell_count == 0 {
        return FieldComparisonSummary {
            compared_cell_count,
            changed_cell_count,
            min_diff: None,
            max_diff: None,
            mean_signed_diff: None,
            mean_abs_diff: None,
            rmse: None,
        };
    }

    let count = compared_cell_count as f64;
    FieldComparisonSummary {
        compared_cell_count,
        changed_cell_count,
        min_diff: Some(min_diff),
        max_diff: Some(max_diff),
        mean_signed_diff: Some(signed_sum / count),
        mean_abs_diff: Some(abs_sum / count),
        rmse: Some((squared_sum / count).sqrt()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_core::{GeoPolygon, GridShape, LatLonGrid};

    fn sample_field(name: &str, values: Vec<f32>) -> Field2D {
        let grid = LatLonGrid::new(
            GridShape::new(2, 2).unwrap(),
            vec![35.0, 35.0, 36.0, 36.0],
            vec![-101.0, -100.0, -101.0, -100.0],
        )
        .unwrap();
        Field2D::new(ProductKey::named(name), "unitless", grid, values).unwrap()
    }

    #[test]
    fn bounds_polygon_creates_closed_rectangular_ring() {
        let polygon = bounds_polygon(NamedGeoBounds::new(-101.0, -100.0, 35.0, 36.0));
        assert_eq!(polygon.exterior.len(), 5);
        assert_eq!(polygon.exterior.first(), polygon.exterior.last());
    }

    #[test]
    fn comparison_summary_tracks_delta_statistics() {
        let left = sample_field("left", vec![1.0, 2.0, 3.0, 4.0]);
        let right = sample_field("right", vec![1.0, 4.0, 3.0, 8.0]);
        let polygon = GeoPolygon::new(
            vec![
                GeoPoint::new(34.0, -102.0),
                GeoPoint::new(34.0, -99.0),
                GeoPoint::new(37.0, -99.0),
                GeoPoint::new(37.0, -102.0),
                GeoPoint::new(34.0, -102.0),
            ],
            Vec::new(),
        );

        let summary = compare_field_values_within_polygon(&left, &right, &polygon);
        assert_eq!(summary.compared_cell_count, 4);
        assert_eq!(summary.changed_cell_count, 2);
        assert_eq!(summary.min_diff, Some(0.0));
        assert_eq!(summary.max_diff, Some(4.0));
        assert_eq!(summary.mean_signed_diff, Some(1.5));
        assert_eq!(summary.mean_abs_diff, Some(1.5));
    }
}
