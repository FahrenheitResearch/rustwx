//! wx_obs_extract: per-case bulk extraction for the visible-observation
//! pilot pipeline. One JSON request -> one JSON response, with each
//! requested field loaded once via rustwx and reused for every patch,
//! point, transect, and threshold summary.
//!
//! This is intentionally a thin wrapper over rustwx_products' public
//! `intelligence` API so the GRIB parsing, projection, and sampling all
//! stay on the Rust side. Python only orchestrates.
//!
//! Typical invocation:
//!
//!     wx_obs_extract.exe --request request.json --out response.json
//!
//! The request shape is documented at the top of `Request` below.

use clap::Parser;
use rustwx_core::{Field2D, FieldPointSampleMethod, GeoPoint, GeoPolygon, ModelId, SourceId};
use rustwx_products::intelligence::{
    ResolvedQueryField, resolve_query_field, sample_query_field_point,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(
    name = "wx_obs_extract",
    about = "Bulk per-case extraction (patches, points, transects, thresholds) using rustwx-products."
)]
struct Cli {
    #[arg(long)]
    request: PathBuf,
    #[arg(long)]
    out: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct Request {
    model: ModelId,
    date_yyyymmdd: String,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    cache_root: PathBuf,
    use_cache: bool,
    case_id: String,
    visible_case_id: String,
    /// Reference (T0) time string the caller used. Echoed back, never
    /// re-emitted to the visible side.
    reference_time_utc: String,
    fields: Vec<FieldRequest>,
    #[serde(default)]
    transects: Vec<TransectRequest>,
}

#[derive(Debug, Clone, Deserialize)]
struct FieldRequest {
    recipe_slug: String,
    /// Forecast hour override (defaults to outer forecast_hour).
    #[serde(default)]
    forecast_hour: Option<u16>,
    #[serde(default)]
    patches: Vec<PatchRequest>,
    #[serde(default)]
    points: Vec<PointRequest>,
    #[serde(default)]
    thresholds: Vec<ThresholdRequest>,
    /// If true, this field is also used to evaluate any transects in the
    /// outer transect list (the field becomes the transect product).
    #[serde(default)]
    use_for_transects: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct PatchRequest {
    patch_id: String,
    /// Polygon as list of [lat, lon] pairs (closed ring, last point may
    /// repeat the first). Lat first to match rustwx convention.
    polygon_lat_lon: Vec<[f64; 2]>,
    /// Optional downsampled grid resolution (e.g. 21 for 21x21 matrix).
    #[serde(default)]
    downsample_n: Option<usize>,
    /// Optional per-patch threshold definitions evaluated against ONLY
    /// the in-polygon cells (so the denominator is patch-local, not
    /// full grid).
    #[serde(default)]
    thresholds: Vec<ThresholdRequest>,
}

#[derive(Debug, Clone, Deserialize)]
struct PointRequest {
    point_id: String,
    lat: f64,
    lon: f64,
    /// Public masked label the policy will see ("nearest_metro_north", etc).
    /// Carried through verbatim so the orchestrator does not have to
    /// re-key on lat/lon.
    label: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ThresholdRequest {
    threshold_id: String,
    op: ThresholdOp,
    value: f64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ThresholdOp {
    Gte,
    Gt,
    Lte,
    Lt,
}

#[derive(Debug, Clone, Deserialize)]
struct TransectRequest {
    transect_id: String,
    start_lat: f64,
    start_lon: f64,
    end_lat: f64,
    end_lon: f64,
    sample_count: usize,
    /// Field recipe to evaluate the transect against. If omitted, every
    /// field with `use_for_transects: true` produces a sample list.
    #[serde(default)]
    recipe_slug: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct Response {
    schema_version: &'static str,
    case_id: String,
    visible_case_id: String,
    run: RunEcho,
    fields: Vec<FieldResult>,
    transects: Vec<TransectResult>,
    timing_ms: BTreeMap<String, u128>,
}

#[derive(Debug, Clone, Serialize)]
struct RunEcho {
    model: String,
    date_yyyymmdd: String,
    cycle_utc: u8,
    forecast_hour: u16,
    source: String,
}

#[derive(Debug, Clone, Serialize)]
struct FieldResult {
    recipe_slug: String,
    title: String,
    units: String,
    grid_shape: GridShape,
    valid_cell_count: usize,
    patches: Vec<PatchResult>,
    points: Vec<PointResult>,
    thresholds: Vec<ThresholdResult>,
}

#[derive(Debug, Clone, Serialize)]
struct GridShape {
    lat_count: usize,
    lon_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct PatchResult {
    patch_id: String,
    method: String,
    included_cell_count: usize,
    valid_cell_count: usize,
    missing_cell_count: usize,
    stats: BTreeMap<String, f64>,
    downsampled_matrix: Option<DownsampledMatrix>,
    patch_thresholds: Vec<ThresholdResult>,
}

#[derive(Debug, Clone, Serialize)]
struct DownsampledMatrix {
    n: usize,
    south_lat: f64,
    north_lat: f64,
    west_lon: f64,
    east_lon: f64,
    /// Row-major matrix of nearest-cell values (NaN for outside cells).
    values: Vec<Vec<Option<f32>>>,
}

#[derive(Debug, Clone, Serialize)]
struct PointResult {
    point_id: String,
    label: String,
    lat: f64,
    lon: f64,
    method: String,
    value: Option<f32>,
}

#[derive(Debug, Clone, Serialize)]
struct ThresholdResult {
    threshold_id: String,
    op: String,
    value: f64,
    matched_cell_count: usize,
    valid_cell_count: usize,
    fraction: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct TransectResult {
    transect_id: String,
    recipe_slug: String,
    start: [f64; 2],
    end: [f64; 2],
    sample_count: usize,
    samples: Vec<TransectSample>,
}

#[derive(Debug, Clone, Serialize)]
struct TransectSample {
    index: usize,
    fraction: f64,
    distance_km: f64,
    lat: f64,
    lon: f64,
    value: Option<f32>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("wx_obs_extract: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let request_text = fs::read_to_string(&cli.request)?;
    let request: Request = serde_json::from_str(&request_text)?;

    let mut timing_ms: BTreeMap<String, u128> = BTreeMap::new();
    let total_start = Instant::now();

    let mut field_results = Vec::with_capacity(request.fields.len());
    let mut transect_field_lookup: BTreeMap<String, ResolvedQueryField> = BTreeMap::new();

    for field_req in &request.fields {
        let fhour = field_req.forecast_hour.unwrap_or(request.forecast_hour);
        let load_start = Instant::now();
        let resolved = resolve_query_field(
            request.model,
            &request.date_yyyymmdd,
            Some(request.cycle_utc),
            fhour,
            request.source,
            &field_req.recipe_slug,
            &request.cache_root,
            request.use_cache,
        )?;
        timing_ms.insert(
            format!("load_field:{}", field_req.recipe_slug),
            load_start.elapsed().as_millis(),
        );

        let result = build_field_result(&resolved, field_req)?;
        if field_req.use_for_transects {
            transect_field_lookup.insert(field_req.recipe_slug.clone(), resolved);
        }
        field_results.push(result);
    }

    // Transects: evaluate each transect against every recipe in the
    // lookup table, or against the explicitly named recipe.
    let mut transect_results: Vec<TransectResult> = Vec::new();
    for tr in &request.transects {
        if let Some(slug) = tr.recipe_slug.as_deref() {
            if let Some(field) = transect_field_lookup.get(slug) {
                transect_results.push(evaluate_transect(field, tr, slug)?);
            } else {
                // Resolve on demand if not in lookup.
                let on_demand = resolve_query_field(
                    request.model,
                    &request.date_yyyymmdd,
                    Some(request.cycle_utc),
                    request.forecast_hour,
                    request.source,
                    slug,
                    &request.cache_root,
                    request.use_cache,
                )?;
                transect_results.push(evaluate_transect(&on_demand, tr, slug)?);
            }
        } else {
            for (slug, field) in &transect_field_lookup {
                let mut tr_clone = tr.clone();
                tr_clone.transect_id = format!("{}:{}", tr.transect_id, slug);
                transect_results.push(evaluate_transect(field, &tr_clone, slug)?);
            }
        }
    }

    timing_ms.insert("total".to_string(), total_start.elapsed().as_millis());

    let response = Response {
        schema_version: "wx.wx_obs_extract.v1",
        case_id: request.case_id.clone(),
        visible_case_id: request.visible_case_id.clone(),
        run: RunEcho {
            model: request.model.to_string(),
            date_yyyymmdd: request.date_yyyymmdd.clone(),
            cycle_utc: request.cycle_utc,
            forecast_hour: request.forecast_hour,
            source: request.source.to_string(),
        },
        fields: field_results,
        transects: transect_results,
        timing_ms,
    };
    let body = serde_json::to_string_pretty(&response)?;
    fs::write(&cli.out, body)?;
    Ok(())
}

fn build_field_result(
    resolved: &ResolvedQueryField,
    field_req: &FieldRequest,
) -> Result<FieldResult, Box<dyn std::error::Error>> {
    let field = &resolved.field;
    let mut valid_cell_count = 0usize;
    for v in &field.values {
        if v.is_finite() {
            valid_cell_count += 1;
        }
    }

    let mut patch_results = Vec::with_capacity(field_req.patches.len());
    for patch in &field_req.patches {
        patch_results.push(build_patch_result(field, patch));
    }

    let mut point_results = Vec::with_capacity(field_req.points.len());
    for point in &field_req.points {
        let geo = GeoPoint::new(point.lat, point.lon);
        let sample =
            sample_query_field_point(resolved, geo, FieldPointSampleMethod::InverseDistance4);
        point_results.push(PointResult {
            point_id: point.point_id.clone(),
            label: point.label.clone(),
            lat: point.lat,
            lon: point.lon,
            method: format!("{:?}", sample.sample.method),
            value: sample.sample.value,
        });
    }

    let mut threshold_results = Vec::with_capacity(field_req.thresholds.len());
    for thr in &field_req.thresholds {
        threshold_results.push(build_threshold_result(field, thr, valid_cell_count));
    }

    Ok(FieldResult {
        recipe_slug: resolved.metadata.recipe_slug.clone(),
        title: resolved.metadata.title.clone(),
        units: resolved.metadata.units.clone(),
        grid_shape: GridShape {
            lat_count: field.grid.shape.ny,
            lon_count: field.grid.shape.nx,
        },
        valid_cell_count,
        patches: patch_results,
        points: point_results,
        thresholds: threshold_results,
    })
}

fn polygon_from_lat_lon(points: &[[f64; 2]]) -> GeoPolygon {
    let pts: Vec<GeoPoint> = points.iter().map(|p| GeoPoint::new(p[0], p[1])).collect();
    GeoPolygon::new(pts, Vec::new())
}

fn build_patch_result(field: &Field2D, patch: &PatchRequest) -> PatchResult {
    let polygon = polygon_from_lat_lon(&patch.polygon_lat_lon);
    let bounds = polygon.bounds();
    let mut included = 0usize;
    let mut valid = 0usize;
    let mut missing = 0usize;
    let mut values: Vec<f32> = Vec::new();
    for idx in 0..field.grid.shape.len() {
        let pt = GeoPoint::new(
            field.grid.lat_deg[idx] as f64,
            field.grid.lon_deg[idx] as f64,
        );
        let in_bounds = bounds.map(|b| b.contains(pt)).unwrap_or(true);
        if !in_bounds {
            continue;
        }
        if !polygon.contains(pt) {
            continue;
        }
        included += 1;
        let v = field.values[idx];
        if v.is_finite() {
            valid += 1;
            values.push(v);
        } else {
            missing += 1;
        }
    }

    let mut patch_thresholds: Vec<ThresholdResult> = Vec::with_capacity(patch.thresholds.len());
    for thr in &patch.thresholds {
        let mut matched = 0usize;
        for v in &values {
            let v = *v as f64;
            let hit = match thr.op {
                ThresholdOp::Gte => v >= thr.value,
                ThresholdOp::Gt => v > thr.value,
                ThresholdOp::Lte => v <= thr.value,
                ThresholdOp::Lt => v < thr.value,
            };
            if hit {
                matched += 1;
            }
        }
        let fraction = if valid > 0 {
            Some(matched as f64 / valid as f64)
        } else {
            None
        };
        patch_thresholds.push(ThresholdResult {
            threshold_id: thr.threshold_id.clone(),
            op: format!("{:?}", thr.op).to_lowercase(),
            value: thr.value,
            matched_cell_count: matched,
            valid_cell_count: valid,
            fraction,
        });
    }

    let stats = compute_stats(&mut values);
    let downsampled = patch
        .downsample_n
        .map(|n| build_downsampled_matrix(field, &polygon, n));
    PatchResult {
        patch_id: patch.patch_id.clone(),
        method: "cell_centers_within_polygon".to_string(),
        included_cell_count: included,
        valid_cell_count: valid,
        missing_cell_count: missing,
        stats,
        downsampled_matrix: downsampled,
        patch_thresholds,
    }
}

fn compute_stats(values: &mut [f32]) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    if values.is_empty() {
        return out;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = values.len();
    let sum: f64 = values.iter().map(|v| *v as f64).sum();
    let mean = sum / n as f64;
    out.insert("count".to_string(), n as f64);
    out.insert("min".to_string(), values[0] as f64);
    out.insert("max".to_string(), values[n - 1] as f64);
    out.insert("mean".to_string(), mean);
    for (k, q) in [
        ("p10", 0.10),
        ("p25", 0.25),
        ("p50", 0.50),
        ("p75", 0.75),
        ("p90", 0.90),
    ] {
        out.insert(k.to_string(), percentile(values, q));
    }
    out
}

fn percentile(sorted: &[f32], q: f64) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return f64::NAN;
    }
    if n == 1 {
        return sorted[0] as f64;
    }
    let pos = q * (n as f64 - 1.0);
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        return sorted[lo] as f64;
    }
    let frac = pos - lo as f64;
    let lo_v = sorted[lo] as f64;
    let hi_v = sorted[hi] as f64;
    lo_v + (hi_v - lo_v) * frac
}

fn build_downsampled_matrix(field: &Field2D, polygon: &GeoPolygon, n: usize) -> DownsampledMatrix {
    let bounds = polygon.bounds();
    let (south, north, west, east) = match bounds {
        Some(b) => (
            b.south_lat_deg,
            b.north_lat_deg,
            b.west_lon_deg,
            b.east_lon_deg,
        ),
        None => (0.0, 0.0, 0.0, 0.0),
    };
    let mut values: Vec<Vec<Option<f32>>> = vec![vec![None; n]; n];
    for i in 0..n {
        let frac_lat = if n == 1 {
            0.5
        } else {
            i as f64 / (n - 1) as f64
        };
        let lat = south + (north - south) * frac_lat;
        for j in 0..n {
            let frac_lon = if n == 1 {
                0.5
            } else {
                j as f64 / (n - 1) as f64
            };
            let lon = west + (east - west) * frac_lon;
            let geo = GeoPoint::new(lat, lon);
            if !polygon.contains(geo) {
                continue;
            }
            let sample = field.sample_point(geo, FieldPointSampleMethod::Nearest);
            values[i][j] = sample.value;
        }
    }
    DownsampledMatrix {
        n,
        south_lat: south,
        north_lat: north,
        west_lon: west,
        east_lon: east,
        values,
    }
}

fn build_threshold_result(
    field: &Field2D,
    thr: &ThresholdRequest,
    valid_cell_count: usize,
) -> ThresholdResult {
    let mut matched = 0usize;
    for v in &field.values {
        if !v.is_finite() {
            continue;
        }
        let v = *v as f64;
        let hit = match thr.op {
            ThresholdOp::Gte => v >= thr.value,
            ThresholdOp::Gt => v > thr.value,
            ThresholdOp::Lte => v <= thr.value,
            ThresholdOp::Lt => v < thr.value,
        };
        if hit {
            matched += 1;
        }
    }
    let fraction = if valid_cell_count > 0 {
        Some(matched as f64 / valid_cell_count as f64)
    } else {
        None
    };
    ThresholdResult {
        threshold_id: thr.threshold_id.clone(),
        op: format!("{:?}", thr.op).to_lowercase(),
        value: thr.value,
        matched_cell_count: matched,
        valid_cell_count,
        fraction,
    }
}

fn evaluate_transect(
    resolved: &ResolvedQueryField,
    tr: &TransectRequest,
    slug: &str,
) -> Result<TransectResult, Box<dyn std::error::Error>> {
    let n = tr.sample_count.max(2);
    let mut samples = Vec::with_capacity(n);
    for i in 0..n {
        let frac = i as f64 / (n - 1) as f64;
        let lat = tr.start_lat + (tr.end_lat - tr.start_lat) * frac;
        let lon = tr.start_lon + (tr.end_lon - tr.start_lon) * frac;
        let geo = GeoPoint::new(lat, lon);
        let sample =
            sample_query_field_point(resolved, geo, FieldPointSampleMethod::InverseDistance4);
        let distance_km = great_circle_km(tr.start_lat, tr.start_lon, lat, lon);
        samples.push(TransectSample {
            index: i,
            fraction: frac,
            distance_km,
            lat,
            lon,
            value: sample.sample.value,
        });
    }
    Ok(TransectResult {
        transect_id: tr.transect_id.clone(),
        recipe_slug: slug.to_string(),
        start: [tr.start_lat, tr.start_lon],
        end: [tr.end_lat, tr.end_lon],
        sample_count: n,
        samples,
    })
}

fn great_circle_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r_earth_km = 6371.0088_f64;
    let to_rad = std::f64::consts::PI / 180.0;
    let dlat = (lat2 - lat1) * to_rad;
    let dlon = (lon2 - lon1) * to_rad;
    let a = (dlat / 2.0).sin().powi(2)
        + (lat1 * to_rad).cos() * (lat2 * to_rad).cos() * (dlon / 2.0).sin().powi(2);
    2.0 * r_earth_km * a.sqrt().atan2((1.0 - a).sqrt())
}
