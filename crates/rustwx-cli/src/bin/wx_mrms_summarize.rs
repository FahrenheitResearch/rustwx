//! wx_mrms_summarize: bulk per-case MRMS QPE / PrecipRate summary.
//!
//! Reads gzipped MRMS GRIB2 files via `grib-core`, summarizes the
//! values inside each requested polygon, and emits a per-file
//! timeseries of stats and threshold counts.
//!
//! Request shape:
//! {
//!   "case_id": "...",
//!   "products": [
//!     {
//!       "product_id": "RadarOnly_QPE_01H",
//!       "files": ["...gz", "..."]
//!     }
//!   ],
//!   "patches": [
//!     {
//!       "patch_id": "patch_focus_60km_box",
//!       "polygon_lat_lon": [[lat,lon],...]
//!     }
//!   ],
//!   "thresholds": [
//!     {"threshold_id":"qpe1h_ge_1in","op":"gte","value_mm":25.4}
//!   ],
//!   "reference_time_utc": "2022-07-26T08:00:00Z"
//! }

use clap::Parser;
use flate2::read::GzDecoder;
use grib_core::grib2::{Grib2File, unpack_message_normalized};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(
    name = "wx_mrms_summarize",
    about = "Per-case MRMS QPE polygon summary"
)]
struct Cli {
    #[arg(long)]
    request: PathBuf,
    #[arg(long)]
    out: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct Request {
    case_id: String,
    visible_case_id: String,
    reference_time_utc: String,
    products: Vec<ProductRequest>,
    patches: Vec<PatchRequest>,
    #[serde(default)]
    thresholds: Vec<ThresholdRequest>,
    #[serde(default)]
    point_overlap: Option<PointOverlapRequest>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProductRequest {
    product_id: String,
    files: Vec<PathBuf>,
    #[serde(default)]
    /// MRMS scale factor: stored values are in mm; some products use
    /// scale=10 internally but unpack_message_normalized already applies
    /// scale/offset, so this is left as 1.0 unless overridden.
    scale_to_mm: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct PatchRequest {
    patch_id: String,
    polygon_lat_lon: Vec<[f64; 2]>,
}

#[derive(Debug, Clone, Deserialize)]
struct ThresholdRequest {
    threshold_id: String,
    op: ThresholdOp,
    value_mm: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct PointOverlapRequest {
    points_csv: PathBuf,
    masks: Vec<PointMaskRequest>,
}

#[derive(Debug, Clone, Deserialize)]
struct PointMaskRequest {
    mask_id: String,
    field: String,
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

#[derive(Debug, Clone, Serialize)]
struct Response {
    schema_version: &'static str,
    case_id: String,
    visible_case_id: String,
    reference_time_utc: String,
    products: Vec<ProductResult>,
    timing_ms: BTreeMap<String, u128>,
}

#[derive(Debug, Clone, Serialize)]
struct ProductResult {
    product_id: String,
    file_count: usize,
    parsed_count: usize,
    failed_count: usize,
    samples: Vec<FileSample>,
}

#[derive(Debug, Clone, Serialize)]
struct FileSample {
    file_path: String,
    file_basename: String,
    valid_time_utc: Option<String>,
    valid_time_label: Option<String>,
    grid_shape: Option<[u32; 2]>,
    bbox_deg: Option<[f64; 4]>,
    parameter: Option<MrmsParameter>,
    patches: Vec<PatchSample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    point_overlap: Option<PointOverlapSample>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct MrmsParameter {
    discipline: u8,
    parameter_category: u8,
    parameter_number: u8,
    forecast_time: u32,
    level_type: u8,
    level_value: f64,
}

#[derive(Debug, Clone, Serialize)]
struct PatchSample {
    patch_id: String,
    included_cell_count: usize,
    valid_cell_count: usize,
    missing_cell_count: usize,
    stats: BTreeMap<String, f64>,
    thresholds: Vec<ThresholdResult>,
}

#[derive(Debug, Clone, Serialize)]
struct ThresholdResult {
    threshold_id: String,
    op: String,
    value_mm: f64,
    matched_cell_count: usize,
    valid_cell_count: usize,
    fraction: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    component_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    largest_component_cell_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    largest_component_fraction: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct PointOverlapSample {
    points_csv: String,
    point_count: usize,
    valid_mrms_point_count: usize,
    masks: Vec<PointMaskResult>,
}

#[derive(Debug, Clone, Serialize)]
struct PointMaskResult {
    mask_id: String,
    field: String,
    op: String,
    value: f64,
    mask_point_count: usize,
    mask_valid_mrms_point_count: usize,
    thresholds: Vec<PointOverlapThresholdResult>,
}

#[derive(Debug, Clone, Serialize)]
struct PointOverlapThresholdResult {
    threshold_id: String,
    value_mm: f64,
    mrms_hit_total_point_count: usize,
    mrms_hit_inside_mask_count: usize,
    mrms_hit_outside_mask_count: usize,
    mask_mrms_miss_count: usize,
    fraction_mask_with_mrms_hit: Option<f64>,
    fraction_mrms_hit_inside_mask: Option<f64>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("wx_mrms_summarize: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let req: Request = serde_json::from_slice(&std::fs::read(&cli.request)?)?;
    let mut timing: BTreeMap<String, u128> = BTreeMap::new();
    let total_start = Instant::now();

    let ref_dt = parse_iso_utc(&req.reference_time_utc)?;

    // Pre-compute polygon bounding boxes to skip cells outside.
    let polygons: Vec<Polygon> = req
        .patches
        .iter()
        .map(|p| Polygon::from_lat_lon(&p.polygon_lat_lon))
        .collect();

    let mut product_results = Vec::with_capacity(req.products.len());
    for prod in &req.products {
        let prod_start = Instant::now();
        let scale = prod.scale_to_mm.unwrap_or(1.0);
        let mut samples = Vec::with_capacity(prod.files.len());
        let mut parsed = 0usize;
        let mut failed = 0usize;
        for file_path in &prod.files {
            let file_basename = file_path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            let result = process_file(
                file_path,
                scale,
                &req.patches,
                &polygons,
                &req.thresholds,
                req.point_overlap.as_ref(),
                &ref_dt,
            );
            match result {
                Ok(sample) => {
                    parsed += 1;
                    samples.push(sample);
                }
                Err(err) => {
                    failed += 1;
                    samples.push(FileSample {
                        file_path: file_path.to_string_lossy().to_string(),
                        file_basename,
                        valid_time_utc: None,
                        valid_time_label: None,
                        grid_shape: None,
                        bbox_deg: None,
                        parameter: None,
                        patches: Vec::new(),
                        point_overlap: None,
                        error: Some(format!("{err}")),
                    });
                }
            }
        }
        product_results.push(ProductResult {
            product_id: prod.product_id.clone(),
            file_count: prod.files.len(),
            parsed_count: parsed,
            failed_count: failed,
            samples,
        });
        timing.insert(
            format!("product:{}", prod.product_id),
            prod_start.elapsed().as_millis(),
        );
    }

    timing.insert("total".to_string(), total_start.elapsed().as_millis());
    let resp = Response {
        schema_version: "wx.wx_mrms_summarize.v1",
        case_id: req.case_id.clone(),
        visible_case_id: req.visible_case_id.clone(),
        reference_time_utc: req.reference_time_utc.clone(),
        products: product_results,
        timing_ms: timing,
    };
    std::fs::write(&cli.out, serde_json::to_string_pretty(&resp)?)?;
    Ok(())
}

fn process_file(
    path: &std::path::Path,
    scale: f64,
    patch_reqs: &[PatchRequest],
    polygons: &[Polygon],
    thresholds: &[ThresholdRequest],
    point_overlap_req: Option<&PointOverlapRequest>,
    ref_dt: &DateTimeParts,
) -> Result<FileSample, Box<dyn std::error::Error>> {
    let basename = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let bytes = read_maybe_gz(path)?;
    let parsed = Grib2File::from_bytes(&bytes)?;
    if parsed.messages.is_empty() {
        return Err("no messages in GRIB file".into());
    }
    // Use the first message; MRMS QPE files contain a single message.
    let msg = &parsed.messages[0];
    let values = unpack_message_normalized(msg)?;
    let nx = msg.grid.nx as usize;
    let ny = msg.grid.ny as usize;
    if values.len() != nx * ny {
        return Err(format!(
            "value count {} mismatch with grid {}x{} = {}",
            values.len(),
            nx,
            ny,
            nx * ny
        )
        .into());
    }

    // MRMS regular lat/lon grid: lat1 = north edge, lat2 = south edge,
    // dy is positive (degrees per row); dx is positive degrees per col.
    // Scan mode 0x00 means rows go N->S; check sign of dy.
    let lat1 = msg.grid.lat1;
    let lat2 = msg.grid.lat2;
    let lon1 = msg.grid.lon1;
    let lon2 = msg.grid.lon2;
    let mut west = if lon1 > 180.0 { lon1 - 360.0 } else { lon1 };
    let mut east = if lon2 > 180.0 { lon2 - 360.0 } else { lon2 };
    if east < west {
        // Wrap: e.g. lon1=230, lon2=300 => west=-130, east=-60 already handled
        std::mem::swap(&mut west, &mut east);
    }
    let south = lat1.min(lat2);
    let north = lat1.max(lat2);
    let dy = (lat1 - lat2).abs() / (ny as f64 - 1.0).max(1.0);
    let dx = (lon2 - lon1).abs() / (nx as f64 - 1.0).max(1.0);
    let row_top_to_bottom = lat1 > lat2;

    let mut patch_samples = Vec::with_capacity(patch_reqs.len());
    for (pi, patch) in patch_reqs.iter().enumerate() {
        let polygon = &polygons[pi];
        let bb = polygon.bounds();
        // Compute pixel index range to scan.
        let bb_south = bb.0;
        let bb_north = bb.1;
        let bb_west = bb.2;
        let bb_east = bb.3;
        if bb_north < south || bb_south > north || bb_east < west || bb_west > east {
            patch_samples.push(PatchSample {
                patch_id: patch.patch_id.clone(),
                included_cell_count: 0,
                valid_cell_count: 0,
                missing_cell_count: 0,
                stats: BTreeMap::new(),
                thresholds: Vec::new(),
            });
            continue;
        }
        let row_lo: usize;
        let row_hi: usize;
        if row_top_to_bottom {
            row_lo = (((lat1 - bb_north) / dy).max(0.0)).floor() as usize;
            row_hi = (((lat1 - bb_south) / dy).max(0.0)).ceil() as usize;
        } else {
            row_lo = (((bb_south - lat1) / dy).max(0.0)).floor() as usize;
            row_hi = (((bb_north - lat1) / dy).max(0.0)).ceil() as usize;
        }
        let row_lo = row_lo.min(ny - 1);
        let row_hi = row_hi.min(ny - 1);
        let col_lo = (((bb_west - west) / dx).max(0.0)).floor() as usize;
        let col_hi = (((bb_east - west) / dx).max(0.0)).ceil() as usize;
        let col_lo = col_lo.min(nx - 1);
        let col_hi = col_hi.min(nx - 1);

        let mut included = 0usize;
        let mut valid = 0usize;
        let mut missing = 0usize;
        let local_cols = col_hi - col_lo + 1;
        let local_rows = row_hi - row_lo + 1;
        let mut keep: Vec<f64> = Vec::new();
        let mut valid_cells: Vec<(usize, usize, f64)> = Vec::new();
        for r in row_lo..=row_hi {
            let lat = if row_top_to_bottom {
                lat1 - (r as f64) * dy
            } else {
                lat1 + (r as f64) * dy
            };
            for c in col_lo..=col_hi {
                let lon = west + (c as f64) * dx;
                if !polygon.contains(lat, lon) {
                    continue;
                }
                let idx = r * nx + c;
                included += 1;
                let v = values[idx];
                if v.is_finite() && v > -990.0 && v < 1.0e6 {
                    valid += 1;
                    let scaled = v * scale;
                    keep.push(scaled);
                    valid_cells.push((r - row_lo, c - col_lo, scaled));
                } else {
                    missing += 1;
                }
            }
        }
        let stats = compute_stats(&mut keep);
        let mut thr_results = Vec::with_capacity(thresholds.len());
        for thr in thresholds {
            let mut matched = 0usize;
            for v in &keep {
                let hit = threshold_hit(*v, thr);
                if hit {
                    matched += 1;
                }
            }
            let fraction = if valid > 0 {
                Some(matched as f64 / valid as f64)
            } else {
                None
            };
            let (component_count, largest_component_cell_count) =
                threshold_component_stats(&valid_cells, local_rows, local_cols, thr);
            thr_results.push(ThresholdResult {
                threshold_id: thr.threshold_id.clone(),
                op: format!("{:?}", thr.op).to_lowercase(),
                value_mm: thr.value_mm,
                matched_cell_count: matched,
                valid_cell_count: valid,
                fraction,
                component_count: Some(component_count),
                largest_component_cell_count: Some(largest_component_cell_count),
                largest_component_fraction: if matched > 0 {
                    Some(largest_component_cell_count as f64 / matched as f64)
                } else {
                    None
                },
            });
        }
        patch_samples.push(PatchSample {
            patch_id: patch.patch_id.clone(),
            included_cell_count: included,
            valid_cell_count: valid,
            missing_cell_count: missing,
            stats,
            thresholds: thr_results,
        });
    }

    let point_overlap = if let Some(req) = point_overlap_req {
        Some(compute_point_overlap(
            req,
            thresholds,
            &values,
            nx,
            ny,
            lat1,
            west,
            dx,
            dy,
            row_top_to_bottom,
            scale,
        )?)
    } else {
        None
    };

    let valid_dt = compute_valid_time(msg);
    let valid_iso = valid_dt.as_ref().map(|dt| dt.to_iso());
    let valid_label = valid_dt
        .as_ref()
        .map(|dt| dt.relative_label_minutes(ref_dt));

    Ok(FileSample {
        file_path: path.to_string_lossy().to_string(),
        file_basename: basename,
        valid_time_utc: valid_iso,
        valid_time_label: valid_label,
        grid_shape: Some([msg.grid.nx, msg.grid.ny]),
        bbox_deg: Some([south, north, west, east]),
        parameter: Some(MrmsParameter {
            discipline: msg.discipline,
            parameter_category: msg.product.parameter_category,
            parameter_number: msg.product.parameter_number,
            forecast_time: msg.product.forecast_time,
            level_type: msg.product.level_type,
            level_value: msg.product.level_value,
        }),
        patches: patch_samples,
        point_overlap,
        error: None,
    })
}

fn threshold_hit(value: f64, threshold: &ThresholdRequest) -> bool {
    match threshold.op {
        ThresholdOp::Gte => value >= threshold.value_mm,
        ThresholdOp::Gt => value > threshold.value_mm,
        ThresholdOp::Lte => value <= threshold.value_mm,
        ThresholdOp::Lt => value < threshold.value_mm,
    }
}

fn point_mask_hit(value: f64, mask: &PointMaskRequest) -> bool {
    if !value.is_finite() {
        return false;
    }
    match mask.op {
        ThresholdOp::Gte => value >= mask.value,
        ThresholdOp::Gt => value > mask.value,
        ThresholdOp::Lte => value <= mask.value,
        ThresholdOp::Lt => value < mask.value,
    }
}

#[derive(Debug, Clone)]
struct PointMaskAccum {
    mask_point_count: usize,
    mask_valid_mrms_point_count: usize,
    mrms_hit_inside_mask_count: Vec<usize>,
    mask_mrms_miss_count: Vec<usize>,
}

fn compute_point_overlap(
    req: &PointOverlapRequest,
    thresholds: &[ThresholdRequest],
    values: &[f64],
    nx: usize,
    ny: usize,
    lat1: f64,
    west: f64,
    dx: f64,
    dy: f64,
    row_top_to_bottom: bool,
    scale: f64,
) -> Result<PointOverlapSample, Box<dyn std::error::Error>> {
    let file = File::open(&req.points_csv)?;
    let mut reader = BufReader::new(file);
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let header_cols: Vec<String> = header
        .trim_end_matches(['\r', '\n'])
        .split(',')
        .map(|s| s.to_string())
        .collect();
    let lat_idx = header_cols
        .iter()
        .position(|name| name == "lat")
        .ok_or("point overlap CSV is missing lat column")?;
    let lon_idx = header_cols
        .iter()
        .position(|name| name == "lon")
        .ok_or("point overlap CSV is missing lon column")?;
    let mut field_indices = Vec::with_capacity(req.masks.len());
    for mask in &req.masks {
        let idx = header_cols
            .iter()
            .position(|name| name == &mask.field)
            .ok_or_else(|| format!("point overlap CSV is missing field {}", mask.field))?;
        field_indices.push(idx);
    }

    let mut accums = vec![
        PointMaskAccum {
            mask_point_count: 0,
            mask_valid_mrms_point_count: 0,
            mrms_hit_inside_mask_count: vec![0; thresholds.len()],
            mask_mrms_miss_count: vec![0; thresholds.len()],
        };
        req.masks.len()
    ];
    let mut point_count = 0usize;
    let mut valid_mrms_point_count = 0usize;
    let mut mrms_hit_total_point_count = vec![0usize; thresholds.len()];

    let mut line = String::new();
    while reader.read_line(&mut line)? > 0 {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            line.clear();
            continue;
        }
        let cols: Vec<&str> = trimmed.split(',').collect();
        point_count += 1;
        let lat = cols
            .get(lat_idx)
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(f64::NAN);
        let lon = cols
            .get(lon_idx)
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(f64::NAN);
        let mrms_value = sample_mrms_nearest(
            values,
            nx,
            ny,
            lat,
            lon,
            lat1,
            west,
            dx,
            dy,
            row_top_to_bottom,
        )
        .map(|v| v * scale);
        let mut threshold_hits = vec![false; thresholds.len()];
        if let Some(value) = mrms_value {
            valid_mrms_point_count += 1;
            for (ti, threshold) in thresholds.iter().enumerate() {
                let hit = threshold_hit(value, threshold);
                threshold_hits[ti] = hit;
                if hit {
                    mrms_hit_total_point_count[ti] += 1;
                }
            }
        }

        for (mi, mask) in req.masks.iter().enumerate() {
            let field_value = cols
                .get(field_indices[mi])
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(f64::NAN);
            if !point_mask_hit(field_value, mask) {
                continue;
            }
            let accum = &mut accums[mi];
            accum.mask_point_count += 1;
            if mrms_value.is_some() {
                accum.mask_valid_mrms_point_count += 1;
                for (ti, hit) in threshold_hits.iter().enumerate() {
                    if *hit {
                        accum.mrms_hit_inside_mask_count[ti] += 1;
                    } else {
                        accum.mask_mrms_miss_count[ti] += 1;
                    }
                }
            }
        }
        line.clear();
    }

    let masks = req
        .masks
        .iter()
        .zip(accums.iter())
        .map(|(mask, accum)| {
            let thresholds_out = thresholds
                .iter()
                .enumerate()
                .map(|(ti, threshold)| {
                    let inside = accum.mrms_hit_inside_mask_count[ti];
                    let total_hit = mrms_hit_total_point_count[ti];
                    PointOverlapThresholdResult {
                        threshold_id: threshold.threshold_id.clone(),
                        value_mm: threshold.value_mm,
                        mrms_hit_total_point_count: total_hit,
                        mrms_hit_inside_mask_count: inside,
                        mrms_hit_outside_mask_count: total_hit.saturating_sub(inside),
                        mask_mrms_miss_count: accum.mask_mrms_miss_count[ti],
                        fraction_mask_with_mrms_hit: if accum.mask_valid_mrms_point_count > 0 {
                            Some(inside as f64 / accum.mask_valid_mrms_point_count as f64)
                        } else {
                            None
                        },
                        fraction_mrms_hit_inside_mask: if total_hit > 0 {
                            Some(inside as f64 / total_hit as f64)
                        } else {
                            None
                        },
                    }
                })
                .collect();
            PointMaskResult {
                mask_id: mask.mask_id.clone(),
                field: mask.field.clone(),
                op: format!("{:?}", mask.op).to_lowercase(),
                value: mask.value,
                mask_point_count: accum.mask_point_count,
                mask_valid_mrms_point_count: accum.mask_valid_mrms_point_count,
                thresholds: thresholds_out,
            }
        })
        .collect();

    Ok(PointOverlapSample {
        points_csv: req.points_csv.to_string_lossy().to_string(),
        point_count,
        valid_mrms_point_count,
        masks,
    })
}

fn sample_mrms_nearest(
    values: &[f64],
    nx: usize,
    ny: usize,
    lat: f64,
    lon: f64,
    lat1: f64,
    west: f64,
    dx: f64,
    dy: f64,
    row_top_to_bottom: bool,
) -> Option<f64> {
    if !lat.is_finite() || !lon.is_finite() || dx <= 0.0 || dy <= 0.0 {
        return None;
    }
    let row_f = if row_top_to_bottom {
        (lat1 - lat) / dy
    } else {
        (lat - lat1) / dy
    };
    let col_f = (lon - west) / dx;
    let row = row_f.round() as isize;
    let col = col_f.round() as isize;
    if row < 0 || col < 0 || row >= ny as isize || col >= nx as isize {
        return None;
    }
    let value = values[row as usize * nx + col as usize];
    if value.is_finite() && value > -990.0 && value < 1.0e6 {
        Some(value)
    } else {
        None
    }
}

fn threshold_component_stats(
    valid_cells: &[(usize, usize, f64)],
    rows: usize,
    cols: usize,
    threshold: &ThresholdRequest,
) -> (usize, usize) {
    if rows == 0 || cols == 0 || valid_cells.is_empty() {
        return (0, 0);
    }
    let mut mask = vec![false; rows * cols];
    for &(r, c, value) in valid_cells {
        if r < rows && c < cols && threshold_hit(value, threshold) {
            mask[r * cols + c] = true;
        }
    }
    let mut seen = vec![false; rows * cols];
    let mut component_count = 0usize;
    let mut largest = 0usize;
    let mut queue = VecDeque::new();
    for idx in 0..mask.len() {
        if !mask[idx] || seen[idx] {
            continue;
        }
        component_count += 1;
        seen[idx] = true;
        queue.clear();
        queue.push_back(idx);
        let mut size = 0usize;
        while let Some(cur) = queue.pop_front() {
            size += 1;
            let r = cur / cols;
            let c = cur % cols;
            let neighbors = [
                (r.wrapping_sub(1), c, r > 0),
                (r + 1, c, r + 1 < rows),
                (r, c.wrapping_sub(1), c > 0),
                (r, c + 1, c + 1 < cols),
            ];
            for (nr, nc, valid) in neighbors {
                if !valid {
                    continue;
                }
                let next = nr * cols + nc;
                if mask[next] && !seen[next] {
                    seen[next] = true;
                    queue.push_back(next);
                }
            }
        }
        largest = largest.max(size);
    }
    (component_count, largest)
}

fn read_maybe_gz(path: &std::path::Path) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut f = File::open(path)?;
    let mut head = [0u8; 2];
    let n = f.read(&mut head)?;
    let mut full = Vec::with_capacity(2_000_000);
    full.extend_from_slice(&head[..n]);
    let mut rest = Vec::new();
    f.read_to_end(&mut rest)?;
    full.extend_from_slice(&rest);
    if n >= 2 && head[0] == 0x1f && head[1] == 0x8b {
        let mut decoder = GzDecoder::new(&full[..]);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out)?;
        Ok(out)
    } else {
        Ok(full)
    }
}

#[derive(Debug, Clone, Copy)]
struct DateTimeParts {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
}

impl DateTimeParts {
    fn to_iso(&self) -> String {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )
    }

    fn to_unix_seconds(self) -> i64 {
        // Convert to days since Unix epoch then to seconds. Approximate
        // using a Gregorian-calendar Julian-day computation.
        let a = (14 - self.month as i64) / 12;
        let y = self.year as i64 + 4800 - a;
        let m = self.month as i64 + 12 * a - 3;
        let jdn = self.day as i64 + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
        let days_since_epoch = jdn - 2440588;
        days_since_epoch * 86_400
            + self.hour as i64 * 3600
            + self.minute as i64 * 60
            + self.second as i64
    }

    fn relative_label_minutes(&self, ref_dt: &DateTimeParts) -> String {
        let dt_s = self.to_unix_seconds();
        let ref_s = ref_dt.to_unix_seconds();
        let delta_min = ((dt_s - ref_s) as f64 / 60.0).round() as i64;
        if delta_min == 0 {
            "T0".to_string()
        } else if delta_min < 0 {
            format!("T_minus_{}min", -delta_min)
        } else {
            format!("T_plus_{}min", delta_min)
        }
    }
}

fn parse_iso_utc(s: &str) -> Result<DateTimeParts, Box<dyn std::error::Error>> {
    let s = s.trim();
    let s = s.trim_end_matches('Z');
    if s.len() < 19 {
        return Err(format!("invalid iso datetime: {s}").into());
    }
    let year: i32 = s[0..4].parse()?;
    let month: u32 = s[5..7].parse()?;
    let day: u32 = s[8..10].parse()?;
    let hour: u32 = s[11..13].parse()?;
    let minute: u32 = s[14..16].parse()?;
    let second: u32 = s[17..19].parse()?;
    Ok(DateTimeParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
    })
}

fn compute_valid_time(msg: &grib_core::grib2::Grib2Message) -> Option<DateTimeParts> {
    use chrono::Datelike;
    use chrono::Timelike;
    let ref_naive = msg.reference_time;
    // forecast_time in time_range_unit; for MRMS analysis it's typically 0.
    let unit = msg.product.time_range_unit;
    let t = msg.product.forecast_time;
    let secs: i64 = match unit {
        0 => t as i64 * 60,     // minute
        1 => t as i64 * 3600,   // hour
        2 => t as i64 * 86_400, // day
        13 => t as i64,         // second
        _ => 0,
    };
    let dt = ref_naive + chrono::Duration::seconds(secs);
    Some(DateTimeParts {
        year: dt.year(),
        month: dt.month(),
        day: dt.day(),
        hour: dt.hour(),
        minute: dt.minute(),
        second: dt.second(),
    })
}

#[derive(Debug, Clone)]
struct Polygon {
    pts: Vec<(f64, f64)>, // (lat, lon)
}

impl Polygon {
    fn from_lat_lon(pts: &[[f64; 2]]) -> Self {
        Self {
            pts: pts.iter().map(|p| (p[0], p[1])).collect(),
        }
    }

    /// (south, north, west, east)
    fn bounds(&self) -> (f64, f64, f64, f64) {
        let mut south = f64::INFINITY;
        let mut north = f64::NEG_INFINITY;
        let mut west = f64::INFINITY;
        let mut east = f64::NEG_INFINITY;
        for (lat, lon) in &self.pts {
            south = south.min(*lat);
            north = north.max(*lat);
            west = west.min(*lon);
            east = east.max(*lon);
        }
        (south, north, west, east)
    }

    /// Standard ray-casting test (point-in-polygon) for closed polygons
    /// in the lat/lon plane. Sufficient for tens-of-km patches at
    /// mid-latitudes; not intended for polygons that cross the IDL.
    fn contains(&self, lat: f64, lon: f64) -> bool {
        let n = self.pts.len();
        if n < 3 {
            return false;
        }
        let mut inside = false;
        let mut j = n - 1;
        for i in 0..n {
            let (lat_i, lon_i) = self.pts[i];
            let (lat_j, lon_j) = self.pts[j];
            let intersect = ((lat_i > lat) != (lat_j > lat))
                && (lon < (lon_j - lon_i) * (lat - lat_i) / (lat_j - lat_i + f64::EPSILON) + lon_i);
            if intersect {
                inside = !inside;
            }
            j = i;
        }
        inside
    }
}

fn compute_stats(values: &mut [f64]) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    if values.is_empty() {
        return out;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = values.len();
    let sum: f64 = values.iter().sum();
    let mean = sum / n as f64;
    out.insert("count".to_string(), n as f64);
    out.insert("min".to_string(), values[0]);
    out.insert("max".to_string(), values[n - 1]);
    out.insert("mean".to_string(), mean);
    for (k, q) in [
        ("p10", 0.10),
        ("p25", 0.25),
        ("p50", 0.50),
        ("p75", 0.75),
        ("p90", 0.90),
        ("p99", 0.99),
    ] {
        out.insert(k.to_string(), percentile(values, q));
    }
    out
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return f64::NAN;
    }
    if n == 1 {
        return sorted[0];
    }
    let pos = q * (n as f64 - 1.0);
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    let frac = pos - lo as f64;
    sorted[lo] + (sorted[hi] - sorted[lo]) * frac
}
