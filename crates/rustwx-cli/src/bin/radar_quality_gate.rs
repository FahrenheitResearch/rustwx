use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::Serialize;
use serde_json::Value;

const RADAR_POLAR_SIDECAR_SCHEMA: &str = "rustwx.radar.polar_sidecar.v2";
const REQUIRED_GATE_FLAG_MEANINGS: &[(&str, u64)] = &[
    ("valid", 0b0000_0001),
    ("missing", 0b0000_0010),
    ("range_folded", 0b0000_0100),
    ("filtered", 0b0000_1000),
    ("derived", 0b0001_0000),
    ("dealiased", 0b0010_0000),
];

#[derive(Debug, Parser)]
#[command(
    name = "radar_quality_gate",
    about = "Validate rustwx radar tile manifests against visual/QC quality gates"
)]
struct Cli {
    /// Tile or all-tilts manifest JSON path. Repeat for multiple manifests.
    #[arg(long, required = true)]
    manifest: Vec<PathBuf>,

    /// Fail reflectivity manifests that remove more than this gate fraction.
    #[arg(long)]
    max_reflectivity_removed_fraction: Option<f64>,

    /// Fail velocity manifests above this fold-like jump fraction.
    #[arg(long)]
    max_velocity_fold_fraction: Option<f64>,

    /// Fail velocity manifests above this severe jump count.
    #[arg(long)]
    max_velocity_severe_jumps: Option<u64>,

    /// Fail velocity manifests above this maximum absolute neighbor jump.
    #[arg(long)]
    max_velocity_max_jump_ms: Option<f64>,

    /// Fail manifests whose selected product has fewer finite gates than this.
    #[arg(long)]
    min_product_finite_gates: Option<u64>,

    /// Fail manifests whose selected product minimum value is below this.
    #[arg(long)]
    min_product_min_value: Option<f64>,

    /// Fail manifests whose selected product maximum value is below this.
    #[arg(long)]
    min_product_max_value: Option<f64>,

    /// Fail manifests whose selected product maximum value is above this.
    #[arg(long)]
    max_product_max_value: Option<f64>,

    /// Fail manifests whose product provenance source does not match this value.
    #[arg(long)]
    require_product_source: Option<String>,

    /// Fail manifests whose product provenance inputs do not include this value.
    #[arg(long)]
    require_product_input: Vec<String>,

    /// Fail manifests whose product provenance method does not match this value.
    #[arg(long)]
    require_product_method: Option<String>,

    /// Fail manifests that do not have a valid numeric polar sidecar record.
    #[arg(long)]
    require_numeric_sidecar: bool,

    /// Fail numeric sidecar manifests whose value_meanings do not include this name, label, or value.
    #[arg(long)]
    require_sidecar_value_meaning: Vec<String>,

    /// Fail manifests that hard-clip pixels to the requested tile-selection bounds.
    #[arg(long)]
    require_unclipped_bounds: bool,

    /// Optional JSON summary path.
    #[arg(long)]
    summary_out: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
struct QualityThresholds {
    max_reflectivity_removed_fraction: Option<f64>,
    max_velocity_fold_fraction: Option<f64>,
    max_velocity_severe_jumps: Option<u64>,
    max_velocity_max_jump_ms: Option<f64>,
    min_product_finite_gates: Option<u64>,
    min_product_min_value: Option<f64>,
    min_product_max_value: Option<f64>,
    max_product_max_value: Option<f64>,
    require_product_source: Option<String>,
    require_product_input: Vec<String>,
    require_product_method: Option<String>,
    require_numeric_sidecar: bool,
    require_sidecar_value_meaning: Vec<String>,
    require_unclipped_bounds: bool,
}

#[derive(Debug, Serialize)]
struct QualitySummary {
    ok: bool,
    thresholds: QualityThresholds,
    manifests: Vec<ManifestSummary>,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ManifestSummary {
    path: String,
    ok: bool,
    entries: Vec<EntrySummary>,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct EntrySummary {
    label: String,
    product: Option<String>,
    product_qc: Option<ProductSummary>,
    product_provenance: Option<ProductProvenanceSummary>,
    numeric_sidecar: Option<NumericSidecarSummary>,
    bounds: Option<[f64; 4]>,
    clip_to_bounds: Option<bool>,
    sampling_bounds: Option<[f64; 4]>,
    reflectivity_qc: Option<ReflectivitySummary>,
    velocity_qc: Option<VelocitySummary>,
    dealias_qc: Option<DealiasSummary>,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ProductSummary {
    product: String,
    finite_gate_count: u64,
    min_value: f64,
    max_value: f64,
    mean_value: f64,
}

#[derive(Debug, Serialize)]
struct ProductProvenanceSummary {
    source: String,
    derived: bool,
    inputs: Vec<String>,
    method: Option<String>,
}

#[derive(Debug, Serialize)]
struct NumericSidecarSummary {
    schema: String,
    manifest_path: String,
    values_path: String,
    gate_flags_path: String,
    radial_count: u64,
    max_gate_count: u64,
    gate_count: u64,
    processing_state: String,
}

#[derive(Debug, Serialize)]
struct ReflectivitySummary {
    finite_gate_count: u64,
    removed_gate_count: u64,
    removed_gate_fraction: f64,
}

#[derive(Debug, Serialize)]
struct VelocitySummary {
    nyquist_ms: Option<f64>,
    finite_gate_count: u64,
    fold_like_jump_count: u64,
    severe_jump_count: u64,
    fold_like_jump_fraction: f64,
    max_abs_jump_ms: f64,
}

#[derive(Debug, Serialize)]
struct DealiasSummary {
    attempted: bool,
    accepted: bool,
    forced: bool,
    decision: String,
    changed_gate_count: u64,
    original_fold_like_jumps: Option<u64>,
    original_severe_jumps: Option<u64>,
    candidate_fold_like_jumps: Option<u64>,
    candidate_severe_jumps: Option<u64>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let thresholds = QualityThresholds {
        max_reflectivity_removed_fraction: cli.max_reflectivity_removed_fraction,
        max_velocity_fold_fraction: cli.max_velocity_fold_fraction,
        max_velocity_severe_jumps: cli.max_velocity_severe_jumps,
        max_velocity_max_jump_ms: cli.max_velocity_max_jump_ms,
        min_product_finite_gates: cli.min_product_finite_gates,
        min_product_min_value: cli.min_product_min_value,
        min_product_max_value: cli.min_product_max_value,
        max_product_max_value: cli.max_product_max_value,
        require_product_source: cli.require_product_source,
        require_product_input: cli.require_product_input,
        require_product_method: cli.require_product_method,
        require_numeric_sidecar: cli.require_numeric_sidecar,
        require_sidecar_value_meaning: cli.require_sidecar_value_meaning,
        require_unclipped_bounds: cli.require_unclipped_bounds,
    };
    let summary = evaluate_manifest_paths(&cli.manifest, thresholds)?;
    let text = serde_json::to_string_pretty(&summary)?;

    if let Some(path) = cli.summary_out.as_ref() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        fs::write(path, text.as_bytes()).with_context(|| format!("write {}", path.display()))?;
    }

    println!("{text}");
    if !summary.ok {
        bail!(
            "radar quality gate failed with {} failure(s)",
            summary.failures.len()
        );
    }
    Ok(())
}

fn evaluate_manifest_paths(
    paths: &[PathBuf],
    thresholds: QualityThresholds,
) -> Result<QualitySummary> {
    let mut manifests = Vec::with_capacity(paths.len());
    let mut failures = Vec::new();
    for path in paths {
        let value: Value = serde_json::from_slice(
            &fs::read(path).with_context(|| format!("read {}", path.display()))?,
        )
        .with_context(|| format!("parse {}", path.display()))?;
        let summary = evaluate_manifest_value(path, &value, &thresholds)
            .with_context(|| format!("evaluate {}", path.display()))?;
        failures.extend(summary.failures.iter().cloned());
        manifests.push(summary);
    }

    Ok(QualitySummary {
        ok: failures.is_empty(),
        thresholds,
        manifests,
        failures,
    })
}

fn evaluate_manifest_value(
    path: &Path,
    value: &Value,
    thresholds: &QualityThresholds,
) -> Result<ManifestSummary> {
    let manifest_values = manifest_entries(value);
    let mut entries = Vec::with_capacity(manifest_values.len());
    let mut failures = Vec::new();

    if value
        .get("ok")
        .and_then(Value::as_bool)
        .is_some_and(|ok| !ok)
    {
        failures.push(format!("{} has ok=false", path.display()));
    }
    if manifest_values.is_empty() {
        failures.push(format!("{} has no manifest entries", path.display()));
    }

    for (index, entry) in manifest_values.iter().enumerate() {
        let summary = evaluate_entry(
            index,
            entry,
            thresholds,
            path.parent().unwrap_or_else(|| Path::new(".")),
        )?;
        failures.extend(
            summary
                .failures
                .iter()
                .map(|failure| format!("{}: {failure}", path.display())),
        );
        entries.push(summary);
    }

    Ok(ManifestSummary {
        path: path.display().to_string(),
        ok: failures.is_empty(),
        entries,
        failures,
    })
}

fn manifest_entries(value: &Value) -> Vec<&Value> {
    value
        .get("manifests")
        .and_then(Value::as_array)
        .map(|manifests| manifests.iter().collect())
        .unwrap_or_else(|| vec![value])
}

fn evaluate_entry(
    index: usize,
    value: &Value,
    thresholds: &QualityThresholds,
    manifest_dir: &Path,
) -> Result<EntrySummary> {
    let label = entry_label(index, value);
    let product = value
        .get("product")
        .and_then(Value::as_str)
        .map(str::to_string);
    let product_qc = parse_product_qc(value)?;
    let product_provenance = parse_product_provenance(value)?;
    let numeric_sidecar = parse_numeric_sidecar(value)?;
    let bounds = parse_bounds(value, "bounds")?;
    let sampling_bounds = parse_bounds(value, "sampling_bounds")?;
    let clip_to_bounds = value.get("clip_to_bounds").and_then(Value::as_bool);
    let reflectivity_qc = parse_reflectivity_qc(value)?;
    let velocity_qc = parse_velocity_qc(value)?;
    let dealias_qc = parse_dealias_qc(value)?;
    let mut failures = Vec::new();

    if thresholds.require_unclipped_bounds {
        if clip_to_bounds != Some(false) {
            failures.push(format!(
                "{label} has clip_to_bounds={:?}; expected false",
                clip_to_bounds
            ));
        }
        match (bounds, sampling_bounds) {
            (Some(tile_bounds), Some(sample_bounds)) => {
                if !bounds_contains(sample_bounds, tile_bounds) {
                    failures.push(format!(
                        "{label} sampling_bounds {:?} do not cover tile bounds {:?}",
                        sample_bounds, tile_bounds
                    ));
                }
            }
            (None, _) => failures.push(format!("{label} is missing bounds")),
            (_, None) => failures.push(format!("{label} is missing sampling_bounds")),
        }
    }

    if thresholds.require_numeric_sidecar {
        match numeric_sidecar.as_ref() {
            Some(sidecar) => failures.extend(validate_numeric_sidecar(
                &label,
                sidecar,
                manifest_dir,
                &thresholds.require_sidecar_value_meaning,
            )?),
            None => failures.push(format!("{label} missing numeric sidecar")),
        }
    } else if !thresholds.require_sidecar_value_meaning.is_empty() {
        failures.push(format!(
            "{label} cannot require sidecar value meanings without --require-numeric-sidecar"
        ));
    }

    if thresholds.require_product_source.is_some()
        || !thresholds.require_product_input.is_empty()
        || thresholds.require_product_method.is_some()
    {
        match product_provenance.as_ref() {
            Some(provenance) => {
                if let Some(required) = thresholds.require_product_source.as_ref() {
                    if provenance.source != *required {
                        failures.push(format!(
                            "{label} product provenance source {} does not match {}",
                            provenance.source, required
                        ));
                    }
                }
                for required in &thresholds.require_product_input {
                    if !provenance.inputs.iter().any(|input| input == required) {
                        failures.push(format!(
                            "{label} product provenance inputs {:?} do not include {}",
                            provenance.inputs, required
                        ));
                    }
                }
                if let Some(required) = thresholds.require_product_method.as_ref() {
                    if provenance.method.as_deref() != Some(required.as_str()) {
                        failures.push(format!(
                            "{label} product provenance method {:?} does not match {}",
                            provenance.method, required
                        ));
                    }
                }
            }
            None => failures.push(format!("{label} missing product provenance")),
        }
    }

    if let Some(qc) = product_qc.as_ref() {
        if let Some(min_finite) = thresholds.min_product_finite_gates {
            if qc.finite_gate_count < min_finite {
                failures.push(format!(
                    "{label} product finite gates {} below {}",
                    qc.finite_gate_count, min_finite
                ));
            }
        }
        if let Some(min_value) = thresholds.min_product_min_value {
            if qc.min_value < min_value {
                failures.push(format!(
                    "{label} product min value {:.4} below {:.4}",
                    qc.min_value, min_value
                ));
            }
        }
        if let Some(min_max_value) = thresholds.min_product_max_value {
            if qc.max_value < min_max_value {
                failures.push(format!(
                    "{label} product max value {:.4} below {:.4}",
                    qc.max_value, min_max_value
                ));
            }
        }
        if let Some(max_value) = thresholds.max_product_max_value {
            if qc.max_value > max_value {
                failures.push(format!(
                    "{label} product max value {:.4} exceeds {:.4}",
                    qc.max_value, max_value
                ));
            }
        }
    }

    if let Some(qc) = reflectivity_qc.as_ref() {
        if let Some(max_removed) = thresholds.max_reflectivity_removed_fraction {
            if qc.removed_gate_fraction > max_removed {
                failures.push(format!(
                    "{label} reflectivity removed fraction {:.4} exceeds {:.4}",
                    qc.removed_gate_fraction, max_removed
                ));
            }
        }
    }

    if let Some(qc) = velocity_qc.as_ref() {
        if let Some(max_fraction) = thresholds.max_velocity_fold_fraction {
            if qc.fold_like_jump_fraction > max_fraction {
                failures.push(format!(
                    "{label} velocity fold-like jump fraction {:.4} exceeds {:.4}",
                    qc.fold_like_jump_fraction, max_fraction
                ));
            }
        }
        if let Some(max_severe) = thresholds.max_velocity_severe_jumps {
            if qc.severe_jump_count > max_severe {
                failures.push(format!(
                    "{label} velocity severe jumps {} exceed {}",
                    qc.severe_jump_count, max_severe
                ));
            }
        }
        if let Some(max_jump) = thresholds.max_velocity_max_jump_ms {
            if qc.max_abs_jump_ms > max_jump {
                failures.push(format!(
                    "{label} velocity max jump {:.2} exceeds {:.2}",
                    qc.max_abs_jump_ms, max_jump
                ));
            }
        }
    }

    if let Some(qc) = dealias_qc.as_ref() {
        if qc.forced {
            failures.push(format!("{label} dealias output was forced"));
        }
        if qc.accepted {
            match (
                qc.original_fold_like_jumps,
                qc.original_severe_jumps,
                qc.candidate_fold_like_jumps,
                qc.candidate_severe_jumps,
            ) {
                (
                    Some(original_fold),
                    Some(original_severe),
                    Some(candidate_fold),
                    Some(candidate_severe),
                ) => {
                    if candidate_fold > original_fold {
                        failures.push(format!(
                            "{label} accepted dealias increased fold-like jumps from {original_fold} to {candidate_fold}"
                        ));
                    }
                    if candidate_severe > original_severe {
                        failures.push(format!(
                            "{label} accepted dealias increased severe jumps from {original_severe} to {candidate_severe}"
                        ));
                    }
                }
                _ => failures.push(format!(
                    "{label} accepted dealias is missing original/candidate continuity scores"
                )),
            }
        }
    }

    Ok(EntrySummary {
        label,
        product,
        product_qc,
        product_provenance,
        numeric_sidecar,
        bounds,
        clip_to_bounds,
        sampling_bounds,
        reflectivity_qc,
        velocity_qc,
        dealias_qc,
        failures,
    })
}

fn entry_label(index: usize, value: &Value) -> String {
    if let Some(name) = value.get("name").and_then(Value::as_str) {
        return name.to_string();
    }
    let product = value
        .get("product")
        .and_then(Value::as_str)
        .unwrap_or("radar");
    let sweep = value.get("sweep_index").and_then(Value::as_u64);
    let elevation = value.get("elevation_deg").and_then(Value::as_f64);
    match (sweep, elevation) {
        (Some(sweep), Some(elevation)) => format!("{product}_sweep{sweep}_el{elevation:.2}"),
        _ => format!("entry#{index}"),
    }
}

fn parse_product_qc(value: &Value) -> Result<Option<ProductSummary>> {
    let Some(qc) = non_null_object(value, "product_qc") else {
        return Ok(None);
    };
    Ok(Some(ProductSummary {
        product: qc
            .get("product")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        finite_gate_count: required_u64(qc, "finite_gate_count")?,
        min_value: required_f64(qc, "min_value")?,
        max_value: required_f64(qc, "max_value")?,
        mean_value: required_f64(qc, "mean_value")?,
    }))
}

fn parse_product_provenance(value: &Value) -> Result<Option<ProductProvenanceSummary>> {
    let Some(provenance) = non_null_object(value, "product_provenance") else {
        return Ok(None);
    };
    let inputs = provenance
        .get("inputs")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(Some(ProductProvenanceSummary {
        source: provenance
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        derived: provenance
            .get("derived")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        inputs,
        method: provenance
            .get("method")
            .and_then(Value::as_str)
            .map(str::to_string),
    }))
}

fn parse_numeric_sidecar(value: &Value) -> Result<Option<NumericSidecarSummary>> {
    let Some(sidecar) = non_null_object(value, "numeric_sidecar") else {
        return Ok(None);
    };
    Ok(Some(NumericSidecarSummary {
        schema: required_str(sidecar, "schema")?.to_string(),
        manifest_path: required_str(sidecar, "manifest_path")?.to_string(),
        values_path: required_str(sidecar, "values_path")?.to_string(),
        gate_flags_path: required_str(sidecar, "gate_flags_path")?.to_string(),
        radial_count: required_u64(sidecar, "radial_count")?,
        max_gate_count: required_u64(sidecar, "max_gate_count")?,
        gate_count: required_u64(sidecar, "gate_count")?,
        processing_state: required_str(sidecar, "processing_state")?.to_string(),
    }))
}

fn parse_bounds(value: &Value, field: &str) -> Result<Option<[f64; 4]>> {
    let Some(values) = value.get(field) else {
        return Ok(None);
    };
    let Some(values) = values.as_array() else {
        bail!("{field} must be an array");
    };
    if values.len() != 4 {
        bail!("{field} must contain west,south,east,north");
    }
    Ok(Some([
        values[0]
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("{field}[0] must be a number"))?,
        values[1]
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("{field}[1] must be a number"))?,
        values[2]
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("{field}[2] must be a number"))?,
        values[3]
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("{field}[3] must be a number"))?,
    ]))
}

fn bounds_contains(outer: [f64; 4], inner: [f64; 4]) -> bool {
    const EPS: f64 = 1e-9;
    outer[0] <= inner[0] + EPS
        && outer[1] <= inner[1] + EPS
        && outer[2] + EPS >= inner[2]
        && outer[3] + EPS >= inner[3]
}

fn validate_numeric_sidecar(
    label: &str,
    sidecar: &NumericSidecarSummary,
    manifest_dir: &Path,
    required_value_meanings: &[String],
) -> Result<Vec<String>> {
    let mut failures = Vec::new();
    if sidecar.schema != RADAR_POLAR_SIDECAR_SCHEMA {
        failures.push(format!(
            "{label} numeric sidecar schema {} does not match {}",
            sidecar.schema, RADAR_POLAR_SIDECAR_SCHEMA
        ));
    }
    if sidecar.radial_count == 0 || sidecar.max_gate_count == 0 || sidecar.gate_count == 0 {
        failures.push(format!("{label} numeric sidecar has empty gate geometry"));
    }
    let expected_cells = sidecar
        .radial_count
        .checked_mul(sidecar.max_gate_count)
        .unwrap_or(0);
    let expected_value_bytes = expected_cells.checked_mul(4).unwrap_or(0);

    let manifest_path = resolve_record_path(manifest_dir, &sidecar.manifest_path);
    let values_path = resolve_record_path(manifest_dir, &sidecar.values_path);
    let flags_path = resolve_record_path(manifest_dir, &sidecar.gate_flags_path);
    for (field, path) in [
        ("manifest_path", &manifest_path),
        ("values_path", &values_path),
        ("gate_flags_path", &flags_path),
    ] {
        if !path.is_file() {
            failures.push(format!(
                "{label} numeric sidecar {field} does not exist: {}",
                path.display()
            ));
        }
    }
    if values_path.is_file() {
        let actual = fs::metadata(&values_path)
            .with_context(|| format!("stat {}", values_path.display()))?
            .len();
        if actual != expected_value_bytes {
            failures.push(format!(
                "{label} numeric sidecar values byte length {actual} does not match expected {expected_value_bytes}"
            ));
        }
    }
    if flags_path.is_file() {
        let actual = fs::metadata(&flags_path)
            .with_context(|| format!("stat {}", flags_path.display()))?
            .len();
        if actual != expected_cells {
            failures.push(format!(
                "{label} numeric sidecar gate flag byte length {actual} does not match expected {expected_cells}"
            ));
        }
    }

    if manifest_path.is_file() {
        let manifest: Value = serde_json::from_slice(
            &fs::read(&manifest_path)
                .with_context(|| format!("read {}", manifest_path.display()))?,
        )
        .with_context(|| format!("parse {}", manifest_path.display()))?;
        if manifest.get("schema").and_then(Value::as_str) != Some(RADAR_POLAR_SIDECAR_SCHEMA) {
            failures.push(format!(
                "{label} numeric sidecar manifest has unsupported schema"
            ));
        }
        failures.extend(validate_numeric_sidecar_manifest_fields(
            label,
            sidecar,
            &manifest,
            required_value_meanings,
        ));
        if manifest.get("radial_count").and_then(Value::as_u64) != Some(sidecar.radial_count) {
            failures.push(format!("{label} numeric sidecar radial_count mismatch"));
        }
        if manifest.get("max_gate_count").and_then(Value::as_u64) != Some(sidecar.max_gate_count) {
            failures.push(format!("{label} numeric sidecar max_gate_count mismatch"));
        }
        if manifest
            .pointer("/site/elevation_m")
            .and_then(Value::as_f64)
            .is_none()
        {
            failures.push(format!(
                "{label} numeric sidecar is missing site elevation_m"
            ));
        }
    }

    Ok(failures)
}

fn validate_numeric_sidecar_manifest_fields(
    label: &str,
    sidecar: &NumericSidecarSummary,
    manifest: &Value,
    required_value_meanings: &[String],
) -> Vec<String> {
    let mut failures = Vec::new();
    if manifest.get("sidecar_version").and_then(Value::as_u64) != Some(2) {
        failures.push(format!(
            "{label} numeric sidecar manifest must declare sidecar_version=2"
        ));
    }
    if manifest.get("ok").and_then(Value::as_bool) != Some(true) {
        failures.push(format!("{label} numeric sidecar manifest is not ok"));
    }
    for field in [
        "product",
        "product_name",
        "units",
        "scan_time_utc",
        "processing_state",
        "values_encoding",
        "gate_flags_encoding",
    ] {
        if !manifest
            .get(field)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
        {
            failures.push(format!(
                "{label} numeric sidecar manifest is missing string field {field}"
            ));
        }
    }
    if manifest.get("values_encoding").and_then(Value::as_str)
        != Some("f32_le_row_major_radial_gate_nan_missing")
    {
        failures.push(format!(
            "{label} numeric sidecar values_encoding is not the v2 f32 row-major encoding"
        ));
    }
    if manifest.get("gate_flags_encoding").and_then(Value::as_str)
        != Some("u8_bitmask_row_major_radial_gate")
    {
        failures.push(format!(
            "{label} numeric sidecar gate_flags_encoding is not the v2 u8 mask encoding"
        ));
    }
    match manifest.get("gate_flag_meanings").and_then(Value::as_array) {
        Some(meanings) => {
            for (name, mask) in REQUIRED_GATE_FLAG_MEANINGS {
                let present = meanings.iter().any(|meaning| {
                    meaning.get("name").and_then(Value::as_str) == Some(*name)
                        && meaning.get("mask").and_then(Value::as_u64) == Some(*mask)
                        && meaning
                            .get("description")
                            .and_then(Value::as_str)
                            .is_some_and(|description| !description.trim().is_empty())
                });
                if !present {
                    failures.push(format!(
                        "{label} numeric sidecar gate_flag_meanings missing {name} mask {mask}"
                    ));
                }
            }
        }
        None => failures.push(format!(
            "{label} numeric sidecar manifest is missing gate_flag_meanings"
        )),
    }
    failures.extend(validate_sidecar_value_meanings(
        label,
        manifest,
        required_value_meanings,
    ));
    if !manifest
        .get("source_key_or_url")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
    {
        failures.push(format!(
            "{label} numeric sidecar manifest is missing source_key_or_url"
        ));
    }
    if !manifest
        .get("product_provenance")
        .is_some_and(Value::is_object)
    {
        failures.push(format!(
            "{label} numeric sidecar manifest is missing product_provenance"
        ));
    }
    for field in ["lat", "lon", "elevation_m"] {
        if manifest
            .pointer(&format!("/site/{field}"))
            .and_then(Value::as_f64)
            .is_none()
        {
            failures.push(format!(
                "{label} numeric sidecar manifest is missing site {field}"
            ));
        }
    }
    for field in [
        "sweep_index",
        "radial_count",
        "max_gate_count",
        "gate_count",
    ] {
        if manifest.get(field).and_then(Value::as_u64).is_none() {
            failures.push(format!(
                "{label} numeric sidecar manifest is missing numeric field {field}"
            ));
        }
    }
    if manifest
        .get("elevation_deg")
        .and_then(Value::as_f64)
        .is_none()
    {
        failures.push(format!(
            "{label} numeric sidecar manifest is missing elevation_deg"
        ));
    }
    if !manifest.get("nyquist_velocity_ms").is_some() {
        failures.push(format!(
            "{label} numeric sidecar manifest is missing nyquist_velocity_ms"
        ));
    }

    let Some(radials) = manifest.get("radials").and_then(Value::as_array) else {
        failures.push(format!(
            "{label} numeric sidecar manifest is missing radials array"
        ));
        return failures;
    };
    if radials.len() as u64 != sidecar.radial_count {
        failures.push(format!(
            "{label} numeric sidecar manifest radial array length {} does not match {}",
            radials.len(),
            sidecar.radial_count
        ));
    }

    let mut summed_gate_count = 0u64;
    for (index, radial) in radials.iter().enumerate() {
        let prefix = format!("{label} numeric sidecar radial[{index}]");
        for field in [
            "radial_index",
            "gate_count",
            "first_gate_range_m",
            "gate_spacing_m",
        ] {
            if radial.get(field).and_then(Value::as_u64).is_none() {
                failures.push(format!("{prefix} is missing numeric field {field}"));
            }
        }
        for field in ["azimuth_deg", "elevation_deg", "azimuth_spacing_deg"] {
            if radial.get(field).and_then(Value::as_f64).is_none() {
                failures.push(format!("{prefix} is missing numeric field {field}"));
            }
        }
        for field in [
            "scale",
            "offset",
            "nyquist_velocity_ms",
            "data_word_size_bits",
        ] {
            if !radial.get(field).is_some() {
                failures.push(format!("{prefix} is missing field {field}"));
            }
        }
        if let Some(gate_count) = radial.get("gate_count").and_then(Value::as_u64) {
            summed_gate_count = summed_gate_count.saturating_add(gate_count);
            if gate_count == 0 {
                failures.push(format!("{prefix} has zero gate_count"));
            }
        }
        if radial
            .get("gate_spacing_m")
            .and_then(Value::as_u64)
            .is_some_and(|spacing| spacing == 0)
        {
            failures.push(format!("{prefix} has zero gate_spacing_m"));
        }
    }
    if summed_gate_count != sidecar.gate_count {
        failures.push(format!(
            "{label} numeric sidecar manifest radial gate sum {summed_gate_count} does not match {}",
            sidecar.gate_count
        ));
    }

    failures
}

fn validate_sidecar_value_meanings(
    label: &str,
    manifest: &Value,
    required_value_meanings: &[String],
) -> Vec<String> {
    let mut failures = Vec::new();
    let meanings = manifest.get("value_meanings");
    let meanings = match meanings {
        Some(value) if value.is_null() => None,
        Some(value) => match value.as_array() {
            Some(meanings) => Some(meanings),
            None => {
                failures.push(format!(
                    "{label} numeric sidecar value_meanings must be an array"
                ));
                None
            }
        },
        None => None,
    };

    if let Some(meanings) = meanings {
        for (index, meaning) in meanings.iter().enumerate() {
            let prefix = format!("{label} numeric sidecar value_meanings[{index}]");
            if !meaning
                .get("value")
                .and_then(Value::as_f64)
                .is_some_and(f64::is_finite)
            {
                failures.push(format!("{prefix} is missing finite value"));
            }
            for field in ["name", "label", "description"] {
                if !meaning
                    .get(field)
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
                {
                    failures.push(format!("{prefix} is missing string field {field}"));
                }
            }
        }
    }

    if required_value_meanings.is_empty() {
        return failures;
    }

    let Some(meanings) = meanings else {
        failures.push(format!(
            "{label} numeric sidecar manifest is missing required value_meanings"
        ));
        return failures;
    };
    for required in required_value_meanings {
        if !meanings
            .iter()
            .any(|meaning| sidecar_value_meaning_matches(meaning, required))
        {
            failures.push(format!(
                "{label} numeric sidecar value_meanings do not include {required}"
            ));
        }
    }
    failures
}

fn sidecar_value_meaning_matches(meaning: &Value, required: &str) -> bool {
    for field in ["name", "label"] {
        if meaning
            .get(field)
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(required))
        {
            return true;
        }
    }
    required
        .parse::<f64>()
        .ok()
        .zip(meaning.get("value").and_then(Value::as_f64))
        .is_some_and(|(required, value)| (required - value).abs() <= 0.001)
}

fn parse_reflectivity_qc(value: &Value) -> Result<Option<ReflectivitySummary>> {
    let Some(qc) = non_null_object(value, "reflectivity_qc") else {
        return Ok(None);
    };
    Ok(Some(ReflectivitySummary {
        finite_gate_count: required_u64(qc, "finite_gate_count")?,
        removed_gate_count: required_u64(qc, "removed_gate_count")?,
        removed_gate_fraction: required_f64(qc, "removed_gate_fraction")?,
    }))
}

fn parse_velocity_qc(value: &Value) -> Result<Option<VelocitySummary>> {
    let Some(qc) = non_null_object(value, "velocity_qc") else {
        return Ok(None);
    };
    Ok(Some(VelocitySummary {
        nyquist_ms: qc.get("nyquist_ms").and_then(Value::as_f64),
        finite_gate_count: required_u64(qc, "finite_gate_count")?,
        fold_like_jump_count: required_u64(qc, "fold_like_jump_count")?,
        severe_jump_count: required_u64(qc, "severe_jump_count")?,
        fold_like_jump_fraction: required_f64(qc, "fold_like_jump_fraction")?,
        max_abs_jump_ms: required_f64(qc, "max_abs_jump_ms")?,
    }))
}

fn parse_dealias_qc(value: &Value) -> Result<Option<DealiasSummary>> {
    let Some(qc) = non_null_object(value, "dealias_qc") else {
        return Ok(None);
    };
    let original = qc.get("original_score").filter(|value| !value.is_null());
    let candidate = qc.get("candidate_score").filter(|value| !value.is_null());
    Ok(Some(DealiasSummary {
        attempted: qc
            .get("attempted")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        accepted: qc.get("accepted").and_then(Value::as_bool).unwrap_or(false),
        forced: qc.get("forced").and_then(Value::as_bool).unwrap_or(false),
        decision: qc
            .get("decision")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        changed_gate_count: qc
            .get("changed_gate_count")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        original_fold_like_jumps: original
            .and_then(|score| score.get("fold_like_jumps"))
            .and_then(Value::as_u64),
        original_severe_jumps: original
            .and_then(|score| score.get("severe_jumps"))
            .and_then(Value::as_u64),
        candidate_fold_like_jumps: candidate
            .and_then(|score| score.get("fold_like_jumps"))
            .and_then(Value::as_u64),
        candidate_severe_jumps: candidate
            .and_then(|score| score.get("severe_jumps"))
            .and_then(Value::as_u64),
    }))
}

fn non_null_object<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    value.get(field).filter(|value| !value.is_null())
}

fn required_u64(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .with_context(|| format!("missing numeric field {field}"))
}

fn required_f64(value: &Value, field: &str) -> Result<f64> {
    value
        .get(field)
        .and_then(Value::as_f64)
        .with_context(|| format!("missing numeric field {field}"))
}

fn required_str<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("missing string field {field}"))
}

fn resolve_record_path(manifest_dir: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else if manifest_dir.join(&path).exists() {
        manifest_dir.join(path)
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_conservative_reflectivity_and_improved_dealias() {
        let value = json!({
            "ok": true,
            "name": "ktlx_vel",
            "product": "vel",
            "reflectivity_qc": {
                "finite_gate_count": 1000,
                "removed_gate_count": 10,
                "removed_gate_fraction": 0.01
            },
            "velocity_qc": {
                "finite_gate_count": 1000,
                "fold_like_jump_count": 5,
                "severe_jump_count": 2,
                "fold_like_jump_fraction": 0.002,
                "max_abs_jump_ms": 55.0
            },
            "dealias_qc": {
                "attempted": true,
                "accepted": true,
                "forced": false,
                "decision": "candidate_accepted",
                "changed_gate_count": 100,
                "original_score": {
                    "fold_like_jumps": 100,
                    "severe_jumps": 50
                },
                "candidate_score": {
                    "fold_like_jumps": 20,
                    "severe_jumps": 5
                }
            }
        });
        let thresholds = QualityThresholds {
            max_reflectivity_removed_fraction: Some(0.05),
            max_velocity_fold_fraction: Some(0.005),
            max_velocity_severe_jumps: Some(5),
            max_velocity_max_jump_ms: Some(80.0),
            min_product_finite_gates: None,
            min_product_min_value: None,
            min_product_max_value: None,
            max_product_max_value: None,
            require_product_source: None,
            require_product_input: Vec::new(),
            require_product_method: None,
            require_numeric_sidecar: false,
            require_sidecar_value_meaning: Vec::new(),
            require_unclipped_bounds: false,
        };

        let summary =
            evaluate_manifest_value(Path::new("manifest.json"), &value, &thresholds).unwrap();

        assert!(summary.ok);
        assert_eq!(summary.entries[0].failures.len(), 0);
    }

    #[test]
    fn rejects_overclean_reflectivity_and_worse_accepted_dealias() {
        let value = json!({
            "ok": true,
            "name": "bad_vel",
            "product": "vel",
            "reflectivity_qc": {
                "finite_gate_count": 1000,
                "removed_gate_count": 120,
                "removed_gate_fraction": 0.12
            },
            "velocity_qc": {
                "finite_gate_count": 1000,
                "fold_like_jump_count": 20,
                "severe_jump_count": 9,
                "fold_like_jump_fraction": 0.02,
                "max_abs_jump_ms": 150.0
            },
            "dealias_qc": {
                "attempted": true,
                "accepted": true,
                "forced": false,
                "decision": "candidate_accepted",
                "changed_gate_count": 100,
                "original_score": {
                    "fold_like_jumps": 10,
                    "severe_jumps": 5
                },
                "candidate_score": {
                    "fold_like_jumps": 12,
                    "severe_jumps": 7
                }
            }
        });
        let thresholds = QualityThresholds {
            max_reflectivity_removed_fraction: Some(0.05),
            max_velocity_fold_fraction: Some(0.005),
            max_velocity_severe_jumps: Some(5),
            max_velocity_max_jump_ms: Some(80.0),
            min_product_finite_gates: None,
            min_product_min_value: None,
            min_product_max_value: None,
            max_product_max_value: None,
            require_product_source: None,
            require_product_input: Vec::new(),
            require_product_method: None,
            require_numeric_sidecar: false,
            require_sidecar_value_meaning: Vec::new(),
            require_unclipped_bounds: false,
        };

        let summary =
            evaluate_manifest_value(Path::new("manifest.json"), &value, &thresholds).unwrap();

        assert!(!summary.ok);
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("reflectivity removed fraction") })
        );
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("accepted dealias increased fold-like jumps") })
        );
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("accepted dealias increased severe jumps") })
        );
    }

    #[test]
    fn gates_generic_product_qc_ranges() {
        let value = json!({
            "ok": true,
            "name": "ksjt_cc",
            "product": "cc",
            "product_qc": {
                "product": "cc",
                "finite_gate_count": 2000,
                "min_value": 0.2,
                "max_value": 1.0,
                "mean_value": 0.91
            }
        });
        let thresholds = QualityThresholds {
            max_reflectivity_removed_fraction: None,
            max_velocity_fold_fraction: None,
            max_velocity_severe_jumps: None,
            max_velocity_max_jump_ms: None,
            min_product_finite_gates: Some(1000),
            min_product_min_value: Some(0.0),
            min_product_max_value: Some(0.95),
            max_product_max_value: Some(1.05),
            require_product_source: None,
            require_product_input: Vec::new(),
            require_product_method: None,
            require_numeric_sidecar: false,
            require_sidecar_value_meaning: Vec::new(),
            require_unclipped_bounds: false,
        };

        let summary =
            evaluate_manifest_value(Path::new("manifest.json"), &value, &thresholds).unwrap();

        assert!(summary.ok);

        let too_low = json!({
            "ok": true,
            "name": "bad_cc",
            "product": "cc",
            "product_qc": {
                "product": "cc",
                "finite_gate_count": 20,
                "min_value": -0.2,
                "max_value": 1.4,
                "mean_value": 0.5
            }
        });
        let summary =
            evaluate_manifest_value(Path::new("manifest.json"), &too_low, &thresholds).unwrap();

        assert!(!summary.ok);
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("product finite gates 20 below 1000") })
        );
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("product min value -0.2000 below 0.0000") })
        );
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("product max value 1.4000 exceeds 1.0500") })
        );
    }

    #[test]
    fn gates_numeric_sidecar_presence_and_manifest_shape() {
        let root = std::env::temp_dir().join(format!(
            "rustwx-radar-quality-sidecar-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let sidecar_manifest = root.join("polar_sidecar_manifest.json");
        let values_path = root.join("polar_values_f32le.bin");
        let flags_path = root.join("polar_gate_flags_u8.bin");
        fs::write(
            &sidecar_manifest,
            serde_json::to_vec_pretty(&json!({
                "schema": RADAR_POLAR_SIDECAR_SCHEMA,
                "sidecar_version": 2,
                "ok": true,
                "name": "ktlx_ref",
                "site": {
                    "id": "KTLX",
                    "name": "Oklahoma City",
                    "state": "OK",
                    "lat": 35.333,
                    "lon": -97.277,
                    "elevation_m": 389.4
                },
                "product": "ref",
                "product_name": "Reflectivity",
                "units": "dBZ",
                "value_meanings": [
                    {
                        "value": 7.0,
                        "name": "heavy_rain",
                        "label": "Heavy Rain",
                        "description": "High-reflectivity rain or positive-KDP heavy rain."
                    }
                ],
                "product_provenance": {
                    "source": "native",
                    "derived": false
                },
                "source_key_or_url": "s3://nexrad/KTLX",
                "scan_time_utc": "2026-05-11T00:00:00Z",
                "sweep_index": 0,
                "elevation_deg": 0.5,
                "nyquist_velocity_ms": null,
                "processing_state": "raw",
                "radial_count": 1,
                "max_gate_count": 4,
                "gate_count": 4,
                "values_path": values_path.display().to_string(),
                "values_encoding": "f32_le_row_major_radial_gate_nan_missing",
                "gate_flags_path": flags_path.display().to_string(),
                "gate_flags_encoding": "u8_bitmask_row_major_radial_gate",
                "gate_flag_meanings": [
                    {"bit": 0, "mask": 1, "name": "valid", "description": "finite value"},
                    {"bit": 1, "mask": 2, "name": "missing", "description": "missing value"},
                    {"bit": 2, "mask": 4, "name": "range_folded", "description": "range folded"},
                    {"bit": 3, "mask": 8, "name": "filtered", "description": "filtered by QC"},
                    {"bit": 4, "mask": 16, "name": "derived", "description": "derived product"},
                    {"bit": 5, "mask": 32, "name": "dealiased", "description": "dealiased velocity"}
                ],
                "radials": [{
                    "radial_index": 0,
                    "azimuth_deg": 0.0,
                    "elevation_deg": 0.5,
                    "azimuth_spacing_deg": 1.0,
                    "gate_count": 4,
                    "first_gate_range_m": 0,
                    "gate_spacing_m": 250,
                    "nyquist_velocity_ms": null,
                    "data_word_size_bits": 8,
                    "scale": 2.0,
                    "offset": 66.0
                }],
                "qc": {}
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(&values_path, [0u8; 16]).unwrap();
        fs::write(&flags_path, [1u8; 4]).unwrap();
        let value = json!({
            "ok": true,
            "name": "ktlx_ref",
            "product": "ref",
            "numeric_sidecar": {
                "schema": RADAR_POLAR_SIDECAR_SCHEMA,
                "manifest_path": sidecar_manifest.display().to_string(),
                "values_path": values_path.display().to_string(),
                "gate_flags_path": flags_path.display().to_string(),
                "radial_count": 1,
                "max_gate_count": 4,
                "gate_count": 4,
                "processing_state": "raw"
            }
        });
        let thresholds = QualityThresholds {
            max_reflectivity_removed_fraction: None,
            max_velocity_fold_fraction: None,
            max_velocity_severe_jumps: None,
            max_velocity_max_jump_ms: None,
            min_product_finite_gates: None,
            min_product_min_value: None,
            min_product_max_value: None,
            max_product_max_value: None,
            require_product_source: None,
            require_product_input: Vec::new(),
            require_product_method: None,
            require_numeric_sidecar: true,
            require_sidecar_value_meaning: vec![
                "heavy_rain".to_string(),
                "Heavy Rain".to_string(),
                "7".to_string(),
            ],
            require_unclipped_bounds: false,
        };

        let summary =
            evaluate_manifest_value(&root.join("tiles_manifest.json"), &value, &thresholds)
                .unwrap();

        assert!(summary.ok);
        assert_eq!(
            summary.entries[0]
                .numeric_sidecar
                .as_ref()
                .map(|sidecar| sidecar.processing_state.as_str()),
            Some("raw")
        );

        let mut missing_meaning_thresholds = thresholds.clone();
        missing_meaning_thresholds.require_sidecar_value_meaning = vec!["large_hail".to_string()];
        let missing = evaluate_manifest_value(
            &root.join("tiles_manifest.json"),
            &value,
            &missing_meaning_thresholds,
        )
        .unwrap();
        assert!(!missing.ok);
        assert!(
            missing
                .failures
                .iter()
                .any(|failure| { failure.contains("value_meanings do not include large_hail") })
        );

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn gates_unclipped_tile_bounds_metadata() {
        let thresholds = QualityThresholds {
            max_reflectivity_removed_fraction: None,
            max_velocity_fold_fraction: None,
            max_velocity_severe_jumps: None,
            max_velocity_max_jump_ms: None,
            min_product_finite_gates: None,
            min_product_min_value: None,
            min_product_max_value: None,
            max_product_max_value: None,
            require_product_source: None,
            require_product_input: Vec::new(),
            require_product_method: None,
            require_numeric_sidecar: false,
            require_sidecar_value_meaning: Vec::new(),
            require_unclipped_bounds: true,
        };
        let value = json!({
            "ok": true,
            "name": "ksjt_ref_z12",
            "product": "ref",
            "bounds": [-100.9, 31.0, -100.2, 31.7],
            "clip_to_bounds": false,
            "sampling_bounds": [-105.3, 27.2, -95.6, 35.5]
        });

        let summary =
            evaluate_manifest_value(Path::new("manifest.json"), &value, &thresholds).unwrap();

        assert!(summary.ok);
        assert_eq!(summary.entries[0].clip_to_bounds, Some(false));
        assert_eq!(
            summary.entries[0].sampling_bounds,
            Some([-105.3, 27.2, -95.6, 35.5])
        );

        let clipped = json!({
            "ok": true,
            "name": "bad_clip",
            "product": "ref",
            "bounds": [-100.9, 31.0, -100.2, 31.7],
            "clip_to_bounds": true,
            "sampling_bounds": [-100.9, 31.0, -100.2, 31.7]
        });
        let summary =
            evaluate_manifest_value(Path::new("manifest.json"), &clipped, &thresholds).unwrap();

        assert!(!summary.ok);
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("clip_to_bounds=Some(true)") })
        );

        let too_small = json!({
            "ok": true,
            "name": "bad_sampling_bounds",
            "product": "ref",
            "bounds": [-100.9, 31.0, -100.2, 31.7],
            "clip_to_bounds": false,
            "sampling_bounds": [-100.8, 31.1, -100.3, 31.6]
        });
        let summary =
            evaluate_manifest_value(Path::new("manifest.json"), &too_small, &thresholds).unwrap();

        assert!(!summary.ok);
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("do not cover tile bounds") })
        );
    }

    #[test]
    fn gates_product_provenance() {
        let value = json!({
            "ok": true,
            "name": "ksjt_kdp",
            "product": "kdp",
            "product_provenance": {
                "source": "derived",
                "derived": true,
                "inputs": ["phi"],
                "method": "centered_phi_range_derivative"
            }
        });
        let thresholds = QualityThresholds {
            max_reflectivity_removed_fraction: None,
            max_velocity_fold_fraction: None,
            max_velocity_severe_jumps: None,
            max_velocity_max_jump_ms: None,
            min_product_finite_gates: None,
            min_product_min_value: None,
            min_product_max_value: None,
            max_product_max_value: None,
            require_product_source: Some("derived".to_string()),
            require_product_input: vec!["phi".to_string()],
            require_product_method: Some("centered_phi_range_derivative".to_string()),
            require_numeric_sidecar: false,
            require_sidecar_value_meaning: Vec::new(),
            require_unclipped_bounds: false,
        };

        let summary =
            evaluate_manifest_value(Path::new("manifest.json"), &value, &thresholds).unwrap();

        assert!(summary.ok);
        assert_eq!(
            summary.entries[0]
                .product_provenance
                .as_ref()
                .and_then(|provenance| provenance.method.as_deref()),
            Some("centered_phi_range_derivative")
        );

        let missing = json!({
            "ok": true,
            "name": "bad_kdp",
            "product": "kdp"
        });
        let summary =
            evaluate_manifest_value(Path::new("manifest.json"), &missing, &thresholds).unwrap();

        assert!(!summary.ok);
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("missing product provenance") })
        );
    }
}
