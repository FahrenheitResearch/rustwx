use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use rustwx_radar::{
    radar_polar_to_lat_lon, RadarPolarSample, RadarPolarSampleMethod, RadarPolarSidecar,
    RadarPolarSidecarManifest,
};

#[derive(Debug, Parser)]
#[command(about = "Sample a Rust-native NEXRAD polar sidecar at a lat/lon")]
struct Cli {
    /// Path to polar_sidecar_manifest.json.
    #[arg(long)]
    manifest: PathBuf,

    /// Query latitude. Required with --lon unless using --azimuth-deg and --ground-range-m.
    #[arg(long)]
    lat: Option<f64>,

    /// Query longitude. Required with --lat unless using --azimuth-deg and --ground-range-m.
    #[arg(long)]
    lon: Option<f64>,

    /// Derive the query point from radar-relative azimuth, in degrees clockwise from north.
    #[arg(long)]
    azimuth_deg: Option<f32>,

    /// Derive the query point from radar-relative ground range, in meters.
    #[arg(long)]
    ground_range_m: Option<f64>,

    /// Sampling method.
    #[arg(long, value_enum, default_value_t = SampleMethodArg::Nearest)]
    method: SampleMethodArg,

    /// Optional path for the JSON sample response.
    #[arg(long)]
    summary_out: Option<PathBuf>,

    /// Fail if the sampled gate does not contain a finite numeric value.
    #[arg(long, default_value_t = false)]
    require_value: bool,

    /// Fail if the sampled gate value is below this threshold.
    #[arg(long)]
    min_value: Option<f32>,

    /// Fail if the sampled gate value is above this threshold.
    #[arg(long)]
    max_value: Option<f32>,

    /// Fail if the sampled units do not match this string.
    #[arg(long)]
    require_units: Option<String>,

    /// Fail if the sampled product short name does not match this value.
    #[arg(long)]
    require_product: Option<String>,

    /// Fail if the sample method in the response does not match this value.
    #[arg(long)]
    require_sample_method: Option<String>,

    /// Fail unless the response has a categorical value label.
    #[arg(long, default_value_t = false)]
    require_value_label: bool,

    /// Fail unless the response value label exactly matches this value.
    #[arg(long)]
    expect_value_label: Option<String>,

    /// Fail unless processing_state contains this token. Repeatable.
    #[arg(long)]
    require_processing_state: Vec<String>,

    /// Fail unless the sample gate flags include this value. Repeatable.
    #[arg(long)]
    require_gate_flag: Vec<String>,

    /// Fail unless the sampled gate is marked raw.
    #[arg(long, default_value_t = false)]
    require_raw: bool,

    /// Fail unless the sampled gate is marked dealiased.
    #[arg(long, default_value_t = false)]
    require_dealiased: bool,

    /// Fail unless the sampled gate is marked filtered.
    #[arg(long, default_value_t = false)]
    require_filtered: bool,

    /// Fail unless the sampled gate is marked derived.
    #[arg(long, default_value_t = false)]
    require_derived: bool,

    /// Fail unless product_provenance.source matches this value.
    #[arg(long)]
    require_product_source: Option<String>,

    /// Fail unless product_provenance.method matches this value.
    #[arg(long)]
    require_product_method: Option<String>,

    /// Fail unless product_provenance.inputs includes this value. Repeatable.
    #[arg(long)]
    require_product_input: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SampleMethodArg {
    Nearest,
    Interpolated,
}

impl From<SampleMethodArg> for RadarPolarSampleMethod {
    fn from(value: SampleMethodArg) -> Self {
        match value {
            SampleMethodArg::Nearest => Self::Nearest,
            SampleMethodArg::Interpolated => Self::Interpolated,
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let sidecar = RadarPolarSidecar::open(&cli.manifest)?;
    let (lat, lon) = resolve_query_point(&cli, &sidecar.manifest)?;
    validate_lat_lon(lat, lon)?;

    let sample = sidecar
        .sample_lat_lon(lat, lon, cli.method.into())
        .with_context(|| {
            format!("lat/lon {lat:.6},{lon:.6} is outside the sidecar sweep coverage")
        })?;
    validate_sample_expectations(&sample, &cli)?;
    let text = serde_json::to_string_pretty(&sample)?;
    if let Some(path) = cli.summary_out.as_ref() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        fs::write(path, text.as_bytes()).with_context(|| format!("write {}", path.display()))?;
    }
    println!("{text}");
    Ok(())
}

fn validate_sample_expectations(sample: &RadarPolarSample, cli: &Cli) -> Result<()> {
    let mut failures = Vec::new();
    let value = sample.value.filter(|value| value.is_finite());
    if cli.require_value || cli.min_value.is_some() || cli.max_value.is_some() {
        if value.is_none() {
            failures.push("sample value is missing or non-finite".to_string());
        }
    }
    if let Some(min_value) = cli.min_value {
        if !min_value.is_finite() {
            bail!("--min-value must be finite");
        }
        if value.is_some_and(|value| value < min_value) {
            failures.push(format!(
                "sample value {} is below required minimum {}",
                sample.value.unwrap_or(f32::NAN),
                min_value
            ));
        }
    }
    if let Some(max_value) = cli.max_value {
        if !max_value.is_finite() {
            bail!("--max-value must be finite");
        }
        if value.is_some_and(|value| value > max_value) {
            failures.push(format!(
                "sample value {} is above required maximum {}",
                sample.value.unwrap_or(f32::NAN),
                max_value
            ));
        }
    }
    if let Some(expected) = cli.require_units.as_deref() {
        if sample.units != expected {
            failures.push(format!(
                "sample units {} did not match required units {expected}",
                sample.units
            ));
        }
    }
    if let Some(expected) = cli.require_product.as_deref() {
        if !eq_ignore_ascii_case(&sample.product, expected) {
            failures.push(format!(
                "sample product {} did not match required product {expected}",
                sample.product
            ));
        }
    }
    if let Some(expected) = cli.require_sample_method.as_deref() {
        if !eq_ignore_ascii_case(&sample.method, expected) {
            failures.push(format!(
                "sample method {} did not match required method {expected}",
                sample.method
            ));
        }
    }
    if cli.require_value_label
        && !sample
            .value_label
            .as_deref()
            .is_some_and(|label| !label.trim().is_empty())
    {
        failures.push("sample value_label is missing".to_string());
    }
    if let Some(expected) = cli.expect_value_label.as_deref() {
        if sample.value_label.as_deref() != Some(expected) {
            failures.push(format!(
                "sample value_label {:?} did not match required label {expected}",
                sample.value_label
            ));
        }
    }
    for token in &cli.require_processing_state {
        if !contains_ignore_ascii_case(&sample.processing_state, token) {
            failures.push(format!(
                "sample processing_state {} did not include required token {token}",
                sample.processing_state
            ));
        }
    }
    for flag in &cli.require_gate_flag {
        if !sample
            .gate_flags
            .iter()
            .any(|value| eq_ignore_ascii_case(value, flag))
        {
            failures.push(format!("sample gate_flags did not include {flag}"));
        }
    }
    for (required, present, label) in [
        (cli.require_raw, sample.raw, "raw"),
        (cli.require_dealiased, sample.dealiased, "dealiased"),
        (cli.require_filtered, sample.filtered, "filtered"),
        (cli.require_derived, sample.derived, "derived"),
    ] {
        if required && !present {
            failures.push(format!("sample was not marked {label}"));
        }
    }
    if let Some(expected) = cli.require_product_source.as_deref() {
        let source = sample
            .product_provenance
            .get("source")
            .and_then(serde_json::Value::as_str);
        if !source.is_some_and(|source| eq_ignore_ascii_case(source, expected)) {
            failures.push(format!(
                "sample product_provenance.source {:?} did not match {expected}",
                source
            ));
        }
    }
    if let Some(expected) = cli.require_product_method.as_deref() {
        let method = sample
            .product_provenance
            .get("method")
            .and_then(serde_json::Value::as_str);
        if method != Some(expected) {
            failures.push(format!(
                "sample product_provenance.method {:?} did not match {expected}",
                method
            ));
        }
    }
    let provenance_inputs = sample
        .product_provenance
        .get("inputs")
        .and_then(serde_json::Value::as_array);
    for expected in &cli.require_product_input {
        let present = provenance_inputs.is_some_and(|inputs| {
            inputs.iter().any(|input| {
                input
                    .as_str()
                    .is_some_and(|input| eq_ignore_ascii_case(input, expected))
            })
        });
        if !present {
            failures.push(format!(
                "sample product_provenance.inputs did not include {expected}"
            ));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        bail!(
            "radar sidecar sample validation failed:\n- {}",
            failures.join("\n- ")
        )
    }
}

fn eq_ignore_ascii_case(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

fn resolve_query_point(cli: &Cli, manifest: &RadarPolarSidecarManifest) -> Result<(f64, f64)> {
    match (cli.lat, cli.lon, cli.azimuth_deg, cli.ground_range_m) {
        (Some(lat), Some(lon), None, None) => Ok((lat, lon)),
        (None, None, Some(azimuth_deg), Some(ground_range_m)) => {
            if !azimuth_deg.is_finite() || !ground_range_m.is_finite() || ground_range_m < 0.0 {
                bail!("azimuth and ground range must be finite, with non-negative range");
            }
            Ok(radar_polar_to_lat_lon(
                manifest.site.lat,
                manifest.site.lon,
                azimuth_deg,
                ground_range_m,
            ))
        }
        _ => bail!(
            "provide either --lat with --lon or --azimuth-deg with --ground-range-m, but not both"
        ),
    }
}

fn validate_lat_lon(lat: f64, lon: f64) -> Result<()> {
    if !lat.is_finite()
        || !lon.is_finite()
        || !(-90.0..=90.0).contains(&lat)
        || !(-180.0..=180.0).contains(&lon)
    {
        bail!("lat/lon must be finite geographic coordinates");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_radar::radar_lat_lon_to_polar;
    use serde_json::json;

    fn sample_manifest() -> RadarPolarSidecarManifest {
        serde_json::from_value(json!({
            "schema": "rustwx.radar.polar_sidecar.v2",
            "sidecar_version": 2,
            "ok": true,
            "name": "sample",
            "site": {
                "id": "KTLX",
                "name": "Oklahoma City",
                "state": "OK",
                "lat": 35.333,
                "lon": -97.277,
                "elevation_m": 389.4,
                "feedhorn_height_m": 20.0,
                "antenna_elevation_m": 409.4
            },
            "product": "ref",
            "product_name": "Reflectivity",
            "units": "dBZ",
            "product_provenance": {"source": "native", "derived": false},
            "source_key_or_url": "s3://nexrad/KTLX",
            "scan_time_utc": "2026-05-11T00:00:00Z",
            "sweep_index": 0,
            "elevation_deg": 0.5,
            "nyquist_velocity_ms": null,
            "processing_state": "raw",
            "radial_count": 0,
            "max_gate_count": 0,
            "gate_count": 0,
            "values_path": "polar_values_f32le.bin",
            "values_encoding": "f32_le_row_major_radial_gate_nan_missing",
            "gate_flags_path": "polar_gate_flags_u8.bin",
            "gate_flags_encoding": "u8_bitmask_row_major_radial_gate",
            "radials": [],
            "qc": {
                "product_qc": null,
                "velocity_qc": null,
                "dealias_qc": null,
                "velocity_quality_qc": null,
                "reflectivity_qc": null
            }
        }))
        .unwrap()
    }

    #[test]
    fn resolves_direct_lat_lon_query() {
        let cli = Cli {
            manifest: PathBuf::from("polar_sidecar_manifest.json"),
            lat: Some(35.4),
            lon: Some(-97.2),
            azimuth_deg: None,
            ground_range_m: None,
            method: SampleMethodArg::Nearest,
            summary_out: None,
            require_value: false,
            min_value: None,
            max_value: None,
            require_units: None,
            require_product: None,
            require_sample_method: None,
            require_value_label: false,
            expect_value_label: None,
            require_processing_state: Vec::new(),
            require_gate_flag: Vec::new(),
            require_raw: false,
            require_dealiased: false,
            require_filtered: false,
            require_derived: false,
            require_product_source: None,
            require_product_method: None,
            require_product_input: Vec::new(),
        };
        assert_eq!(
            resolve_query_point(&cli, &sample_manifest()).unwrap(),
            (35.4, -97.2)
        );
    }

    #[test]
    fn resolves_radar_relative_query() {
        let cli = Cli {
            manifest: PathBuf::from("polar_sidecar_manifest.json"),
            lat: None,
            lon: None,
            azimuth_deg: Some(0.0),
            ground_range_m: Some(111_139.0),
            method: SampleMethodArg::Nearest,
            summary_out: None,
            require_value: false,
            min_value: None,
            max_value: None,
            require_units: None,
            require_product: None,
            require_sample_method: None,
            require_value_label: false,
            expect_value_label: None,
            require_processing_state: Vec::new(),
            require_gate_flag: Vec::new(),
            require_raw: false,
            require_dealiased: false,
            require_filtered: false,
            require_derived: false,
            require_product_source: None,
            require_product_method: None,
            require_product_input: Vec::new(),
        };
        let (lat, lon) = resolve_query_point(&cli, &sample_manifest()).unwrap();
        let polar = radar_lat_lon_to_polar(35.333, -97.277, lat, lon);
        assert!((polar.azimuth_deg - 0.0).abs() < 1.0e-6);
        assert!((polar.ground_range_m - 111_139.0).abs() < 1.0e-6);
        assert!(lat > 36.332 && lat < 36.333);
        assert!((lon + 97.277).abs() < 1.0e-6);
    }

    fn expectation_cli() -> Cli {
        Cli {
            manifest: PathBuf::from("polar_sidecar_manifest.json"),
            lat: None,
            lon: None,
            azimuth_deg: Some(0.0),
            ground_range_m: Some(50_000.0),
            method: SampleMethodArg::Nearest,
            summary_out: None,
            require_value: true,
            min_value: Some(0.0),
            max_value: Some(16.0),
            require_units: Some("category".to_string()),
            require_product: Some("hhc".to_string()),
            require_sample_method: Some("nearest".to_string()),
            require_value_label: true,
            expect_value_label: Some("Heavy Rain".to_string()),
            require_processing_state: vec!["derived".to_string()],
            require_gate_flag: vec!["valid".to_string(), "derived".to_string()],
            require_raw: false,
            require_dealiased: false,
            require_filtered: false,
            require_derived: true,
            require_product_source: Some("derived".to_string()),
            require_product_method: Some("dual_pol_rule_hca_v1".to_string()),
            require_product_input: vec!["ref".to_string(), "zdr".to_string()],
        }
    }

    fn labeled_hca_sample() -> RadarPolarSample {
        serde_json::from_str(
            r#"{
                "schema": "rustwx.radar.polar_sidecar.v2",
                "method": "nearest",
                "lat": 35.0,
                "lon": -97.0,
                "value": 7.0,
                "value_label": "Heavy Rain",
                "units": "category",
                "product": "hhc",
                "product_name": "Hydrometeor Class (HHC)",
                "sweep_index": 0,
                "elevation_deg": 0.5,
                "nyquist_velocity_ms": null,
                "azimuth_deg": 0.0,
                "ground_range_m": 50000.0,
                "range_m": 50000.0,
                "radial_index": 0,
                "radial_azimuth_deg": 0.0,
                "radial_elevation_deg": 0.5,
                "azimuth_spacing_deg": 0.5,
                "gate_index": 1,
                "gate_fraction": 1.0,
                "first_gate_range_m": 0,
                "gate_spacing_m": 250,
                "gate_flags": ["valid", "derived"],
                "gate_flag_bits": 17,
                "processing_state": "derived",
                "raw": false,
                "dealiased": false,
                "filtered": false,
                "derived": true,
                "product_provenance": {
                    "source": "derived",
                    "derived": true,
                    "inputs": ["ref", "zdr", "cc", "phi"],
                    "method": "dual_pol_rule_hca_v1"
                },
                "source_key_or_url": "s3://nexrad/KTLX",
                "scan_time_utc": "2026-05-11T00:00:00Z",
                "qc": {
                    "product_qc": null,
                    "velocity_qc": null,
                    "dealias_qc": null,
                    "velocity_quality_qc": null,
                    "reflectivity_qc": null
                },
                "site": {
                    "id": "KTLX",
                    "name": "Oklahoma City",
                    "state": "OK",
                    "lat": 35.333,
                    "lon": -97.277,
                    "elevation_m": 389.4,
                    "feedhorn_height_m": 20.0,
                    "antenna_elevation_m": 409.4
                }
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn validates_sample_expectations() {
        validate_sample_expectations(&labeled_hca_sample(), &expectation_cli()).unwrap();
    }

    #[test]
    fn rejects_missing_required_sample_label() {
        let mut sample = labeled_hca_sample();
        sample.value_label = None;
        let err = validate_sample_expectations(&sample, &expectation_cli()).unwrap_err();
        assert!(err.to_string().contains("value_label"));
    }
}
