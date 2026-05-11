use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use rustwx_radar::{
    radar_polar_to_lat_lon, RadarPolarSampleMethod, RadarPolarSidecar, RadarPolarSidecarManifest,
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
        };
        let (lat, lon) = resolve_query_point(&cli, &sample_manifest()).unwrap();
        let polar = radar_lat_lon_to_polar(35.333, -97.277, lat, lon);
        assert!((polar.azimuth_deg - 0.0).abs() < 1.0e-6);
        assert!((polar.ground_range_m - 111_139.0).abs() < 1.0e-6);
        assert!(lat > 36.332 && lat < 36.333);
        assert!((lon + 97.277).abs() < 1.0e-6);
    }
}
