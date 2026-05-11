use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Parser)]
#[command(
    name = "radar_benchmark_gate",
    about = "Validate rustwx radar tile benchmark manifests against repeatable speed gates"
)]
struct Cli {
    /// Benchmark or tile manifest JSON path. Repeat for multiple manifests.
    #[arg(long, required = true)]
    manifest: Vec<PathBuf>,

    /// Fail any entry whose total render time exceeds this many milliseconds.
    #[arg(long)]
    max_total_ms: Option<u128>,

    /// Fail any entry below this candidate-tiles-per-second throughput.
    #[arg(long)]
    min_tiles_per_second: Option<f64>,

    /// Fail any entry above this milliseconds-per-candidate-tile cost.
    #[arg(long)]
    max_ms_per_candidate_tile: Option<f64>,

    /// Require a supersample factor to appear in benchmark entries. Repeatable.
    #[arg(long)]
    require_sample_factor: Vec<u8>,

    /// Optional JSON summary path.
    #[arg(long)]
    summary_out: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
struct GateThresholds {
    max_total_ms: Option<u128>,
    min_tiles_per_second: Option<f64>,
    max_ms_per_candidate_tile: Option<f64>,
    require_sample_factor: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct GateSummary {
    ok: bool,
    thresholds: GateThresholds,
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
    sample_factor: Option<u8>,
    candidate_tile_count: u64,
    tile_count: u64,
    total_ms: u128,
    tiles_per_second: f64,
    ms_per_candidate_tile: f64,
    failures: Vec<String>,
}

#[derive(Debug)]
struct BenchmarkEntry {
    label: String,
    sample_factor: Option<u8>,
    candidate_tile_count: u64,
    tile_count: u64,
    total_ms: u128,
    tiles_per_second: f64,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let thresholds = GateThresholds {
        max_total_ms: cli.max_total_ms,
        min_tiles_per_second: cli.min_tiles_per_second,
        max_ms_per_candidate_tile: cli.max_ms_per_candidate_tile,
        require_sample_factor: cli.require_sample_factor,
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
            "radar benchmark gate failed with {} failure(s)",
            summary.failures.len()
        );
    }
    Ok(())
}

fn evaluate_manifest_paths(paths: &[PathBuf], thresholds: GateThresholds) -> Result<GateSummary> {
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

    Ok(GateSummary {
        ok: failures.is_empty(),
        thresholds,
        manifests,
        failures,
    })
}

fn evaluate_manifest_value(
    path: &Path,
    value: &Value,
    thresholds: &GateThresholds,
) -> Result<ManifestSummary> {
    let entries = extract_entries(path, value)?;
    let mut summaries = Vec::with_capacity(entries.len());
    let mut failures = Vec::new();

    if value
        .get("ok")
        .and_then(Value::as_bool)
        .is_some_and(|ok| !ok)
    {
        failures.push(format!("{} has ok=false", path.display()));
    }

    let present_factors = entries
        .iter()
        .filter_map(|entry| entry.sample_factor)
        .collect::<BTreeSet<_>>();
    for factor in &thresholds.require_sample_factor {
        if !present_factors.contains(factor) {
            failures.push(format!(
                "{} missing required sample_factor={factor}",
                path.display()
            ));
        }
    }

    for entry in entries {
        let mut entry_failures = Vec::new();
        if entry.candidate_tile_count == 0 {
            entry_failures.push(format!("{} has zero candidate tiles", entry.label));
        }
        if let Some(max_total_ms) = thresholds.max_total_ms {
            if entry.total_ms > max_total_ms {
                entry_failures.push(format!(
                    "{} total_ms={} exceeds max_total_ms={max_total_ms}",
                    entry.label, entry.total_ms
                ));
            }
        }
        if let Some(min_tiles_per_second) = thresholds.min_tiles_per_second {
            if entry.tiles_per_second < min_tiles_per_second {
                entry_failures.push(format!(
                    "{} tiles_per_second={:.2} below min_tiles_per_second={:.2}",
                    entry.label, entry.tiles_per_second, min_tiles_per_second
                ));
            }
        }
        let ms_per_candidate_tile = if entry.candidate_tile_count > 0 {
            entry.total_ms as f64 / entry.candidate_tile_count as f64
        } else {
            f64::INFINITY
        };
        if let Some(max_ms_per_candidate_tile) = thresholds.max_ms_per_candidate_tile {
            if ms_per_candidate_tile > max_ms_per_candidate_tile {
                entry_failures.push(format!(
                    "{} ms_per_candidate_tile={:.2} exceeds max_ms_per_candidate_tile={:.2}",
                    entry.label, ms_per_candidate_tile, max_ms_per_candidate_tile
                ));
            }
        }

        failures.extend(
            entry_failures
                .iter()
                .map(|failure| format!("{}: {failure}", path.display())),
        );
        summaries.push(EntrySummary {
            label: entry.label,
            sample_factor: entry.sample_factor,
            candidate_tile_count: entry.candidate_tile_count,
            tile_count: entry.tile_count,
            total_ms: entry.total_ms,
            tiles_per_second: round2(entry.tiles_per_second),
            ms_per_candidate_tile: round2(ms_per_candidate_tile),
            failures: entry_failures,
        });
    }

    Ok(ManifestSummary {
        path: path.display().to_string(),
        ok: failures.is_empty(),
        entries: summaries,
        failures,
    })
}

fn extract_entries(path: &Path, value: &Value) -> Result<Vec<BenchmarkEntry>> {
    if let Some(entries) = value.get("entries").and_then(Value::as_array) {
        return entries
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let sample_factor = optional_u8(entry, "sample_factor")?;
                let candidate_tile_count = required_u64(entry, "candidate_tile_count")?;
                let tile_count = required_u64(entry, "tile_count")?;
                let total_ms = required_u128(entry, "total_ms")?;
                let tiles_per_second = optional_f64(entry, "tiles_per_second")
                    .unwrap_or_else(|| throughput(candidate_tile_count, total_ms));
                Ok(BenchmarkEntry {
                    label: sample_factor
                        .map(|factor| format!("sample_factor={factor}"))
                        .unwrap_or_else(|| format!("entry#{index}")),
                    sample_factor,
                    candidate_tile_count,
                    tile_count,
                    total_ms,
                    tiles_per_second,
                })
            })
            .collect();
    }

    let candidate_tile_count = value
        .get("candidate_tile_count")
        .or_else(|| value.get("total_candidate_tile_count"))
        .and_then(Value::as_u64)
        .with_context(|| {
            format!(
                "{} missing candidate_tile_count or total_candidate_tile_count",
                path.display()
            )
        })?;
    let tile_count = value
        .get("tile_count")
        .or_else(|| value.get("total_tile_count"))
        .and_then(Value::as_u64)
        .with_context(|| format!("{} missing tile_count or total_tile_count", path.display()))?;
    let total_ms = required_u128(value, "total_ms")?;
    let tiles_per_second = optional_f64(value, "tiles_per_second")
        .unwrap_or_else(|| throughput(candidate_tile_count, total_ms));

    Ok(vec![BenchmarkEntry {
        label: value
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| "manifest".to_string()),
        sample_factor: optional_u8(value, "sample_factor")?,
        candidate_tile_count,
        tile_count,
        total_ms,
        tiles_per_second,
    }])
}

fn required_u64(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .with_context(|| format!("missing numeric field {field}"))
}

fn required_u128(value: &Value, field: &str) -> Result<u128> {
    required_u64(value, field).map(u128::from)
}

fn optional_u8(value: &Value, field: &str) -> Result<Option<u8>> {
    let Some(raw) = value.get(field).and_then(Value::as_u64) else {
        return Ok(None);
    };
    Ok(Some(
        u8::try_from(raw).with_context(|| format!("{field} exceeds u8 range"))?,
    ))
}

fn optional_f64(value: &Value, field: &str) -> Option<f64> {
    value.get(field).and_then(Value::as_f64)
}

fn throughput(candidate_tile_count: u64, total_ms: u128) -> f64 {
    if total_ms > 0 {
        candidate_tile_count as f64 / (total_ms as f64 / 1000.0)
    } else {
        candidate_tile_count as f64
    }
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_fast_supersample_benchmark() {
        let value = json!({
            "ok": true,
            "entries": [
                {
                    "sample_factor": 1,
                    "candidate_tile_count": 10,
                    "tile_count": 8,
                    "total_ms": 20,
                    "tiles_per_second": 500.0
                }
            ]
        });
        let thresholds = GateThresholds {
            max_total_ms: Some(25),
            min_tiles_per_second: Some(400.0),
            max_ms_per_candidate_tile: Some(2.5),
            require_sample_factor: vec![1],
        };

        let summary =
            evaluate_manifest_value(Path::new("bench.json"), &value, &thresholds).unwrap();

        assert!(summary.ok);
        assert_eq!(summary.entries[0].ms_per_candidate_tile, 2.0);
    }

    #[test]
    fn reports_slow_entries_and_missing_sample_factor() {
        let value = json!({
            "ok": true,
            "entries": [
                {
                    "sample_factor": 1,
                    "candidate_tile_count": 10,
                    "tile_count": 8,
                    "total_ms": 80
                }
            ]
        });
        let thresholds = GateThresholds {
            max_total_ms: Some(100),
            min_tiles_per_second: Some(100.0),
            max_ms_per_candidate_tile: Some(4.0),
            require_sample_factor: vec![1, 2],
        };

        let summary =
            evaluate_manifest_value(Path::new("bench.json"), &value, &thresholds).unwrap();

        assert!(!summary.ok);
        assert_eq!(summary.failures.len(), 2);
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("missing required sample_factor=2") })
        );
        assert!(
            summary
                .failures
                .iter()
                .any(|failure| { failure.contains("ms_per_candidate_tile=8.00") })
        );
    }
}
