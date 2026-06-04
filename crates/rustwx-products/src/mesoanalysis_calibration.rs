use serde_json::Value;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

mod types;
pub use types::*;
mod gates;
pub use gates::evaluate_surface_mesoanalysis_calibration_gate;
mod aggregation;
mod confidence;
mod helpers;
mod history;
mod parsing;
mod summaries;
#[cfg(test)]
use crate::mesoanalysis::{
    CONFIDENCE_RELIABILITY_MAX_RANKED_HIGH_MINUS_LOW_MAE,
    CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS,
};
#[cfg(test)]
use gates::push_max_confidence_calibration_gate_check;
pub use history::{
    build_surface_mesoanalysis_innovation_history, merge_surface_mesoanalysis_innovation_history,
    query_surface_mesoanalysis_innovation_history, read_surface_mesoanalysis_innovation_history,
    write_surface_mesoanalysis_innovation_history,
    write_surface_mesoanalysis_innovation_query_report,
    write_surface_mesoanalysis_innovation_wxstore_index,
};
#[cfg(test)]
use history::{source_wxstore_index_records, station_wxstore_index_records};
use parsing::build_surface_mesoanalysis_calibration_report_from_values;

pub fn discover_surface_mesoanalysis_run_reports(roots: &[PathBuf]) -> io::Result<Vec<PathBuf>> {
    let mut reports = Vec::new();
    for root in roots {
        discover_surface_mesoanalysis_run_reports_one(root, &mut reports)?;
    }
    reports.sort();
    reports.dedup();
    Ok(reports)
}

pub fn build_surface_mesoanalysis_calibration_report(
    paths: &[PathBuf],
) -> SurfaceMesoanalysisCalibrationReport {
    let values = paths.iter().map(|path| {
        let value = fs::read(path)
            .map_err(|error| format!("read failed: {error}"))
            .and_then(|bytes| {
                serde_json::from_slice::<Value>(&bytes)
                    .map_err(|error| format!("JSON parse failed: {error}"))
            });
        (path.clone(), value)
    });
    build_surface_mesoanalysis_calibration_report_from_values(values)
}

pub fn write_surface_mesoanalysis_calibration_report(
    path: &Path,
    report: &SurfaceMesoanalysisCalibrationReport,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}

fn discover_surface_mesoanalysis_run_reports_one(
    root: &Path,
    reports: &mut Vec<PathBuf>,
) -> io::Result<()> {
    let metadata = fs::metadata(root)?;
    if metadata.is_file() {
        if root.file_name().and_then(|name| name.to_str()) == Some("run_report.json") {
            reports.push(root.to_path_buf());
        }
        return Ok(());
    }
    if !metadata.is_dir() {
        return Ok(());
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries {
            let Ok(entry) = entry else {
                continue;
            };
            let path = entry.path();
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if metadata.is_dir() {
                stack.push(path);
            } else if path.file_name().and_then(|name| name.to_str()) == Some("run_report.json") {
                reports.push(path);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod contract_tests;

#[cfg(test)]
mod tests;
