use super::SurfaceMesoanalysisCalibrationCase;
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) fn string_at(value: &Value, path: &[&str]) -> Option<String> {
    value_at(value, path)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

pub(super) fn string_vec_at(value: &Value, path: &[&str]) -> Vec<String> {
    let Some(value) = value_at(value, path) else {
        return Vec::new();
    };
    if let Some(raw) = value.as_str() {
        return vec![raw.to_string()];
    }
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
}

pub(super) fn normalized_case_tags(raw_tags: &[String]) -> Vec<String> {
    let mut tags = raw_tags
        .iter()
        .map(|tag| tag.trim())
        .filter(|tag| !tag.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    tags.sort();
    tags.dedup();
    tags
}

pub(super) fn bool_at(value: &Value, path: &[&str]) -> Option<bool> {
    value_at(value, path).and_then(Value::as_bool)
}

pub(super) fn usize_at(value: &Value, path: &[&str]) -> Option<usize> {
    u64_at(value, path).and_then(|value| usize::try_from(value).ok())
}

pub(super) fn u64_at(value: &Value, path: &[&str]) -> Option<u64> {
    value_at(value, path).and_then(Value::as_u64)
}

pub(super) fn f64_at(value: &Value, path: &[&str]) -> Option<f64> {
    value_at(value, path)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
}

pub(super) fn value_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    if current.is_null() {
        None
    } else {
        Some(current)
    }
}

pub(super) fn push_f64(values: &mut Vec<f64>, value: Option<f64>) {
    if let Some(value) = value.filter(|value| value.is_finite()) {
        values.push(value);
    }
}

pub(super) fn push_usize_as_f64(values: &mut Vec<f64>, value: Option<usize>) {
    if let Some(value) = value {
        values.push(value as f64);
    }
}

pub(super) fn push_u128_as_f64(values: &mut Vec<f64>, value: Option<u128>) {
    if let Some(value) = value {
        values.push(value as f64);
    }
}

pub(super) fn increment_count(counts: &mut BTreeMap<String, usize>, key: String) {
    *counts.entry(key).or_insert(0usize) += 1;
}

pub(super) fn non_empty_or_missing(value: &str) -> String {
    if value.is_empty() {
        "<missing>".to_string()
    } else {
        value.to_string()
    }
}

pub(super) fn cycle_key(cycle: Option<u8>) -> String {
    cycle
        .map(|cycle| format!("{cycle:02}"))
        .unwrap_or_else(|| "<missing>".to_string())
}

pub(super) fn forecast_hour_key(forecast_hour: u16) -> String {
    format!("f{forecast_hour:03}")
}

pub(super) fn case_signature(case: &SurfaceMesoanalysisCalibrationCase) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        non_empty_or_missing(&case.model),
        non_empty_or_missing(&case.model_source),
        non_empty_or_missing(&case.date),
        cycle_key(case.cycle),
        forecast_hour_key(case.forecast_hour)
    )
}

pub(super) fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

pub(super) fn weighted_mean(weighted_sum: f64, weight: usize) -> Option<f64> {
    if weight == 0 {
        None
    } else {
        Some(weighted_sum / weight as f64)
    }
}

pub(super) fn weighted_rmse(square_weighted_sum: f64, weight: usize) -> Option<f64> {
    weighted_mean(square_weighted_sum, weight).map(f64::sqrt)
}

pub(super) fn mean_if_count(sum: f64, count: usize) -> Option<f64> {
    if count == 0 {
        None
    } else {
        Some(sum / count as f64)
    }
}

pub(super) fn rmse_if_count(square_sum: f64, count: usize) -> Option<f64> {
    mean_if_count(square_sum, count).map(f64::sqrt)
}

pub(super) fn max(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(f64::max)
}

pub(super) fn min(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(f64::min)
}

pub(super) fn option_delta(candidate: Option<f64>, baseline: Option<f64>) -> Option<f64> {
    candidate
        .zip(baseline)
        .map(|(candidate, baseline)| candidate - baseline)
}

pub(super) fn option_less_than_zero(value: Option<f64>) -> bool {
    value.map(|value| value < 0.0).unwrap_or(false)
}

pub(super) fn option_greater_than_zero(value: Option<f64>) -> bool {
    value.map(|value| value > 0.0).unwrap_or(false)
}
