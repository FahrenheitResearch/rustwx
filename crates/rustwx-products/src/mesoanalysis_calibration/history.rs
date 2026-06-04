use super::aggregation::{SourceAggregateAccumulator, StationAggregateAccumulator};
use super::*;
use chrono::Utc;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

const INNOVATION_HISTORY_WATCHLIST_LIMIT: usize = 25;

pub fn build_surface_mesoanalysis_innovation_history(
    report: &SurfaceMesoanalysisCalibrationReport,
) -> SurfaceMesoanalysisInnovationHistory {
    let mut station_series =
        BTreeMap::<String, SurfaceMesoanalysisStationInnovationHistorySeries>::new();
    let mut source_series =
        BTreeMap::<String, SurfaceMesoanalysisSourceInnovationHistorySeries>::new();

    for case in &report.cases {
        let history_case = innovation_history_case(case);
        for (station_key, station) in &case.stations {
            station_series
                .entry(station_key.clone())
                .or_insert_with(|| SurfaceMesoanalysisStationInnovationHistorySeries {
                    station_id: station.station_id.clone(),
                    source: station.source.clone(),
                    aggregate: report.aggregate.stations.get(station_key).cloned(),
                    entries: Vec::new(),
                })
                .entries
                .push(SurfaceMesoanalysisStationInnovationHistoryEntry {
                    case: history_case.clone(),
                    sample_count: station.sample_count,
                    variables: station.variables.clone(),
                });
        }
        for (source_name, source) in &case.sources {
            source_series
                .entry(source_name.clone())
                .or_insert_with(|| SurfaceMesoanalysisSourceInnovationHistorySeries {
                    source: source_name.clone(),
                    aggregate: report.aggregate.sources.get(source_name).cloned(),
                    entries: Vec::new(),
                })
                .entries
                .push(SurfaceMesoanalysisSourceInnovationHistoryEntry {
                    case: history_case.clone(),
                    sampled_observation_count: source.sampled_observation_count,
                    variables: source.variables.clone(),
                });
        }
    }

    let mut history = SurfaceMesoanalysisInnovationHistory {
        schema: "rustwx.surface_mesoanalysis.innovation_history.v1".to_string(),
        generated_at: Utc::now().to_rfc3339(),
        calibration_schema: report.schema.clone(),
        calibration_generated_at: report.generated_at.clone(),
        case_count: report.loaded_case_count,
        station_series,
        source_series,
        station_watchlist: Vec::new(),
        source_watchlist: Vec::new(),
    };
    refresh_innovation_history_watchlists(&mut history);
    history
}

pub fn read_surface_mesoanalysis_innovation_history(
    path: &Path,
) -> Result<SurfaceMesoanalysisInnovationHistory, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

pub fn merge_surface_mesoanalysis_innovation_history(
    existing: Option<SurfaceMesoanalysisInnovationHistory>,
    incoming: SurfaceMesoanalysisInnovationHistory,
    max_entries_per_series: Option<usize>,
) -> SurfaceMesoanalysisInnovationHistory {
    let mut merged = existing.unwrap_or_else(|| SurfaceMesoanalysisInnovationHistory {
        schema: "rustwx.surface_mesoanalysis.innovation_history.v1".to_string(),
        generated_at: incoming.generated_at.clone(),
        calibration_schema: incoming.calibration_schema.clone(),
        calibration_generated_at: incoming.calibration_generated_at.clone(),
        case_count: 0,
        station_series: BTreeMap::new(),
        source_series: BTreeMap::new(),
        station_watchlist: Vec::new(),
        source_watchlist: Vec::new(),
    });
    merged.schema = "rustwx.surface_mesoanalysis.innovation_history.v1".to_string();
    merged.generated_at = Utc::now().to_rfc3339();
    merged.calibration_schema = incoming.calibration_schema.clone();
    merged.calibration_generated_at = incoming.calibration_generated_at.clone();

    for (key, incoming_series) in incoming.station_series {
        let series = merged.station_series.entry(key).or_insert_with(|| {
            SurfaceMesoanalysisStationInnovationHistorySeries {
                station_id: incoming_series.station_id.clone(),
                source: incoming_series.source.clone(),
                aggregate: None,
                entries: Vec::new(),
            }
        });
        series.station_id = incoming_series.station_id;
        series.source = incoming_series.source;
        series.entries = merged_station_history_entries(
            std::mem::take(&mut series.entries),
            incoming_series.entries,
            max_entries_per_series,
        );
    }

    for (key, incoming_series) in incoming.source_series {
        let series = merged.source_series.entry(key).or_insert_with(|| {
            SurfaceMesoanalysisSourceInnovationHistorySeries {
                source: incoming_series.source.clone(),
                aggregate: None,
                entries: Vec::new(),
            }
        });
        series.source = incoming_series.source;
        series.entries = merged_source_history_entries(
            std::mem::take(&mut series.entries),
            incoming_series.entries,
            max_entries_per_series,
        );
    }

    refresh_innovation_history_aggregates(&mut merged);
    merged.case_count = innovation_history_case_count(&merged);
    refresh_innovation_history_watchlists(&mut merged);
    merged
}

pub fn write_surface_mesoanalysis_innovation_history(
    path: &Path,
    history: &SurfaceMesoanalysisInnovationHistory,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(history)?)?;
    Ok(())
}

pub fn query_surface_mesoanalysis_innovation_history(
    history: &SurfaceMesoanalysisInnovationHistory,
    request: SurfaceMesoanalysisInnovationQueryRequest,
) -> SurfaceMesoanalysisInnovationQueryReport {
    let mut history = history.clone();
    refresh_innovation_history_aggregates(&mut history);
    refresh_innovation_history_watchlists(&mut history);
    let station_matches = history
        .station_watchlist
        .iter()
        .filter(|item| innovation_query_matches_station(item, &request))
        .cloned()
        .collect::<Vec<_>>();
    let source_matches = history
        .source_watchlist
        .iter()
        .filter(|item| innovation_query_matches_source(item, &request))
        .cloned()
        .collect::<Vec<_>>();

    SurfaceMesoanalysisInnovationQueryReport {
        schema: "rustwx.surface_mesoanalysis.innovation_query.v1".to_string(),
        generated_at: Utc::now().to_rfc3339(),
        history_schema: history.schema,
        history_generated_at: history.generated_at,
        history_case_count: history.case_count,
        request: request.clone(),
        matched_station_watchlist_count: station_matches.len(),
        matched_source_watchlist_count: source_matches.len(),
        station_watchlist: station_matches
            .into_iter()
            .take(request.top)
            .collect::<Vec<_>>(),
        source_watchlist: source_matches
            .into_iter()
            .take(request.top)
            .collect::<Vec<_>>(),
    }
}

pub fn write_surface_mesoanalysis_innovation_query_report(
    path: &Path,
    report: &SurfaceMesoanalysisInnovationQueryReport,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}

pub fn write_surface_mesoanalysis_innovation_wxstore_index(
    root: &Path,
    history: &SurfaceMesoanalysisInnovationHistory,
) -> Result<SurfaceMesoanalysisInnovationWxStoreIndexManifest, Box<dyn std::error::Error>> {
    fs::create_dir_all(root)?;
    let mut history = history.clone();
    refresh_innovation_history_aggregates(&mut history);
    refresh_innovation_history_watchlists(&mut history);

    let station_index_path = root.join("station_index.jsonl");
    let source_index_path = root.join("source_index.jsonl");
    let station_watchlist_path = root.join("station_watchlist.json");
    let source_watchlist_path = root.join("source_watchlist.json");
    let station_records = station_wxstore_index_records(&history);
    let source_records = source_wxstore_index_records(&history);
    write_jsonl(&station_index_path, station_records.iter())?;
    write_jsonl(&source_index_path, source_records.iter())?;
    fs::write(
        &station_watchlist_path,
        serde_json::to_vec_pretty(&history.station_watchlist)?,
    )?;
    fs::write(
        &source_watchlist_path,
        serde_json::to_vec_pretty(&history.source_watchlist)?,
    )?;

    let manifest = SurfaceMesoanalysisInnovationWxStoreIndexManifest {
        schema: "rustwx.surface_mesoanalysis.innovation_wxstore_index.v1".to_string(),
        generated_at: Utc::now().to_rfc3339(),
        history_schema: history.schema,
        history_generated_at: history.generated_at,
        history_case_count: history.case_count,
        station_series_count: history.station_series.len(),
        source_series_count: history.source_series.len(),
        station_index_path: station_index_path.clone(),
        source_index_path: source_index_path.clone(),
        station_watchlist_path: station_watchlist_path.clone(),
        source_watchlist_path: source_watchlist_path.clone(),
        query_policy: SurfaceMesoanalysisInnovationWxStoreQueryPolicy {
            station_keys: vec![
                "station_key".to_string(),
                "station_id".to_string(),
                "source".to_string(),
            ],
            source_keys: vec!["source".to_string()],
            variable_key: "variable".to_string(),
            sortable_fields: vec![
                "case_count".to_string(),
                "mean_abs_analysis_error".to_string(),
                "mean_abs_error_improvement".to_string(),
                "mean_candidate_minus_background_mae".to_string(),
                "severity_score".to_string(),
            ],
            notes: vec![
                "station_index.jsonl is one station-field aggregate per line".to_string(),
                "source_index.jsonl is one source-field aggregate per line".to_string(),
                "watchlist fields are denormalized onto matching index records when present"
                    .to_string(),
            ],
        },
    };
    fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(manifest)
}

fn write_jsonl<'a, T, I>(path: &Path, records: I) -> Result<(), Box<dyn std::error::Error>>
where
    T: Serialize + 'a,
    I: IntoIterator<Item = &'a T>,
{
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    for record in records {
        serde_json::to_writer(&mut writer, record)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

pub(super) fn station_wxstore_index_records(
    history: &SurfaceMesoanalysisInnovationHistory,
) -> Vec<SurfaceMesoanalysisInnovationWxStoreStationRecord> {
    let watchlist = history
        .station_watchlist
        .iter()
        .map(|item| {
            (
                (item.station_key.clone(), item.variable.clone()),
                item.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut records = Vec::new();
    for (station_key, series) in &history.station_series {
        let Some(aggregate) = series.aggregate.as_ref() else {
            continue;
        };
        for (variable, variable_aggregate) in &aggregate.variables {
            records.push(SurfaceMesoanalysisInnovationWxStoreStationRecord {
                station_key: station_key.clone(),
                station_id: aggregate.station_id.clone(),
                source: aggregate.source.clone(),
                variable: variable.clone(),
                case_count: variable_aggregate.case_count,
                sample_count: aggregate.sample_count,
                observation_count: variable_aggregate.observation_count,
                mean_background_error: variable_aggregate.mean_background_error,
                mean_analysis_error: variable_aggregate.mean_analysis_error,
                mean_abs_background_error: variable_aggregate.mean_abs_background_error,
                mean_abs_analysis_error: variable_aggregate.mean_abs_analysis_error,
                mean_abs_error_improvement: variable_aggregate.mean_abs_error_improvement,
                background_rmse: variable_aggregate.background_rmse,
                analysis_rmse: variable_aggregate.analysis_rmse,
                max_abs_background_error: variable_aggregate.max_abs_background_error,
                max_abs_analysis_error: variable_aggregate.max_abs_analysis_error,
                watchlist: watchlist
                    .get(&(station_key.clone(), variable.clone()))
                    .cloned(),
            });
        }
    }
    records
}

pub(super) fn source_wxstore_index_records(
    history: &SurfaceMesoanalysisInnovationHistory,
) -> Vec<SurfaceMesoanalysisInnovationWxStoreSourceRecord> {
    let watchlist = history
        .source_watchlist
        .iter()
        .map(|item| ((item.source.clone(), item.variable.clone()), item.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut records = Vec::new();
    for (source, series) in &history.source_series {
        let Some(aggregate) = series.aggregate.as_ref() else {
            continue;
        };
        for (variable, variable_aggregate) in &aggregate.variables {
            records.push(SurfaceMesoanalysisInnovationWxStoreSourceRecord {
                source: source.clone(),
                variable: variable.clone(),
                case_count: variable_aggregate.case_count,
                mean_sampled_observation_count: aggregate.mean_sampled_observation_count,
                mean_observation_count: variable_aggregate.mean_observation_count,
                mean_background_mae: variable_aggregate.mean_background_mae,
                mean_candidate_mae: variable_aggregate.mean_candidate_mae,
                mean_candidate_minus_background_mae: variable_aggregate
                    .mean_candidate_minus_background_mae,
                mean_background_rmse: variable_aggregate.mean_background_rmse,
                mean_candidate_rmse: variable_aggregate.mean_candidate_rmse,
                mean_candidate_minus_background_rmse: variable_aggregate
                    .mean_candidate_minus_background_rmse,
                candidate_beats_background_mae_case_count: variable_aggregate
                    .candidate_beats_background_mae_case_count,
                candidate_loses_background_mae_case_count: variable_aggregate
                    .candidate_loses_background_mae_case_count,
                worst_candidate_minus_background_mae: variable_aggregate
                    .worst_candidate_minus_background_mae,
                watchlist: watchlist.get(&(source.clone(), variable.clone())).cloned(),
            });
        }
    }
    records
}

fn innovation_query_matches_station(
    item: &SurfaceMesoanalysisStationInnovationWatchItem,
    request: &SurfaceMesoanalysisInnovationQueryRequest,
) -> bool {
    innovation_query_matches_min_case_count(item.case_count, request.min_case_count)
        && innovation_query_matches_one_of(
            &request.stations,
            [item.station_key.as_str(), item.station_id.as_str()],
        )
        && innovation_query_matches_one_of(&request.sources, [item.source.as_str()])
        && innovation_query_matches_one_of(&request.variables, [item.variable.as_str()])
}

fn innovation_query_matches_source(
    item: &SurfaceMesoanalysisSourceInnovationWatchItem,
    request: &SurfaceMesoanalysisInnovationQueryRequest,
) -> bool {
    request.stations.is_empty()
        && innovation_query_matches_min_case_count(item.case_count, request.min_case_count)
        && innovation_query_matches_one_of(&request.sources, [item.source.as_str()])
        && innovation_query_matches_one_of(&request.variables, [item.variable.as_str()])
}

fn innovation_query_matches_min_case_count(
    case_count: usize,
    min_case_count: Option<usize>,
) -> bool {
    min_case_count
        .map(|threshold| case_count >= threshold)
        .unwrap_or(true)
}

fn innovation_query_matches_one_of<'a, I>(requested: &[String], candidates: I) -> bool
where
    I: IntoIterator<Item = &'a str>,
{
    requested.is_empty()
        || candidates
            .into_iter()
            .any(|candidate| requested.iter().any(|requested| requested == candidate))
}

fn merged_station_history_entries(
    existing: Vec<SurfaceMesoanalysisStationInnovationHistoryEntry>,
    incoming: Vec<SurfaceMesoanalysisStationInnovationHistoryEntry>,
    max_entries_per_series: Option<usize>,
) -> Vec<SurfaceMesoanalysisStationInnovationHistoryEntry> {
    let mut entries_by_case = existing
        .into_iter()
        .map(|entry| (innovation_history_case_key(&entry.case), entry))
        .collect::<BTreeMap<_, _>>();
    for entry in incoming {
        entries_by_case.insert(innovation_history_case_key(&entry.case), entry);
    }
    retained_history_entries(
        entries_by_case.into_values().collect(),
        max_entries_per_series,
    )
}

fn merged_source_history_entries(
    existing: Vec<SurfaceMesoanalysisSourceInnovationHistoryEntry>,
    incoming: Vec<SurfaceMesoanalysisSourceInnovationHistoryEntry>,
    max_entries_per_series: Option<usize>,
) -> Vec<SurfaceMesoanalysisSourceInnovationHistoryEntry> {
    let mut entries_by_case = existing
        .into_iter()
        .map(|entry| (innovation_history_case_key(&entry.case), entry))
        .collect::<BTreeMap<_, _>>();
    for entry in incoming {
        entries_by_case.insert(innovation_history_case_key(&entry.case), entry);
    }
    retained_history_entries(
        entries_by_case.into_values().collect(),
        max_entries_per_series,
    )
}

fn retained_history_entries<T>(mut entries: Vec<T>, max_entries_per_series: Option<usize>) -> Vec<T>
where
    T: InnovationHistoryEntry,
{
    entries.sort_by_key(|entry| innovation_history_case_sort_key(entry.history_case()));
    if let Some(limit) = max_entries_per_series {
        if entries.len() > limit {
            entries.drain(0..entries.len() - limit);
        }
    }
    entries
}

trait InnovationHistoryEntry {
    fn history_case(&self) -> &SurfaceMesoanalysisInnovationHistoryCase;
}

impl InnovationHistoryEntry for SurfaceMesoanalysisStationInnovationHistoryEntry {
    fn history_case(&self) -> &SurfaceMesoanalysisInnovationHistoryCase {
        &self.case
    }
}

impl InnovationHistoryEntry for SurfaceMesoanalysisSourceInnovationHistoryEntry {
    fn history_case(&self) -> &SurfaceMesoanalysisInnovationHistoryCase {
        &self.case
    }
}

fn refresh_innovation_history_aggregates(history: &mut SurfaceMesoanalysisInnovationHistory) {
    for series in history.station_series.values_mut() {
        let mut accumulator =
            StationAggregateAccumulator::new(series.station_id.clone(), series.source.clone());
        for entry in &series.entries {
            accumulator.push(&SurfaceMesoanalysisCalibrationStationCase {
                station_id: series.station_id.clone(),
                source: series.source.clone(),
                sample_count: entry.sample_count,
                variables: entry.variables.clone(),
            });
        }
        series.aggregate = Some(accumulator.finish());
    }

    for series in history.source_series.values_mut() {
        let mut accumulator = SourceAggregateAccumulator::default();
        for entry in &series.entries {
            accumulator.push(&SurfaceMesoanalysisCalibrationSourceCase {
                sampled_observation_count: entry.sampled_observation_count,
                variables: entry.variables.clone(),
            });
        }
        series.aggregate = Some(accumulator.finish());
    }
}

fn refresh_innovation_history_watchlists(history: &mut SurfaceMesoanalysisInnovationHistory) {
    history.station_watchlist = station_innovation_watchlist(&history.station_series);
    history.source_watchlist = source_innovation_watchlist(&history.source_series);
}

fn station_innovation_watchlist(
    series: &BTreeMap<String, SurfaceMesoanalysisStationInnovationHistorySeries>,
) -> Vec<SurfaceMesoanalysisStationInnovationWatchItem> {
    let mut items = Vec::new();
    for (station_key, series) in series {
        let Some(aggregate) = series.aggregate.as_ref() else {
            continue;
        };
        for (variable, variable_aggregate) in &aggregate.variables {
            let mean_abs_analysis_error = variable_aggregate.mean_abs_analysis_error;
            let abs_analysis_bias = variable_aggregate.mean_analysis_error.map(f64::abs);
            let mean_abs_error_improvement = variable_aggregate.mean_abs_error_improvement;
            let severity_score = station_innovation_severity(
                mean_abs_analysis_error,
                abs_analysis_bias,
                mean_abs_error_improvement,
                variable_aggregate.max_abs_analysis_error,
            );
            if severity_score <= 0.0 {
                continue;
            }
            items.push(SurfaceMesoanalysisStationInnovationWatchItem {
                station_key: station_key.clone(),
                station_id: aggregate.station_id.clone(),
                source: aggregate.source.clone(),
                variable: variable.clone(),
                case_count: variable_aggregate.case_count,
                observation_count: variable_aggregate.observation_count,
                mean_abs_analysis_error,
                abs_analysis_bias,
                mean_abs_error_improvement,
                max_abs_analysis_error: variable_aggregate.max_abs_analysis_error,
                severity_score,
                reason: station_innovation_watch_reason(
                    mean_abs_analysis_error,
                    abs_analysis_bias,
                    mean_abs_error_improvement,
                ),
            });
        }
    }
    items.sort_by(|left, right| {
        right
            .severity_score
            .total_cmp(&left.severity_score)
            .then_with(|| left.station_key.cmp(&right.station_key))
            .then_with(|| left.variable.cmp(&right.variable))
    });
    items.truncate(INNOVATION_HISTORY_WATCHLIST_LIMIT);
    items
}

fn source_innovation_watchlist(
    series: &BTreeMap<String, SurfaceMesoanalysisSourceInnovationHistorySeries>,
) -> Vec<SurfaceMesoanalysisSourceInnovationWatchItem> {
    let mut items = Vec::new();
    for (source, series) in series {
        let Some(aggregate) = series.aggregate.as_ref() else {
            continue;
        };
        for (variable, variable_aggregate) in &aggregate.variables {
            let severity_score = source_innovation_severity(
                variable_aggregate.mean_candidate_mae,
                variable_aggregate.mean_candidate_minus_background_mae,
                variable_aggregate.worst_candidate_minus_background_mae,
            );
            if severity_score <= 0.0 {
                continue;
            }
            items.push(SurfaceMesoanalysisSourceInnovationWatchItem {
                source: source.clone(),
                variable: variable.clone(),
                case_count: variable_aggregate.case_count,
                mean_observation_count: variable_aggregate.mean_observation_count,
                mean_candidate_mae: variable_aggregate.mean_candidate_mae,
                mean_candidate_minus_background_mae: variable_aggregate
                    .mean_candidate_minus_background_mae,
                worst_candidate_minus_background_mae: variable_aggregate
                    .worst_candidate_minus_background_mae,
                severity_score,
                reason: source_innovation_watch_reason(
                    variable_aggregate.mean_candidate_minus_background_mae,
                    variable_aggregate.worst_candidate_minus_background_mae,
                ),
            });
        }
    }
    items.sort_by(|left, right| {
        right
            .severity_score
            .total_cmp(&left.severity_score)
            .then_with(|| left.source.cmp(&right.source))
            .then_with(|| left.variable.cmp(&right.variable))
    });
    items.truncate(INNOVATION_HISTORY_WATCHLIST_LIMIT);
    items
}

fn station_innovation_severity(
    mean_abs_analysis_error: Option<f64>,
    abs_analysis_bias: Option<f64>,
    mean_abs_error_improvement: Option<f64>,
    max_abs_analysis_error: Option<f64>,
) -> f64 {
    let analysis_mae = mean_abs_analysis_error.unwrap_or(0.0);
    let bias = abs_analysis_bias.unwrap_or(0.0);
    let negative_improvement_penalty = mean_abs_error_improvement
        .filter(|value| *value < 0.0)
        .map(f64::abs)
        .unwrap_or(0.0);
    let max_error_tail = max_abs_analysis_error.unwrap_or(0.0) * 0.1;
    analysis_mae + bias * 0.25 + negative_improvement_penalty + max_error_tail
}

fn source_innovation_severity(
    mean_candidate_mae: Option<f64>,
    mean_candidate_minus_background_mae: Option<f64>,
    worst_candidate_minus_background_mae: Option<f64>,
) -> f64 {
    let analysis_mae = mean_candidate_mae.unwrap_or(0.0);
    let mean_loss_penalty = mean_candidate_minus_background_mae
        .filter(|value| *value > 0.0)
        .unwrap_or(0.0);
    let worst_loss_penalty = worst_candidate_minus_background_mae
        .filter(|value| *value > 0.0)
        .unwrap_or(0.0)
        * 0.5;
    analysis_mae + mean_loss_penalty + worst_loss_penalty
}

fn station_innovation_watch_reason(
    mean_abs_analysis_error: Option<f64>,
    abs_analysis_bias: Option<f64>,
    mean_abs_error_improvement: Option<f64>,
) -> String {
    if mean_abs_error_improvement
        .map(|value| value < 0.0)
        .unwrap_or(false)
    {
        "analysis_worse_than_background".to_string()
    } else if abs_analysis_bias.unwrap_or(0.0) >= mean_abs_analysis_error.unwrap_or(0.0) * 0.75 {
        "persistent_station_bias".to_string()
    } else {
        "high_station_analysis_error".to_string()
    }
}

fn source_innovation_watch_reason(
    mean_candidate_minus_background_mae: Option<f64>,
    worst_candidate_minus_background_mae: Option<f64>,
) -> String {
    if mean_candidate_minus_background_mae
        .map(|value| value > 0.0)
        .unwrap_or(false)
    {
        "source_mean_worse_than_background".to_string()
    } else if worst_candidate_minus_background_mae
        .map(|value| value > 0.0)
        .unwrap_or(false)
    {
        "source_has_worse_case_than_background".to_string()
    } else {
        "high_source_analysis_error".to_string()
    }
}

fn innovation_history_case_count(history: &SurfaceMesoanalysisInnovationHistory) -> usize {
    let mut cases = BTreeMap::new();
    for series in history.station_series.values() {
        for entry in &series.entries {
            cases.insert(innovation_history_case_key(&entry.case), ());
        }
    }
    for series in history.source_series.values() {
        for entry in &series.entries {
            cases.insert(innovation_history_case_key(&entry.case), ());
        }
    }
    cases.len()
}

fn innovation_history_case_key(case: &SurfaceMesoanalysisInnovationHistoryCase) -> String {
    format!(
        "{}|{}|{}|{}",
        case.case_signature,
        non_empty_or_missing(&case.benchmark_mode),
        case.holdout_strategy.as_deref().unwrap_or("<missing>"),
        case.case_tags.join(",")
    )
}

fn innovation_history_case_sort_key(case: &SurfaceMesoanalysisInnovationHistoryCase) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        non_empty_or_missing(&case.date),
        cycle_key(case.cycle),
        forecast_hour_key(case.forecast_hour),
        non_empty_or_missing(&case.model),
        non_empty_or_missing(&case.model_source),
        innovation_history_case_key(case)
    )
}

fn innovation_history_case(
    case: &SurfaceMesoanalysisCalibrationCase,
) -> SurfaceMesoanalysisInnovationHistoryCase {
    SurfaceMesoanalysisInnovationHistoryCase {
        source_path: case.source_path.clone(),
        case_signature: case_signature(case),
        model: case.model.clone(),
        model_source: case.model_source.clone(),
        model_cycle: case.model_cycle.clone(),
        date: case.date.clone(),
        cycle: case.cycle,
        forecast_hour: case.forecast_hour,
        benchmark_mode: case.benchmark_mode.clone(),
        holdout_strategy: case.holdout_strategy.clone(),
        case_tags: case.case_tags.clone(),
    }
}
