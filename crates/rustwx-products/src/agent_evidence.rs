use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::f64::consts::PI;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceParseError {
    pub message: String,
}

impl EvidenceParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for EvidenceParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for EvidenceParseError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceGap {
    ClimateTeleconnections,
    GlobalExtendedGuidance,
    VerificationData,
    AutomatedFeatureTracking,
    SevereDiagnostics,
    LiteratureContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceCapability {
    pub gap: EvidenceGap,
    pub name: &'static str,
    pub all_rust_artifact: &'static str,
    pub evidence_inputs: Vec<&'static str>,
    pub agent_outputs: Vec<&'static str>,
    pub remaining_external_needs: Vec<&'static str>,
}

pub fn agent_evidence_capabilities() -> Vec<EvidenceCapability> {
    vec![
        EvidenceCapability {
            gap: EvidenceGap::ClimateTeleconnections,
            name: "teleconnection and tropical-forcing table ingest",
            all_rust_artifact: "agent_evidence::{parse_teleconnection_table, parse_rmm_table}",
            evidence_inputs: vec![
                "daily whitespace/CSV index tables",
                "RMM/MJO year-month-day phase tables",
                "runner-published global static plots",
            ],
            agent_outputs: vec![
                "dated index values",
                "RMM phase/amplitude records",
                "explicit source/provenance-ready records",
            ],
            remaining_external_needs: vec![
                "operational feed selection for ENSO subsurface and AAM",
                "scheduled runner mirroring of CPC/BOM-style source tables",
            ],
        },
        EvidenceCapability {
            gap: EvidenceGap::GlobalExtendedGuidance,
            name: "global/extended guidance evidence binding",
            all_rust_artifact: "rustwx-models URL resolvers plus agent_evidence diagnostics",
            evidence_inputs: vec![
                "GFS/GEFS/ECMWF/AIFS/AIGFS/HGEFS model fields",
                "300/500/700/850 mb static plots",
                "sampled jet/trough feature points",
            ],
            agent_outputs: vec![
                "jet translation speed",
                "feature heading",
                "cycle-to-cycle feature displacement",
            ],
            remaining_external_needs: vec![
                "runner jobs for every desired global field/product",
                "objective feature extraction from gridded fields instead of hand-picked points",
            ],
        },
        EvidenceCapability {
            gap: EvidenceGap::VerificationData,
            name: "observed verification table ingest",
            all_rust_artifact: "agent_evidence::{parse_spc_storm_reports_csv, parse_metar_line, parse_sounding_text}",
            evidence_inputs: vec![
                "SPC-style storm report CSV",
                "raw METAR lines",
                "fixed-width observed sounding text",
                "radar sidecar samples from wxstore",
            ],
            agent_outputs: vec![
                "typed storm reports",
                "surface obs records",
                "observed sounding levels",
                "verification-ready point evidence",
            ],
            remaining_external_needs: vec![
                "official source polling and archive retention",
                "NWS warning/LSR polygons and tornado-track shapefiles",
            ],
        },
        EvidenceCapability {
            gap: EvidenceGap::AutomatedFeatureTracking,
            name: "feature motion and line-relative diagnostics",
            all_rust_artifact: "agent_evidence::{compute_feature_translation, line_relative_shear}",
            evidence_inputs: vec![
                "feature lat/lon/time samples",
                "line or boundary azimuth",
                "layer wind vectors",
                "radar/satellite/model-derived boundary points",
            ],
            agent_outputs: vec![
                "translation speed and heading",
                "line-parallel shear",
                "line-normal shear",
                "QLCS/dryline-relative flow evidence",
            ],
            remaining_external_needs: vec![
                "automated boundary picking from imagery/fields",
                "persistent feature-track store in runner output",
            ],
        },
        EvidenceCapability {
            gap: EvidenceGap::SevereDiagnostics,
            name: "agent severe-parameter decomposition",
            all_rust_artifact: "agent_evidence::{decompose_ehi, effective_fixed_stp, cold_pool_recovery}",
            evidence_inputs: vec![
                "CAPE/SRH/CIN/LCL/deep shear samples",
                "before/after cold-pool thermodynamic samples",
                "soundings and model point samples",
            ],
            agent_outputs: vec![
                "EHI component attribution",
                "fixed-layer STP term breakdown",
                "cold-pool recovery deltas",
            ],
            remaining_external_needs: vec![
                "hail-growth-zone and parcel-suite publication as first-class fields",
                "observed mesonet/RAOB verification feeds",
            ],
        },
        EvidenceCapability {
            gap: EvidenceGap::LiteratureContext,
            name: "source-aware conceptual evidence slots",
            all_rust_artifact: "agent_evidence catalog schemas",
            evidence_inputs: vec![
                "agent-curated citation metadata",
                "radar/model/sounding evidence attached to concepts",
            ],
            agent_outputs: vec![
                "clear split between fetched evidence and literature claims",
                "machine-readable unsupported-claim flags",
            ],
            remaining_external_needs: vec![
                "curated severe-weather literature index",
                "citation fetcher/search integration",
            ],
        },
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeleconnectionIndex {
    Ao,
    Nao,
    Pna,
    Epo,
    Wpo,
    Enso34,
    Aam,
    Other,
}

impl TeleconnectionIndex {
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "ao" => Self::Ao,
            "nao" => Self::Nao,
            "pna" => Self::Pna,
            "epo" => Self::Epo,
            "wpo" => Self::Wpo,
            "enso34" | "nino34" | "nino_3_4" | "nino3.4" => Self::Enso34,
            "aam" => Self::Aam,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeleconnectionPoint {
    pub index: TeleconnectionIndex,
    pub date: String,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RmmPoint {
    pub date: String,
    pub rmm1: f64,
    pub rmm2: f64,
    pub phase: u8,
    pub amplitude: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StormReport {
    pub report_type: Option<String>,
    pub time_utc: Option<String>,
    pub magnitude: Option<String>,
    pub location: Option<String>,
    pub county: Option<String>,
    pub state: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub comments: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SoundingLevel {
    pub pressure_hpa: f64,
    pub height_m: Option<f64>,
    pub temperature_c: Option<f64>,
    pub dewpoint_c: Option<f64>,
    pub wind_dir_deg: Option<f64>,
    pub wind_speed_kt: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservedSounding {
    pub source: Option<String>,
    pub levels: Vec<SoundingLevel>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetarObservation {
    pub station: String,
    pub observation_time: Option<String>,
    pub wind_dir_deg: Option<u16>,
    pub wind_speed_kt: Option<u16>,
    pub wind_gust_kt: Option<u16>,
    pub temperature_c: Option<f64>,
    pub dewpoint_c: Option<f64>,
    pub altimeter_hpa: Option<f64>,
    pub raw: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WindVector {
    pub u_ms: f64,
    pub v_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LineRelativeShear {
    pub line_azimuth_deg: f64,
    pub shear_u_ms: f64,
    pub shear_v_ms: f64,
    pub parallel_ms: f64,
    pub normal_ms: f64,
    pub magnitude_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimedGeoPoint {
    pub time_epoch_seconds: i64,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeatureTranslation {
    pub point_count: usize,
    pub elapsed_seconds: i64,
    pub distance_km: f64,
    pub speed_ms: f64,
    pub speed_kt: f64,
    pub heading_deg: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EhiDecomposition {
    pub cape_j_kg: f64,
    pub srh_m2_s2: f64,
    pub value: f64,
    pub cape_term: f64,
    pub srh_term: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FixedStpBreakdown {
    pub mlcape_j_kg: f64,
    pub srh01_m2_s2: f64,
    pub bulk06_ms: f64,
    pub mllcl_m: f64,
    pub mlcin_j_kg: f64,
    pub cape_term: f64,
    pub srh_term: f64,
    pub shear_term: f64,
    pub lcl_term: f64,
    pub cin_term: f64,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RecoverySample {
    pub temperature_c: f64,
    pub dewpoint_c: f64,
    pub sbcape_j_kg: f64,
    pub mlcape_j_kg: f64,
    pub cin_j_kg: f64,
    pub lcl_m: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColdPoolRecovery {
    pub delta_temperature_c: f64,
    pub delta_dewpoint_c: f64,
    pub delta_sbcape_j_kg: f64,
    pub delta_mlcape_j_kg: f64,
    pub delta_cin_j_kg: f64,
    pub delta_lcl_m: f64,
    pub recovered_instability: bool,
    pub reduced_inhibition: bool,
    pub improved_low_lcl: bool,
}

pub fn parse_teleconnection_table(
    index: TeleconnectionIndex,
    content: &str,
) -> Result<Vec<TeleconnectionPoint>, EvidenceParseError> {
    let mut points = Vec::new();
    for line in content.lines() {
        let trimmed = strip_comment(line);
        if trimmed.is_empty() || !starts_like_data(trimmed) {
            continue;
        }
        let fields = split_table_fields(trimmed);
        if let Some((date, value)) = parse_date_value(&fields) {
            points.push(TeleconnectionPoint { index, date, value });
        }
    }
    if points.is_empty() {
        return Err(EvidenceParseError::new(
            "teleconnection table did not contain parseable date/value rows",
        ));
    }
    Ok(points)
}

pub fn parse_rmm_table(content: &str) -> Result<Vec<RmmPoint>, EvidenceParseError> {
    let mut points = Vec::new();
    for line in content.lines() {
        let trimmed = strip_comment(line);
        if trimmed.is_empty() || !starts_like_data(trimmed) {
            continue;
        }
        let fields = split_table_fields(trimmed);
        if fields.len() < 7 {
            continue;
        }
        let Some(year) = parse_i32_field(&fields[0]) else {
            continue;
        };
        let Some(month) = parse_u32_field(&fields[1]) else {
            continue;
        };
        let Some(day) = parse_u32_field(&fields[2]) else {
            continue;
        };
        let Some(rmm1) = parse_f64_field(&fields[3]) else {
            continue;
        };
        let Some(rmm2) = parse_f64_field(&fields[4]) else {
            continue;
        };
        let Some(phase) = parse_u32_field(&fields[5]) else {
            continue;
        };
        let Some(amplitude) = parse_f64_field(&fields[6]) else {
            continue;
        };
        if !(1..=8).contains(&phase) {
            continue;
        }
        points.push(RmmPoint {
            date: format!("{year:04}-{month:02}-{day:02}"),
            rmm1,
            rmm2,
            phase: phase as u8,
            amplitude,
        });
    }
    if points.is_empty() {
        return Err(EvidenceParseError::new(
            "RMM table did not contain parseable year/month/day/rmm1/rmm2/phase/amplitude rows",
        ));
    }
    Ok(points)
}

pub fn parse_spc_storm_reports_csv(content: &str) -> Result<Vec<StormReport>, EvidenceParseError> {
    let mut rows = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(parse_csv_line);
    let Some(header) = rows.next() else {
        return Err(EvidenceParseError::new("storm report CSV is empty"));
    };
    let header_map = header
        .iter()
        .enumerate()
        .map(|(index, name)| (normalize_header(name), index))
        .collect::<BTreeMap<_, _>>();

    let mut reports = Vec::new();
    for row in rows {
        let field = |names: &[&str]| -> Option<String> {
            names
                .iter()
                .find_map(|name| header_map.get(*name))
                .and_then(|idx| row.get(*idx))
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        reports.push(StormReport {
            report_type: field(&["type", "eventtype", "reporttype"]),
            time_utc: field(&["time", "timeutc", "validtime"]),
            magnitude: field(&["fscale", "f_scale", "magnitude", "speed", "size"]),
            location: field(&["location", "city"]),
            county: field(&["county", "countyname"]),
            state: field(&["state", "st"]),
            lat: field(&["lat", "latitude"]).and_then(|value| value.parse().ok()),
            lon: field(&["lon", "longitude"]).and_then(|value| value.parse().ok()),
            comments: field(&["comments", "comment", "remarks"]),
        });
    }
    if reports.is_empty() {
        return Err(EvidenceParseError::new(
            "storm report CSV did not contain data rows",
        ));
    }
    Ok(reports)
}

pub fn parse_sounding_text(content: &str) -> Result<ObservedSounding, EvidenceParseError> {
    let source = content
        .lines()
        .find(|line| line.contains("Observations at") || line.contains("Station"))
        .map(|line| line.trim().to_string());
    let mut levels = Vec::new();
    for line in content.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 8 {
            continue;
        }
        let Some(pressure_hpa) = parse_f64_field(fields[0]) else {
            continue;
        };
        if !(50.0..=1100.0).contains(&pressure_hpa) {
            continue;
        }
        levels.push(SoundingLevel {
            pressure_hpa,
            height_m: parse_f64_field(fields[1]),
            temperature_c: parse_f64_field(fields[2]),
            dewpoint_c: parse_f64_field(fields[3]),
            wind_dir_deg: parse_f64_field(fields[6]).or_else(|| parse_f64_field(fields[4])),
            wind_speed_kt: parse_f64_field(fields[7]).or_else(|| parse_f64_field(fields[5])),
        });
    }
    if levels.is_empty() {
        return Err(EvidenceParseError::new(
            "sounding text did not contain parseable pressure levels",
        ));
    }
    Ok(ObservedSounding { source, levels })
}

pub fn parse_metar_line(raw: &str) -> Result<MetarObservation, EvidenceParseError> {
    let tokens = raw.split_whitespace().collect::<Vec<_>>();
    if tokens.is_empty() {
        return Err(EvidenceParseError::new("METAR line is empty"));
    }
    let offset = usize::from(matches!(tokens[0], "METAR" | "SPECI"));
    let Some(station_token) = tokens.get(offset) else {
        return Err(EvidenceParseError::new("METAR line is missing station id"));
    };
    let station = (*station_token).to_string();
    let observation_time = tokens
        .iter()
        .copied()
        .find(|token| token.len() == 7 && token.ends_with('Z'))
        .map(str::to_string);
    let wind = tokens
        .iter()
        .copied()
        .find(|token| token.ends_with("KT") || token.ends_with("MPS"));
    let temp_dew = tokens
        .iter()
        .copied()
        .find(|token| token.contains('/') && token.chars().any(|c| c.is_ascii_digit()));
    let altimeter = tokens
        .iter()
        .copied()
        .find(|token| token.starts_with('A') || token.starts_with('Q'));
    let (wind_dir_deg, wind_speed_kt, wind_gust_kt) =
        wind.map(parse_metar_wind).unwrap_or((None, None, None));
    let (temperature_c, dewpoint_c) = temp_dew.map(parse_metar_temp_dew).unwrap_or((None, None));
    Ok(MetarObservation {
        station,
        observation_time,
        wind_dir_deg,
        wind_speed_kt,
        wind_gust_kt,
        temperature_c,
        dewpoint_c,
        altimeter_hpa: altimeter.and_then(parse_metar_altimeter_hpa),
        raw: raw.to_string(),
    })
}

pub fn line_relative_shear(
    line_azimuth_deg: f64,
    bottom: WindVector,
    top: WindVector,
) -> LineRelativeShear {
    let shear_u_ms = top.u_ms - bottom.u_ms;
    let shear_v_ms = top.v_ms - bottom.v_ms;
    let azimuth_rad = line_azimuth_deg.to_radians();
    let along_u = azimuth_rad.sin();
    let along_v = azimuth_rad.cos();
    let normal_u = along_v;
    let normal_v = -along_u;
    let parallel_ms = shear_u_ms * along_u + shear_v_ms * along_v;
    let normal_ms = shear_u_ms * normal_u + shear_v_ms * normal_v;
    LineRelativeShear {
        line_azimuth_deg: normalize_degrees(line_azimuth_deg),
        shear_u_ms,
        shear_v_ms,
        parallel_ms,
        normal_ms,
        magnitude_ms: shear_u_ms.hypot(shear_v_ms),
    }
}

pub fn compute_feature_translation(
    points: &[TimedGeoPoint],
) -> Result<FeatureTranslation, EvidenceParseError> {
    if points.len() < 2 {
        return Err(EvidenceParseError::new(
            "feature translation requires at least two points",
        ));
    }
    let mut sorted = points.to_vec();
    sorted.sort_by_key(|point| point.time_epoch_seconds);
    let first = sorted.first().unwrap();
    let last = sorted.last().unwrap();
    let elapsed_seconds = last.time_epoch_seconds - first.time_epoch_seconds;
    if elapsed_seconds <= 0 {
        return Err(EvidenceParseError::new(
            "feature translation requires increasing timestamps",
        ));
    }
    let distance_km = haversine_km(first.lat, first.lon, last.lat, last.lon);
    let speed_ms = distance_km * 1000.0 / elapsed_seconds as f64;
    Ok(FeatureTranslation {
        point_count: sorted.len(),
        elapsed_seconds,
        distance_km,
        speed_ms,
        speed_kt: speed_ms * 1.943_844,
        heading_deg: bearing_deg(first.lat, first.lon, last.lat, last.lon),
    })
}

pub fn decompose_ehi(cape_j_kg: f64, srh_m2_s2: f64) -> EhiDecomposition {
    let cape_term = cape_j_kg / 1600.0;
    let srh_term = srh_m2_s2 / 100.0;
    EhiDecomposition {
        cape_j_kg,
        srh_m2_s2,
        value: cape_j_kg * srh_m2_s2 / 160_000.0,
        cape_term,
        srh_term,
    }
}

pub fn effective_fixed_stp(
    mlcape_j_kg: f64,
    srh01_m2_s2: f64,
    bulk06_ms: f64,
    mllcl_m: f64,
    mlcin_j_kg: f64,
) -> FixedStpBreakdown {
    let cape_term = (mlcape_j_kg / 1500.0).max(0.0);
    let srh_term = (srh01_m2_s2 / 150.0).max(0.0);
    let shear_term = (bulk06_ms / 20.0).clamp(0.0, 1.5);
    let lcl_term = ((2000.0 - mllcl_m) / 1000.0).clamp(0.0, 1.0);
    let cin_term = ((150.0 + mlcin_j_kg) / 125.0).clamp(0.0, 1.0);
    FixedStpBreakdown {
        mlcape_j_kg,
        srh01_m2_s2,
        bulk06_ms,
        mllcl_m,
        mlcin_j_kg,
        cape_term,
        srh_term,
        shear_term,
        lcl_term,
        cin_term,
        value: cape_term * srh_term * shear_term * lcl_term * cin_term,
    }
}

pub fn cold_pool_recovery(before: RecoverySample, after: RecoverySample) -> ColdPoolRecovery {
    let delta_temperature_c = after.temperature_c - before.temperature_c;
    let delta_dewpoint_c = after.dewpoint_c - before.dewpoint_c;
    let delta_sbcape_j_kg = after.sbcape_j_kg - before.sbcape_j_kg;
    let delta_mlcape_j_kg = after.mlcape_j_kg - before.mlcape_j_kg;
    let delta_cin_j_kg = after.cin_j_kg - before.cin_j_kg;
    let delta_lcl_m = after.lcl_m - before.lcl_m;
    ColdPoolRecovery {
        delta_temperature_c,
        delta_dewpoint_c,
        delta_sbcape_j_kg,
        delta_mlcape_j_kg,
        delta_cin_j_kg,
        delta_lcl_m,
        recovered_instability: delta_sbcape_j_kg >= 500.0 || delta_mlcape_j_kg >= 500.0,
        reduced_inhibition: delta_cin_j_kg >= 25.0,
        improved_low_lcl: delta_lcl_m <= -100.0,
    }
}

fn strip_comment(line: &str) -> &str {
    line.split('#').next().unwrap_or("").trim()
}

fn starts_like_data(line: &str) -> bool {
    line.chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit() || ch == '-' || ch == '+')
}

fn split_table_fields(line: &str) -> Vec<String> {
    if line.contains(',') {
        parse_csv_line(line)
    } else {
        line.split_whitespace().map(str::to_string).collect()
    }
}

fn parse_date_value(fields: &[String]) -> Option<(String, f64)> {
    if fields.len() >= 4 {
        if let (Some(year), Some(month), Some(day), Some(value)) = (
            parse_i32_field(&fields[0]),
            parse_u32_field(&fields[1]),
            parse_u32_field(&fields[2]),
            parse_f64_field(&fields[3]),
        ) {
            return Some((format!("{year:04}-{month:02}-{day:02}"), value));
        }
    }
    if fields.len() >= 2 {
        parse_date_token(&fields[0]).zip(parse_f64_field(&fields[1]))
    } else {
        None
    }
}

fn parse_date_token(token: &str) -> Option<String> {
    let trimmed = token.trim();
    if trimmed.len() == 8 && trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return Some(format!(
            "{}-{}-{}",
            &trimmed[0..4],
            &trimmed[4..6],
            &trimmed[6..8]
        ));
    }
    if trimmed.len() == 10 {
        let bytes = trimmed.as_bytes();
        if bytes.get(4) == Some(&b'-') && bytes.get(7) == Some(&b'-') {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn parse_i32_field(value: impl AsRef<str>) -> Option<i32> {
    value.as_ref().trim().parse::<i32>().ok()
}

fn parse_u32_field(value: impl AsRef<str>) -> Option<u32> {
    value.as_ref().trim().parse::<u32>().ok()
}

fn parse_f64_field(value: impl AsRef<str>) -> Option<f64> {
    let cleaned = value.as_ref().trim();
    if cleaned.is_empty()
        || matches!(
            cleaned.to_ascii_lowercase().as_str(),
            "m" | "mm" | "nan" | "////" | "-999" | "-9999"
        )
    {
        return None;
    }
    cleaned
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                current.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current.trim().to_string());
    fields
}

fn normalize_header(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(|ch| ch.to_lowercase())
        .collect()
}

fn parse_metar_wind(token: &str) -> (Option<u16>, Option<u16>, Option<u16>) {
    let is_mps = token.ends_with("MPS");
    let core = token.trim_end_matches("KT").trim_end_matches("MPS");
    if core.len() < 5 {
        return (None, None, None);
    }
    let dir = if &core[0..3] == "VRB" {
        None
    } else {
        core[0..3].parse::<u16>().ok()
    };
    let speed_gust = &core[3..];
    let mut parts = speed_gust.split('G');
    let speed = parts
        .next()
        .and_then(|value| parse_metar_wind_speed(value, is_mps));
    let gust = parts
        .next()
        .and_then(|value| parse_metar_wind_speed(value, is_mps));
    (dir, speed, gust)
}

fn parse_metar_wind_speed(value: &str, is_mps: bool) -> Option<u16> {
    let speed = value.parse::<f64>().ok()?;
    if is_mps {
        Some((speed * 1.943_844).round() as u16)
    } else {
        Some(speed.round() as u16)
    }
}

fn parse_metar_temp_dew(token: &str) -> (Option<f64>, Option<f64>) {
    let mut parts = token.split('/');
    (
        parts.next().and_then(parse_metar_signed_temp),
        parts.next().and_then(parse_metar_signed_temp),
    )
}

fn parse_metar_signed_temp(token: &str) -> Option<f64> {
    if token.is_empty() {
        return None;
    }
    if let Some(rest) = token.strip_prefix('M') {
        rest.parse::<f64>().ok().map(|value| -value)
    } else {
        token.parse::<f64>().ok()
    }
}

fn parse_metar_altimeter_hpa(token: &str) -> Option<f64> {
    if let Some(value) = token.strip_prefix('A') {
        let raw = value.parse::<f64>().ok()?;
        return Some(raw / 100.0 * 33.863_886_666_7);
    }
    if let Some(value) = token.strip_prefix('Q') {
        return value.parse::<f64>().ok();
    }
    None
}

fn normalize_degrees(value: f64) -> f64 {
    value.rem_euclid(360.0)
}

fn haversine_km(a_lat: f64, a_lon: f64, b_lat: f64, b_lon: f64) -> f64 {
    let r_km = 6371.0;
    let dlat = (b_lat - a_lat).to_radians();
    let dlon = (b_lon - a_lon).to_radians();
    let lat1 = a_lat.to_radians();
    let lat2 = b_lat.to_radians();
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * r_km * h.sqrt().asin()
}

fn bearing_deg(a_lat: f64, a_lon: f64, b_lat: f64, b_lon: f64) -> f64 {
    let lat1 = a_lat.to_radians();
    let lat2 = b_lat.to_radians();
    let dlon = (b_lon - a_lon).to_radians();
    let y = dlon.sin() * lat2.cos();
    let x = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * dlon.cos();
    normalize_degrees(y.atan2(x) * 180.0 / PI)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_catalog_covers_all_reported_gap_groups() {
        let capabilities = agent_evidence_capabilities();
        let gaps = capabilities
            .iter()
            .map(|capability| capability.gap)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(gaps.len(), 6);
        assert!(gaps.contains(&EvidenceGap::ClimateTeleconnections));
        assert!(gaps.contains(&EvidenceGap::VerificationData));
        assert!(capabilities.iter().all(|capability| {
            capability.all_rust_artifact.contains("agent_evidence")
                || capability.all_rust_artifact.contains("rustwx-models")
        }));
    }

    #[test]
    fn teleconnection_parser_accepts_common_date_value_tables() {
        let points = parse_teleconnection_table(
            TeleconnectionIndex::Pna,
            "year month day value\n2026 5 1 1.25\n20260502,-0.5\n2026-05-03 0.75\n",
        )
        .unwrap();

        assert_eq!(points.len(), 3);
        assert_eq!(points[0].date, "2026-05-01");
        assert_eq!(points[1].value, -0.5);
        assert_eq!(points[2].index, TeleconnectionIndex::Pna);
    }

    #[test]
    fn rmm_parser_reads_phase_and_amplitude() {
        let points = parse_rmm_table("2026 5 1 -0.4 1.2 8 1.27\n").unwrap();

        assert_eq!(points[0].date, "2026-05-01");
        assert_eq!(points[0].phase, 8);
        assert!((points[0].amplitude - 1.27).abs() < 0.001);
    }

    #[test]
    fn spc_report_parser_handles_quoted_comments() {
        let reports = parse_spc_storm_reports_csv(
            "Time,F_Scale,Location,County,State,Lat,Lon,Comments\n\
             2215,EF2,Moore,Cleveland,OK,35.34,-97.49,\"Tornado, spotter confirmed\"\n",
        )
        .unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].magnitude.as_deref(), Some("EF2"));
        assert_eq!(reports[0].state.as_deref(), Some("OK"));
        assert_eq!(
            reports[0].comments.as_deref(),
            Some("Tornado, spotter confirmed")
        );
    }

    #[test]
    fn sounding_parser_reads_fixed_width_pressure_levels() {
        let sounding = parse_sounding_text(
            "Observations at 00Z\n\
             PRES HGHT TEMP DWPT RELH MIXR DRCT SKNT\n\
             1000.0 120 24.0 20.0 78 15.0 180 20\n\
             850.0 1500 18.0 13.0 72 10.0 210 35\n",
        )
        .unwrap();

        assert_eq!(sounding.levels.len(), 2);
        assert_eq!(sounding.levels[1].wind_dir_deg, Some(210.0));
        assert_eq!(sounding.levels[1].wind_speed_kt, Some(35.0));
    }

    #[test]
    fn metar_parser_extracts_core_surface_obs() {
        let metar = parse_metar_line("KOUN 112253Z 17018G28KT 10SM FEW045 27/20 A2988").unwrap();

        assert_eq!(metar.station, "KOUN");
        assert_eq!(metar.observation_time.as_deref(), Some("112253Z"));
        assert_eq!(metar.wind_dir_deg, Some(170));
        assert_eq!(metar.wind_gust_kt, Some(28));
        assert_eq!(metar.temperature_c, Some(27.0));
        assert_eq!(metar.dewpoint_c, Some(20.0));
        assert!(metar.altimeter_hpa.unwrap() > 1010.0);
    }

    #[test]
    fn metar_parser_accepts_prefixed_metar_and_mps_winds() {
        let metar = parse_metar_line("METAR KJFK 112251Z 18010MPS 10SM 18/12 Q1012").unwrap();

        assert_eq!(metar.station, "KJFK");
        assert_eq!(metar.wind_dir_deg, Some(180));
        assert_eq!(metar.wind_speed_kt, Some(19));
        assert_eq!(metar.altimeter_hpa, Some(1012.0));
    }

    #[test]
    fn line_relative_shear_decomposes_parallel_and_normal_components() {
        let shear = line_relative_shear(
            90.0,
            WindVector {
                u_ms: 5.0,
                v_ms: 0.0,
            },
            WindVector {
                u_ms: 25.0,
                v_ms: 10.0,
            },
        );

        assert!((shear.parallel_ms - 20.0).abs() < 0.001);
        assert!((shear.normal_ms - -10.0).abs() < 0.001);
        assert!((shear.magnitude_ms - 22.360).abs() < 0.01);
    }

    #[test]
    fn feature_translation_reports_speed_and_heading() {
        let translation = compute_feature_translation(&[
            TimedGeoPoint {
                time_epoch_seconds: 0,
                lat: 35.0,
                lon: -100.0,
            },
            TimedGeoPoint {
                time_epoch_seconds: 3600,
                lat: 35.0,
                lon: -99.0,
            },
        ])
        .unwrap();

        assert_eq!(translation.point_count, 2);
        assert!(translation.speed_kt > 45.0 && translation.speed_kt < 55.0);
        assert!(translation.heading_deg > 80.0 && translation.heading_deg < 100.0);
    }

    #[test]
    fn severe_diagnostics_expose_component_terms() {
        let ehi = decompose_ehi(3000.0, 250.0);
        assert!((ehi.value - 4.6875).abs() < 0.0001);

        let stp = effective_fixed_stp(3000.0, 250.0, 30.0, 900.0, -25.0);
        assert!(stp.value > 3.0);
        assert_eq!(stp.lcl_term, 1.0);
    }

    #[test]
    fn cold_pool_recovery_flags_instability_and_inhibition_improvement() {
        let recovery = cold_pool_recovery(
            RecoverySample {
                temperature_c: 18.0,
                dewpoint_c: 16.0,
                sbcape_j_kg: 400.0,
                mlcape_j_kg: 300.0,
                cin_j_kg: -100.0,
                lcl_m: 1200.0,
            },
            RecoverySample {
                temperature_c: 25.0,
                dewpoint_c: 21.0,
                sbcape_j_kg: 1800.0,
                mlcape_j_kg: 1500.0,
                cin_j_kg: -25.0,
                lcl_m: 850.0,
            },
        );

        assert!(recovery.recovered_instability);
        assert!(recovery.reduced_inhibition);
        assert!(recovery.improved_low_lcl);
    }
}
