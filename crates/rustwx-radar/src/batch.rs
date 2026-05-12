use std::collections::BTreeMap;
use std::hash::Hasher;

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};

use crate::aws::{self, NexradObject};
use crate::dealias::{dealias_velocity_file, DealiasMethod};
use crate::nexrad::derived::DerivedProducts;
use crate::nexrad::{Level2File, Level2Sweep, RadarProduct, RadarSite};
use crate::png::{lowest_sweep_with_hca_inputs, lowest_sweep_with_product};
use crate::sidecar::radar_lat_lon_to_polar;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Level2TensorProduct {
    #[serde(alias = "ref")]
    Reflectivity,
    #[serde(alias = "vel")]
    Velocity,
    #[serde(alias = "sw")]
    SpectrumWidth,
    #[serde(alias = "zdr")]
    DifferentialReflectivity,
    #[serde(alias = "cc", alias = "rho")]
    CorrelationCoefficient,
    #[serde(alias = "phi")]
    DifferentialPhase,
    #[serde(alias = "kdp")]
    SpecificDiffPhase,
    #[serde(alias = "hca", alias = "hhc")]
    HydrometeorClass,
    #[serde(alias = "srv")]
    StormRelativeVelocity,
    #[serde(alias = "vil")]
    Vil,
    #[serde(alias = "et", alias = "echo_tops")]
    EchoTops,
}

impl Level2TensorProduct {
    pub fn radar_product(self) -> RadarProduct {
        match self {
            Self::Reflectivity => RadarProduct::Reflectivity,
            Self::Velocity => RadarProduct::Velocity,
            Self::SpectrumWidth => RadarProduct::SpectrumWidth,
            Self::DifferentialReflectivity => RadarProduct::DifferentialReflectivity,
            Self::CorrelationCoefficient => RadarProduct::CorrelationCoefficient,
            Self::DifferentialPhase => RadarProduct::DifferentialPhase,
            Self::SpecificDiffPhase => RadarProduct::SpecificDiffPhase,
            Self::HydrometeorClass => RadarProduct::HydrometeorClass,
            Self::StormRelativeVelocity => RadarProduct::StormRelativeVelocity,
            Self::Vil => RadarProduct::VIL,
            Self::EchoTops => RadarProduct::EchoTops,
        }
    }

    pub fn short_name(self) -> &'static str {
        match self {
            Self::Reflectivity => "REF",
            Self::Velocity => "VEL",
            Self::SpectrumWidth => "SW",
            Self::DifferentialReflectivity => "ZDR",
            Self::CorrelationCoefficient => "CC",
            Self::DifferentialPhase => "PHI",
            Self::SpecificDiffPhase => "KDP",
            Self::HydrometeorClass => "HHC",
            Self::StormRelativeVelocity => "SRV",
            Self::Vil => "VIL",
            Self::EchoTops => "ET",
        }
    }
}

impl From<Level2TensorProduct> for RadarProduct {
    fn from(value: Level2TensorProduct) -> Self {
        value.radar_product()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CartesianGridSpec {
    pub nx: u32,
    pub ny: u32,
    pub center_lat: f64,
    pub center_lon: f64,
    pub resolution_m: f64,
    #[serde(default)]
    pub x_origin_m: f64,
    #[serde(default)]
    pub y_origin_m: f64,
    #[serde(default = "default_grid_projection")]
    pub projection: String,
}

impl Default for CartesianGridSpec {
    fn default() -> Self {
        Self {
            nx: 512,
            ny: 512,
            center_lat: 0.0,
            center_lon: 0.0,
            resolution_m: 1_000.0,
            x_origin_m: -256_000.0,
            y_origin_m: -256_000.0,
            projection: default_grid_projection(),
        }
    }
}

impl CartesianGridSpec {
    pub fn stable_hash(&self) -> u64 {
        let mut hasher = StableHasher::default();
        hasher.write_u32_stable(self.nx);
        hasher.write_u32_stable(self.ny);
        hasher.write_u64_stable(self.center_lat.to_bits());
        hasher.write_u64_stable(self.center_lon.to_bits());
        hasher.write_u64_stable(self.resolution_m.to_bits());
        hasher.write_u64_stable(self.x_origin_m.to_bits());
        hasher.write_u64_stable(self.y_origin_m.to_bits());
        hasher.write_u64_stable(self.projection.len() as u64);
        hasher.write(self.projection.as_bytes());
        hasher.finish()
    }

    pub fn cell_count(&self) -> usize {
        self.nx as usize * self.ny as usize
    }
}

fn default_grid_projection() -> String {
    "local_tangent_cartesian_m".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Level2DedupeKey {
    pub site_id: String,
    pub resolved_s3_key_or_url: String,
    pub parsed_scan_time_ms: i64,
    pub product: Level2TensorProduct,
    pub tensor_grid_spec_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level2TensorOptions {
    pub product: Level2TensorProduct,
    pub grid_spec: CartesianGridSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level2TensorMetadata {
    pub site_id: String,
    pub source_key_or_url: String,
    pub scan_time_ms: i64,
    pub product: Level2TensorProduct,
    pub grid_spec: CartesianGridSpec,
    pub grid_spec_hash: u64,
    pub value_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level2Tensor {
    pub metadata: Level2TensorMetadata,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level2CartesianTensorBuildOptions {
    pub dealias_method: DealiasMethod,
}

impl Default for Level2CartesianTensorBuildOptions {
    fn default() -> Self {
        Self {
            dealias_method: DealiasMethod::SweepContinuity,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedLevel2Volume {
    pub site_id: String,
    pub s3_key: String,
    pub scan_time_utc: DateTime<Utc>,
    pub scan_time_ms: i64,
    pub size: u64,
    pub last_modified: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarBatchRequest {
    #[serde(default)]
    pub request_id: Option<String>,
    pub site_id: String,
    pub target_time_utc: DateTime<Utc>,
    #[serde(default = "default_window_minutes")]
    pub window_minutes: i64,
    #[serde(default = "default_tensor_products")]
    pub products: Vec<Level2TensorProduct>,
    #[serde(default)]
    pub grid_spec: CartesianGridSpec,
}

impl RadarBatchRequest {
    pub fn normalized_products(&self) -> Vec<Level2TensorProduct> {
        if self.products.is_empty() {
            default_tensor_products()
        } else {
            self.products.clone()
        }
    }
}

fn default_window_minutes() -> i64 {
    10
}

fn default_tensor_products() -> Vec<Level2TensorProduct> {
    vec![Level2TensorProduct::Reflectivity]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarBatchResolvedRequest {
    pub request_index: usize,
    pub request_id: Option<String>,
    pub target_time_utc: DateTime<Utc>,
    pub resolved_volume: ResolvedLevel2Volume,
    pub product: Level2TensorProduct,
    pub grid_spec_hash: u64,
    pub dedupe_key: Level2DedupeKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarBatchGroup {
    pub dedupe_key: Level2DedupeKey,
    pub request_indices: Vec<usize>,
    pub request_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarBatchManifest {
    pub generated_at_utc: DateTime<Utc>,
    pub request_count: usize,
    pub resolved_product_request_count: usize,
    pub group_count: usize,
    pub groups: Vec<RadarBatchGroup>,
    pub resolved_requests: Vec<RadarBatchResolvedRequest>,
}

pub fn resolve_nearest_volume(
    site: &str,
    target_time_utc: DateTime<Utc>,
    window_minutes: i64,
) -> anyhow::Result<ResolvedLevel2Volume> {
    let window = Duration::minutes(window_minutes.max(0));
    let start = target_time_utc - window;
    let end = target_time_utc + window;
    let mut dates = vec![target_time_utc.date_naive()];
    if start.date_naive() != target_time_utc.date_naive() {
        dates.push(start.date_naive());
    }
    if end.date_naive() != target_time_utc.date_naive() && !dates.contains(&end.date_naive()) {
        dates.push(end.date_naive());
    }
    dates.sort();
    dates.dedup();

    let mut objects = Vec::new();
    for date in dates {
        objects.extend(aws::list_day(site, date)?);
    }

    select_nearest_volume(site, target_time_utc, window_minutes, &objects).ok_or_else(|| {
        anyhow::anyhow!(
            "no Level-II volume for {site} within +/- {window_minutes} minutes of {}",
            target_time_utc.to_rfc3339()
        )
    })
}

pub fn select_nearest_volume(
    site: &str,
    target_time_utc: DateTime<Utc>,
    window_minutes: i64,
    objects: &[NexradObject],
) -> Option<ResolvedLevel2Volume> {
    let window_ms = window_minutes.max(0) * 60_000;
    let target_ms = target_time_utc.timestamp_millis();
    objects
        .iter()
        .filter_map(|object| {
            let scan_time = parse_level2_object_scan_time(object)?;
            let delta_ms = (scan_time.timestamp_millis() - target_ms).abs();
            if delta_ms > window_ms {
                return None;
            }
            Some((delta_ms, scan_time, object))
        })
        .min_by(|(delta_a, time_a, object_a), (delta_b, time_b, object_b)| {
            delta_a
                .cmp(delta_b)
                .then_with(|| time_a.cmp(time_b))
                .then_with(|| object_a.key.cmp(&object_b.key))
        })
        .map(|(_, scan_time, object)| ResolvedLevel2Volume {
            site_id: site.to_ascii_uppercase(),
            s3_key: object.key.clone(),
            scan_time_utc: scan_time,
            scan_time_ms: scan_time.timestamp_millis(),
            size: object.size,
            last_modified: object.last_modified.clone(),
        })
}

pub fn parse_level2_object_scan_time(object: &NexradObject) -> Option<DateTime<Utc>> {
    parse_level2_object_name_scan_time(&object.display_name).or_else(|| {
        object
            .key
            .rsplit('/')
            .next()
            .and_then(parse_level2_object_name_scan_time)
    })
}

pub fn parse_level2_object_name_scan_time(name: &str) -> Option<DateTime<Utc>> {
    let bytes = name.as_bytes();
    if bytes.len() < 15 {
        return None;
    }
    for idx in 0..=bytes.len() - 15 {
        if !bytes[idx..idx + 8].iter().all(u8::is_ascii_digit) {
            continue;
        }
        if bytes.get(idx + 8) != Some(&b'_') {
            continue;
        }
        if !bytes[idx + 9..idx + 15].iter().all(u8::is_ascii_digit) {
            continue;
        }
        let date = &name[idx..idx + 8];
        let time = &name[idx + 9..idx + 15];
        let year = date[0..4].parse().ok()?;
        let month = date[4..6].parse().ok()?;
        let day = date[6..8].parse().ok()?;
        let hour = time[0..2].parse().ok()?;
        let minute = time[2..4].parse().ok()?;
        let second = time[4..6].parse().ok()?;
        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        let time = NaiveTime::from_hms_opt(hour, minute, second)?;
        return Some(NaiveDateTime::new(date, time).and_utc());
    }
    None
}

pub fn dedupe_key_for(
    volume: &ResolvedLevel2Volume,
    product: Level2TensorProduct,
    grid_spec: &CartesianGridSpec,
) -> Level2DedupeKey {
    Level2DedupeKey {
        site_id: volume.site_id.clone(),
        resolved_s3_key_or_url: volume.s3_key.clone(),
        parsed_scan_time_ms: volume.scan_time_ms,
        product,
        tensor_grid_spec_hash: grid_spec.stable_hash(),
    }
}

pub fn plan_batch_requests(requests: &[RadarBatchRequest]) -> anyhow::Result<RadarBatchManifest> {
    let mut resolved_requests = Vec::new();
    for (request_index, request) in requests.iter().enumerate() {
        let volume = resolve_nearest_volume(
            &request.site_id,
            request.target_time_utc,
            request.window_minutes,
        )?;
        let grid_hash = request.grid_spec.stable_hash();
        for product in request.normalized_products() {
            let dedupe_key = dedupe_key_for(&volume, product, &request.grid_spec);
            resolved_requests.push(RadarBatchResolvedRequest {
                request_index,
                request_id: request.request_id.clone(),
                target_time_utc: request.target_time_utc,
                resolved_volume: volume.clone(),
                product,
                grid_spec_hash: grid_hash,
                dedupe_key,
            });
        }
    }
    Ok(manifest_from_resolved_requests(
        requests.len(),
        resolved_requests,
        Utc::now(),
    ))
}

pub fn manifest_from_resolved_requests(
    request_count: usize,
    resolved_requests: Vec<RadarBatchResolvedRequest>,
    generated_at_utc: DateTime<Utc>,
) -> RadarBatchManifest {
    let groups = group_resolved_requests(&resolved_requests);
    RadarBatchManifest {
        generated_at_utc,
        request_count,
        resolved_product_request_count: resolved_requests.len(),
        group_count: groups.len(),
        groups,
        resolved_requests,
    }
}

pub fn group_resolved_requests(requests: &[RadarBatchResolvedRequest]) -> Vec<RadarBatchGroup> {
    let mut grouped = BTreeMap::<Level2DedupeKey, RadarBatchGroup>::new();
    for request in requests {
        let group = grouped
            .entry(request.dedupe_key.clone())
            .or_insert_with(|| RadarBatchGroup {
                dedupe_key: request.dedupe_key.clone(),
                request_indices: Vec::new(),
                request_ids: Vec::new(),
            });
        group.request_indices.push(request.request_index);
        if let Some(request_id) = &request.request_id {
            group.request_ids.push(request_id.clone());
        }
    }
    grouped.into_values().collect()
}

pub fn build_level2_tensors_stub(
    file: &Level2File,
    site_id: impl Into<String>,
    source_key_or_url: impl Into<String>,
    products: &[Level2TensorProduct],
    grid_spec: &CartesianGridSpec,
) -> Vec<Level2Tensor> {
    let site_id = site_id.into();
    let source_key_or_url = source_key_or_url.into();
    let scan_time_ms = file.unix_timestamp_ms();
    products
        .iter()
        .copied()
        .map(|product| Level2Tensor {
            metadata: Level2TensorMetadata {
                site_id: site_id.clone(),
                source_key_or_url: source_key_or_url.clone(),
                scan_time_ms,
                product,
                grid_spec: grid_spec.clone(),
                grid_spec_hash: grid_spec.stable_hash(),
                value_type: "f32".to_string(),
            },
            values: vec![f32::NAN; grid_spec.cell_count()],
        })
        .collect()
}

pub fn build_level2_cartesian_tensors(
    file: &Level2File,
    site: &RadarSite,
    source_key_or_url: impl Into<String>,
    products: &[Level2TensorProduct],
    grid_spec: &CartesianGridSpec,
) -> Vec<Level2Tensor> {
    build_level2_cartesian_tensors_with_options(
        file,
        site,
        source_key_or_url,
        products,
        grid_spec,
        Level2CartesianTensorBuildOptions::default(),
    )
}

pub fn build_level2_cartesian_tensors_with_options(
    file: &Level2File,
    site: &RadarSite,
    source_key_or_url: impl Into<String>,
    products: &[Level2TensorProduct],
    grid_spec: &CartesianGridSpec,
    options: Level2CartesianTensorBuildOptions,
) -> Vec<Level2Tensor> {
    let source_key_or_url = source_key_or_url.into();
    let scan_time_ms = file.unix_timestamp_ms();
    let dealiased_file;
    let render_file = if options.dealias_method != DealiasMethod::Off
        && products.iter().any(|product| {
            matches!(
                product.radar_product().base_product(),
                RadarProduct::Velocity | RadarProduct::SuperResVelocity
            )
        }) {
        dealiased_file = dealias_velocity_file(file, options.dealias_method);
        &dealiased_file
    } else {
        file
    };
    products
        .iter()
        .copied()
        .map(|product| Level2Tensor {
            metadata: Level2TensorMetadata {
                site_id: site.id.to_string(),
                source_key_or_url: source_key_or_url.clone(),
                scan_time_ms,
                product,
                grid_spec: grid_spec.clone(),
                grid_spec_hash: grid_spec.stable_hash(),
                value_type: "f32".to_string(),
            },
            values: remap_lowest_sweep_to_cartesian(
                render_file,
                site,
                product.radar_product(),
                grid_spec,
            ),
        })
        .collect()
}

fn remap_lowest_sweep_to_cartesian(
    file: &Level2File,
    site: &RadarSite,
    product: RadarProduct,
    grid_spec: &CartesianGridSpec,
) -> Vec<f32> {
    let Some(resolved_sweep) = resolve_lowest_tensor_sweep(file, product) else {
        return vec![f32::NAN; grid_spec.cell_count()];
    };
    let sweep = resolved_sweep.sweep();

    let mut radials = sweep
        .radials
        .iter()
        .filter_map(|radial| {
            radial
                .moments
                .iter()
                .find(|moment| moment.product == product)
                .map(|moment| (normalize_azimuth(radial.azimuth), moment))
        })
        .collect::<Vec<_>>();
    if radials.is_empty() {
        return vec![f32::NAN; grid_spec.cell_count()];
    }
    radials.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let nx = grid_spec.nx as usize;
    let ny = grid_spec.ny as usize;
    let mut out = Vec::with_capacity(nx * ny);
    for row in 0..ny {
        for col in 0..nx {
            let (lat, lon) = grid_cell_lat_lon(grid_spec, row, col);
            let (azimuth_deg, range_m) = site_to_azimuth_range(site, lat, lon);
            let (_, moment) = nearest_radial_by_azimuth(&radials, azimuth_deg);
            out.push(sample_moment_gate(moment, range_m).unwrap_or(f32::NAN));
        }
    }
    out
}

enum ResolvedTensorSweep<'a> {
    Borrowed(&'a Level2Sweep),
    Owned(Level2Sweep),
}

impl ResolvedTensorSweep<'_> {
    fn sweep(&self) -> &Level2Sweep {
        match self {
            Self::Borrowed(sweep) => sweep,
            Self::Owned(sweep) => sweep,
        }
    }
}

fn resolve_lowest_tensor_sweep(
    file: &Level2File,
    product: RadarProduct,
) -> Option<ResolvedTensorSweep<'_>> {
    if let Some((_, sweep)) = lowest_sweep_with_product(file, product) {
        return Some(ResolvedTensorSweep::Borrowed(sweep));
    }

    match product {
        RadarProduct::SpecificDiffPhase => {
            let (_, phi_sweep) = lowest_sweep_with_product(file, RadarProduct::DifferentialPhase)?;
            DerivedProducts::compute_kdp_from_phi_sweep(phi_sweep).map(ResolvedTensorSweep::Owned)
        }
        RadarProduct::HydrometeorClass => {
            let (_, dual_pol_sweep) = lowest_sweep_with_hca_inputs(file)?;
            DerivedProducts::compute_hca_from_dual_pol_sweep(dual_pol_sweep)
                .map(ResolvedTensorSweep::Owned)
        }
        _ => None,
    }
}

fn grid_cell_lat_lon(grid_spec: &CartesianGridSpec, row: usize, col: usize) -> (f64, f64) {
    let x_m = grid_spec.x_origin_m + col as f64 * grid_spec.resolution_m;
    let y_m = grid_spec.y_origin_m
        + (grid_spec.ny.saturating_sub(1) as usize - row) as f64 * grid_spec.resolution_m;
    let lat = grid_spec.center_lat + y_m / 111_139.0;
    let cos_lat = grid_spec.center_lat.to_radians().cos().abs().max(0.01);
    let lon = grid_spec.center_lon + x_m / (111_139.0 * cos_lat);
    (lat, lon)
}

fn site_to_azimuth_range(site: &RadarSite, lat: f64, lon: f64) -> (f32, f64) {
    let polar = radar_lat_lon_to_polar(site.lat, site.lon, lat, lon);
    (polar.azimuth_deg, polar.ground_range_m)
}

fn nearest_radial_by_azimuth<'a>(
    radials: &'a [(f32, &'a crate::nexrad::level2::MomentData)],
    azimuth_deg: f32,
) -> (f32, &'a crate::nexrad::level2::MomentData) {
    debug_assert!(!radials.is_empty());
    let azimuth = normalize_azimuth(azimuth_deg);
    let idx = radials.partition_point(|(radial_azimuth, _)| *radial_azimuth < azimuth);
    let candidates = [
        idx.checked_sub(1),
        (idx < radials.len()).then_some(idx),
        (idx == radials.len()).then_some(0),
    ];
    candidates
        .into_iter()
        .flatten()
        .map(|candidate| {
            let radial = radials[candidate];
            (azimuth_delta(radial.0, azimuth), radial)
        })
        .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(_, radial)| radial)
        .unwrap_or(radials[0])
}

fn sample_moment_gate(moment: &crate::nexrad::level2::MomentData, range_m: f64) -> Option<f32> {
    if moment.gate_size == 0 {
        return None;
    }
    let gate = ((range_m - f64::from(moment.first_gate_range)) / f64::from(moment.gate_size))
        .round() as isize;
    if gate < 0 {
        return None;
    }
    moment
        .data
        .get(gate as usize)
        .copied()
        .filter(|value| value.is_finite())
}

fn normalize_azimuth(value: f32) -> f32 {
    value.rem_euclid(360.0)
}

fn azimuth_delta(a: f32, b: f32) -> f32 {
    let delta = (a - b).abs();
    if delta > 180.0 {
        360.0 - delta
    } else {
        delta
    }
}

#[derive(Default)]
struct StableHasher {
    state: u64,
}

impl StableHasher {
    fn write_u32_stable(&mut self, value: u32) {
        self.write(&value.to_le_bytes());
    }

    fn write_u64_stable(&mut self, value: u64) {
        self.write(&value.to_le_bytes());
    }
}

impl Hasher for StableHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        if self.state == 0 {
            self.state = 0xcbf2_9ce4_8422_2325;
        }
        for byte in bytes {
            self.state ^= *byte as u64;
            self.state = self.state.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexrad::level2::{MomentData, RadialData};

    #[test]
    fn batch_nearest_volume_helper_chooses_closest_synthetic_object() {
        let objects = vec![
            object("2026/05/07/KTLX/KTLX20260507_235500_V06"),
            object("2026/05/08/KTLX/KTLX20260508_000300_V06"),
            object("2026/05/08/KTLX/KTLX20260508_001000_V06"),
        ];
        let target = parse_time("2026-05-08T00:02:00Z");

        let resolved = select_nearest_volume("KTLX", target, 10, &objects).unwrap();

        assert_eq!(resolved.s3_key, "2026/05/08/KTLX/KTLX20260508_000300_V06");
        assert_eq!(
            resolved.scan_time_ms,
            parse_time("2026-05-08T00:03:00Z").timestamp_millis()
        );
    }

    #[test]
    fn batch_dedupe_groups_repeated_tile_requests_into_one_volume_product() {
        let volume = ResolvedLevel2Volume {
            site_id: "KTLX".to_string(),
            s3_key: "2026/05/08/KTLX/KTLX20260508_000300_V06".to_string(),
            scan_time_utc: parse_time("2026-05-08T00:03:00Z"),
            scan_time_ms: parse_time("2026-05-08T00:03:00Z").timestamp_millis(),
            size: 100,
            last_modified: "2026-05-08T00:04:00Z".to_string(),
        };
        let grid = CartesianGridSpec {
            center_lat: 35.0,
            center_lon: -97.0,
            ..CartesianGridSpec::default()
        };
        let key = dedupe_key_for(&volume, Level2TensorProduct::Reflectivity, &grid);
        let resolved_requests = vec![
            resolved_request(0, "tile-a", volume.clone(), key.clone()),
            resolved_request(1, "tile-b", volume, key),
        ];

        let groups = group_resolved_requests(&resolved_requests);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].request_indices, vec![0, 1]);
        assert_eq!(groups[0].request_ids, vec!["tile-a", "tile-b"]);
    }

    #[test]
    fn batch_grid_spec_hash_is_stable() {
        let grid = CartesianGridSpec {
            nx: 256,
            ny: 128,
            center_lat: 35.333,
            center_lon: -97.277,
            resolution_m: 500.0,
            x_origin_m: -64_000.0,
            y_origin_m: -32_000.0,
            projection: "local_tangent_cartesian_m".to_string(),
        };

        assert_eq!(grid.stable_hash(), 0xb857_d76c_8454_684a);
        assert_eq!(grid.stable_hash(), grid.clone().stable_hash());
    }

    #[test]
    fn batch_parses_expected_level2_object_names() {
        let parsed = parse_level2_object_name_scan_time("KTLX20260508_000300_V06.gz").unwrap();

        assert_eq!(parsed, parse_time("2026-05-08T00:03:00Z"));
    }

    #[test]
    fn cartesian_tensor_samples_lowest_sweep_gate() {
        let site = RadarSite {
            id: "KTLX",
            name: "Oklahoma City",
            lat: 35.0,
            lon: -97.0,
            state: "OK",
        };
        let file = Level2File {
            station_id: "KTLX".to_string(),
            volume_date: 20_000,
            volume_time: 0,
            vcp: Some(212),
            site_metadata: None,
            sweeps: vec![crate::nexrad::Level2Sweep {
                elevation_number: 1,
                elevation_angle: 0.5,
                nyquist_velocity: None,
                radials: vec![RadialData {
                    azimuth: 90.0,
                    elevation: 0.5,
                    azimuth_spacing: 1.0,
                    nyquist_velocity: None,
                    radial_status: 0,
                    moments: vec![MomentData {
                        product: RadarProduct::Reflectivity,
                        gate_count: 4,
                        first_gate_range: 0,
                        gate_size: 1_000,
                        data_word_size: None,
                        scale: None,
                        offset: None,
                        raw_data: None,
                        data: vec![1.0, 2.0, 3.0, 4.0],
                    }],
                }],
            }],
            partial: false,
        };
        let grid = CartesianGridSpec {
            nx: 1,
            ny: 1,
            center_lat: 35.0,
            center_lon: -97.0 + 1.0 / (111.139 * 35.0_f64.to_radians().cos()),
            resolution_m: 1_000.0,
            x_origin_m: 0.0,
            y_origin_m: 0.0,
            projection: "local_tangent_cartesian_m".to_string(),
        };

        let tensors = build_level2_cartesian_tensors(
            &file,
            &site,
            "synthetic",
            &[Level2TensorProduct::Reflectivity],
            &grid,
        );

        assert_eq!(tensors.len(), 1);
        assert_eq!(tensors[0].values, vec![2.0]);
    }

    #[test]
    fn tensor_sweep_resolver_derives_kdp_and_hca_for_agent_products() {
        let file = Level2File {
            station_id: "KTLX".to_string(),
            volume_date: 20_000,
            volume_time: 0,
            vcp: Some(212),
            site_metadata: None,
            sweeps: vec![crate::nexrad::Level2Sweep {
                elevation_number: 1,
                elevation_angle: 0.5,
                nyquist_velocity: None,
                radials: vec![RadialData {
                    azimuth: 90.0,
                    elevation: 0.5,
                    azimuth_spacing: 1.0,
                    nyquist_velocity: None,
                    radial_status: 0,
                    moments: vec![
                        moment(RadarProduct::Reflectivity, vec![55.0; 16]),
                        moment(RadarProduct::DifferentialReflectivity, vec![0.8; 16]),
                        moment(RadarProduct::CorrelationCoefficient, vec![0.97; 16]),
                        moment(
                            RadarProduct::DifferentialPhase,
                            (0..16).map(|idx| idx as f32 * 4.0).collect(),
                        ),
                    ],
                }],
            }],
            partial: false,
        };

        let kdp_sweep =
            resolve_lowest_tensor_sweep(&file, RadarProduct::SpecificDiffPhase).unwrap();
        let kdp = kdp_sweep.sweep().radials[0]
            .moments
            .iter()
            .find(|moment| moment.product == RadarProduct::SpecificDiffPhase)
            .unwrap();
        let hca_sweep = resolve_lowest_tensor_sweep(&file, RadarProduct::HydrometeorClass).unwrap();
        let hca = hca_sweep.sweep().radials[0]
            .moments
            .iter()
            .find(|moment| moment.product == RadarProduct::HydrometeorClass)
            .unwrap();

        assert_eq!(kdp.data[8], 8.0);
        assert_eq!(hca.data[8], 7.0);
    }

    fn moment(product: RadarProduct, data: Vec<f32>) -> MomentData {
        MomentData {
            product,
            gate_count: data.len() as u16,
            first_gate_range: 0,
            gate_size: 250,
            data_word_size: None,
            scale: None,
            offset: None,
            raw_data: None,
            data,
        }
    }

    fn object(key: &str) -> NexradObject {
        NexradObject {
            key: key.to_string(),
            size: 42,
            last_modified: "2026-05-08T00:00:00Z".to_string(),
            display_name: key.rsplit('/').next().unwrap().to_string(),
        }
    }

    fn parse_time(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn resolved_request(
        request_index: usize,
        request_id: &str,
        volume: ResolvedLevel2Volume,
        dedupe_key: Level2DedupeKey,
    ) -> RadarBatchResolvedRequest {
        RadarBatchResolvedRequest {
            request_index,
            request_id: Some(request_id.to_string()),
            target_time_utc: volume.scan_time_utc,
            resolved_volume: volume,
            product: Level2TensorProduct::Reflectivity,
            grid_spec_hash: dedupe_key.tensor_grid_spec_hash,
            dedupe_key,
        }
    }
}
