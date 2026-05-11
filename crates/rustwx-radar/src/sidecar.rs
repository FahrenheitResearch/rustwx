use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::nexrad::level2::MomentData;
use crate::nexrad::{radar_site_elevation_m, Level2Sweep, RadarProduct, RadarSite};

pub const RADAR_POLAR_SIDECAR_SCHEMA: &str = "rustwx.radar.polar_sidecar.v2";
const VALUES_FILE_NAME: &str = "polar_values_f32le.bin";
const GATE_FLAGS_FILE_NAME: &str = "polar_gate_flags_u8.bin";

pub const GATE_FLAG_VALID: u8 = 0b0000_0001;
pub const GATE_FLAG_MISSING: u8 = 0b0000_0010;
pub const GATE_FLAG_RANGE_FOLDED: u8 = 0b0000_0100;
pub const GATE_FLAG_FILTERED: u8 = 0b0000_1000;
pub const GATE_FLAG_DERIVED: u8 = 0b0001_0000;
pub const GATE_FLAG_DEALIASED: u8 = 0b0010_0000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarPolarGateFlagMeaning {
    pub bit: u8,
    pub mask: u8,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarPolarSidecarRecord {
    pub schema: String,
    pub manifest_path: PathBuf,
    pub values_path: PathBuf,
    pub gate_flags_path: PathBuf,
    pub radial_count: usize,
    pub max_gate_count: usize,
    pub gate_count: usize,
    pub processing_state: String,
}

#[derive(Debug, Clone)]
pub struct RadarPolarSidecarOptions {
    pub name: String,
    pub source_key_or_url: Option<String>,
    pub scan_time_utc: String,
    pub site_lat: Option<f64>,
    pub site_lon: Option<f64>,
    pub site_elevation_m: Option<f64>,
    pub site_feedhorn_height_m: Option<f64>,
    pub sweep_index: usize,
    pub processing_state: String,
    pub product_provenance: Value,
    pub product_qc: Option<Value>,
    pub velocity_qc: Option<Value>,
    pub dealias_qc: Option<Value>,
    pub velocity_quality_qc: Option<Value>,
    pub reflectivity_qc: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarPolarSidecarManifest {
    pub schema: String,
    pub sidecar_version: u8,
    pub ok: bool,
    pub name: String,
    pub site: RadarPolarSidecarSite,
    pub product: String,
    pub product_name: String,
    pub units: String,
    pub product_provenance: Value,
    pub source_key_or_url: Option<String>,
    pub scan_time_utc: String,
    pub sweep_index: usize,
    pub elevation_deg: f32,
    pub nyquist_velocity_ms: Option<f32>,
    pub processing_state: String,
    pub radial_count: usize,
    pub max_gate_count: usize,
    pub gate_count: usize,
    pub values_path: String,
    pub values_encoding: String,
    pub gate_flags_path: String,
    pub gate_flags_encoding: String,
    #[serde(default = "gate_flag_meanings")]
    pub gate_flag_meanings: Vec<RadarPolarGateFlagMeaning>,
    pub radials: Vec<RadarPolarRadialMeta>,
    pub qc: RadarPolarSidecarQc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarPolarSidecarSite {
    pub id: String,
    pub name: String,
    pub state: String,
    pub lat: f64,
    pub lon: f64,
    pub elevation_m: Option<f64>,
    pub feedhorn_height_m: Option<f64>,
    pub antenna_elevation_m: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarPolarRadialMeta {
    pub radial_index: usize,
    pub azimuth_deg: f32,
    pub elevation_deg: f32,
    pub azimuth_spacing_deg: f32,
    pub gate_count: usize,
    pub first_gate_range_m: u16,
    pub gate_spacing_m: u16,
    pub nyquist_velocity_ms: Option<f32>,
    pub data_word_size_bits: Option<u16>,
    pub scale: Option<f32>,
    pub offset: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarPolarSidecarQc {
    pub product_qc: Option<Value>,
    pub velocity_qc: Option<Value>,
    pub dealias_qc: Option<Value>,
    pub velocity_quality_qc: Option<Value>,
    pub reflectivity_qc: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RadarPolarSampleMethod {
    Nearest,
    Interpolated,
}

impl RadarPolarSampleMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Nearest => "nearest",
            Self::Interpolated => "interpolated",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarPolarSample {
    pub schema: String,
    pub method: String,
    pub lat: f64,
    pub lon: f64,
    pub value: Option<f32>,
    pub units: String,
    pub product: String,
    pub product_name: String,
    pub sweep_index: usize,
    pub elevation_deg: f32,
    pub nyquist_velocity_ms: Option<f32>,
    pub azimuth_deg: f32,
    pub ground_range_m: f64,
    pub range_m: f64,
    pub radial_index: usize,
    pub radial_azimuth_deg: f32,
    pub radial_elevation_deg: f32,
    pub azimuth_spacing_deg: f32,
    pub gate_index: usize,
    pub gate_fraction: f64,
    pub first_gate_range_m: u16,
    pub gate_spacing_m: u16,
    pub gate_flags: Vec<String>,
    pub gate_flag_bits: u8,
    pub processing_state: String,
    pub raw: bool,
    pub dealiased: bool,
    pub filtered: bool,
    pub derived: bool,
    pub product_provenance: Value,
    pub source_key_or_url: Option<String>,
    pub scan_time_utc: String,
    pub qc: RadarPolarSidecarQc,
    pub site: RadarPolarSidecarSite,
}

#[derive(Debug)]
pub struct RadarPolarSidecar {
    pub manifest: RadarPolarSidecarManifest,
    values: Vec<f32>,
    gate_flags: Vec<u8>,
}

pub fn write_polar_sidecar(
    sweep: &Level2Sweep,
    site: &RadarSite,
    product: RadarProduct,
    out_dir: impl AsRef<Path>,
    options: RadarPolarSidecarOptions,
) -> anyhow::Result<RadarPolarSidecarRecord> {
    let out_dir = out_dir.as_ref();
    fs::create_dir_all(out_dir).with_context(|| format!("create {}", out_dir.display()))?;
    let sample_product = product.base_product();
    let radials = sweep
        .radials
        .iter()
        .enumerate()
        .filter_map(|(radial_index, radial)| {
            radial
                .moments
                .iter()
                .find(|moment| moment.product == sample_product)
                .map(|moment| (radial_index, radial, moment))
        })
        .collect::<Vec<_>>();
    if radials.is_empty() {
        bail!("sweep has no radials for {}", product.short_name());
    }

    let radial_count = radials.len();
    let max_gate_count = radials
        .iter()
        .map(|(_, _, moment)| moment.data.len())
        .max()
        .unwrap_or(0);
    if max_gate_count == 0 {
        bail!("sweep has no gates for {}", product.short_name());
    }

    let mut values = vec![f32::NAN; radial_count * max_gate_count];
    let mut gate_flags = vec![GATE_FLAG_MISSING; radial_count * max_gate_count];
    let mut radial_meta = Vec::with_capacity(radial_count);
    let mut gate_count = 0usize;
    for (row, (radial_index, radial, moment)) in radials.iter().enumerate() {
        gate_count += moment.data.len();
        radial_meta.push(RadarPolarRadialMeta {
            radial_index: *radial_index,
            azimuth_deg: radial.azimuth,
            elevation_deg: radial.elevation,
            azimuth_spacing_deg: radial.azimuth_spacing,
            gate_count: moment.data.len(),
            first_gate_range_m: moment.first_gate_range,
            gate_spacing_m: moment.gate_size,
            nyquist_velocity_ms: radial.nyquist_velocity.or(sweep.nyquist_velocity),
            data_word_size_bits: moment.data_word_size,
            scale: moment.scale,
            offset: moment.offset,
        });
        for gate in 0..max_gate_count {
            let index = row * max_gate_count + gate;
            if let Some(value) = moment.data.get(gate).copied() {
                values[index] = value;
                gate_flags[index] =
                    gate_flags_for_value(moment, gate, value, options.processing_state.as_str());
            }
        }
    }

    let values_path = out_dir.join(VALUES_FILE_NAME);
    let gate_flags_path = out_dir.join(GATE_FLAGS_FILE_NAME);
    write_f32_le(&values_path, &values)?;
    fs::write(&gate_flags_path, &gate_flags)
        .with_context(|| format!("write {}", gate_flags_path.display()))?;

    let manifest = RadarPolarSidecarManifest {
        schema: RADAR_POLAR_SIDECAR_SCHEMA.to_string(),
        sidecar_version: 2,
        ok: true,
        name: options.name,
        site: RadarPolarSidecarSite {
            id: site.id.to_string(),
            name: site.name.to_string(),
            state: site.state.to_string(),
            lat: options.site_lat.unwrap_or(site.lat),
            lon: options.site_lon.unwrap_or(site.lon),
            elevation_m: options
                .site_elevation_m
                .or_else(|| radar_site_elevation_m(site.id)),
            feedhorn_height_m: options.site_feedhorn_height_m,
            antenna_elevation_m: options
                .site_elevation_m
                .zip(options.site_feedhorn_height_m)
                .map(|(elevation, feedhorn)| elevation + feedhorn),
        },
        product: product.short_name().to_ascii_lowercase(),
        product_name: product.display_name().to_string(),
        units: product.unit().to_string(),
        product_provenance: options.product_provenance,
        source_key_or_url: options.source_key_or_url,
        scan_time_utc: options.scan_time_utc,
        sweep_index: options.sweep_index,
        elevation_deg: sweep.elevation_angle,
        nyquist_velocity_ms: sweep.nyquist_velocity,
        processing_state: options.processing_state.clone(),
        radial_count,
        max_gate_count,
        gate_count,
        values_path: VALUES_FILE_NAME.to_string(),
        values_encoding: "f32_le_row_major_radial_gate_nan_missing".to_string(),
        gate_flags_path: GATE_FLAGS_FILE_NAME.to_string(),
        gate_flags_encoding: "u8_bitmask_row_major_radial_gate".to_string(),
        gate_flag_meanings: gate_flag_meanings(),
        radials: radial_meta,
        qc: RadarPolarSidecarQc {
            product_qc: options.product_qc,
            velocity_qc: options.velocity_qc,
            dealias_qc: options.dealias_qc,
            velocity_quality_qc: options.velocity_quality_qc,
            reflectivity_qc: options.reflectivity_qc,
        },
    };
    let manifest_path = out_dir.join("polar_sidecar_manifest.json");
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)
        .with_context(|| format!("write {}", manifest_path.display()))?;

    Ok(RadarPolarSidecarRecord {
        schema: RADAR_POLAR_SIDECAR_SCHEMA.to_string(),
        manifest_path,
        values_path,
        gate_flags_path,
        radial_count,
        max_gate_count,
        gate_count,
        processing_state: options.processing_state,
    })
}

impl RadarPolarSidecar {
    pub fn open(manifest_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let manifest_path = manifest_path.as_ref();
        let bytes =
            fs::read(manifest_path).with_context(|| format!("read {}", manifest_path.display()))?;
        let manifest: RadarPolarSidecarManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse {}", manifest_path.display()))?;
        if manifest.schema != RADAR_POLAR_SIDECAR_SCHEMA {
            bail!("unsupported radar sidecar schema {}", manifest.schema);
        }
        if manifest.sidecar_version != 2 {
            bail!(
                "unsupported radar sidecar version {}",
                manifest.sidecar_version
            );
        }
        if !manifest.ok {
            bail!("radar sidecar manifest is not ok");
        }
        if manifest.radials.len() != manifest.radial_count {
            bail!(
                "radar sidecar radial metadata mismatch: got {}, expected {}",
                manifest.radials.len(),
                manifest.radial_count
            );
        }
        let root = manifest_path.parent().unwrap_or_else(|| Path::new("."));
        let values_path = sidecar_data_path(root, &manifest.values_path, "values")?;
        let gate_flags_path = sidecar_data_path(root, &manifest.gate_flags_path, "gate flags")?;
        let values = read_f32_le(&values_path)?;
        let gate_flags = fs::read(&gate_flags_path)
            .with_context(|| format!("read {}", gate_flags_path.display()))?;
        let expected = manifest.radial_count * manifest.max_gate_count;
        if values.len() != expected {
            bail!(
                "radar sidecar value count mismatch: got {}, expected {}",
                values.len(),
                expected
            );
        }
        if gate_flags.len() != expected {
            bail!(
                "radar sidecar gate flag count mismatch: got {}, expected {}",
                gate_flags.len(),
                expected
            );
        }
        Ok(Self {
            manifest,
            values,
            gate_flags,
        })
    }

    pub fn sample_lat_lon(
        &self,
        lat: f64,
        lon: f64,
        method: RadarPolarSampleMethod,
    ) -> Option<RadarPolarSample> {
        let polar =
            radar_lat_lon_to_polar(self.manifest.site.lat, self.manifest.site.lon, lat, lon);
        let cos_elev = f64::from(self.manifest.elevation_deg)
            .to_radians()
            .cos()
            .max(0.1);
        let slant_range_m = polar.ground_range_m / cos_elev;
        match method {
            RadarPolarSampleMethod::Nearest => {
                self.sample_nearest(lat, lon, polar, slant_range_m, method)
            }
            RadarPolarSampleMethod::Interpolated => self
                .sample_interpolated(lat, lon, polar, slant_range_m)
                .or_else(|| self.sample_nearest(lat, lon, polar, slant_range_m, method)),
        }
    }

    fn sample_nearest(
        &self,
        lat: f64,
        lon: f64,
        polar: RadarRelativePolar,
        slant_range_m: f64,
        method: RadarPolarSampleMethod,
    ) -> Option<RadarPolarSample> {
        let row = self.nearest_radial_row(polar.azimuth_deg)?;
        let radial = self.manifest.radials.get(row)?;
        let gate_f = gate_fraction(radial, slant_range_m)?;
        let gate_index = gate_f.round() as usize;
        if gate_index >= radial.gate_count {
            return None;
        }
        let value = self
            .value_at(row, gate_index)
            .filter(|value| value.is_finite());
        let flags = self.flags_at(row, gate_index);
        Some(self.sample_response(
            method,
            value,
            lat,
            lon,
            polar,
            slant_range_m,
            radial,
            gate_index,
            gate_f,
            flags,
        ))
    }

    fn sample_interpolated(
        &self,
        lat: f64,
        lon: f64,
        polar: RadarRelativePolar,
        slant_range_m: f64,
    ) -> Option<RadarPolarSample> {
        let (lo_row, hi_row, az_t) = self.bracketing_radial_rows(polar.azimuth_deg)?;
        let lo = self.sample_radial_range(lo_row, slant_range_m);
        let hi = self.sample_radial_range(hi_row, slant_range_m);
        let value = match (lo, hi) {
            (Some(a), Some(b)) => Some(a + (b - a) * az_t as f32),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        }?;
        let row = self.nearest_radial_row(polar.azimuth_deg)?;
        let radial = self.manifest.radials.get(row)?;
        let gate_f = gate_fraction(radial, slant_range_m)?;
        let gate_index = gate_f.round().clamp(0.0, (radial.gate_count - 1) as f64) as usize;
        let flags = self.flags_at(row, gate_index);
        Some(self.sample_response(
            RadarPolarSampleMethod::Interpolated,
            value.is_finite().then_some(value),
            lat,
            lon,
            polar,
            slant_range_m,
            radial,
            gate_index,
            gate_f,
            flags,
        ))
    }

    fn sample_response(
        &self,
        method: RadarPolarSampleMethod,
        value: Option<f32>,
        lat: f64,
        lon: f64,
        polar: RadarRelativePolar,
        slant_range_m: f64,
        radial: &RadarPolarRadialMeta,
        gate_index: usize,
        gate_fraction: f64,
        flag_bits: u8,
    ) -> RadarPolarSample {
        let processing_state = self.manifest.processing_state.to_ascii_lowercase();
        let raw = processing_state.contains("raw");
        let dealiased =
            processing_state.contains("dealiased") || flag_bits & GATE_FLAG_DEALIASED != 0;
        let filtered = processing_state.contains("filtered") || flag_bits & GATE_FLAG_FILTERED != 0;
        let derived = processing_state.contains("derived") || flag_bits & GATE_FLAG_DERIVED != 0;
        RadarPolarSample {
            schema: RADAR_POLAR_SIDECAR_SCHEMA.to_string(),
            method: method.as_str().to_string(),
            lat,
            lon,
            value,
            units: self.manifest.units.clone(),
            product: self.manifest.product.clone(),
            product_name: self.manifest.product_name.clone(),
            sweep_index: self.manifest.sweep_index,
            elevation_deg: self.manifest.elevation_deg,
            nyquist_velocity_ms: self.manifest.nyquist_velocity_ms,
            azimuth_deg: polar.azimuth_deg,
            ground_range_m: polar.ground_range_m,
            range_m: slant_range_m,
            radial_index: radial.radial_index,
            radial_azimuth_deg: radial.azimuth_deg,
            radial_elevation_deg: radial.elevation_deg,
            azimuth_spacing_deg: radial.azimuth_spacing_deg,
            gate_index,
            gate_fraction,
            first_gate_range_m: radial.first_gate_range_m,
            gate_spacing_m: radial.gate_spacing_m,
            gate_flags: gate_flag_names(flag_bits),
            gate_flag_bits: flag_bits,
            processing_state: self.manifest.processing_state.clone(),
            raw,
            dealiased,
            filtered,
            derived,
            product_provenance: self.manifest.product_provenance.clone(),
            source_key_or_url: self.manifest.source_key_or_url.clone(),
            scan_time_utc: self.manifest.scan_time_utc.clone(),
            qc: self.manifest.qc.clone(),
            site: self.manifest.site.clone(),
        }
    }

    fn nearest_radial_row(&self, azimuth_deg: f32) -> Option<usize> {
        self.manifest
            .radials
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                azimuth_diff(a.azimuth_deg, azimuth_deg)
                    .partial_cmp(&azimuth_diff(b.azimuth_deg, azimuth_deg))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(row, _)| row)
    }

    fn bracketing_radial_rows(&self, azimuth_deg: f32) -> Option<(usize, usize, f64)> {
        if self.manifest.radials.len() < 2 {
            return None;
        }
        let azimuth = normalize_azimuth(azimuth_deg);
        let mut sorted = self
            .manifest
            .radials
            .iter()
            .enumerate()
            .map(|(row, radial)| (row, normalize_azimuth(radial.azimuth_deg)))
            .collect::<Vec<_>>();
        sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let insert_pos = match sorted.binary_search_by(|(_, candidate)| {
            candidate
                .partial_cmp(&azimuth)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            Ok(index) => index,
            Err(index) => index,
        };
        let lo = if insert_pos == 0 {
            sorted.len() - 1
        } else {
            insert_pos - 1
        };
        let hi = if insert_pos >= sorted.len() {
            0
        } else {
            insert_pos
        };
        let lo_az = sorted[lo].1;
        let hi_az = sorted[hi].1;
        let span = azimuth_span(lo_az, hi_az);
        if span <= 0.001 || span > 10.0 {
            return None;
        }
        let offset = azimuth_span(lo_az, azimuth);
        Some((
            sorted[lo].0,
            sorted[hi].0,
            (offset / span).clamp(0.0, 1.0) as f64,
        ))
    }

    fn sample_radial_range(&self, row: usize, slant_range_m: f64) -> Option<f32> {
        let radial = self.manifest.radials.get(row)?;
        let gate_f = gate_fraction(radial, slant_range_m)?;
        let gate_lo = gate_f.floor() as usize;
        if gate_lo >= radial.gate_count {
            return None;
        }
        let v0 = self.value_at(row, gate_lo)?;
        if !v0.is_finite() {
            return None;
        }
        let gate_hi = gate_lo + 1;
        if gate_hi < radial.gate_count {
            if let Some(v1) = self.value_at(row, gate_hi) {
                if v1.is_finite() {
                    let t = (gate_f - gate_lo as f64) as f32;
                    return Some(v0 + (v1 - v0) * t);
                }
            }
        }
        Some(v0)
    }

    fn value_at(&self, row: usize, gate: usize) -> Option<f32> {
        self.values
            .get(row * self.manifest.max_gate_count + gate)
            .copied()
    }

    fn flags_at(&self, row: usize, gate: usize) -> u8 {
        self.gate_flags
            .get(row * self.manifest.max_gate_count + gate)
            .copied()
            .unwrap_or(GATE_FLAG_MISSING)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RadarRelativePolar {
    pub azimuth_deg: f32,
    pub ground_range_m: f64,
}

pub fn radar_lat_lon_to_polar(
    site_lat: f64,
    site_lon: f64,
    lat: f64,
    lon: f64,
) -> RadarRelativePolar {
    let dy_km = (lat - site_lat) * 111.139;
    let dx_km = normalized_lon_delta(lon - site_lon) * 111.139 * site_lat.to_radians().cos();
    let mut azimuth = dx_km.atan2(dy_km).to_degrees();
    if azimuth < 0.0 {
        azimuth += 360.0;
    }
    RadarRelativePolar {
        azimuth_deg: azimuth as f32,
        ground_range_m: dx_km.hypot(dy_km) * 1000.0,
    }
}

pub fn radar_polar_to_lat_lon(
    site_lat: f64,
    site_lon: f64,
    azimuth_deg: f32,
    ground_range_m: f64,
) -> (f64, f64) {
    let range_km = ground_range_m / 1000.0;
    let azimuth_rad = f64::from(azimuth_deg).to_radians();
    let dy_km = range_km * azimuth_rad.cos();
    let dx_km = range_km * azimuth_rad.sin();
    let lat = site_lat + dy_km / 111.139;
    let cos_lat = site_lat.to_radians().cos().abs().max(0.01);
    let lon = site_lon + dx_km / (111.139 * cos_lat);
    (
        lat,
        normalize_lon(site_lon + normalized_lon_delta(lon - site_lon)),
    )
}

fn gate_fraction(radial: &RadarPolarRadialMeta, slant_range_m: f64) -> Option<f64> {
    if radial.gate_spacing_m == 0 {
        return None;
    }
    let gate_f =
        (slant_range_m - f64::from(radial.first_gate_range_m)) / f64::from(radial.gate_spacing_m);
    (gate_f >= 0.0).then_some(gate_f)
}

fn gate_flags_for_value(
    moment: &MomentData,
    gate: usize,
    value: f32,
    processing_state: &str,
) -> u8 {
    let mut flags = 0u8;
    if value.is_finite() {
        flags |= GATE_FLAG_VALID;
    } else {
        flags |= GATE_FLAG_MISSING;
    }
    if let Some(raw) = moment
        .raw_data
        .as_ref()
        .and_then(|raw| raw.get(gate))
        .copied()
    {
        if raw == 0 {
            flags |= GATE_FLAG_MISSING;
        } else if raw == 1 {
            flags |= GATE_FLAG_RANGE_FOLDED;
        } else if !value.is_finite() {
            flags &= !GATE_FLAG_MISSING;
            flags |= GATE_FLAG_FILTERED;
        }
    }
    if processing_state.contains("derived") {
        flags |= GATE_FLAG_DERIVED;
    }
    if processing_state.contains("dealiased") {
        flags |= GATE_FLAG_DEALIASED;
    }
    flags
}

fn gate_flag_names(flags: u8) -> Vec<String> {
    let mut out = Vec::new();
    if flags & GATE_FLAG_VALID != 0 {
        out.push("valid".to_string());
    }
    if flags & GATE_FLAG_MISSING != 0 {
        out.push("missing".to_string());
    }
    if flags & GATE_FLAG_RANGE_FOLDED != 0 {
        out.push("range_folded".to_string());
    }
    if flags & GATE_FLAG_FILTERED != 0 {
        out.push("filtered".to_string());
    }
    if flags & GATE_FLAG_DERIVED != 0 {
        out.push("derived".to_string());
    }
    if flags & GATE_FLAG_DEALIASED != 0 {
        out.push("dealiased".to_string());
    }
    out
}

fn gate_flag_meanings() -> Vec<RadarPolarGateFlagMeaning> {
    [
        (
            0,
            GATE_FLAG_VALID,
            "valid",
            "Decoded finite value is present for this gate.",
        ),
        (
            1,
            GATE_FLAG_MISSING,
            "missing",
            "Gate is absent, below threshold, or encoded as missing.",
        ),
        (
            2,
            GATE_FLAG_RANGE_FOLDED,
            "range_folded",
            "Original Level-II code marked this gate range folded.",
        ),
        (
            3,
            GATE_FLAG_FILTERED,
            "filtered",
            "Value was masked or removed by an explicit quality-control pass.",
        ),
        (
            4,
            GATE_FLAG_DERIVED,
            "derived",
            "Value came from a derived product rather than a native moment.",
        ),
        (
            5,
            GATE_FLAG_DEALIASED,
            "dealiased",
            "Velocity value was produced by an accepted dealiasing pass.",
        ),
    ]
    .into_iter()
    .map(|(bit, mask, name, description)| RadarPolarGateFlagMeaning {
        bit,
        mask,
        name: name.to_string(),
        description: description.to_string(),
    })
    .collect()
}

fn write_f32_le(path: &Path, values: &[f32]) -> anyhow::Result<()> {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
}

fn sidecar_data_path(root: &Path, value: &str, label: &str) -> anyhow::Result<PathBuf> {
    let root = fs::canonicalize(root)
        .with_context(|| format!("canonicalize sidecar root {}", root.display()))?;
    let value_path = Path::new(value);
    let candidate = if value_path.is_absolute() {
        value_path.to_path_buf()
    } else {
        root.join(value_path)
    };
    let path = fs::canonicalize(&candidate)
        .with_context(|| format!("radar sidecar {label} not found: {}", candidate.display()))?;
    if !path.starts_with(&root) {
        bail!(
            "radar sidecar {label} path escapes sidecar root: {}",
            path.display()
        );
    }
    if !path.is_file() {
        bail!("radar sidecar {label} path is missing: {}", path.display());
    }
    Ok(path)
}

fn read_f32_le(path: &Path) -> anyhow::Result<Vec<f32>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() % 4 != 0 {
        bail!("{} is not a whole f32 little-endian array", path.display());
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

fn normalize_azimuth(value: f32) -> f32 {
    let mut value = value % 360.0;
    if value < 0.0 {
        value += 360.0;
    }
    value
}

fn azimuth_diff(a: f32, b: f32) -> f32 {
    let diff = (normalize_azimuth(a) - normalize_azimuth(b)).abs();
    diff.min(360.0 - diff)
}

fn azimuth_span(lo: f32, hi: f32) -> f32 {
    let mut span = normalize_azimuth(hi) - normalize_azimuth(lo);
    if span < 0.0 {
        span += 360.0;
    }
    span
}

fn normalized_lon_delta(delta: f64) -> f64 {
    let mut delta = delta;
    while delta > 180.0 {
        delta -= 360.0;
    }
    while delta < -180.0 {
        delta += 360.0;
    }
    delta
}

fn normalize_lon(lon: f64) -> f64 {
    ((lon + 180.0).rem_euclid(360.0)) - 180.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexrad::level2::{MomentData, RadialData};

    #[test]
    fn polar_sidecar_round_trips_values_masks_and_sampling() {
        let root =
            std::env::temp_dir().join(format!("rustwx-radar-sidecar-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let site = RadarSite {
            id: "KTLX",
            name: "Oklahoma City",
            state: "OK",
            lat: 35.0,
            lon: -97.0,
        };
        let sweep = Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.0,
            nyquist_velocity: None,
            radials: vec![RadialData {
                azimuth: 0.0,
                elevation: 0.0,
                azimuth_spacing: 1.0,
                nyquist_velocity: None,
                radial_status: 1,
                moments: vec![MomentData {
                    product: RadarProduct::Reflectivity,
                    gate_count: 4,
                    first_gate_range: 0,
                    gate_size: 250,
                    data_word_size: Some(8),
                    scale: Some(2.0),
                    offset: Some(66.0),
                    raw_data: Some(vec![2, 0, 1, 106]),
                    data: vec![10.0, f32::NAN, f32::NAN, 20.0],
                }],
            }],
        };

        let record = write_polar_sidecar(
            &sweep,
            &site,
            RadarProduct::Reflectivity,
            &root,
            RadarPolarSidecarOptions {
                name: "test".to_string(),
                source_key_or_url: Some("s3://nexrad/KTLX".to_string()),
                scan_time_utc: "2026-05-11T00:00:00Z".to_string(),
                site_lat: None,
                site_lon: None,
                site_elevation_m: None,
                site_feedhorn_height_m: None,
                sweep_index: 0,
                processing_state: "raw".to_string(),
                product_provenance: serde_json::json!({"source": "native", "derived": false}),
                product_qc: None,
                velocity_qc: None,
                dealias_qc: None,
                velocity_quality_qc: None,
                reflectivity_qc: None,
            },
        )
        .unwrap();

        let sidecar = RadarPolarSidecar::open(&record.manifest_path).unwrap();
        assert_eq!(sidecar.manifest.radial_count, 1);
        assert_eq!(sidecar.manifest.site.elevation_m, Some(389.4));
        assert_eq!(sidecar.manifest.radials[0].scale, Some(2.0));
        assert_eq!(sidecar.manifest.radials[0].offset, Some(66.0));
        assert!(sidecar
            .manifest
            .gate_flag_meanings
            .iter()
            .any(
                |meaning| meaning.name == "range_folded" && meaning.mask == GATE_FLAG_RANGE_FOLDED
            ));
        assert_eq!(sidecar.gate_flags[1] & GATE_FLAG_MISSING, GATE_FLAG_MISSING);
        assert_eq!(
            sidecar.gate_flags[2] & GATE_FLAG_RANGE_FOLDED,
            GATE_FLAG_RANGE_FOLDED
        );

        let lat = site.lat + 750.0 / 111_139.0;
        let sample = sidecar
            .sample_lat_lon(lat, site.lon, RadarPolarSampleMethod::Nearest)
            .unwrap();
        assert_eq!(sample.value, Some(20.0));
        assert_eq!(sample.gate_index, 3);
        assert_eq!(sample.radial_index, 0);
        assert_eq!(sample.units, "dBZ");
        assert!(sample.gate_flags.iter().any(|flag| flag == "valid"));
        assert_eq!(sample.lat, lat);
        assert_eq!(sample.lon, site.lon);
        assert_eq!(sample.first_gate_range_m, 0);
        assert_eq!(sample.gate_spacing_m, 250);
        assert_eq!(sample.azimuth_spacing_deg, 1.0);
        assert_eq!(sample.raw, true);
        assert_eq!(sample.dealiased, false);
        assert_eq!(sample.filtered, false);
        assert_eq!(sample.derived, false);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn radar_relative_coordinates_round_trip_for_sidecar_queries() {
        let site_lat = 35.333;
        let site_lon = -97.277;
        let azimuth_deg = 42.0;
        let range_m = 87_500.0;

        let (lat, lon) = radar_polar_to_lat_lon(site_lat, site_lon, azimuth_deg, range_m);
        let polar = radar_lat_lon_to_polar(site_lat, site_lon, lat, lon);

        assert!((polar.azimuth_deg - azimuth_deg).abs() < 0.001);
        assert!((polar.ground_range_m - range_m).abs() < 0.1);
    }

    #[test]
    fn polar_sidecar_rejects_invalid_manifest_shape_and_escaping_paths() {
        let root = std::env::temp_dir().join(format!(
            "rustwx-radar-sidecar-invalid-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join(VALUES_FILE_NAME), []).unwrap();
        fs::write(root.join(GATE_FLAGS_FILE_NAME), []).unwrap();
        let manifest_path = root.join("polar_sidecar_manifest.json");
        let mut manifest = serde_json::json!({
            "schema": RADAR_POLAR_SIDECAR_SCHEMA,
            "sidecar_version": 1,
            "ok": true,
            "name": "bad",
            "site": {
                "id": "KTLX",
                "name": "Oklahoma City",
                "state": "OK",
                "lat": 35.0,
                "lon": -97.0,
                "elevation_m": 370.0,
                "feedhorn_height_m": 20.0,
                "antenna_elevation_m": 390.0
            },
            "product": "ref",
            "product_name": "Reflectivity",
            "units": "dBZ",
            "product_provenance": {"source": "native", "derived": false},
            "source_key_or_url": "s3://nexrad/KTLX",
            "scan_time_utc": "2026-05-11T00:00:00Z",
            "sweep_index": 0,
            "elevation_deg": 0.0,
            "nyquist_velocity_ms": null,
            "processing_state": "raw",
            "radial_count": 0,
            "max_gate_count": 0,
            "gate_count": 0,
            "values_path": VALUES_FILE_NAME,
            "values_encoding": "f32_le_row_major_radial_gate_nan_missing",
            "gate_flags_path": GATE_FLAGS_FILE_NAME,
            "gate_flags_encoding": "u8_bitmask_row_major_radial_gate",
            "radials": [],
            "qc": {}
        });
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        let err = RadarPolarSidecar::open(&manifest_path).unwrap_err();
        assert!(err
            .to_string()
            .contains("unsupported radar sidecar version"));

        manifest["sidecar_version"] = serde_json::json!(2);
        manifest["ok"] = serde_json::json!(false);
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        let err = RadarPolarSidecar::open(&manifest_path).unwrap_err();
        assert!(err.to_string().contains("manifest is not ok"));

        manifest["ok"] = serde_json::json!(true);
        manifest["radial_count"] = serde_json::json!(1);
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        let err = RadarPolarSidecar::open(&manifest_path).unwrap_err();
        assert!(err.to_string().contains("radial metadata mismatch"));

        let outside_values = root
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!(
                "rustwx-radar-sidecar-outside-values-{}",
                std::process::id()
            ));
        fs::write(&outside_values, []).unwrap();
        manifest["radial_count"] = serde_json::json!(0);
        manifest["values_path"] = serde_json::json!(format!(
            "../{}",
            outside_values.file_name().unwrap().to_string_lossy()
        ));
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        let err = RadarPolarSidecar::open(&manifest_path).unwrap_err();
        assert!(err.to_string().contains("escapes sidecar root"));

        let _ = fs::remove_file(outside_values);
        let _ = fs::remove_dir_all(&root);
    }
}
