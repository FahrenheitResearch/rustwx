use std::path::Path;

use image::ImageFormat;
use serde::{Deserialize, Serialize};
use sharprs::Profile as SharprsProfile;
use sharprs::profile::StationInfo;
use sharprs::render::{
    Canvas, ComputedParams, NativeParcelFlavor, compute_all_params, native_parcel_summaries,
    render_full_sounding,
};

use crate::ecape::{
    ExternalEcapeAnnotationContext, ExternalEcapeSummary, NativeParcelContext, ParcelFlavor,
};
use crate::error::SoundingBridgeError;

const MS_TO_KTS: f64 = 1.943_844_492_440_604_6;
const KTS_TO_MS: f64 = 0.514_444_444_444_444_5;
const PRESSURE_MONOTONIC_TOLERANCE_HPA: f64 = 1.0e-6;
const HEIGHT_MONOTONIC_TOLERANCE_M: f64 = 1.0e-6;
const DEWPOINT_TEMPERATURE_TOLERANCE_C: f64 = 1.0e-6;

const ECAPE_BG: [u8; 4] = [10, 10, 22, 255];
const ECAPE_TITLE_BG: [u8; 4] = [30, 30, 50, 255];
const ECAPE_BORDER: [u8; 4] = [50, 50, 70, 255];
const ECAPE_TEXT: [u8; 4] = [230, 230, 230, 255];
const ECAPE_TEXT_DIM: [u8; 4] = [150, 150, 170, 255];
const ECAPE_TEXT_HEADER: [u8; 4] = [110, 220, 255, 255];
const ECAPE_VALUE: [u8; 4] = [255, 210, 90, 255];
const ECAPE_COLUMN_HEADER: [u8; 4] = [170, 190, 220, 255];

const ECAPE_PANEL_MARGIN: i32 = 20;
const ECAPE_PANEL_PADDING: i32 = 18;
const ECAPE_PANEL_TITLE_H: i32 = 28;
const ECAPE_LINE_H: i32 = Canvas::font_height() + 8;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SoundingMetadata {
    pub station_id: String,
    pub valid_time: String,
    pub latitude_deg: Option<f64>,
    pub longitude_deg: Option<f64>,
    pub elevation_m: Option<f64>,
}

impl SoundingMetadata {
    pub fn to_station_info(&self) -> StationInfo {
        StationInfo {
            station_id: self.station_id.clone(),
            latitude: self.latitude_deg.unwrap_or(f64::NAN),
            longitude: self.longitude_deg.unwrap_or(f64::NAN),
            elevation: self.elevation_m.unwrap_or(f64::NAN),
            datetime: self.valid_time.clone(),
        }
    }

    pub fn from_station_info(station: &StationInfo) -> Self {
        Self {
            station_id: station.station_id.clone(),
            valid_time: station.datetime.clone(),
            latitude_deg: finite_or_none(station.latitude),
            longitude_deg: finite_or_none(station.longitude),
            elevation_m: finite_or_none(station.elevation),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SoundingColumn {
    pub pressure_hpa: Vec<f64>,
    pub height_m_msl: Vec<f64>,
    pub temperature_c: Vec<f64>,
    pub dewpoint_c: Vec<f64>,
    pub u_ms: Vec<f64>,
    pub v_ms: Vec<f64>,
    #[serde(default)]
    pub omega_pa_s: Vec<f64>,
    #[serde(default)]
    pub metadata: SoundingMetadata,
}

impl SoundingColumn {
    pub fn len(&self) -> usize {
        self.pressure_hpa.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pressure_hpa.is_empty()
    }

    pub fn validate(&self) -> Result<(), SoundingBridgeError> {
        let expected = self.pressure_hpa.len();
        if expected < 2 {
            return Err(SoundingBridgeError::InvalidLength {
                field: "pressure_hpa",
                expected_at_least: 2,
                actual: expected,
            });
        }

        validate_len("height_m_msl", self.height_m_msl.len(), expected)?;
        validate_len("temperature_c", self.temperature_c.len(), expected)?;
        validate_len("dewpoint_c", self.dewpoint_c.len(), expected)?;
        validate_len("u_ms", self.u_ms.len(), expected)?;
        validate_len("v_ms", self.v_ms.len(), expected)?;
        if !self.omega_pa_s.is_empty() {
            validate_len("omega_pa_s", self.omega_pa_s.len(), expected)?;
        }

        validate_finite("pressure_hpa", &self.pressure_hpa)?;
        validate_finite("height_m_msl", &self.height_m_msl)?;
        validate_finite("temperature_c", &self.temperature_c)?;
        validate_finite("dewpoint_c", &self.dewpoint_c)?;
        validate_finite("u_ms", &self.u_ms)?;
        validate_finite("v_ms", &self.v_ms)?;
        if !self.omega_pa_s.is_empty() {
            validate_finite("omega_pa_s", &self.omega_pa_s)?;
        }

        validate_monotonic_non_increasing(
            "pressure_hpa",
            &self.pressure_hpa,
            PRESSURE_MONOTONIC_TOLERANCE_HPA,
        )?;
        validate_monotonic_non_decreasing(
            "height_m_msl",
            &self.height_m_msl,
            HEIGHT_MONOTONIC_TOLERANCE_M,
        )?;
        validate_dewpoint_not_above_temperature(
            &self.temperature_c,
            &self.dewpoint_c,
            DEWPOINT_TEMPERATURE_TOLERANCE_C,
        )?;

        Ok(())
    }

    pub fn to_sharprs_profile(&self) -> Result<SharprsProfile, SoundingBridgeError> {
        self.validate()?;

        let u_kts: Vec<f64> = self.u_ms.iter().map(|value| value * MS_TO_KTS).collect();
        let v_kts: Vec<f64> = self.v_ms.iter().map(|value| value * MS_TO_KTS).collect();

        SharprsProfile::from_uv(
            &self.pressure_hpa,
            &self.height_m_msl,
            &self.temperature_c,
            &self.dewpoint_c,
            &u_kts,
            &v_kts,
            &self.omega_pa_s,
            self.metadata.to_station_info(),
        )
        .map_err(Into::into)
    }

    pub fn from_sharprs_profile(profile: &SharprsProfile) -> Self {
        Self {
            pressure_hpa: profile.pres.clone(),
            height_m_msl: profile.hght.clone(),
            temperature_c: profile.tmpc.clone(),
            dewpoint_c: profile.dwpc.clone(),
            u_ms: profile.u.iter().map(|value| value * KTS_TO_MS).collect(),
            v_ms: profile.v.iter().map(|value| value * KTS_TO_MS).collect(),
            omega_pa_s: if profile.omeg.iter().any(|value| value.is_finite()) {
                profile.omeg.clone()
            } else {
                Vec::new()
            },
            metadata: SoundingMetadata::from_station_info(&profile.station),
        }
    }
}

#[derive(Debug)]
pub struct NativeSounding {
    pub profile: SharprsProfile,
    pub params: ComputedParams,
}

impl NativeSounding {
    pub fn from_column(column: &SoundingColumn) -> Result<Self, SoundingBridgeError> {
        let profile = column.to_sharprs_profile()?;
        let params = compute_all_params(&profile);
        Ok(Self { profile, params })
    }

    pub fn render_full_png(&self) -> Vec<u8> {
        render_full_sounding(&self.profile, &self.params)
    }

    pub fn render_full_png_with_ecape(
        &self,
        ecape: &ExternalEcapeSummary,
    ) -> Result<Vec<u8>, SoundingBridgeError> {
        let annotation = ecape.annotation_context(&native_parcel_contexts(&self.params))?;
        let base_png = self.render_full_png();
        append_external_ecape_block(&base_png, &annotation)
    }

    pub fn write_full_png<P: AsRef<Path>>(&self, path: P) -> Result<(), SoundingBridgeError> {
        std::fs::write(path, self.render_full_png())?;
        Ok(())
    }

    pub fn write_full_png_with_ecape<P: AsRef<Path>>(
        &self,
        ecape: &ExternalEcapeSummary,
        path: P,
    ) -> Result<(), SoundingBridgeError> {
        std::fs::write(path, self.render_full_png_with_ecape(ecape)?)?;
        Ok(())
    }
}

pub fn render_full_sounding_png(column: &SoundingColumn) -> Result<Vec<u8>, SoundingBridgeError> {
    Ok(NativeSounding::from_column(column)?.render_full_png())
}

pub fn render_full_sounding_with_ecape_png(
    column: &SoundingColumn,
    ecape: &ExternalEcapeSummary,
) -> Result<Vec<u8>, SoundingBridgeError> {
    NativeSounding::from_column(column)?.render_full_png_with_ecape(ecape)
}

pub fn write_full_sounding_png<P: AsRef<Path>>(
    column: &SoundingColumn,
    path: P,
) -> Result<(), SoundingBridgeError> {
    NativeSounding::from_column(column)?.write_full_png(path)
}

pub fn write_full_sounding_with_ecape_png<P: AsRef<Path>>(
    column: &SoundingColumn,
    ecape: &ExternalEcapeSummary,
    path: P,
) -> Result<(), SoundingBridgeError> {
    NativeSounding::from_column(column)?.write_full_png_with_ecape(ecape, path)
}

fn append_external_ecape_block(
    base_png: &[u8],
    annotation: &ExternalEcapeAnnotationContext,
) -> Result<Vec<u8>, SoundingBridgeError> {
    let base_image = image::load_from_memory_with_format(base_png, ImageFormat::Png)?.to_rgba8();
    let width = base_image.width();
    let height = base_image.height();
    let block_height = ecape_block_height(width as i32, annotation) as u32;

    let mut canvas = Canvas::new(width, height + block_height, ECAPE_BG);
    let raw = base_image.as_raw();
    canvas.pixels[..raw.len()].copy_from_slice(raw);

    draw_external_ecape_block(&mut canvas, height as i32, width as i32, annotation);
    Ok(canvas.to_png())
}

fn draw_external_ecape_block(
    canvas: &mut Canvas,
    top_y: i32,
    total_width: i32,
    annotation: &ExternalEcapeAnnotationContext,
) {
    let panel_x = ECAPE_PANEL_MARGIN;
    let panel_y = top_y + ECAPE_PANEL_MARGIN;
    let panel_w = total_width - ECAPE_PANEL_MARGIN * 2;
    let panel_h = ecape_panel_height(total_width, annotation);
    let inner_w = panel_w - ECAPE_PANEL_PADDING * 2;

    canvas.draw_line(0, top_y, total_width - 1, top_y, ECAPE_BORDER);
    canvas.draw_rect(panel_x, panel_y, panel_w, panel_h, ECAPE_BORDER);
    fill_rect(
        canvas,
        panel_x + 1,
        panel_y + 1,
        panel_w - 2,
        ECAPE_PANEL_TITLE_H - 1,
        ECAPE_TITLE_BG,
    );

    canvas.draw_text(
        "EXTERNAL ECAPE COMPANION",
        panel_x + ECAPE_PANEL_PADDING,
        panel_y + 8,
        ECAPE_TEXT_HEADER,
    );

    let mut y = panel_y + ECAPE_PANEL_TITLE_H + ECAPE_PANEL_PADDING;
    canvas.draw_text(
        "ANNOTATION ONLY - SHARPRS REMAINS THE NATIVE SOUNDING ENGINE",
        panel_x + ECAPE_PANEL_PADDING,
        y,
        ECAPE_TEXT_DIM,
    );
    y += ECAPE_LINE_H + 4;

    for line in wrap_prefixed_line("SOURCE", &annotation.source_label, inner_w) {
        canvas.draw_text(&line, panel_x + ECAPE_PANEL_PADDING, y, ECAPE_TEXT);
        y += ECAPE_LINE_H;
    }

    if let Some(storm_motion) = &annotation.storm_motion_label {
        for line in wrap_prefixed_line("STORM MOTION", storm_motion, inner_w) {
            canvas.draw_text(&line, panel_x + ECAPE_PANEL_PADDING, y, ECAPE_TEXT);
            y += ECAPE_LINE_H;
        }
    }

    y += 4;

    let col_pcl = panel_x + ECAPE_PANEL_PADDING;
    let col_ratio = panel_x + panel_w - ECAPE_PANEL_PADDING;
    let col_cin = col_ratio - 18 * 8;
    let col_cape = col_cin - 14 * 8;
    let col_ecape = col_cape - 14 * 8;

    canvas.draw_text("PCL", col_pcl, y, ECAPE_COLUMN_HEADER);
    canvas.draw_text_right("ECAPE", col_ecape, y, ECAPE_COLUMN_HEADER);
    canvas.draw_text_right("CAPE", col_cape, y, ECAPE_COLUMN_HEADER);
    canvas.draw_text_right("CIN", col_cin, y, ECAPE_COLUMN_HEADER);
    canvas.draw_text_right("ECAPE/CAPE", col_ratio, y, ECAPE_COLUMN_HEADER);
    y += ECAPE_LINE_H;

    for row in &annotation.rows {
        canvas.draw_text(row.parcel.short_label(), col_pcl, y, ECAPE_TEXT);
        canvas.draw_text_right(
            &format_value(row.ecape_j_kg, "J/KG"),
            col_ecape,
            y,
            ECAPE_VALUE,
        );
        canvas.draw_text_right(
            &format_value(row.native_cape_j_kg, "J/KG"),
            col_cape,
            y,
            ECAPE_TEXT,
        );
        canvas.draw_text_right(
            &format_value(row.native_cin_j_kg, "J/KG"),
            col_cin,
            y,
            ECAPE_TEXT_DIM,
        );
        canvas.draw_text_right(
            &format_ratio(row.ecape_fraction_of_cape),
            col_ratio,
            y,
            ECAPE_TEXT,
        );
        y += ECAPE_LINE_H;
    }

    if !annotation.notes.is_empty() {
        y += 4;
        for note in &annotation.notes {
            for line in wrap_prefixed_line("NOTE", note, inner_w) {
                canvas.draw_text(&line, panel_x + ECAPE_PANEL_PADDING, y, ECAPE_TEXT_DIM);
                y += ECAPE_LINE_H;
            }
        }
    }
}

fn ecape_block_height(total_width: i32, annotation: &ExternalEcapeAnnotationContext) -> i32 {
    ecape_panel_height(total_width, annotation) + ECAPE_PANEL_MARGIN * 2
}

fn ecape_panel_height(total_width: i32, annotation: &ExternalEcapeAnnotationContext) -> i32 {
    let panel_w = total_width - ECAPE_PANEL_MARGIN * 2;
    let inner_w = panel_w - ECAPE_PANEL_PADDING * 2;
    let source_lines = wrap_prefixed_line("SOURCE", &annotation.source_label, inner_w);
    let storm_motion_lines = annotation
        .storm_motion_label
        .as_deref()
        .map(|label| wrap_prefixed_line("STORM MOTION", label, inner_w))
        .unwrap_or_default();
    let note_lines = annotation
        .notes
        .iter()
        .flat_map(|note| wrap_prefixed_line("NOTE", note, inner_w))
        .count() as i32;

    let mut content_h = ECAPE_LINE_H;
    content_h += 4;
    content_h += source_lines.len() as i32 * ECAPE_LINE_H;
    content_h += storm_motion_lines.len() as i32 * ECAPE_LINE_H;
    content_h += 4;
    content_h += ECAPE_LINE_H;
    content_h += annotation.rows.len() as i32 * ECAPE_LINE_H;
    if note_lines > 0 {
        content_h += 4;
        content_h += note_lines * ECAPE_LINE_H;
    }

    ECAPE_PANEL_TITLE_H + ECAPE_PANEL_PADDING + content_h + ECAPE_PANEL_PADDING
}

fn native_parcel_contexts(params: &ComputedParams) -> Vec<NativeParcelContext> {
    native_parcel_summaries(params)
        .into_iter()
        .map(|summary| NativeParcelContext {
            parcel: match summary.flavor {
                NativeParcelFlavor::SurfaceBased => ParcelFlavor::SurfaceBased,
                NativeParcelFlavor::MixedLayer => ParcelFlavor::MixedLayer,
                NativeParcelFlavor::MostUnstable => ParcelFlavor::MostUnstable,
            },
            cape_j_kg: summary.cape_j_kg,
            cin_j_kg: summary.cin_j_kg,
            lcl_m_agl: summary.lcl_m_agl,
            lfc_m_agl: summary.lfc_m_agl,
            el_m_agl: summary.el_m_agl,
        })
        .collect()
}

fn format_value(value: f64, units: &str) -> String {
    if value.is_finite() {
        format!("{value:.0} {units}")
    } else {
        "N/A".to_string()
    }
}

fn format_ratio(ratio: Option<f64>) -> String {
    match ratio {
        Some(value) if value.is_finite() => format!("{:.0}%", value * 100.0),
        _ => "N/A".to_string(),
    }
}

fn wrap_prefixed_line(prefix: &str, text: &str, max_width: i32) -> Vec<String> {
    let prefix = format!("{prefix}: ");
    let continuation = " ".repeat(prefix.len());
    let max_chars = max_chars_for_width(max_width)
        .saturating_sub(prefix.len())
        .max(1);
    let wrapped = wrap_text_to_chars(text.trim(), max_chars);

    if wrapped.is_empty() {
        return vec![prefix.trim_end().to_string()];
    }

    wrapped
        .into_iter()
        .enumerate()
        .map(|(index, line)| {
            if index == 0 {
                format!("{prefix}{line}")
            } else {
                format!("{continuation}{line}")
            }
        })
        .collect()
}

fn wrap_text_to_chars(text: &str, max_chars: usize) -> Vec<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let mut lines = Vec::new();
    let mut current = String::new();

    for word in trimmed.split_whitespace() {
        for chunk in split_token(word, max_chars) {
            if current.is_empty() {
                current.push_str(&chunk);
            } else if current.len() + 1 + chunk.len() <= max_chars {
                current.push(' ');
                current.push_str(&chunk);
            } else {
                lines.push(current);
                current = chunk;
            }
        }
    }

    if !current.is_empty() {
        lines.push(current);
    }

    lines
}

fn split_token(token: &str, max_chars: usize) -> Vec<String> {
    if token.chars().count() <= max_chars {
        return vec![token.to_string()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    for ch in token.chars() {
        current.push(ch);
        if current.chars().count() == max_chars {
            chunks.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

fn max_chars_for_width(width: i32) -> usize {
    (((width + 1).max(1)) / 8).max(1) as usize
}

fn fill_rect(canvas: &mut Canvas, x: i32, y: i32, w: i32, h: i32, color: [u8; 4]) {
    for py in y..(y + h).max(y) {
        for px in x..(x + w).max(x) {
            canvas.put_pixel(px, py, color);
        }
    }
}

fn validate_len(
    field: &'static str,
    actual: usize,
    expected: usize,
) -> Result<(), SoundingBridgeError> {
    if actual == expected {
        Ok(())
    } else {
        Err(SoundingBridgeError::LengthMismatch {
            field,
            expected,
            actual,
        })
    }
}

fn validate_finite(field: &'static str, values: &[f64]) -> Result<(), SoundingBridgeError> {
    for (index, value) in values.iter().copied().enumerate() {
        if !value.is_finite() {
            return Err(SoundingBridgeError::InvalidValue {
                field,
                reason: format!("index {index} must be finite, got {value}"),
            });
        }
    }

    Ok(())
}

fn validate_monotonic_non_increasing(
    field: &'static str,
    values: &[f64],
    tolerance: f64,
) -> Result<(), SoundingBridgeError> {
    for index in 1..values.len() {
        let previous = values[index - 1];
        let current = values[index];
        if current > previous + tolerance {
            return Err(SoundingBridgeError::InvalidValue {
                field,
                reason: format!(
                    "values must be monotonic non-increasing, but index {index} rose from {previous} to {current}"
                ),
            });
        }
    }

    Ok(())
}

fn validate_monotonic_non_decreasing(
    field: &'static str,
    values: &[f64],
    tolerance: f64,
) -> Result<(), SoundingBridgeError> {
    for index in 1..values.len() {
        let previous = values[index - 1];
        let current = values[index];
        if current + tolerance < previous {
            return Err(SoundingBridgeError::InvalidValue {
                field,
                reason: format!(
                    "values must be monotonic non-decreasing, but index {index} fell from {previous} to {current}"
                ),
            });
        }
    }

    Ok(())
}

fn validate_dewpoint_not_above_temperature(
    temperature_c: &[f64],
    dewpoint_c: &[f64],
    tolerance_c: f64,
) -> Result<(), SoundingBridgeError> {
    for index in 0..temperature_c.len() {
        let temperature = temperature_c[index];
        let dewpoint = dewpoint_c[index];
        if dewpoint > temperature + tolerance_c {
            return Err(SoundingBridgeError::InvalidValue {
                field: "dewpoint_c",
                reason: format!(
                    "index {index} has dewpoint {dewpoint} C above temperature {temperature} C"
                ),
            });
        }
    }

    Ok(())
}

fn finite_or_none(value: f64) -> Option<f64> {
    if value.is_finite() { Some(value) } else { None }
}
