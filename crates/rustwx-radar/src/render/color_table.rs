use crate::nexrad::RadarProduct;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const LUT_SIZE: usize = 8192;
const REFLECTIVITY_NSSL_II_PAL: &str =
    include_str!("../../assets/color_tables/reflectivity_nssl_ii.pal");
const VELOCITY_RADARSCOPE_BVEL_PAL: &str =
    include_str!("../../assets/color_tables/velocity_radarscope_bvel.pal");
const CC_DEFAULT_PAL: &str = include_str!("../../assets/color_tables/cc_default.pal");
const KDP_DEFAULT_PAL: &[u8] = include_bytes!("../../assets/color_tables/kdp_default.pal");
const VIL_DEFAULT_PAL: &str = include_str!("../../assets/color_tables/vil_default.pal");
const ECHO_TOPS_ENHANCED_PAL: &str =
    include_str!("../../assets/color_tables/echo_tops_enhanced.pal");

/// Named preset styles for each product
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ColorTablePreset {
    Default,
    GR2Analyst,
    NSSL,
    Classic,
    Dark,
    Colorblind,
}

impl ColorTablePreset {
    pub fn all() -> &'static [Self] {
        &[
            Self::Default,
            Self::GR2Analyst,
            Self::NSSL,
            Self::Classic,
            Self::Dark,
            Self::Colorblind,
        ]
    }

    pub fn label(&self) -> &str {
        match self {
            Self::Default => "NWS Default",
            Self::GR2Analyst => "GR2Analyst",
            Self::NSSL => "NSSL",
            Self::Classic => "Classic",
            Self::Dark => "Dark",
            Self::Colorblind => "Colorblind",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ColorTableSelection {
    Preset(ColorTablePreset),
    Custom(String),
}

impl Default for ColorTableSelection {
    fn default() -> Self {
        Self::Preset(ColorTablePreset::Default)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ColorEntry {
    pub value: f32,
    pub r: u8,
    pub g: u8,
    pub b: u8,
    pub a: u8,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SerializableColorTable {
    pub name: String,
    pub min_value: f32,
    pub max_value: f32,
    pub entries: Vec<(f32, u8, u8, u8, u8)>,
}

impl SerializableColorTable {
    pub fn from_color_table(ct: &ColorTable) -> Self {
        Self {
            name: ct.name.clone(),
            min_value: ct.min_value,
            max_value: ct.max_value,
            entries: ct
                .entries
                .iter()
                .map(|e| (e.value, e.r, e.g, e.b, e.a))
                .collect(),
        }
    }
    pub fn to_color_table(&self) -> ColorTable {
        let entries: Vec<ColorEntry> = self
            .entries
            .iter()
            .map(|&(value, r, g, b, a)| ColorEntry { value, r, g, b, a })
            .collect();
        ColorTable::from_entries_vec(&self.name, entries)
    }
}

#[derive(Debug, Clone)]
pub struct ColorTable {
    pub name: String,
    pub entries: Vec<ColorEntry>,
    pub min_value: f32,
    pub max_value: f32,
    lut: Vec<[u8; 4]>,
    lut_scale: f32,
}

impl ColorTable {
    fn build_lut(name: &str, entries: Vec<ColorEntry>, min_value: f32, max_value: f32) -> Self {
        let range = max_value - min_value;
        let lut_scale = if range > 0.0 {
            (LUT_SIZE - 1) as f32 / range
        } else {
            1.0
        };
        let mut lut = Vec::with_capacity(LUT_SIZE);
        for i in 0..LUT_SIZE {
            let value = min_value + (i as f32) / lut_scale;
            lut.push(interpolate_entries(&entries, value));
        }
        ColorTable {
            name: name.to_string(),
            entries,
            min_value,
            max_value,
            lut,
            lut_scale,
        }
    }

    fn from_entries_vec(name: &str, mut entries: Vec<ColorEntry>) -> Self {
        entries.sort_by(|a, b| {
            a.value
                .partial_cmp(&b.value)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let min_value = entries.first().map(|e| e.value).unwrap_or(0.0);
        let max_value = entries.last().map(|e| e.value).unwrap_or(1.0);
        Self::build_lut(name, entries, min_value, max_value)
    }

    #[inline(always)]
    pub fn color_for_value(&self, value: f32) -> [u8; 4] {
        if value.is_nan() || value < self.min_value {
            return [0, 0, 0, 0];
        }
        if value >= self.max_value {
            if let Some(last) = self.entries.last() {
                return [last.r, last.g, last.b, last.a];
            }
            return [0, 0, 0, 0];
        }
        // Use sub-LUT linear interpolation for smoother gradients between LUT entries.
        // This eliminates visible banding even at modest LUT sizes.
        let fidx = (value - self.min_value) * self.lut_scale;
        let idx0 = (fidx as usize).min(LUT_SIZE - 1);
        let idx1 = (idx0 + 1).min(LUT_SIZE - 1);
        if idx0 == idx1 {
            return self.lut[idx0];
        }
        let t = fidx - idx0 as f32;
        let c0 = self.lut[idx0];
        let c1 = self.lut[idx1];
        [
            (c0[0] as f32 + t * (c1[0] as f32 - c0[0] as f32)) as u8,
            (c0[1] as f32 + t * (c1[1] as f32 - c0[1] as f32)) as u8,
            (c0[2] as f32 + t * (c1[2] as f32 - c0[2] as f32)) as u8,
            (c0[3] as f32 + t * (c1[3] as f32 - c0[3] as f32)) as u8,
        ]
    }

    pub fn for_product(product: RadarProduct) -> Self {
        Self::for_product_preset(product, ColorTablePreset::Default)
    }

    pub fn for_product_preset(product: RadarProduct, preset: ColorTablePreset) -> Self {
        match preset {
            ColorTablePreset::Default => nws_table(product),
            ColorTablePreset::GR2Analyst => gr2a_table(product),
            ColorTablePreset::NSSL => nssl_table(product),
            ColorTablePreset::Classic => classic_table(product),
            ColorTablePreset::Dark => dark_table(product),
            ColorTablePreset::Colorblind => colorblind_table(product),
        }
    }

    /// Create a new color table where values below `min_val` map to transparent.
    pub fn with_min_value(&self, min_val: f32) -> Self {
        let mut entries: Vec<ColorEntry> = self
            .entries
            .iter()
            .filter(|e| e.value >= min_val)
            .cloned()
            .collect();
        // Insert a transparent entry just below the threshold
        entries.insert(
            0,
            ColorEntry {
                value: min_val - 0.01,
                r: 0,
                g: 0,
                b: 0,
                a: 0,
            },
        );
        Self::from_entries_vec(&self.name, entries)
    }

    pub fn generate_legend_pixels(&self, height: usize) -> Vec<[u8; 4]> {
        (0..height)
            .map(|i| {
                let t = 1.0 - (i as f32 / height as f32);
                self.color_for_value(self.min_value + t * (self.max_value - self.min_value))
            })
            .collect()
    }

    pub fn from_pal_file(path: &Path) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        Self::from_pal_string(&content, path.file_stem()?.to_str()?)
    }

    /// Parse GRLevelX / RadarScope .pal format.
    /// Supports: Color:, Color4:, SolidColor:, SolidColor4:, Scale:, Offset:,
    /// dual-RGB gradients, semicolon comments, +/- value prefixes.
    pub fn from_pal_string(content: &str, name: &str) -> Option<Self> {
        let mut entries = Vec::new();
        let mut scale: f32 = 1.0;
        let mut offset: f32 = 0.0;

        for raw_line in content.lines() {
            // Strip semicolon and // comments
            let line = raw_line.split(';').next().unwrap_or("").trim();
            let line = line.split("//").next().unwrap_or("").trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let lower = line.to_ascii_lowercase();

            // Parse header statements
            if lower.starts_with("scale:") {
                let Some((_, val)) = line.split_once(':') else {
                    continue;
                };
                if let Ok(v) = val.trim().parse::<f32>() {
                    scale = v;
                }
                continue;
            }
            if lower.starts_with("offset:") {
                let Some((_, val)) = line.split_once(':') else {
                    continue;
                };
                if let Ok(v) = val.trim().parse::<f32>() {
                    offset = v;
                }
                continue;
            }
            if lower.starts_with("product:")
                || lower.starts_with("units:")
                || lower.starts_with("step:")
                || lower.starts_with("nd:")
                || lower.starts_with("rf:")
                || lower.starts_with("decimals:")
                || lower.starts_with("nd ")
                || lower.starts_with("rf ")
            {
                continue;
            }

            // Strip color statement prefixes
            let (line, has_alpha_prefix) = if lower.starts_with("solidcolor4:") {
                (line.split_once(':')?.1.trim(), true)
            } else if lower.starts_with("solidcolor:") {
                (line.split_once(':')?.1.trim(), false)
            } else if lower.starts_with("color4:") {
                (line.split_once(':')?.1.trim(), true)
            } else if lower.starts_with("color:") {
                (line.split_once(':')?.1.trim(), false)
            } else {
                (line, false)
            };

            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 4 {
                continue;
            }

            let Ok(raw_value) = parts[0].parse::<f32>() else {
                continue;
            };
            // .pal values are display values. Scale/Offset describe
            // display = internal * scale + offset, while rustwx renders
            // decoded internal moment values.
            let value = if scale.abs() > f32::EPSILON {
                (raw_value - offset) / scale
            } else {
                raw_value
            };

            let Ok(r) = parts[1].parse::<u8>() else {
                continue;
            };
            let Ok(g) = parts[2].parse::<u8>() else {
                continue;
            };
            let Ok(b) = parts[3].parse::<u8>() else {
                continue;
            };

            if has_alpha_prefix && parts.len() >= 5 {
                // Color4: value R G B A [R2 G2 B2 A2]
                let a = parts[4].parse::<u8>().unwrap_or(255);
                entries.push(ColorEntry { value, r, g, b, a });
                // If gradient end color specified (8 color components), add midpoint hint
                // but the start color is what matters for our interpolation model
            } else {
                // Color: value R G B [R2 G2 B2]
                let a = 255u8;
                entries.push(ColorEntry { value, r, g, b, a });
            }
        }
        if entries.len() < 2 {
            return None;
        }
        Some(Self::from_entries_vec(name, entries))
    }

    #[allow(dead_code)]
    pub fn from_csv_file(path: &Path) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        Self::from_csv_string(&content, path.file_stem()?.to_str()?)
    }

    pub fn from_csv_string(content: &str, name: &str) -> Option<Self> {
        let mut entries = Vec::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
                continue;
            }
            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() >= 4 {
                if let (Ok(v), Ok(r), Ok(g), Ok(b)) = (
                    parts[0].parse::<f32>(),
                    parts[1].parse::<u8>(),
                    parts[2].parse::<u8>(),
                    parts[3].parse::<u8>(),
                ) {
                    let a = parts
                        .get(4)
                        .and_then(|s| s.parse::<u8>().ok())
                        .unwrap_or(255);
                    entries.push(ColorEntry {
                        value: v,
                        r,
                        g,
                        b,
                        a,
                    });
                }
            }
        }
        if entries.len() < 2 {
            return None;
        }
        Some(Self::from_entries_vec(name, entries))
    }
}

fn interpolate_entries(entries: &[ColorEntry], value: f32) -> [u8; 4] {
    if entries.is_empty() {
        return [0, 0, 0, 0];
    }
    if value <= entries[0].value {
        let e = &entries[0];
        return [e.r, e.g, e.b, e.a];
    }
    let last = entries.len() - 1;
    if value >= entries[last].value {
        let e = &entries[last];
        return [e.r, e.g, e.b, e.a];
    }
    let mut lo = 0usize;
    let mut hi = last;
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        if entries[mid].value <= value {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    let lower = &entries[lo];
    let upper = &entries[hi];
    let span = upper.value - lower.value;
    if span.abs() < 1e-6 {
        return [lower.r, lower.g, lower.b, lower.a];
    }
    let t = ((value - lower.value) / span).clamp(0.0, 1.0);
    [
        (lower.r as f32 + t * (upper.r as f32 - lower.r as f32)) as u8,
        (lower.g as f32 + t * (upper.g as f32 - lower.g as f32)) as u8,
        (lower.b as f32 + t * (upper.b as f32 - lower.b as f32)) as u8,
        (lower.a as f32 + t * (upper.a as f32 - lower.a as f32)) as u8,
    ]
}

#[inline]
fn ce(value: f32, r: u8, g: u8, b: u8, a: u8) -> ColorEntry {
    ColorEntry { value, r, g, b, a }
}

fn base(product: RadarProduct) -> RadarProduct {
    product.base_product()
}

fn embedded_pal_table(name: &str, content: &str) -> Option<ColorTable> {
    ColorTable::from_pal_string(content, name)
}

fn embedded_pal_table_bytes(name: &str, content: &[u8]) -> Option<ColorTable> {
    let content = String::from_utf8_lossy(content);
    ColorTable::from_pal_string(&content, name)
}

fn nws_table(product: RadarProduct) -> ColorTable {
    let p = base(product);
    match p {
        RadarProduct::Reflectivity => {
            embedded_pal_table("Reflectivity (NSSL II)", REFLECTIVITY_NSSL_II_PAL).unwrap_or_else(
                || {
                    ColorTable::from_entries_vec(
                        "Reflectivity (NWS)",
                        vec![
                            ce(-30.0, 0, 0, 0, 0),
                            ce(-20.0, 100, 100, 100, 180),
                            ce(-10.0, 150, 150, 150, 200),
                            ce(0.0, 118, 118, 118, 220),
                            ce(5.0, 0, 236, 236, 255),
                            ce(10.0, 1, 160, 246, 255),
                            ce(15.0, 0, 0, 246, 255),
                            ce(20.0, 0, 255, 0, 255),
                            ce(25.0, 0, 200, 0, 255),
                            ce(30.0, 0, 144, 0, 255),
                            ce(35.0, 255, 255, 0, 255),
                            ce(40.0, 231, 192, 0, 255),
                            ce(45.0, 255, 144, 0, 255),
                            ce(50.0, 255, 0, 0, 255),
                            ce(55.0, 214, 0, 0, 255),
                            ce(60.0, 255, 0, 255, 255),
                            ce(65.0, 200, 0, 255, 255),
                            ce(70.0, 153, 85, 201, 255),
                            ce(75.0, 255, 255, 255, 255),
                            ce(80.0, 255, 255, 255, 255),
                        ],
                    )
                },
            )
        }
        RadarProduct::Velocity | RadarProduct::StormRelativeVelocity => {
            embedded_pal_table("Velocity (RadarScope BV)", VELOCITY_RADARSCOPE_BVEL_PAL)
                .unwrap_or_else(|| {
                    ColorTable::from_entries_vec(
                        "Velocity (NWS)",
                        vec![
                            ce(-120.0, 140, 0, 170, 255),
                            ce(-100.0, 180, 0, 170, 255),
                            ce(-80.0, 210, 0, 120, 255),
                            ce(-64.0, 235, 0, 70, 255),
                            ce(-50.0, 245, 40, 60, 255),
                            ce(-36.0, 250, 90, 90, 255),
                            ce(-26.0, 255, 135, 135, 255),
                            ce(-20.0, 255, 170, 170, 255),
                            ce(-10.0, 245, 210, 210, 255),
                            ce(-1.0, 170, 170, 170, 220),
                            ce(0.0, 0, 0, 0, 0),
                            ce(1.0, 170, 170, 170, 220),
                            ce(10.0, 200, 245, 200, 255),
                            ce(20.0, 135, 235, 135, 255),
                            ce(26.0, 60, 220, 90, 255),
                            ce(36.0, 0, 200, 90, 255),
                            ce(50.0, 0, 170, 140, 255),
                            ce(64.0, 0, 140, 220, 255),
                            ce(80.0, 0, 100, 235, 255),
                            ce(100.0, 0, 160, 255, 255),
                            ce(120.0, 140, 240, 255, 255),
                        ],
                    )
                })
        }
        RadarProduct::SpectrumWidth => ColorTable::from_entries_vec(
            "Spectrum Width (NWS)",
            vec![
                ce(0.0, 0, 0, 0, 0),
                ce(2.0, 100, 100, 100, 200),
                ce(5.0, 0, 150, 0, 255),
                ce(10.0, 0, 255, 0, 255),
                ce(15.0, 255, 255, 0, 255),
                ce(20.0, 255, 150, 0, 255),
                ce(25.0, 255, 0, 0, 255),
                ce(30.0, 200, 0, 0, 255),
                ce(40.0, 255, 255, 255, 255),
            ],
        ),
        RadarProduct::DifferentialReflectivity => ColorTable::from_entries_vec(
            "ZDR (NWS)",
            vec![
                ce(-8.0, 0, 0, 128, 255),
                ce(-4.0, 0, 0, 255, 255),
                ce(-2.0, 0, 150, 255, 255),
                ce(-1.0, 0, 200, 200, 255),
                ce(0.0, 100, 100, 100, 200),
                ce(1.0, 0, 200, 0, 255),
                ce(2.0, 255, 255, 0, 255),
                ce(4.0, 255, 128, 0, 255),
                ce(6.0, 255, 0, 0, 255),
                ce(8.0, 200, 0, 200, 255),
            ],
        ),
        RadarProduct::CorrelationCoefficient => embedded_pal_table("CC (GRS)", CC_DEFAULT_PAL)
            .unwrap_or_else(|| {
                ColorTable::from_entries_vec(
                    "CC (NWS)",
                    vec![
                        ce(0.2, 0, 0, 0, 0),
                        ce(0.5, 128, 0, 128, 255),
                        ce(0.7, 0, 0, 200, 255),
                        ce(0.8, 0, 150, 255, 255),
                        ce(0.85, 0, 200, 200, 255),
                        ce(0.90, 0, 200, 0, 255),
                        ce(0.93, 255, 255, 0, 255),
                        ce(0.95, 255, 128, 0, 255),
                        ce(0.97, 255, 0, 0, 255),
                        ce(0.99, 200, 0, 200, 255),
                        ce(1.05, 255, 255, 255, 255),
                    ],
                )
            }),
        RadarProduct::DifferentialPhase => ColorTable::from_entries_vec(
            "PhiDP (NWS)",
            vec![
                ce(0.0, 128, 0, 128, 255),
                ce(30.0, 0, 0, 200, 255),
                ce(60.0, 0, 150, 255, 255),
                ce(90.0, 0, 200, 200, 255),
                ce(120.0, 0, 200, 0, 255),
                ce(150.0, 0, 255, 0, 255),
                ce(180.0, 255, 255, 0, 255),
                ce(210.0, 255, 200, 0, 255),
                ce(240.0, 255, 128, 0, 255),
                ce(270.0, 255, 0, 0, 255),
                ce(300.0, 200, 0, 0, 255),
                ce(360.0, 200, 0, 200, 255),
            ],
        ),
        RadarProduct::SpecificDiffPhase => embedded_pal_table_bytes("KDP (GRS)", KDP_DEFAULT_PAL)
            .unwrap_or_else(|| {
                ColorTable::from_entries_vec(
                    "KDP (NWS)",
                    vec![
                        ce(-2.0, 128, 0, 128, 255),
                        ce(-1.0, 0, 0, 200, 255),
                        ce(0.0, 100, 100, 100, 200),
                        ce(0.5, 0, 200, 0, 255),
                        ce(1.0, 0, 255, 0, 255),
                        ce(2.0, 255, 255, 0, 255),
                        ce(3.0, 255, 200, 0, 255),
                        ce(5.0, 255, 128, 0, 255),
                        ce(7.0, 255, 0, 0, 255),
                        ce(10.0, 200, 0, 200, 255),
                    ],
                )
            }),
        RadarProduct::VIL => {
            embedded_pal_table("VIL (GRS)", VIL_DEFAULT_PAL).unwrap_or_else(|| {
                ColorTable::from_entries_vec(
                    "VIL (NWS)",
                    vec![
                        ce(0.0, 0, 0, 0, 0),
                        ce(1.0, 0, 130, 0, 200),
                        ce(5.0, 0, 200, 0, 255),
                        ce(10.0, 0, 255, 0, 255),
                        ce(15.0, 200, 255, 0, 255),
                        ce(25.0, 255, 255, 0, 255),
                        ce(30.0, 255, 200, 0, 255),
                        ce(40.0, 255, 128, 0, 255),
                        ce(50.0, 255, 0, 0, 255),
                        ce(60.0, 200, 0, 0, 255),
                        ce(70.0, 180, 0, 180, 255),
                        ce(80.0, 255, 0, 255, 255),
                    ],
                )
            })
        }
        RadarProduct::EchoTops => {
            embedded_pal_table("Echo Tops (Enhanced)", ECHO_TOPS_ENHANCED_PAL).unwrap_or_else(
                || {
                    ColorTable::from_entries_vec(
                        "Echo Tops (NWS)",
                        vec![
                            ce(0.0, 0, 0, 0, 0),
                            ce(1.0, 0, 0, 180, 200),
                            ce(3.0, 0, 100, 255, 255),
                            ce(5.0, 0, 200, 255, 255),
                            ce(7.0, 0, 200, 0, 255),
                            ce(10.0, 0, 255, 0, 255),
                            ce(12.0, 255, 255, 0, 255),
                            ce(15.0, 255, 200, 0, 255),
                            ce(17.0, 255, 0, 0, 255),
                            ce(20.0, 200, 0, 0, 255),
                        ],
                    )
                },
            )
        }
        _ => nws_table(RadarProduct::Reflectivity),
    }
}

fn gr2a_table(product: RadarProduct) -> ColorTable {
    let p = base(product);
    match p {
        RadarProduct::Reflectivity => ColorTable::from_entries_vec(
            "Reflectivity (GR2A)",
            vec![
                ce(-30.0, 0, 0, 0, 0),
                ce(-20.0, 64, 64, 64, 160),
                ce(-10.0, 128, 128, 128, 200),
                ce(0.0, 180, 180, 180, 220),
                ce(5.0, 0, 255, 255, 255),
                ce(10.0, 0, 180, 255, 255),
                ce(15.0, 0, 0, 255, 255),
                ce(20.0, 0, 255, 0, 255),
                ce(25.0, 0, 200, 0, 255),
                ce(30.0, 0, 128, 0, 255),
                ce(35.0, 255, 255, 0, 255),
                ce(40.0, 255, 200, 0, 255),
                ce(45.0, 255, 128, 0, 255),
                ce(50.0, 255, 0, 0, 255),
                ce(55.0, 200, 0, 0, 255),
                ce(60.0, 255, 0, 255, 255),
                ce(65.0, 200, 100, 255, 255),
                ce(70.0, 200, 100, 255, 255),
                ce(75.0, 255, 200, 255, 255),
                ce(80.0, 255, 255, 255, 255),
            ],
        ),
        RadarProduct::Velocity => ColorTable::from_entries_vec(
            "Velocity (GR2A)",
            vec![
                ce(-120.0, 255, 0, 255, 255),
                ce(-80.0, 160, 0, 0, 255),
                ce(-64.0, 255, 0, 0, 255),
                ce(-50.0, 230, 80, 0, 255),
                ce(-36.0, 255, 160, 0, 255),
                ce(-26.0, 255, 220, 0, 255),
                ce(-20.0, 255, 240, 150, 255),
                ce(-10.0, 180, 60, 60, 255),
                ce(-1.0, 120, 70, 70, 200),
                ce(0.0, 0, 0, 0, 0),
                ce(1.0, 70, 120, 70, 200),
                ce(10.0, 60, 180, 60, 255),
                ce(20.0, 150, 240, 150, 255),
                ce(26.0, 0, 220, 0, 255),
                ce(36.0, 0, 160, 255, 255),
                ce(50.0, 0, 80, 230, 255),
                ce(64.0, 0, 0, 255, 255),
                ce(80.0, 0, 0, 160, 255),
                ce(120.0, 0, 255, 255, 255),
            ],
        ),
        _ => {
            let mut t = nws_table(p);
            t.name = format!("{} (GR2A)", t.name.split(" (").next().unwrap_or(&t.name));
            t
        }
    }
}

fn nssl_table(product: RadarProduct) -> ColorTable {
    let p = base(product);
    match p {
        RadarProduct::Reflectivity => ColorTable::from_entries_vec(
            "Reflectivity (NSSL)",
            vec![
                ce(-30.0, 0, 0, 0, 0),
                ce(-20.0, 80, 80, 80, 160),
                ce(-10.0, 130, 130, 130, 200),
                ce(0.0, 150, 200, 255, 220),
                ce(5.0, 100, 180, 255, 255),
                ce(10.0, 50, 130, 255, 255),
                ce(15.0, 0, 70, 200, 255),
                ce(20.0, 0, 230, 0, 255),
                ce(25.0, 0, 180, 0, 255),
                ce(30.0, 0, 130, 0, 255),
                ce(35.0, 255, 255, 0, 255),
                ce(40.0, 255, 180, 0, 255),
                ce(45.0, 255, 100, 0, 255),
                ce(50.0, 255, 0, 0, 255),
                ce(55.0, 220, 0, 0, 255),
                ce(60.0, 255, 0, 200, 255),
                ce(65.0, 180, 60, 255, 255),
                ce(70.0, 180, 60, 255, 255),
                ce(75.0, 220, 200, 255, 255),
                ce(80.0, 255, 255, 255, 255),
            ],
        ),
        RadarProduct::Velocity => ColorTable::from_entries_vec(
            "Velocity (NSSL)",
            vec![
                ce(-120.0, 200, 0, 200, 255),
                ce(-80.0, 180, 0, 0, 255),
                ce(-64.0, 255, 30, 30, 255),
                ce(-50.0, 220, 60, 0, 255),
                ce(-36.0, 255, 140, 0, 255),
                ce(-26.0, 255, 200, 80, 255),
                ce(-10.0, 160, 60, 60, 240),
                ce(-1.0, 100, 60, 60, 180),
                ce(0.0, 0, 0, 0, 0),
                ce(1.0, 60, 100, 60, 180),
                ce(10.0, 60, 160, 60, 240),
                ce(26.0, 80, 200, 255, 255),
                ce(36.0, 0, 140, 255, 255),
                ce(50.0, 0, 60, 220, 255),
                ce(64.0, 30, 30, 255, 255),
                ce(80.0, 0, 0, 180, 255),
                ce(120.0, 0, 200, 200, 255),
            ],
        ),
        _ => {
            let mut t = nws_table(p);
            t.name = format!("{} (NSSL)", t.name.split(" (").next().unwrap_or(&t.name));
            t
        }
    }
}

fn classic_table(product: RadarProduct) -> ColorTable {
    let p = base(product);
    match p {
        RadarProduct::Reflectivity => ColorTable::from_entries_vec(
            "Reflectivity (Classic)",
            vec![
                ce(-30.0, 0, 0, 0, 0),
                ce(0.0, 0, 0, 0, 0),
                ce(5.0, 0, 200, 200, 255),
                ce(10.0, 0, 128, 200, 255),
                ce(15.0, 0, 0, 200, 255),
                ce(20.0, 0, 200, 0, 255),
                ce(25.0, 0, 160, 0, 255),
                ce(30.0, 0, 120, 0, 255),
                ce(35.0, 200, 200, 0, 255),
                ce(40.0, 200, 160, 0, 255),
                ce(45.0, 200, 100, 0, 255),
                ce(50.0, 200, 0, 0, 255),
                ce(55.0, 160, 0, 0, 255),
                ce(60.0, 200, 0, 200, 255),
                ce(65.0, 160, 0, 255, 255),
                ce(70.0, 160, 60, 160, 255),
                ce(75.0, 200, 200, 200, 255),
                ce(80.0, 255, 255, 255, 255),
            ],
        ),
        _ => {
            let mut t = nws_table(p);
            t.name = format!("{} (Classic)", t.name.split(" (").next().unwrap_or(&t.name));
            t
        }
    }
}

fn dark_table(product: RadarProduct) -> ColorTable {
    let mut base_t = nws_table(product);
    let base_name = base_t
        .name
        .split(" (")
        .next()
        .unwrap_or(&base_t.name)
        .to_string();
    for entry in &mut base_t.entries {
        if entry.a < 200 {
            entry.a = (entry.a as f32 * 0.7) as u8;
        }
        entry.r = (entry.r as f32 * 1.1).min(255.0) as u8;
        entry.g = (entry.g as f32 * 1.1).min(255.0) as u8;
        entry.b = (entry.b as f32 * 1.1).min(255.0) as u8;
    }
    ColorTable::from_entries_vec(&format!("{} (Dark)", base_name), base_t.entries)
}

fn colorblind_table(product: RadarProduct) -> ColorTable {
    let p = base(product);
    match p {
        RadarProduct::Reflectivity => ColorTable::from_entries_vec(
            "Reflectivity (Colorblind)",
            vec![
                ce(-30.0, 0, 0, 0, 0),
                ce(0.0, 230, 230, 230, 200),
                ce(10.0, 171, 217, 233, 255),
                ce(20.0, 44, 123, 182, 255),
                ce(30.0, 255, 255, 191, 255),
                ce(40.0, 253, 174, 97, 255),
                ce(50.0, 215, 25, 28, 255),
                ce(60.0, 128, 0, 38, 255),
                ce(70.0, 77, 0, 75, 255),
                ce(80.0, 255, 255, 255, 255),
            ],
        ),
        RadarProduct::Velocity => ColorTable::from_entries_vec(
            "Velocity (Colorblind)",
            vec![
                ce(-120.0, 165, 0, 38, 255),
                ce(-64.0, 215, 48, 39, 255),
                ce(-36.0, 244, 109, 67, 255),
                ce(-20.0, 253, 174, 97, 255),
                ce(-5.0, 254, 224, 144, 255),
                ce(-1.0, 200, 200, 200, 180),
                ce(0.0, 0, 0, 0, 0),
                ce(1.0, 200, 200, 200, 180),
                ce(5.0, 171, 217, 233, 255),
                ce(20.0, 116, 173, 209, 255),
                ce(36.0, 69, 117, 180, 255),
                ce(64.0, 49, 54, 149, 255),
                ce(120.0, 0, 0, 80, 255),
            ],
        ),
        _ => {
            let mut t = nws_table(p);
            t.name = format!(
                "{} (Colorblind)",
                t.name.split(" (").next().unwrap_or(&t.name)
            );
            t
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColorTableManager {
    pub selections: HashMap<String, ColorTableSelection>,
    #[serde(default)]
    pub custom_tables: HashMap<String, SerializableColorTable>,
    #[serde(skip)]
    pub path_input: String,
    #[serde(skip)]
    pub status_message: Option<String>,
}

impl Default for ColorTableManager {
    fn default() -> Self {
        Self {
            selections: HashMap::new(),
            custom_tables: HashMap::new(),
            path_input: String::new(),
            status_message: None,
        }
    }
}

impl ColorTableManager {
    fn config_file_path() -> Option<PathBuf> {
        #[cfg(target_os = "windows")]
        {
            std::env::var("APPDATA")
                .ok()
                .map(|p| PathBuf::from(p).join("rustdar").join("colortables.json"))
        }
        #[cfg(not(target_os = "windows"))]
        {
            std::env::var("HOME").ok().map(|h| {
                PathBuf::from(h)
                    .join(".config")
                    .join("rustdar")
                    .join("colortables.json")
            })
        }
    }

    pub fn load_persisted() -> Self {
        if let Some(path) = Self::config_file_path() {
            if let Ok(content) = std::fs::read_to_string(&path) {
                if let Ok(mgr) = serde_json::from_str::<ColorTableManager>(&content) {
                    log::info!("Loaded {} custom color tables", mgr.custom_tables.len());
                    return mgr;
                }
            }
        }
        Self::default()
    }

    pub fn save(&self) {
        if let Some(path) = Self::config_file_path() {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Ok(json) = serde_json::to_string_pretty(self) {
                let _ = std::fs::write(&path, json);
            }
        }
    }

    fn product_key(product: RadarProduct) -> String {
        format!("{:?}", product.base_product())
    }

    pub fn selection_for(&self, product: RadarProduct) -> ColorTableSelection {
        self.selections
            .get(&Self::product_key(product))
            .cloned()
            .unwrap_or_default()
    }

    pub fn set_selection(&mut self, product: RadarProduct, sel: ColorTableSelection) {
        self.selections.insert(Self::product_key(product), sel);
        self.save();
    }

    pub fn resolve(&self, product: RadarProduct) -> ColorTable {
        match self.selection_for(product) {
            ColorTableSelection::Preset(preset) => ColorTable::for_product_preset(product, preset),
            ColorTableSelection::Custom(name) => self
                .custom_tables
                .get(&name)
                .map(|s| s.to_color_table())
                .unwrap_or_else(|| ColorTable::for_product(product)),
        }
    }

    pub fn load_from_file(&mut self, path: &Path) -> Result<String, String> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        let content = std::fs::read_to_string(path).map_err(|e| format!("Read error: {}", e))?;
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("Custom")
            .to_string();
        let table = match ext.as_str() {
            "pal" | "pal3" | "wctpal" | "txt" => ColorTable::from_pal_string(&content, &name)
                .ok_or_else(|| {
                    "Failed to parse color table (need at least 2 color entries)".to_string()
                })?,
            "csv" => ColorTable::from_csv_string(&content, &name)
                .ok_or_else(|| "Failed to parse .csv color table".to_string())?,
            _ => ColorTable::from_pal_string(&content, &name)
                .or_else(|| ColorTable::from_csv_string(&content, &name))
                .ok_or_else(|| "Unrecognized color table format".to_string())?,
        };
        let table_name = table.name.clone();
        self.custom_tables.insert(
            table_name.clone(),
            SerializableColorTable::from_color_table(&table),
        );
        self.save();
        Ok(table_name)
    }

    pub fn available_names(&self) -> Vec<(String, ColorTableSelection)> {
        let mut names: Vec<(String, ColorTableSelection)> = ColorTablePreset::all()
            .iter()
            .map(|p| (p.label().to_string(), ColorTableSelection::Preset(*p)))
            .collect();
        let mut custom_names: Vec<String> = self.custom_tables.keys().cloned().collect();
        custom_names.sort();
        for c in custom_names {
            names.push((c.clone(), ColorTableSelection::Custom(c)));
        }
        names
    }

    pub fn remove_custom(&mut self, name: &str) {
        self.custom_tables.remove(name);
        let keys_to_reset: Vec<String> = self
            .selections
            .iter()
            .filter(|(_, s)| matches!(s, ColorTableSelection::Custom(n) if n == name))
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys_to_reset {
            self.selections.insert(k, ColorTableSelection::default());
        }
        self.save();
    }

    pub fn selected_label(&self, product: RadarProduct) -> String {
        match self.selection_for(product) {
            ColorTableSelection::Preset(p) => p.label().to_string(),
            ColorTableSelection::Custom(n) => n,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pal_parser_handles_case_and_scale_metadata() {
        let reflectivity =
            ColorTable::from_pal_string(REFLECTIVITY_NSSL_II_PAL, "reflectivity").unwrap();
        assert!(reflectivity.entries.len() >= 10);
        assert!(reflectivity.min_value <= 5.0);

        let cc = ColorTable::from_pal_string(CC_DEFAULT_PAL, "cc").unwrap();
        assert!(cc.min_value <= 0.21);
        assert!(cc.max_value >= 0.99);

        let echo_tops = ColorTable::from_pal_string(ECHO_TOPS_ENHANCED_PAL, "et").unwrap();
        assert!(echo_tops.max_value < 25.0);
    }

    #[test]
    fn default_velocity_palette_is_operational_not_gray() {
        let velocity = ColorTable::for_product(RadarProduct::Velocity);
        let inbound = velocity.color_for_value(-20.0);
        let outbound = velocity.color_for_value(20.0);

        assert!(inbound[1] > inbound[0]);
        assert!(outbound[0] > outbound[1]);
        assert!(outbound[3] > 0);
    }
}
