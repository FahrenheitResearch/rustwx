use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use rayon::prelude::*;
use rustwx_core::{
    CanonicalField, CycleSpec, FieldSelector, ModelId, ModelRunRequest, SelectedField2D, SourceId,
};
use rustwx_io::earth2_archive::{Earth2EnsembleSelector, Earth2EnsembleStat};
use rustwx_io::{extract_fields_partial_from_model_bytes, fetch_bytes_with_cache, FetchRequest};
use rustwx_models::{plot_recipe, plot_recipe_fetch_plan, LatestRun};
use rustwx_render::{
    map_frame_aspect_ratio_for_mode, save_png_profile_with_options, Color, ColorScale,
    DiscreteColorScale, ExtendMode, MapRenderRequest, PngCompressionMode, PngWriteOptions,
    ProductVisualMode,
};
use serde::{Deserialize, Serialize};

use crate::direct::{render_direct_recipe_from_selected_fields, DirectBatchRequest};
use crate::places::PlaceLabelOverlay;
use crate::shared_context::{
    static_chrome_scale, static_supersample_factor, static_supersample_sharpen,
    static_title_with_suffix, DomainSpec,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GribEnsembleStat {
    Mean,
    Std,
    Min,
    Max,
    P10,
    P50,
    P90,
    ProbExceed,
}

impl GribEnsembleStat {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Mean => "mean",
            Self::Std => "std",
            Self::Min => "min",
            Self::Max => "max",
            Self::P10 => "p10",
            Self::P50 => "p50",
            Self::P90 => "p90",
            Self::ProbExceed => "prob_exceed",
        }
    }

    fn percentile(self) -> Option<f32> {
        match self {
            Self::P10 => Some(0.10),
            Self::P50 => Some(0.50),
            Self::P90 => Some(0.90),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    Gt,
    Ge,
    Lt,
    Le,
}

impl CompareOp {
    fn compare(self, value: f32, threshold: f32) -> bool {
        match self {
            Self::Gt => value > threshold,
            Self::Ge => value >= threshold,
            Self::Lt => value < threshold,
            Self::Le => value <= threshold,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::Gt => "gt",
            Self::Ge => "ge",
            Self::Lt => "lt",
            Self::Le => "le",
        }
    }

    fn symbol(self) -> &'static str {
        match self {
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::Lt => "<",
            Self::Le => "<=",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GribEnsembleRenderRequest {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub recipe_slug: String,
    pub member_products: Vec<String>,
    pub stat: GribEnsembleStat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold_op: Option<CompareOp>,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub place_label_overlay: Option<PlaceLabelOverlay>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GribEnsembleMemberFetch {
    pub member_product: String,
    pub resolved_url: String,
    pub bytes: u64,
    pub cache_hit: bool,
}

struct GribEnsembleMemberFields {
    fields: HashMap<FieldSelector, SelectedField2D>,
    fetch: GribEnsembleMemberFetch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GribEnsembleRenderReport {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub recipe_slug: String,
    pub stat: GribEnsembleStat,
    pub member_count: usize,
    pub member_products: Vec<String>,
    pub output_path: PathBuf,
    pub member_fetches: Vec<GribEnsembleMemberFetch>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub timings_ms: BTreeMap<String, u128>,
}

fn default_output_width() -> u32 {
    1600
}

fn default_output_height() -> u32 {
    900
}

fn default_png_compression() -> PngCompressionMode {
    PngCompressionMode::Default
}

pub fn default_grib_member_products(model: ModelId) -> Option<Vec<String>> {
    match model {
        ModelId::Gefs => Some(
            std::iter::once("pgrb2ap5/gec00".to_string())
                .chain((1..=30).map(|idx| format!("pgrb2ap5/gep{idx:02}")))
                .collect(),
        ),
        _ => None,
    }
}

pub fn expand_member_template(template: &str, members: &[String]) -> Vec<String> {
    members
        .iter()
        .map(|member| template.replace("{member}", member))
        .collect()
}

fn extract_ensemble_member_fields(
    request: &GribEnsembleRenderRequest,
    cycle: &CycleSpec,
    product: &str,
    variable_patterns_template: &[String],
    selectors: &[FieldSelector],
) -> Result<GribEnsembleMemberFields, String> {
    let model_request = ModelRunRequest::new(
        request.model,
        cycle.clone(),
        request.forecast_hour,
        product.to_string(),
    )
    .map_err(|error| error.to_string())?;
    let fetch = FetchRequest {
        request: model_request,
        source_override: Some(request.source),
        variable_patterns: variable_patterns_template.to_vec(),
        earth2_ensemble: None,
    };
    let fetched = fetch_bytes_with_cache(&fetch, &request.cache_root, request.use_cache)
        .map_err(|error| error.to_string())?;
    let partial = extract_fields_partial_from_model_bytes(
        request.model,
        &fetched.result.bytes,
        Some(&fetched.bytes_path),
        selectors,
    )
    .map_err(|error| error.to_string())?;
    if !partial.missing.is_empty() {
        return Err(format!(
            "member product '{}' is missing selectors: {:?}",
            product, partial.missing
        ));
    }

    let mut fields = HashMap::new();
    for field in partial.extracted {
        fields.insert(field.selector, field);
    }
    Ok(GribEnsembleMemberFields {
        fields,
        fetch: GribEnsembleMemberFetch {
            member_product: product.to_string(),
            resolved_url: fetched.result.url.clone(),
            bytes: fetched.result.bytes.len() as u64,
            cache_hit: fetched.cache_hit,
        },
    })
}

pub fn run_grib_ensemble_render(
    request: &GribEnsembleRenderRequest,
) -> Result<GribEnsembleRenderReport, Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    if request.member_products.is_empty() {
        return Err("GRIB ensemble reducer needs at least one member product".into());
    }
    if request.stat == GribEnsembleStat::ProbExceed && request.threshold.is_none() {
        return Err("--threshold is required for prob_exceed".into());
    }

    fs::create_dir_all(&request.out_dir)?;
    fs::create_dir_all(&request.cache_root)?;

    let recipe = plot_recipe(&request.recipe_slug)
        .ok_or_else(|| format!("unknown direct recipe '{}'", request.recipe_slug))?;
    let plan = plot_recipe_fetch_plan(recipe.slug, request.model)?;
    let selectors = plan.selectors();
    let cycle = CycleSpec::new(&request.date_yyyymmdd, request.cycle_utc)?;

    let mut member_fields = Vec::<HashMap<FieldSelector, SelectedField2D>>::new();
    let mut member_fetches = Vec::<GribEnsembleMemberFetch>::new();
    let member_fetch_extract_start = Instant::now();
    // NOMADS production paths are full-GRIB only; non-empty patterns would
    // create recipe-specific cache keys and miss the already cached member GRIB.
    let variable_patterns_template = if request.source == SourceId::Nomads {
        Vec::new()
    } else {
        plan.idx_patterns()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    let extracted_members = request
        .member_products
        .par_iter()
        .map(|product| {
            extract_ensemble_member_fields(
                request,
                &cycle,
                product,
                &variable_patterns_template,
                &selectors,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|message| -> Box<dyn std::error::Error> { message.into() })?;
    for member in extracted_members {
        member_fetches.push(member.fetch);
        member_fields.push(member.fields);
    }
    let member_fetch_extract_ms = member_fetch_extract_start.elapsed().as_millis();

    let threshold = request.threshold.unwrap_or(f32::NAN);
    let op = request.threshold_op.unwrap_or(CompareOp::Gt);
    let mut reduced = HashMap::<FieldSelector, SelectedField2D>::new();
    let reduce_start = Instant::now();
    for selector in selectors {
        let fields = member_fields
            .iter()
            .map(|member| {
                member
                    .get(&selector)
                    .ok_or_else(|| format!("member extraction lost selector {}", selector.key()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        reduced.insert(
            selector,
            reduce_fields(selector, &fields, request.stat, threshold, op)?,
        );
    }
    let reduce_ms = reduce_start.elapsed().as_millis();

    let latest = LatestRun {
        model: request.model,
        cycle,
        source: request.source,
    };
    let suffix = if request.stat == GribEnsembleStat::ProbExceed {
        format!(
            "{}_{}_{}",
            request.stat.slug(),
            op.slug(),
            compact_threshold_slug(threshold)
        )
    } else {
        request.stat.slug().to_string()
    };

    let render_start = Instant::now();
    let output_path = if matches!(
        request.stat,
        GribEnsembleStat::Std | GribEnsembleStat::ProbExceed
    ) {
        render_spread_or_probability_map(request, recipe.title, &reduced, &latest, &suffix)?
    } else {
        let direct_request = DirectBatchRequest {
            model: request.model,
            date_yyyymmdd: request.date_yyyymmdd.clone(),
            cycle_override_utc: Some(request.cycle_utc),
            forecast_hour: request.forecast_hour,
            source: request.source,
            domain: request.domain.clone(),
            out_dir: request.out_dir.clone(),
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
            recipe_slugs: vec![request.recipe_slug.clone()],
            product_overrides: HashMap::new(),
            contour_mode: crate::derived::NativeContourRenderMode::Automatic,
            native_fill_level_multiplier: 1,
            output_width: request.output_width,
            output_height: request.output_height,
            png_compression: request.png_compression,
            custom_poi_overlay: None,
            place_label_overlay: request.place_label_overlay.clone(),
            output_suffix: None,
            subtitle_left_override: None,
            subtitle_right_override: None,
            earth2_ensemble: grib_stat_as_earth2_selector(request.stat),
        };
        render_direct_recipe_from_selected_fields(
            &direct_request,
            &latest,
            &request.recipe_slug,
            &reduced,
            format!("grib_ensemble_{}", request.stat.slug()),
            format!(
                "ensemble-reducer://{}/{}",
                request.model, request.recipe_slug
            ),
            format!("ensemble:{}:{}", request.model, suffix),
        )?
        .output_path
    };
    let render_ms = render_start.elapsed().as_millis();

    let mut timings_ms = BTreeMap::new();
    timings_ms.insert("member_fetch_extract".to_string(), member_fetch_extract_ms);
    timings_ms.insert("reduce".to_string(), reduce_ms);
    timings_ms.insert("render".to_string(), render_ms);
    timings_ms.insert("total".to_string(), total_start.elapsed().as_millis());

    Ok(GribEnsembleRenderReport {
        model: request.model,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: request.cycle_utc,
        forecast_hour: request.forecast_hour,
        source: request.source,
        recipe_slug: request.recipe_slug.clone(),
        stat: request.stat,
        member_count: request.member_products.len(),
        member_products: request.member_products.clone(),
        output_path,
        member_fetches,
        timings_ms,
    })
}

fn reduce_fields(
    selector: FieldSelector,
    fields: &[&SelectedField2D],
    stat: GribEnsembleStat,
    threshold: f32,
    op: CompareOp,
) -> Result<SelectedField2D, Box<dyn std::error::Error>> {
    let first = fields.first().ok_or("no fields to reduce")?;
    for field in fields.iter().skip(1) {
        if field.grid.shape != first.grid.shape {
            return Err(
                format!("selector {} has inconsistent member grids", selector.key()).into(),
            );
        }
    }
    let len = first.values.len();
    let mut out = Vec::with_capacity(len);
    match stat {
        GribEnsembleStat::Mean => {
            for idx in 0..len {
                let mut sum = 0.0f32;
                let mut count = 0usize;
                for field in fields {
                    let value = field.values[idx];
                    if value.is_finite() {
                        sum += value;
                        count += 1;
                    }
                }
                out.push(if count > 0 {
                    sum / count as f32
                } else {
                    f32::NAN
                });
            }
        }
        GribEnsembleStat::Std => {
            for idx in 0..len {
                let mut sum = 0.0f32;
                let mut count = 0usize;
                for field in fields {
                    let value = field.values[idx];
                    if value.is_finite() {
                        sum += value;
                        count += 1;
                    }
                }
                if count == 0 {
                    out.push(f32::NAN);
                    continue;
                }
                let mean = sum / count as f32;
                let mut var_sum = 0.0f32;
                for field in fields {
                    let value = field.values[idx];
                    if value.is_finite() {
                        let diff = value - mean;
                        var_sum += diff * diff;
                    }
                }
                out.push((var_sum / count as f32).sqrt());
            }
        }
        GribEnsembleStat::Min => {
            for idx in 0..len {
                let mut value_min = f32::INFINITY;
                let mut found = false;
                for field in fields {
                    let value = field.values[idx];
                    if value.is_finite() {
                        value_min = value_min.min(value);
                        found = true;
                    }
                }
                out.push(if found { value_min } else { f32::NAN });
            }
        }
        GribEnsembleStat::Max => {
            for idx in 0..len {
                let mut value_max = f32::NEG_INFINITY;
                let mut found = false;
                for field in fields {
                    let value = field.values[idx];
                    if value.is_finite() {
                        value_max = value_max.max(value);
                        found = true;
                    }
                }
                out.push(if found { value_max } else { f32::NAN });
            }
        }
        GribEnsembleStat::P10 | GribEnsembleStat::P50 | GribEnsembleStat::P90 => {
            let q = stat.percentile().unwrap_or(0.5);
            let mut values = Vec::with_capacity(fields.len());
            for idx in 0..len {
                values.clear();
                for field in fields {
                    let value = field.values[idx];
                    if value.is_finite() {
                        values.push(value);
                    }
                }
                out.push(if values.is_empty() {
                    f32::NAN
                } else {
                    percentile(&mut values, q)
                });
            }
        }
        GribEnsembleStat::ProbExceed => {
            let threshold = comparison_threshold_value(selector, first.units.as_str(), threshold);
            for idx in 0..len {
                let mut count = 0usize;
                let mut hits = 0usize;
                for field in fields {
                    let value = field.values[idx];
                    if value.is_finite() {
                        count += 1;
                        if op.compare(value, threshold) {
                            hits += 1;
                        }
                    }
                }
                out.push(if count > 0 {
                    hits as f32 / count as f32
                } else {
                    f32::NAN
                });
            }
        }
    }
    let units = if stat == GribEnsembleStat::ProbExceed {
        "probability"
    } else {
        first.units.as_str()
    };
    let mut reduced = SelectedField2D::new(selector, units, first.grid.clone(), out)?;
    if let Some(projection) = first.projection.clone() {
        reduced = reduced.with_projection(projection);
    }
    Ok(reduced)
}

fn grib_stat_as_earth2_selector(stat: GribEnsembleStat) -> Option<Earth2EnsembleSelector> {
    Some(Earth2EnsembleSelector::Statistic(match stat {
        GribEnsembleStat::Mean => Earth2EnsembleStat::Mean,
        GribEnsembleStat::Std => Earth2EnsembleStat::Std,
        GribEnsembleStat::Min => Earth2EnsembleStat::Min,
        GribEnsembleStat::Max => Earth2EnsembleStat::Max,
        GribEnsembleStat::P10 => Earth2EnsembleStat::P10,
        GribEnsembleStat::P50 => Earth2EnsembleStat::P50,
        GribEnsembleStat::P90 => Earth2EnsembleStat::P90,
        GribEnsembleStat::ProbExceed => return None,
    }))
}

fn comparison_threshold_value(selector: FieldSelector, units: &str, threshold: f32) -> f32 {
    if matches!(
        selector.field,
        CanonicalField::Temperature | CanonicalField::Dewpoint
    ) && units.eq_ignore_ascii_case("K")
        && (-100.0..=80.0).contains(&threshold)
    {
        threshold + 273.15
    } else {
        threshold
    }
}

fn comparison_threshold_label(selector: FieldSelector, units: &str, threshold: f32) -> String {
    if matches!(
        selector.field,
        CanonicalField::Temperature | CanonicalField::Dewpoint
    ) && (-100.0..=80.0).contains(&threshold)
    {
        format_compact_threshold(threshold, "C")
    } else if matches!(
        selector.field,
        CanonicalField::Temperature | CanonicalField::Dewpoint
    ) && (150.0..=400.0).contains(&threshold)
    {
        format_compact_threshold(threshold, "K")
    } else {
        format_compact_threshold(threshold, units)
    }
}

fn format_compact_threshold(value: f32, units: &str) -> String {
    let value = if (value - value.round()).abs() < 0.01 {
        format!("{:.0}", value)
    } else {
        format!("{value:.2}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    };
    let units = units.trim();
    if units.is_empty() {
        value
    } else {
        format!("{value} {units}")
    }
}

fn percentile(values: &mut [f32], q: f32) -> f32 {
    values.sort_by(|a, b| a.total_cmp(b));
    if values.len() == 1 {
        return values[0];
    }
    let pos = q.clamp(0.0, 1.0) * (values.len() - 1) as f32;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        values[lo]
    } else {
        let weight = pos - lo as f32;
        values[lo] * (1.0 - weight) + values[hi] * weight
    }
}

fn render_spread_or_probability_map(
    request: &GribEnsembleRenderRequest,
    recipe_title: &str,
    reduced: &HashMap<FieldSelector, SelectedField2D>,
    latest: &LatestRun,
    suffix: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let recipe = plot_recipe(&request.recipe_slug)
        .ok_or_else(|| format!("unknown direct recipe '{}'", request.recipe_slug))?;
    let selector = recipe
        .filled
        .selector
        .ok_or("ensemble recipe filled field has no selector")?;
    let field = reduced
        .get(&selector)
        .ok_or_else(|| format!("missing reduced filled selector {}", selector.key()))?;
    let mut core = field.clone().into_field2d();
    if request.stat == GribEnsembleStat::ProbExceed {
        core.units = "probability".to_string();
    }

    let visual_mode = ProductVisualMode::FilledMeteorology;
    let projected = crate::direct::build_projected_map_with_projection(
        &field.grid.lat_deg,
        &field.grid.lon_deg,
        field.projection.as_ref(),
        request.domain.bounds,
        map_frame_aspect_ratio_for_mode(
            visual_mode,
            request.output_width,
            request.output_height,
            true,
            true,
        ),
    )?;
    let scale = match request.stat {
        GribEnsembleStat::ProbExceed => probability_scale(),
        _ => spread_scale(&core.values),
    };
    let mut render_request = MapRenderRequest::new(core.into(), scale);
    let title = if request.stat == GribEnsembleStat::ProbExceed {
        let threshold = request.threshold.unwrap_or(f32::NAN);
        let op = request.threshold_op.unwrap_or(CompareOp::Gt);
        format!(
            "{} Ensemble Probability {} {}",
            recipe_title,
            op.symbol(),
            comparison_threshold_label(selector, field.units.as_str(), threshold)
        )
    } else {
        format!("{} Ensemble {}", recipe_title, request.stat.slug())
    };
    render_request.title = Some(static_title_with_suffix(title));
    render_request.subtitle_left = Some(crate::shared_context::model_time_subtitle(
        request.model,
        &request.date_yyyymmdd,
        latest.cycle.hour_utc,
        request.forecast_hour,
    ));
    render_request.subtitle_right = Some(format!(
        "{} | n={}",
        crate::shared_context::source_subtitle(latest.source),
        request.member_products.len()
    ));
    render_request.width = request.output_width;
    render_request.height = request.output_height;
    render_request.chrome_scale = static_chrome_scale();
    render_request.supersample_factor = static_supersample_factor();
    render_request.supersample_sharpen = static_supersample_sharpen();
    crate::plot_design::StaticPlotDesign::new(request.domain.bounds, visual_mode)
        .apply_to_request(&mut render_request);
    render_request.apply_projected_map(&projected);

    let output_path = request.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_{}_{}.png",
        request.model.as_str().replace('-', "_"),
        request.date_yyyymmdd,
        latest.cycle.hour_utc,
        request.forecast_hour,
        request.domain.slug,
        request.recipe_slug,
        suffix
    ));
    save_png_profile_with_options(
        &render_request,
        &output_path,
        &PngWriteOptions {
            compression: request.png_compression,
        },
    )?;
    Ok(output_path)
}

fn spread_scale(values: &[f32]) -> ColorScale {
    let mut finite = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if finite.is_empty() {
        finite.push(1.0);
    }
    finite.sort_by(|a, b| a.total_cmp(b));
    let p99 = percentile(&mut finite, 0.99).max(1.0) as f64;
    let step = (p99 / 20.0).max(0.1);
    let levels = (0..=20).map(|idx| idx as f64 * step).collect::<Vec<_>>();
    ColorScale::Discrete(DiscreteColorScale {
        levels,
        colors: sequential_blue_colors(20),
        extend: ExtendMode::Max,
        mask_below: None,
    })
}

fn probability_scale() -> ColorScale {
    ColorScale::Discrete(DiscreteColorScale {
        levels: (0..=20).map(|idx| idx as f64 / 20.0).collect(),
        colors: sequential_red_colors(20),
        extend: ExtendMode::Neither,
        mask_below: None,
    })
}

fn sequential_blue_colors(count: usize) -> Vec<Color> {
    (0..count)
        .map(|idx| {
            let t = idx as f64 / (count.saturating_sub(1).max(1)) as f64;
            Color::rgba(
                lerp(245.0, 35.0, t) as u8,
                lerp(249.0, 105.0, t) as u8,
                lerp(255.0, 190.0, t) as u8,
                255,
            )
        })
        .collect()
}

fn sequential_red_colors(count: usize) -> Vec<Color> {
    (0..count)
        .map(|idx| {
            let t = idx as f64 / (count.saturating_sub(1).max(1)) as f64;
            Color::rgba(
                lerp(255.0, 178.0, t) as u8,
                lerp(250.0, 24.0, t) as u8,
                lerp(240.0, 43.0, t) as u8,
                255,
            )
        })
        .collect()
}

fn lerp(left: f64, right: f64, t: f64) -> f64 {
    left + (right - left) * t.clamp(0.0, 1.0)
}

fn compact_threshold_slug(value: f32) -> String {
    format!("{value:.2}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .replace('-', "m")
        .replace('.', "p")
}
