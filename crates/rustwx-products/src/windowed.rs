use crate::cache::{load_bincode, store_bincode};
use crate::hrrr::{
    DomainSpec, SURFACE_PATTERNS, build_projected_map, decode_cache_path, fetch_hrrr_subset,
    load_or_decode_surface, resolve_hrrr_run,
};
use grib_core::grib2::{Grib2File, Grib2Message, unpack_message_normalized};
use rustwx_calc::{max_window_fields, sum_window_fields};
use rustwx_core::{Field2D, ProductKey, SourceId};
use rustwx_render::{
    ColorScale, ExtendMode, MapRenderRequest, Solar07Palette, Solar07Product, palette_scale,
    save_png,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use wrf_render::render::map_frame_aspect_ratio;

const OUTPUT_WIDTH: u32 = 1200;
const OUTPUT_HEIGHT: u32 = 900;
const APCP_PATTERNS: &[&str] = &["APCP:surface"];
const UH25_PATTERNS: &[&str] = &["MXUPHL:5000-2000 m above ground"];
const MM_PER_INCH: f64 = 25.4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HrrrWindowedProduct {
    Qpf1h,
    Qpf6h,
    Qpf12h,
    Qpf24h,
    QpfTotal,
    Uh25km1h,
    Uh25km3h,
    Uh25kmRunMax,
}

impl HrrrWindowedProduct {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Qpf1h => "qpf_1h",
            Self::Qpf6h => "qpf_6h",
            Self::Qpf12h => "qpf_12h",
            Self::Qpf24h => "qpf_24h",
            Self::QpfTotal => "qpf_total",
            Self::Uh25km1h => "uh_2to5km_1h_max",
            Self::Uh25km3h => "uh_2to5km_3h_max",
            Self::Uh25kmRunMax => "uh_2to5km_run_max",
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Self::Qpf1h => "1-h QPF",
            Self::Qpf6h => "6-h QPF",
            Self::Qpf12h => "12-h QPF",
            Self::Qpf24h => "24-h QPF",
            Self::QpfTotal => "Total QPF",
            Self::Uh25km1h => "Updraft Helicity: 2-5 km AGL (1 h max)",
            Self::Uh25km3h => "Updraft Helicity: 2-5 km AGL (3 h max)",
            Self::Uh25kmRunMax => "Updraft Helicity: 2-5 km AGL (run max)",
        }
    }

    fn is_qpf(self) -> bool {
        matches!(
            self,
            Self::Qpf1h | Self::Qpf6h | Self::Qpf12h | Self::Qpf24h | Self::QpfTotal
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedBatchRequest {
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub products: Vec<HrrrWindowedProduct>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedSharedTiming {
    pub fetch_geometry_ms: u128,
    pub decode_geometry_ms: u128,
    pub project_ms: u128,
    pub fetch_surface_ms: u128,
    pub decode_surface_ms: u128,
    pub fetch_nat_ms: u128,
    pub decode_nat_ms: u128,
    pub geometry_fetch_cache_hit: bool,
    pub geometry_decode_cache_hit: bool,
    pub surface_hours_loaded: Vec<u16>,
    pub nat_hours_loaded: Vec<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedProductTiming {
    pub compute_ms: u128,
    pub render_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedProductMetadata {
    pub strategy: String,
    pub contributing_forecast_hours: Vec<u16>,
    pub window_hours: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedRenderedProduct {
    pub product: HrrrWindowedProduct,
    pub output_path: PathBuf,
    pub timing: HrrrWindowedProductTiming,
    pub metadata: HrrrWindowedProductMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedBlocker {
    pub product: HrrrWindowedProduct,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedBatchReport {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub shared_timing: HrrrWindowedSharedTiming,
    pub products: Vec<HrrrWindowedRenderedProduct>,
    pub blockers: Vec<HrrrWindowedBlocker>,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowedFieldRecord {
    hours: u16,
    values: Vec<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HrrrApcpDecode {
    windows: Vec<WindowedFieldRecord>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HrrrUhDecode {
    windows: Vec<WindowedFieldRecord>,
}

#[derive(Debug, Clone)]
struct ComputedWindowedField {
    field: Field2D,
    title: String,
    metadata: HrrrWindowedProductMetadata,
    scale: ColorScale,
}

pub fn run_hrrr_windowed_batch(
    request: &HrrrWindowedBatchRequest,
) -> Result<HrrrWindowedBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let total_start = Instant::now();
    let latest = resolve_hrrr_run(
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.source,
    )?;

    let geometry_fetch_start = Instant::now();
    let geometry_subset = fetch_hrrr_subset(
        latest.cycle.clone(),
        request.forecast_hour,
        latest.source,
        "sfc",
        SURFACE_PATTERNS,
        &request.cache_root,
        request.use_cache,
    )?;
    let fetch_geometry_ms = geometry_fetch_start.elapsed().as_millis();

    let geometry_decode_start = Instant::now();
    let surface_geometry = load_or_decode_surface(
        &decode_cache_path(&request.cache_root, &geometry_subset.request, "surface"),
        &geometry_subset.bytes,
        request.use_cache,
    )?;
    let decode_geometry_ms = geometry_decode_start.elapsed().as_millis();

    let project_start = Instant::now();
    let projected = build_projected_map(
        &surface_geometry.value,
        request.domain.bounds,
        map_frame_aspect_ratio(OUTPUT_WIDTH, OUTPUT_HEIGHT, true, true),
    )?;
    let project_ms = project_start.elapsed().as_millis();
    let grid = surface_geometry.value.core_grid()?;

    let (planned_products, mut blockers, surface_hours, nat_hours) =
        plan_windowed_products(&request.products, request.forecast_hour);

    let mut fetch_surface_ms = 0u128;
    let mut decode_surface_ms = 0u128;
    let mut fetch_nat_ms = 0u128;
    let mut decode_nat_ms = 0u128;

    let mut apcp_by_hour = BTreeMap::<u16, Result<HrrrApcpDecode, String>>::new();
    for hour in &surface_hours {
        let start = Instant::now();
        let subset = fetch_hrrr_subset(
            latest.cycle.clone(),
            *hour,
            latest.source,
            "sfc",
            APCP_PATTERNS,
            &request.cache_root,
            request.use_cache,
        );
        fetch_surface_ms += start.elapsed().as_millis();
        let result = subset.map_err(|err| err.to_string()).and_then(|subset| {
            let decode_start = Instant::now();
            let decoded = load_or_decode_apcp(
                &decode_cache_path(&request.cache_root, &subset.request, "windowed_apcp"),
                &subset.bytes,
                request.use_cache,
            )
            .map_err(|err| err.to_string());
            decode_surface_ms += decode_start.elapsed().as_millis();
            decoded
        });
        if !apcp_by_hour.contains_key(hour) {
            apcp_by_hour.insert(*hour, result);
        }
    }

    let mut uh_by_hour = BTreeMap::<u16, Result<HrrrUhDecode, String>>::new();
    for hour in &nat_hours {
        let start = Instant::now();
        let subset = fetch_hrrr_subset(
            latest.cycle.clone(),
            *hour,
            latest.source,
            "nat",
            UH25_PATTERNS,
            &request.cache_root,
            request.use_cache,
        );
        fetch_nat_ms += start.elapsed().as_millis();
        let result = subset.map_err(|err| err.to_string()).and_then(|subset| {
            let decode_start = Instant::now();
            let decoded = load_or_decode_uh25(
                &decode_cache_path(&request.cache_root, &subset.request, "windowed_uh25"),
                &subset.bytes,
                request.use_cache,
            )
            .map_err(|err| err.to_string());
            decode_nat_ms += decode_start.elapsed().as_millis();
            decoded
        });
        if !uh_by_hour.contains_key(hour) {
            uh_by_hour.insert(*hour, result);
        }
    }

    let mut rendered = Vec::new();
    for product in planned_products {
        let compute_start = Instant::now();
        let computed = if product.is_qpf() {
            compute_qpf_product(product, request.forecast_hour, &grid, &apcp_by_hour)
        } else {
            compute_uh_product(product, request.forecast_hour, &grid, &uh_by_hour)
        };
        let compute_ms = compute_start.elapsed().as_millis();

        let computed = match computed {
            Ok(value) => value,
            Err(reason) => {
                blockers.push(HrrrWindowedBlocker { product, reason });
                continue;
            }
        };

        let output_path = request.out_dir.join(format!(
            "rustwx_hrrr_{}_{}z_f{:03}_{}_{}.png",
            request.date_yyyymmdd,
            latest.cycle.hour_utc,
            request.forecast_hour,
            request.domain.slug,
            product.slug()
        ));
        let render_start = Instant::now();
        let mut render_request = if matches!(
            product,
            HrrrWindowedProduct::Uh25km1h
                | HrrrWindowedProduct::Uh25km3h
                | HrrrWindowedProduct::Uh25kmRunMax
        ) {
            MapRenderRequest::for_core_solar07_product(computed.field.clone(), Solar07Product::Uh)
        } else {
            MapRenderRequest::from_core_field(computed.field.clone(), computed.scale.clone())
        };
        render_request.width = OUTPUT_WIDTH;
        render_request.height = OUTPUT_HEIGHT;
        render_request.title = Some(computed.title.clone());
        render_request.subtitle_left = Some(format!(
            "{} {}Z F{:03}  HRRR",
            request.date_yyyymmdd, latest.cycle.hour_utc, request.forecast_hour
        ));
        render_request.subtitle_right = Some(format!(
            "source: {} | {}",
            latest.source, computed.metadata.strategy
        ));
        render_request.projected_domain = Some(rustwx_render::ProjectedDomain {
            x: projected.projected_x.clone(),
            y: projected.projected_y.clone(),
            extent: projected.extent.clone(),
        });
        render_request.projected_lines = projected.lines.clone();
        save_png(&render_request, &output_path)?;
        let render_ms = render_start.elapsed().as_millis();

        rendered.push(HrrrWindowedRenderedProduct {
            product,
            output_path,
            timing: HrrrWindowedProductTiming {
                compute_ms,
                render_ms,
                total_ms: compute_ms + render_ms,
            },
            metadata: computed.metadata,
        });
    }

    Ok(HrrrWindowedBatchReport {
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: latest.source,
        domain: request.domain.clone(),
        shared_timing: HrrrWindowedSharedTiming {
            fetch_geometry_ms,
            decode_geometry_ms,
            project_ms,
            fetch_surface_ms,
            decode_surface_ms,
            fetch_nat_ms,
            decode_nat_ms,
            geometry_fetch_cache_hit: geometry_subset.fetched.cache_hit,
            geometry_decode_cache_hit: surface_geometry.cache_hit,
            surface_hours_loaded: surface_hours.into_iter().collect(),
            nat_hours_loaded: nat_hours.into_iter().collect(),
        },
        products: rendered,
        blockers,
        total_ms: total_start.elapsed().as_millis(),
    })
}

fn plan_windowed_products(
    products: &[HrrrWindowedProduct],
    forecast_hour: u16,
) -> (
    Vec<HrrrWindowedProduct>,
    Vec<HrrrWindowedBlocker>,
    BTreeSet<u16>,
    BTreeSet<u16>,
) {
    let mut seen = BTreeSet::new();
    let mut planned = Vec::new();
    let mut blockers = Vec::new();
    let mut surface_hours = BTreeSet::new();
    let mut nat_hours = BTreeSet::new();

    for &product in products {
        if !seen.insert(product.slug().to_string()) {
            continue;
        }

        match product {
            HrrrWindowedProduct::Qpf1h => {
                if forecast_hour < 1 {
                    blockers.push(blocker(
                        product,
                        "1-h QPF requires forecast hour >= 1 because HRRR APCP windows start at 0-1 h",
                    ));
                    continue;
                }
                surface_hours.insert(forecast_hour);
            }
            HrrrWindowedProduct::Qpf6h => {
                if forecast_hour < 6 {
                    blockers.push(blocker(product, "6-h QPF requires forecast hour >= 6"));
                    continue;
                }
                surface_hours.extend((forecast_hour - 5)..=forecast_hour);
            }
            HrrrWindowedProduct::Qpf12h => {
                if forecast_hour < 12 {
                    blockers.push(blocker(product, "12-h QPF requires forecast hour >= 12"));
                    continue;
                }
                surface_hours.extend((forecast_hour - 11)..=forecast_hour);
            }
            HrrrWindowedProduct::Qpf24h => {
                if forecast_hour < 24 {
                    blockers.push(blocker(product, "24-h QPF requires forecast hour >= 24"));
                    continue;
                }
                surface_hours.extend((forecast_hour - 23)..=forecast_hour);
            }
            HrrrWindowedProduct::QpfTotal => {
                if forecast_hour < 1 {
                    blockers.push(blocker(product, "total QPF requires forecast hour >= 1"));
                    continue;
                }
                surface_hours.extend(1..=forecast_hour);
            }
            HrrrWindowedProduct::Uh25km1h => {
                if forecast_hour < 1 {
                    blockers.push(blocker(
                        product,
                        "1-h UH max requires forecast hour >= 1 because native UH windows start at 0-1 h",
                    ));
                    continue;
                }
                nat_hours.insert(forecast_hour);
            }
            HrrrWindowedProduct::Uh25km3h => {
                if forecast_hour < 3 {
                    blockers.push(blocker(product, "3-h UH max requires forecast hour >= 3"));
                    continue;
                }
                nat_hours.extend((forecast_hour - 2)..=forecast_hour);
            }
            HrrrWindowedProduct::Uh25kmRunMax => {
                if forecast_hour < 1 {
                    blockers.push(blocker(product, "run-max UH requires forecast hour >= 1"));
                    continue;
                }
                nat_hours.extend(1..=forecast_hour);
            }
        }

        planned.push(product);
    }

    (planned, blockers, surface_hours, nat_hours)
}

fn blocker(product: HrrrWindowedProduct, reason: impl Into<String>) -> HrrrWindowedBlocker {
    HrrrWindowedBlocker {
        product,
        reason: reason.into(),
    }
}

fn load_or_decode_apcp(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
) -> Result<HrrrApcpDecode, Box<dyn std::error::Error>> {
    if use_cache {
        if let Some(cached) = load_bincode::<HrrrApcpDecode>(path)? {
            return Ok(cached);
        }
    }
    let decoded = decode_apcp(bytes)?;
    if use_cache {
        store_bincode(path, &decoded)?;
    }
    Ok(decoded)
}

fn load_or_decode_uh25(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
) -> Result<HrrrUhDecode, Box<dyn std::error::Error>> {
    if use_cache {
        if let Some(cached) = load_bincode::<HrrrUhDecode>(path)? {
            return Ok(cached);
        }
    }
    let decoded = decode_uh25(bytes)?;
    if use_cache {
        store_bincode(path, &decoded)?;
    }
    Ok(decoded)
}

fn decode_apcp(bytes: &[u8]) -> Result<HrrrApcpDecode, Box<dyn std::error::Error>> {
    let grib = Grib2File::from_bytes(bytes)?;
    let mut windows = Vec::new();
    for message in &grib.messages {
        if message.discipline == 0
            && message.product.parameter_category == 1
            && message.product.parameter_number == 8
            && message.product.level_type == 1
        {
            let hours = time_range_hours(message)
                .ok_or("APCP message missing hourly time-range metadata")?;
            if windows
                .iter()
                .any(|record: &WindowedFieldRecord| record.hours == hours)
            {
                continue;
            }
            windows.push(WindowedFieldRecord {
                hours,
                values: unpack_message_normalized(message)?,
            });
        }
    }
    if windows.is_empty() {
        return Err("no APCP surface accumulation fields were found in subset".into());
    }
    windows.sort_by_key(|record| record.hours);
    Ok(HrrrApcpDecode { windows })
}

fn decode_uh25(bytes: &[u8]) -> Result<HrrrUhDecode, Box<dyn std::error::Error>> {
    let grib = Grib2File::from_bytes(bytes)?;
    let mut windows = Vec::new();
    for message in &grib.messages {
        if is_uh25_message(message) {
            let hours = time_range_hours(message)
                .ok_or("native UH message missing hourly max-window metadata")?;
            if windows
                .iter()
                .any(|record: &WindowedFieldRecord| record.hours == hours)
            {
                continue;
            }
            windows.push(WindowedFieldRecord {
                hours,
                values: unpack_message_normalized(message)?,
            });
        }
    }
    if windows.is_empty() {
        return Err("no native 2-5 km UH max fields were found in subset".into());
    }
    windows.sort_by_key(|record| record.hours);
    Ok(HrrrUhDecode { windows })
}

fn is_uh25_message(message: &Grib2Message) -> bool {
    matches!(
        (
            message.product.parameter_category,
            message.product.parameter_number
        ),
        (7, 199) | (7, 15)
    ) && matches!(message.product.level_type, 103 | 118)
        && (message.product.level_value - 5000.0).abs() < 0.25
}

fn time_range_hours(message: &Grib2Message) -> Option<u16> {
    message.product.statistical_time_range_hours()
}

fn compute_qpf_product(
    product: HrrrWindowedProduct,
    forecast_hour: u16,
    grid: &rustwx_core::LatLonGrid,
    apcp_by_hour: &BTreeMap<u16, Result<HrrrApcpDecode, String>>,
) -> Result<ComputedWindowedField, String> {
    let (window_hours, title) = match product {
        HrrrWindowedProduct::Qpf1h => (Some(1), "1-h QPF"),
        HrrrWindowedProduct::Qpf6h => (Some(6), "6-h QPF"),
        HrrrWindowedProduct::Qpf12h => (Some(12), "12-h QPF"),
        HrrrWindowedProduct::Qpf24h => (Some(24), "24-h QPF"),
        HrrrWindowedProduct::QpfTotal => (None, "Total QPF"),
        _ => return Err(format!("{} is not a QPF product", product.slug())),
    };

    let (values_mm, strategy, contributing_hours) = match window_hours {
        Some(window) => {
            let end = apcp_by_hour
                .get(&forecast_hour)
                .ok_or_else(|| format!("missing APCP fetch for F{:03}", forecast_hour))?
                .as_ref()
                .map_err(Clone::clone)?;
            if let Some(direct) = select_window(&end.windows, window) {
                (
                    direct.to_vec(),
                    format!("direct APCP {}h accumulation", window),
                    vec![forecast_hour],
                )
            } else {
                let start_hour = forecast_hour + 1 - window;
                let hours = (start_hour..=forecast_hour).collect::<Vec<_>>();
                let increments = collect_apcp_windows(apcp_by_hour, &hours, 1)?;
                (
                    sum_window_fields(grid.shape, &increments).map_err(|err| err.to_string())?,
                    format!("sum of {} hourly APCP increments", window),
                    hours,
                )
            }
        }
        None => {
            let end = apcp_by_hour
                .get(&forecast_hour)
                .ok_or_else(|| format!("missing APCP fetch for F{:03}", forecast_hour))?
                .as_ref()
                .map_err(Clone::clone)?;
            if let Some(direct) = select_window(&end.windows, forecast_hour) {
                (
                    direct.to_vec(),
                    format!("direct APCP {}h accumulation", forecast_hour),
                    vec![forecast_hour],
                )
            } else {
                let hours = (1..=forecast_hour).collect::<Vec<_>>();
                let increments = collect_apcp_windows(apcp_by_hour, &hours, 1)?;
                (
                    sum_window_fields(grid.shape, &increments).map_err(|err| err.to_string())?,
                    "sum of all available hourly APCP increments".to_string(),
                    hours,
                )
            }
        }
    };

    let values_in = values_mm
        .into_iter()
        .map(|value| value / MM_PER_INCH)
        .collect::<Vec<_>>();
    let field = Field2D::new(
        ProductKey::named(product.slug()),
        "in",
        grid.clone(),
        values_in.iter().map(|&value| value as f32).collect(),
    )
    .map_err(|err| err.to_string())?;

    Ok(ComputedWindowedField {
        field,
        title: title.to_string(),
        metadata: HrrrWindowedProductMetadata {
            strategy,
            contributing_forecast_hours: contributing_hours,
            window_hours,
        },
        scale: ColorScale::Discrete(qpf_scale()),
    })
}

fn compute_uh_product(
    product: HrrrWindowedProduct,
    forecast_hour: u16,
    grid: &rustwx_core::LatLonGrid,
    uh_by_hour: &BTreeMap<u16, Result<HrrrUhDecode, String>>,
) -> Result<ComputedWindowedField, String> {
    let (values, strategy, contributing_hours, window_hours) = match product {
        HrrrWindowedProduct::Uh25km1h => {
            let decoded = uh_by_hour
                .get(&forecast_hour)
                .ok_or_else(|| format!("missing native UH fetch for F{:03}", forecast_hour))?
                .as_ref()
                .map_err(Clone::clone)?;
            let values = select_window(&decoded.windows, 1)
                .ok_or_else(|| format!("native UH F{:03} missing 1-hour max field", forecast_hour))?
                .to_vec();
            (
                values,
                "direct native 1-hour UH max".to_string(),
                vec![forecast_hour],
                Some(1),
            )
        }
        HrrrWindowedProduct::Uh25km3h => {
            let hours = ((forecast_hour - 2)..=forecast_hour).collect::<Vec<_>>();
            let windows = collect_uh_windows(uh_by_hour, &hours, 1)?;
            (
                max_window_fields(grid.shape, &windows).map_err(|err| err.to_string())?,
                "max of native hourly UH maxima across trailing 3 hours".to_string(),
                hours,
                Some(3),
            )
        }
        HrrrWindowedProduct::Uh25kmRunMax => {
            let hours = (1..=forecast_hour).collect::<Vec<_>>();
            let windows = collect_uh_windows(uh_by_hour, &hours, 1)?;
            (
                max_window_fields(grid.shape, &windows).map_err(|err| err.to_string())?,
                "run max of native hourly UH maxima".to_string(),
                hours,
                None,
            )
        }
        _ => return Err(format!("{} is not a UH product", product.slug())),
    };

    let field = Field2D::new(
        ProductKey::named(product.slug()),
        "m^2/s^2",
        grid.clone(),
        values.iter().map(|&value| value as f32).collect(),
    )
    .map_err(|err| err.to_string())?;

    Ok(ComputedWindowedField {
        field,
        title: product.title().to_string(),
        metadata: HrrrWindowedProductMetadata {
            strategy,
            contributing_forecast_hours: contributing_hours,
            window_hours,
        },
        scale: ColorScale::Solar07(Solar07Product::Uh.scale_preset()),
    })
}

fn collect_apcp_windows<'a>(
    apcp_by_hour: &'a BTreeMap<u16, Result<HrrrApcpDecode, String>>,
    hours: &[u16],
    window_hours: u16,
) -> Result<Vec<&'a [f64]>, String> {
    let mut out = Vec::with_capacity(hours.len());
    for &hour in hours {
        let decoded = apcp_by_hour
            .get(&hour)
            .ok_or_else(|| format!("missing APCP fetch for F{:03}", hour))?
            .as_ref()
            .map_err(Clone::clone)?;
        let window = select_window(&decoded.windows, window_hours).ok_or_else(|| {
            format!(
                "APCP F{:03} missing {}-hour accumulation field",
                hour, window_hours
            )
        })?;
        out.push(window);
    }
    Ok(out)
}

fn collect_uh_windows<'a>(
    uh_by_hour: &'a BTreeMap<u16, Result<HrrrUhDecode, String>>,
    hours: &[u16],
    window_hours: u16,
) -> Result<Vec<&'a [f64]>, String> {
    let mut out = Vec::with_capacity(hours.len());
    for &hour in hours {
        let decoded = uh_by_hour
            .get(&hour)
            .ok_or_else(|| format!("missing native UH fetch for F{:03}", hour))?
            .as_ref()
            .map_err(Clone::clone)?;
        let window = select_window(&decoded.windows, window_hours).ok_or_else(|| {
            format!(
                "native UH F{:03} missing {}-hour max field",
                hour, window_hours
            )
        })?;
        out.push(window);
    }
    Ok(out)
}

fn select_window(records: &[WindowedFieldRecord], hours: u16) -> Option<&[f64]> {
    records
        .iter()
        .find(|record| record.hours == hours)
        .map(|record| record.values.as_slice())
}

fn qpf_scale() -> rustwx_render::DiscreteColorScale {
    palette_scale(
        Solar07Palette::Precip,
        vec![
            0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0, 10.0,
        ],
        ExtendMode::Max,
        Some(0.01),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_core::{GridShape, LatLonGrid};

    fn tiny_grid() -> LatLonGrid {
        LatLonGrid::new(
            GridShape::new(2, 1).unwrap(),
            vec![40.0, 40.0],
            vec![-100.0, -99.0],
        )
        .unwrap()
    }

    #[test]
    fn plan_windowed_products_blocks_short_forecast_hours() {
        let (planned, blockers, surface_hours, nat_hours) = plan_windowed_products(
            &[HrrrWindowedProduct::Qpf24h, HrrrWindowedProduct::Uh25km3h],
            2,
        );
        assert!(planned.is_empty());
        assert_eq!(blockers.len(), 2);
        assert!(surface_hours.is_empty());
        assert!(nat_hours.is_empty());
    }

    #[test]
    fn compute_qpf_prefers_direct_window_when_available() {
        let mut apcp = BTreeMap::new();
        apcp.insert(
            6,
            Ok(HrrrApcpDecode {
                windows: vec![
                    WindowedFieldRecord {
                        hours: 1,
                        values: vec![0.5, 0.25],
                    },
                    WindowedFieldRecord {
                        hours: 6,
                        values: vec![12.7, 25.4],
                    },
                ],
            }),
        );
        let computed =
            compute_qpf_product(HrrrWindowedProduct::Qpf6h, 6, &tiny_grid(), &apcp).unwrap();
        assert_eq!(computed.metadata.strategy, "direct APCP 6h accumulation");
        assert_eq!(computed.metadata.contributing_forecast_hours, vec![6]);
        assert_eq!(computed.field.values, vec![0.5_f32, 1.0_f32]);
    }

    #[test]
    fn compute_qpf_total_falls_back_to_hourly_sum() {
        let mut apcp = BTreeMap::new();
        for hour in 1..=3 {
            apcp.insert(
                hour,
                Ok(HrrrApcpDecode {
                    windows: vec![WindowedFieldRecord {
                        hours: 1,
                        values: vec![25.4, 12.7],
                    }],
                }),
            );
        }
        let computed =
            compute_qpf_product(HrrrWindowedProduct::QpfTotal, 3, &tiny_grid(), &apcp).unwrap();
        assert_eq!(
            computed.metadata.strategy,
            "sum of all available hourly APCP increments"
        );
        assert_eq!(computed.field.values, vec![3.0_f32, 1.5_f32]);
    }

    #[test]
    fn compute_uh_run_max_takes_pointwise_maximum() {
        let mut uh = BTreeMap::new();
        uh.insert(
            1,
            Ok(HrrrUhDecode {
                windows: vec![WindowedFieldRecord {
                    hours: 1,
                    values: vec![50.0, 10.0],
                }],
            }),
        );
        uh.insert(
            2,
            Ok(HrrrUhDecode {
                windows: vec![WindowedFieldRecord {
                    hours: 1,
                    values: vec![25.0, 40.0],
                }],
            }),
        );
        let computed =
            compute_uh_product(HrrrWindowedProduct::Uh25kmRunMax, 2, &tiny_grid(), &uh).unwrap();
        assert_eq!(computed.field.values, vec![50.0_f32, 40.0_f32]);
        assert_eq!(
            computed.metadata.strategy,
            "run max of native hourly UH maxima"
        );
    }
}
