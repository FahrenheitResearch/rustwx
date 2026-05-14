use crate::direct::build_projected_map_with_projection;
use crate::plot_design::StaticPlotDesign;
use crate::shared_context::{
    DomainSpec, static_chrome_scale, static_supersample_factor, static_supersample_sharpen,
    static_title_with_suffix,
};
use rustwx_core::{Field2D, GridProjection, GridShape, LatLonGrid, ModelId, ProductKey};
use rustwx_models::{PlotRecipe, RenderStyle, plot_recipe};
use rustwx_render::{
    Color, ColorScale, DiscreteColorScale, ExtendMode, MapRenderRequest, PngCompressionMode,
    PngWriteOptions, ProductVisualMode, WeatherPalette, WeatherProduct, palette_scale,
    save_png_profile_with_options,
};
use rustwx_render::{DerivedProductStyle, ProjectedDomain};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const WXA_DENSE2D_MAGIC: &[u8; 8] = b"WXAD2D1!";
const WXA_DENSE2D_VERSION: u32 = 1;
const WXA_DENSE2D_HEADER_LEN: usize = 64;
const WXA_DENSE2D_INDEX_RECORD_LEN: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxaDense2dMeta {
    pub schema: String,
    pub model: String,
    pub run: String,
    pub member: Option<String>,
    pub variable: String,
    pub units: String,
    pub nx: usize,
    pub ny: usize,
    pub forecast_hours: Vec<u32>,
    pub chunk_y: usize,
    pub chunk_x: usize,
    pub dtype: String,
    pub codec: String,
    pub grid: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxaDense2dIndexRecord {
    pub forecast_hour: u32,
    pub chunk_y: usize,
    pub chunk_x: usize,
    pub y_count: usize,
    pub x_count: usize,
    pub raw_len: usize,
    pub offset: usize,
    pub len: usize,
    pub min: f32,
    pub max: f32,
    pub valid_count: u32,
}

#[derive(Debug, Clone)]
pub struct WxaDense2dGrid {
    pub meta: WxaDense2dMeta,
    pub forecast_hour: u32,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct WxaStaticPlotRequest {
    pub wxa_path: PathBuf,
    pub forecast_hour: u32,
    pub out_dir: PathBuf,
    pub width: u32,
    pub height: u32,
    pub png_compression: PngCompressionMode,
    pub bounds_override: Option<(f64, f64, f64, f64)>,
    pub title_override: Option<String>,
    pub subtitle_left: Option<String>,
    pub subtitle_right: Option<String>,
    pub output_suffix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxaRenderedPlot {
    pub product_slug: String,
    pub title: String,
    pub model: String,
    pub run: String,
    pub member: Option<String>,
    pub forecast_hour: u32,
    pub units: String,
    pub nx: usize,
    pub ny: usize,
    pub bounds: [f64; 4],
    pub output_path: PathBuf,
    pub wxa_path: PathBuf,
    pub render_ms: u128,
}

#[derive(Debug, Clone, Copy)]
struct WxaDense2dHeader {
    metadata_len: usize,
    index_count: usize,
    index_offset: usize,
    payload_offset: usize,
}

#[derive(Debug, Clone)]
struct WxaGridGeometry {
    grid: LatLonGrid,
    projection: Option<GridProjection>,
    bounds: [f64; 4],
}

pub fn read_wxa_dense2d_metadata(
    path: &Path,
) -> Result<(WxaDense2dMeta, Vec<WxaDense2dIndexRecord>), Box<dyn std::error::Error>> {
    let bytes = fs::read(path)?;
    let header = parse_wxa_dense2d_header(&bytes)?;
    let meta_end = WXA_DENSE2D_HEADER_LEN + header.metadata_len;
    if meta_end > bytes.len() {
        return Err("WXA metadata exceeds file length".into());
    }
    let meta: WxaDense2dMeta = serde_json::from_slice(&bytes[WXA_DENSE2D_HEADER_LEN..meta_end])?;
    let records = parse_wxa_index(&bytes, header)?;
    Ok((meta, records))
}

pub fn read_wxa_dense2d_grid(
    path: &Path,
    forecast_hour: u32,
) -> Result<WxaDense2dGrid, Box<dyn std::error::Error>> {
    let bytes = fs::read(path)?;
    let header = parse_wxa_dense2d_header(&bytes)?;
    let meta_end = WXA_DENSE2D_HEADER_LEN + header.metadata_len;
    if meta_end > bytes.len() {
        return Err("WXA metadata exceeds file length".into());
    }
    let meta: WxaDense2dMeta = serde_json::from_slice(&bytes[WXA_DENSE2D_HEADER_LEN..meta_end])?;
    validate_wxa_meta(&meta)?;
    let index = parse_wxa_index(&bytes, header)?;
    let mut values = vec![f32::NAN; meta.nx * meta.ny];
    let mut found = false;
    for record in index
        .iter()
        .filter(|record| record.forecast_hour == forecast_hour)
    {
        let end = record.offset + record.len;
        if end > bytes.len() {
            return Err("WXA chunk exceeds file length".into());
        }
        let decoded = zstd::stream::decode_all(&bytes[record.offset..end])?;
        if decoded.len() != record.raw_len {
            return Err(format!(
                "WXA chunk raw length mismatch: got {}, expected {}",
                decoded.len(),
                record.raw_len
            )
            .into());
        }
        let y0 = record.chunk_y * meta.chunk_y;
        let x0 = record.chunk_x * meta.chunk_x;
        let mut src = 0usize;
        for yy in 0..record.y_count {
            for xx in 0..record.x_count {
                let dst = (y0 + yy) * meta.nx + (x0 + xx);
                values[dst] = f32::from_le_bytes([
                    decoded[src],
                    decoded[src + 1],
                    decoded[src + 2],
                    decoded[src + 3],
                ]);
                src += 4;
            }
        }
        found = true;
    }
    if !found {
        return Err(format!(
            "forecast hour f{forecast_hour:03} is not available in {}",
            path.display()
        )
        .into());
    }
    Ok(WxaDense2dGrid {
        meta,
        forecast_hour,
        values,
    })
}

pub fn available_wxa_products(
    spatial_root: &Path,
    model: &str,
    run: &str,
    member: Option<&str>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let dir = wxa_member_dir(spatial_root, model, run, member);
    let mut products = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("wxa") {
            if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
                products.push(stem.to_string());
            }
        }
    }
    products.sort();
    Ok(products)
}

pub fn wxa_product_path(
    spatial_root: &Path,
    model: &str,
    run: &str,
    member: Option<&str>,
    product: &str,
) -> PathBuf {
    wxa_member_dir(spatial_root, model, run, member).join(format!("{product}.wxa"))
}

pub fn wxa_member_dir(
    spatial_root: &Path,
    model: &str,
    run: &str,
    member: Option<&str>,
) -> PathBuf {
    let mut dir = spatial_root.join(model).join(run);
    if let Some(member) = member {
        dir = dir.join("members").join(member);
    }
    dir
}

pub fn render_wxa_static_plot(
    request: &WxaStaticPlotRequest,
) -> Result<WxaRenderedPlot, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let wxa = read_wxa_dense2d_grid(&request.wxa_path, request.forecast_hour)?;
    let geometry = geometry_from_wxa_meta(&wxa.meta)?;
    let bounds = request.bounds_override.unwrap_or((
        geometry.bounds[0],
        geometry.bounds[2],
        geometry.bounds[1],
        geometry.bounds[3],
    ));
    let model = model_id_from_wxa(&wxa.meta.model);
    let title = request
        .title_override
        .clone()
        .unwrap_or_else(|| product_title(&wxa.meta.variable));
    let mut map_request = build_wxa_map_request(
        &wxa,
        &geometry,
        bounds,
        request.width,
        request.height,
        &title,
    )?;
    map_request.subtitle_left = request
        .subtitle_left
        .clone()
        .or_else(|| subtitle_for_wxa_time(model, &wxa.meta.run, wxa.forecast_hour));
    map_request.subtitle_right = request
        .subtitle_right
        .clone()
        .or_else(|| Some("source: wxstore wxa".to_string()));
    let suffix = request
        .output_suffix
        .as_deref()
        .map(sanitize_slug)
        .filter(|value| !value.is_empty())
        .map(|value| format!("_{value}"))
        .unwrap_or_default();
    let filename = format!(
        "rustwx_wxa_{}_{}_f{:03}_{}{}.png",
        sanitize_slug(&wxa.meta.model),
        sanitize_slug(&wxa.meta.run),
        wxa.forecast_hour,
        sanitize_slug(&wxa.meta.variable),
        suffix
    );
    fs::create_dir_all(&request.out_dir)?;
    let output_path = request.out_dir.join(filename);
    save_png_profile_with_options(
        &map_request,
        &output_path,
        &PngWriteOptions {
            compression: request.png_compression,
        },
    )?;
    Ok(WxaRenderedPlot {
        product_slug: wxa.meta.variable,
        title,
        model: wxa.meta.model,
        run: wxa.meta.run,
        member: wxa.meta.member,
        forecast_hour: wxa.forecast_hour,
        units: wxa.meta.units,
        nx: wxa.meta.nx,
        ny: wxa.meta.ny,
        bounds: geometry.bounds,
        output_path,
        wxa_path: request.wxa_path.clone(),
        render_ms: started.elapsed().as_millis(),
    })
}

fn build_wxa_map_request(
    wxa: &WxaDense2dGrid,
    geometry: &WxaGridGeometry,
    bounds: (f64, f64, f64, f64),
    width: u32,
    height: u32,
    title: &str,
) -> Result<MapRenderRequest, Box<dyn std::error::Error>> {
    let field = Field2D::new(
        ProductKey::named(wxa.meta.variable.clone()),
        wxa.meta.units.clone(),
        geometry.grid.clone(),
        wxa.values.clone(),
    )?;
    let (scale, visual_mode, tick_step) =
        plot_style_for_wxa_product(&wxa.meta.variable, field.units.as_str());
    let mut request = MapRenderRequest::from_core_field(field, scale);
    request.title = Some(static_title_with_suffix(title));
    request.width = width;
    request.height = height;
    request.chrome_scale = static_chrome_scale();
    request.supersample_factor = static_supersample_factor();
    request.supersample_sharpen = static_supersample_sharpen();
    request.cbar_tick_step = tick_step;
    StaticPlotDesign::new(bounds, visual_mode).apply_to_request(&mut request);

    let target_ratio =
        rustwx_render::map_frame_aspect_ratio_for_mode_with_domain_frame_and_chrome_scale(
            visual_mode,
            width,
            height,
            true,
            true,
            request.domain_frame.is_some(),
            static_chrome_scale(),
        );
    let projected = build_projected_map_with_projection(
        &geometry.grid.lat_deg,
        &geometry.grid.lon_deg,
        geometry.projection.as_ref(),
        bounds,
        target_ratio,
    )?;
    request.projected_domain = Some(ProjectedDomain {
        x: projected.projected_x,
        y: projected.projected_y,
        extent: projected.extent,
    });
    request.projected_lines = projected.lines;
    request.projected_polygons = projected.polygons;
    request.inverse_raster_projection = projected.inverse_raster_projection;
    Ok(request)
}

fn plot_style_for_wxa_product(
    product_slug: &str,
    units: &str,
) -> (ColorScale, ProductVisualMode, Option<f64>) {
    if let Some(recipe) = plot_recipe(product_slug) {
        if let Some(selector) = recipe.filled.selector {
            let scale = crate::plot_design::operational_fill_scale_for_recipe(recipe, selector);
            let visual_mode = visual_mode_for_direct_recipe(recipe, selector);
            return (scale, visual_mode, None);
        }
    }

    if let Some(product) = WeatherProduct::from_product_name(product_slug) {
        return (
            ColorScale::Weather(product.scale_preset()),
            product.default_visual_mode(),
            product.default_tick_step(),
        );
    }

    if let Some(style) = DerivedProductStyle::from_product_name(product_slug) {
        return (
            ColorScale::Discrete(style.scale()),
            style.default_visual_mode(),
            style.default_tick_step(),
        );
    }

    let lower = product_slug.to_ascii_lowercase();
    if lower.contains("qpf") || lower.contains("precip") {
        return (
            ColorScale::Discrete(crate::qpf::qpf_inches_scale()),
            ProductVisualMode::FilledMeteorology,
            None,
        );
    }
    if lower.contains("reflectivity") {
        return (
            ColorScale::Discrete(generic_reflectivity_scale()),
            ProductVisualMode::SevereDiagnostic,
            None,
        );
    }
    if lower.contains("wind") || units.eq_ignore_ascii_case("kt") {
        return (
            ColorScale::Discrete(generic_wind_speed_scale()),
            ProductVisualMode::FilledMeteorology,
            Some(10.0),
        );
    }
    if lower.contains("temperature") || lower.contains("dewpoint") || units.contains("deg") {
        let levels = if units.eq_ignore_ascii_case("degF") {
            range_step(-60.0, 121.0, 2.0)
        } else {
            range_step(-50.0, 51.0, 1.0)
        };
        return (
            ColorScale::Discrete(palette_scale(
                WeatherPalette::Temperature,
                levels,
                ExtendMode::Both,
                None,
            )),
            ProductVisualMode::FilledMeteorology,
            Some(10.0),
        );
    }
    (
        ColorScale::Discrete(fallback_scale()),
        ProductVisualMode::FilledMeteorology,
        None,
    )
}

fn visual_mode_for_direct_recipe(
    recipe: &PlotRecipe,
    selector: rustwx_core::FieldSelector,
) -> ProductVisualMode {
    if matches!(recipe.style, RenderStyle::WeatherHeight)
        || matches!(
            selector.vertical,
            rustwx_core::VerticalSelector::IsobaricHpa(_)
        )
    {
        return ProductVisualMode::UpperAirAnalysis;
    }

    let slug = recipe.slug.to_ascii_lowercase();
    if [
        "cape", "cin", "stp", "scp", "ehi", "srh", "shear", "lapse", "uh", "helicity",
    ]
    .iter()
    .any(|token| slug.contains(token))
    {
        return ProductVisualMode::SevereDiagnostic;
    }

    ProductVisualMode::FilledMeteorology
}

fn geometry_from_wxa_meta(
    meta: &WxaDense2dMeta,
) -> Result<WxaGridGeometry, Box<dyn std::error::Error>> {
    let shape = GridShape::new(meta.nx, meta.ny)?;
    let grid_type = meta.grid.get("type").and_then(Value::as_str).unwrap_or("");
    let (lat, lon, projection) = match grid_type {
        "regular_latlon" => regular_latlon_arrays(meta)?,
        "rectilinear_latlon" => rectilinear_latlon_arrays(meta)?,
        "hrrr_lambert_crop" => hrrr_lambert_crop_arrays(meta)?,
        "curvilinear_latlon_sampled" => sampled_curvilinear_latlon_arrays(meta)?,
        _ => fallback_global_latlon_arrays(meta),
    };
    let bounds = meta
        .grid
        .get("bounds")
        .and_then(value_f64_array)
        .and_then(|bounds| {
            (bounds.len() == 4).then(|| [bounds[0], bounds[1], bounds[2], bounds[3]])
        })
        .or_else(|| bounds_from_latlon(&lat, &lon))
        .ok_or("WXA grid metadata does not include usable bounds")?;
    Ok(WxaGridGeometry {
        grid: LatLonGrid::new(shape, lat, lon)?,
        projection,
        bounds,
    })
}

fn regular_latlon_arrays(
    meta: &WxaDense2dMeta,
) -> Result<(Vec<f32>, Vec<f32>, Option<GridProjection>), Box<dyn std::error::Error>> {
    let lat_start = meta_f64(&meta.grid, "lat_start").ok_or("regular WXA missing lat_start")?;
    let lon_start = meta_f64(&meta.grid, "lon_start").ok_or("regular WXA missing lon_start")?;
    let lat_step = meta_f64(&meta.grid, "lat_step")
        .or_else(|| {
            meta_f64(&meta.grid, "lat_end")
                .map(|end| (end - lat_start) / meta.ny.saturating_sub(1).max(1) as f64)
        })
        .ok_or("regular WXA missing lat_step/lat_end")?;
    let lon_step = meta_f64(&meta.grid, "lon_step")
        .or_else(|| {
            meta_f64(&meta.grid, "lon_end")
                .map(|end| (end - lon_start) / meta.nx.saturating_sub(1).max(1) as f64)
        })
        .ok_or("regular WXA missing lon_step/lon_end")?;
    let mut lat = Vec::with_capacity(meta.nx * meta.ny);
    let mut lon = Vec::with_capacity(meta.nx * meta.ny);
    for y in 0..meta.ny {
        for x in 0..meta.nx {
            lat.push((lat_start + y as f64 * lat_step) as f32);
            lon.push(normalize_lon(lon_start + x as f64 * lon_step) as f32);
        }
    }
    Ok((lat, lon, Some(GridProjection::Geographic)))
}

fn rectilinear_latlon_arrays(
    meta: &WxaDense2dMeta,
) -> Result<(Vec<f32>, Vec<f32>, Option<GridProjection>), Box<dyn std::error::Error>> {
    let lat_axis =
        meta_f64_array(&meta.grid, "lat_axis").ok_or("rectilinear WXA missing lat_axis")?;
    let lon_axis =
        meta_f64_array(&meta.grid, "lon_axis").ok_or("rectilinear WXA missing lon_axis")?;
    if lat_axis.len() != meta.ny || lon_axis.len() != meta.nx {
        return Err("rectilinear WXA axis lengths do not match grid shape".into());
    }
    let mut lat = Vec::with_capacity(meta.nx * meta.ny);
    let mut lon = Vec::with_capacity(meta.nx * meta.ny);
    for lat_value in &lat_axis {
        for lon_value in &lon_axis {
            lat.push(*lat_value as f32);
            lon.push(normalize_lon(*lon_value) as f32);
        }
    }
    Ok((lat, lon, Some(GridProjection::Geographic)))
}

fn hrrr_lambert_crop_arrays(
    meta: &WxaDense2dMeta,
) -> Result<(Vec<f32>, Vec<f32>, Option<GridProjection>), Box<dyn std::error::Error>> {
    let hrrr = HrrrLambert::from_meta(&meta.grid);
    let x_start = meta_f64(&meta.grid, "x_start").unwrap_or(0.0).max(0.0) as usize;
    let y_start = meta_f64(&meta.grid, "y_start").unwrap_or(0.0).max(0.0) as usize;
    let full_ny = meta_f64(&meta.grid, "full_ny")
        .map(|value| value.max(1.0) as usize)
        .unwrap_or(hrrr.ny);
    let mut lat = Vec::with_capacity(meta.nx * meta.ny);
    let mut lon = Vec::with_capacity(meta.nx * meta.ny);
    for y in 0..meta.ny {
        let stored_y = y_start + y;
        let projected_y = full_ny.saturating_sub(1).saturating_sub(stored_y);
        for x in 0..meta.nx {
            let (lat_value, lon_value) = hrrr.latlon_at(x_start + x, projected_y);
            lat.push(lat_value as f32);
            lon.push(lon_value as f32);
        }
    }
    Ok((
        lat,
        lon,
        Some(GridProjection::LambertConformal {
            standard_parallel_1_deg: hrrr.latin1,
            standard_parallel_2_deg: hrrr.latin2,
            central_meridian_deg: normalize_lon(hrrr.lov),
        }),
    ))
}

fn sampled_curvilinear_latlon_arrays(
    meta: &WxaDense2dMeta,
) -> Result<(Vec<f32>, Vec<f32>, Option<GridProjection>), Box<dyn std::error::Error>> {
    let sample = meta
        .grid
        .get("sample")
        .and_then(Value::as_object)
        .ok_or("sampled curvilinear WXA missing sample metadata")?;
    let sample_nx = sample
        .get("nx")
        .and_then(Value::as_u64)
        .ok_or("sampled curvilinear WXA missing sample nx")? as usize;
    let sample_ny = sample
        .get("ny")
        .and_then(Value::as_u64)
        .ok_or("sampled curvilinear WXA missing sample ny")? as usize;
    let xs = value_f64_array(sample.get("x").ok_or("sampled curvilinear WXA missing x")?)
        .ok_or("invalid sampled curvilinear x")?;
    let ys = value_f64_array(sample.get("y").ok_or("sampled curvilinear WXA missing y")?)
        .ok_or("invalid sampled curvilinear y")?;
    let lats = value_f64_array(
        sample
            .get("lat")
            .ok_or("sampled curvilinear WXA missing lat")?,
    )
    .ok_or("invalid sampled curvilinear lat")?;
    let lons = value_f64_array(
        sample
            .get("lon")
            .ok_or("sampled curvilinear WXA missing lon")?,
    )
    .ok_or("invalid sampled curvilinear lon")?;
    if sample_nx < 2
        || sample_ny < 2
        || xs.len() != sample_nx
        || ys.len() != sample_ny
        || lats.len() != sample_nx * sample_ny
        || lons.len() != sample_nx * sample_ny
    {
        return Err("sampled curvilinear WXA metadata has inconsistent dimensions".into());
    }

    let mut lat = Vec::with_capacity(meta.nx * meta.ny);
    let mut lon = Vec::with_capacity(meta.nx * meta.ny);
    for y in 0..meta.ny {
        let sy1 = upper_axis_index(&ys, y as f64).clamp(1, sample_ny - 1);
        let sy0 = sy1 - 1;
        let ty = fraction_between(ys[sy0], ys[sy1], y as f64);
        for x in 0..meta.nx {
            let sx1 = upper_axis_index(&xs, x as f64).clamp(1, sample_nx - 1);
            let sx0 = sx1 - 1;
            let tx = fraction_between(xs[sx0], xs[sx1], x as f64);
            let i00 = sy0 * sample_nx + sx0;
            let i10 = sy0 * sample_nx + sx1;
            let i01 = sy1 * sample_nx + sx0;
            let i11 = sy1 * sample_nx + sx1;
            let lat_value = bilerp(lats[i00], lats[i10], lats[i01], lats[i11], tx, ty);
            let lon00 = lons[i00];
            let lon_value = bilerp(
                lon00,
                lon00 + normalized_lon_delta(lons[i10] - lon00),
                lon00 + normalized_lon_delta(lons[i01] - lon00),
                lon00 + normalized_lon_delta(lons[i11] - lon00),
                tx,
                ty,
            );
            lat.push(lat_value as f32);
            lon.push(normalize_lon(lon_value) as f32);
        }
    }
    Ok((lat, lon, None))
}

fn fallback_global_latlon_arrays(
    meta: &WxaDense2dMeta,
) -> (Vec<f32>, Vec<f32>, Option<GridProjection>) {
    let mut lat = Vec::with_capacity(meta.nx * meta.ny);
    let mut lon = Vec::with_capacity(meta.nx * meta.ny);
    for y in 0..meta.ny {
        let lat_value = 90.0 - y as f64 * 180.0 / meta.ny.saturating_sub(1).max(1) as f64;
        for x in 0..meta.nx {
            let lon_value = x as f64 * 360.0 / meta.nx.max(1) as f64 - 180.0;
            lat.push(lat_value as f32);
            lon.push(lon_value as f32);
        }
    }
    (lat, lon, Some(GridProjection::Geographic))
}

fn parse_wxa_dense2d_header(bytes: &[u8]) -> Result<WxaDense2dHeader, Box<dyn std::error::Error>> {
    if bytes.len() < WXA_DENSE2D_HEADER_LEN {
        return Err("file too short for WXA header".into());
    }
    if &bytes[0..8] != WXA_DENSE2D_MAGIC {
        return Err("bad WXA dense2d magic".into());
    }
    let version = u32_from(&bytes[8..12])?;
    if version != WXA_DENSE2D_VERSION {
        return Err(format!("unsupported WXA dense2d version {version}").into());
    }
    let header = WxaDense2dHeader {
        metadata_len: u32_from(&bytes[12..16])? as usize,
        index_count: u64_from(&bytes[16..24])? as usize,
        index_offset: u64_from(&bytes[24..32])? as usize,
        payload_offset: u64_from(&bytes[32..40])? as usize,
    };
    if header.index_offset < WXA_DENSE2D_HEADER_LEN || header.payload_offset < header.index_offset {
        return Err("invalid WXA dense2d offsets".into());
    }
    Ok(header)
}

fn parse_wxa_index(
    bytes: &[u8],
    header: WxaDense2dHeader,
) -> Result<Vec<WxaDense2dIndexRecord>, Box<dyn std::error::Error>> {
    let index_end = header.index_offset + header.index_count * WXA_DENSE2D_INDEX_RECORD_LEN;
    if index_end > bytes.len() || header.payload_offset > bytes.len() {
        return Err("WXA index exceeds file length".into());
    }
    let mut records = Vec::with_capacity(header.index_count);
    let mut offset = header.index_offset;
    for _ in 0..header.index_count {
        records.push(WxaDense2dIndexRecord {
            forecast_hour: u32_from(&bytes[offset..offset + 4])?,
            chunk_y: u32_from(&bytes[offset + 4..offset + 8])? as usize,
            chunk_x: u32_from(&bytes[offset + 8..offset + 12])? as usize,
            y_count: u32_from(&bytes[offset + 12..offset + 16])? as usize,
            x_count: u32_from(&bytes[offset + 16..offset + 20])? as usize,
            raw_len: u32_from(&bytes[offset + 20..offset + 24])? as usize,
            offset: u64_from(&bytes[offset + 24..offset + 32])? as usize,
            len: u64_from(&bytes[offset + 32..offset + 40])? as usize,
            min: f32_from(&bytes[offset + 40..offset + 44])?,
            max: f32_from(&bytes[offset + 44..offset + 48])?,
            valid_count: u32_from(&bytes[offset + 48..offset + 52])?,
        });
        offset += WXA_DENSE2D_INDEX_RECORD_LEN;
    }
    Ok(records)
}

fn validate_wxa_meta(meta: &WxaDense2dMeta) -> Result<(), Box<dyn std::error::Error>> {
    if meta.schema != "wxstore.wxa.dense2d.v1" {
        return Err(format!("unsupported WXA schema '{}'", meta.schema).into());
    }
    if meta.dtype != "f32_le" {
        return Err(format!("unsupported WXA dtype '{}'", meta.dtype).into());
    }
    if meta.codec != "zstd_level_1" {
        return Err(format!("unsupported WXA codec '{}'", meta.codec).into());
    }
    if meta.nx == 0 || meta.ny == 0 || meta.chunk_x == 0 || meta.chunk_y == 0 {
        return Err("WXA metadata has invalid grid/chunk dimensions".into());
    }
    Ok(())
}

fn product_title(product_slug: &str) -> String {
    plot_recipe(product_slug)
        .map(|recipe| recipe.title.to_string())
        .or_else(|| {
            WeatherProduct::from_product_name(product_slug)
                .map(|product| product.display_title().to_string())
        })
        .or_else(|| {
            DerivedProductStyle::from_product_name(product_slug)
                .map(|style| style.display_title().to_string())
        })
        .unwrap_or_else(|| humanize_slug(product_slug))
}

fn subtitle_for_wxa_time(model: Option<ModelId>, run: &str, forecast_hour: u32) -> Option<String> {
    let model = model?;
    let (date, cycle) = parse_wxa_run_time(run)?;
    let forecast_hour_u16 = u16::try_from(forecast_hour).ok()?;
    Some(crate::shared_context::model_time_subtitle(
        model,
        &date,
        cycle,
        forecast_hour_u16,
    ))
}

fn parse_wxa_run_time(run: &str) -> Option<(String, u8)> {
    let date = run.get(0..8)?.to_string();
    if !date.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    for part in run.split('_') {
        let lower = part.to_ascii_lowercase();
        if let Some(hour) = lower
            .strip_suffix('z')
            .and_then(|value| value.parse::<u8>().ok())
        {
            return Some((date, hour));
        }
    }
    None
}

fn model_id_from_wxa(model: &str) -> Option<ModelId> {
    match model.to_ascii_lowercase().as_str() {
        "hrrr" => Some(ModelId::Hrrr),
        "gfs" => Some(ModelId::Gfs),
        "ecmwf" | "ecmwf-open-data" | "ecmwf_open_data" | "ecmwf_ifs" => {
            Some(ModelId::EcmwfOpenData)
        }
        "aifs" | "aifs_ens" | "aifs_ensemble" => Some(ModelId::Aifs),
        _ => None,
    }
}

pub fn common_forecast_hours(paths: &[PathBuf]) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
    let mut common: Option<BTreeSet<u32>> = None;
    for path in paths {
        let (meta, _) = read_wxa_dense2d_metadata(path)?;
        let hours = meta.forecast_hours.into_iter().collect::<BTreeSet<_>>();
        common = Some(match common {
            Some(existing) => existing.intersection(&hours).copied().collect(),
            None => hours,
        });
    }
    Ok(common.unwrap_or_default().into_iter().collect())
}

fn bounds_from_latlon(lat: &[f32], lon: &[f32]) -> Option<[f64; 4]> {
    if lat.len() != lon.len() || lat.is_empty() {
        return None;
    }
    let mut west = f64::INFINITY;
    let mut east = f64::NEG_INFINITY;
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    for (&lat, &lon) in lat.iter().zip(lon.iter()) {
        let lat = lat as f64;
        let lon = lon as f64;
        if !lat.is_finite() || !lon.is_finite() {
            continue;
        }
        west = west.min(lon);
        east = east.max(lon);
        south = south.min(lat);
        north = north.max(lat);
    }
    (west.is_finite() && east.is_finite() && south.is_finite() && north.is_finite())
        .then_some([west, south, east, north])
}

fn fallback_scale() -> DiscreteColorScale {
    DiscreteColorScale {
        levels: range_step(-1.0, 1.01, 0.05),
        colors: palette_scale(
            WeatherPalette::Temperature,
            range_step(-1.0, 1.01, 0.05),
            ExtendMode::Both,
            None,
        )
        .colors,
        extend: ExtendMode::Both,
        mask_below: None,
    }
}

fn generic_reflectivity_scale() -> DiscreteColorScale {
    DiscreteColorScale {
        levels: vec![
            10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0,
        ],
        colors: vec![
            Color::rgba(245, 250, 255, 255),
            Color::rgba(178, 220, 255, 255),
            Color::rgba(70, 145, 230, 255),
            Color::rgba(34, 160, 170, 255),
            Color::rgba(35, 170, 80, 255),
            Color::rgba(220, 220, 50, 255),
            Color::rgba(244, 150, 35, 255),
            Color::rgba(220, 50, 45, 255),
            Color::rgba(156, 45, 155, 255),
            Color::rgba(118, 68, 142, 255),
            Color::rgba(155, 155, 155, 255),
            Color::rgba(100, 100, 100, 255),
        ],
        extend: ExtendMode::Max,
        mask_below: Some(10.0),
    }
}

fn generic_wind_speed_scale() -> DiscreteColorScale {
    DiscreteColorScale {
        levels: vec![
            5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0,
            75.0, 80.0,
        ],
        colors: palette_scale(
            WeatherPalette::Winds,
            vec![
                5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0,
                75.0, 80.0,
            ],
            ExtendMode::Both,
            None,
        )
        .colors,
        extend: ExtendMode::Both,
        mask_below: None,
    }
}

fn range_step(start: f64, end: f64, step: f64) -> Vec<f64> {
    let mut values = Vec::new();
    let mut v = start;
    while v <= end + step * 0.25 {
        values.push((v * 1000.0).round() / 1000.0);
        v += step;
    }
    values
}

fn sanitize_slug(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if matches!(ch, '_' | '-' | '.') {
            out.push(ch);
        } else if ch.is_whitespace() {
            out.push('_');
        }
    }
    out.trim_matches(['_', '-', '.']).to_string()
}

fn humanize_slug(slug: &str) -> String {
    slug.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn meta_f64(meta: &Value, key: &str) -> Option<f64> {
    meta.get(key).and_then(Value::as_f64)
}

fn meta_f64_array(meta: &Value, key: &str) -> Option<Vec<f64>> {
    value_f64_array(meta.get(key)?)
}

fn value_f64_array(value: &Value) -> Option<Vec<f64>> {
    value
        .as_array()?
        .iter()
        .map(Value::as_f64)
        .collect::<Option<Vec<_>>>()
}

fn upper_axis_index(axis: &[f64], value: f64) -> usize {
    axis.iter()
        .position(|axis_value| *axis_value >= value)
        .unwrap_or_else(|| axis.len().saturating_sub(1))
}

fn fraction_between(a: f64, b: f64, value: f64) -> f64 {
    if (b - a).abs() <= f64::EPSILON {
        0.0
    } else {
        ((value - a) / (b - a)).clamp(0.0, 1.0)
    }
}

fn bilerp(v00: f64, v10: f64, v01: f64, v11: f64, tx: f64, ty: f64) -> f64 {
    let top = v00 * (1.0 - tx) + v10 * tx;
    let bottom = v01 * (1.0 - tx) + v11 * tx;
    top * (1.0 - ty) + bottom * ty
}

fn normalize_lon(mut lon: f64) -> f64 {
    while lon > 180.0 {
        lon -= 360.0;
    }
    while lon <= -180.0 {
        lon += 360.0;
    }
    lon
}

fn normalized_lon_delta(mut delta: f64) -> f64 {
    while delta > 180.0 {
        delta -= 360.0;
    }
    while delta < -180.0 {
        delta += 360.0;
    }
    delta
}

fn u32_from(bytes: &[u8]) -> Result<u32, Box<dyn std::error::Error>> {
    Ok(u32::from_le_bytes(bytes.try_into()?))
}

fn u64_from(bytes: &[u8]) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(u64::from_le_bytes(bytes.try_into()?))
}

fn f32_from(bytes: &[u8]) -> Result<f32, Box<dyn std::error::Error>> {
    Ok(f32::from_le_bytes(bytes.try_into()?))
}

#[derive(Debug, Clone, Copy)]
struct HrrrLambert {
    ny: usize,
    lat1: f64,
    lon1: f64,
    dx: f64,
    dy: f64,
    latin1: f64,
    latin2: f64,
    lov: f64,
    earth_radius_m: f64,
}

impl HrrrLambert {
    fn from_meta(meta: &Value) -> Self {
        Self {
            ny: meta_f64(meta, "full_ny").unwrap_or(1059.0) as usize,
            lat1: meta_f64(meta, "lat1").unwrap_or(21.138123),
            lon1: meta_f64(meta, "lon1").unwrap_or(237.280472),
            dx: meta_f64(meta, "dx_m").unwrap_or(3000.0),
            dy: meta_f64(meta, "dy_m").unwrap_or(3000.0),
            latin1: meta_f64(meta, "latin1").unwrap_or(38.5),
            latin2: meta_f64(meta, "latin2").unwrap_or(38.5),
            lov: meta_f64(meta, "lov").unwrap_or(262.5),
            earth_radius_m: meta_f64(meta, "earth_radius_m").unwrap_or(6_371_229.0),
        }
    }

    fn latlon_at(&self, x: usize, y: usize) -> (f64, f64) {
        let n = self.n();
        let f = self.f(n);
        let lon1 = Self::normalize_lon_east(self.lon1);
        let lov = Self::normalize_lon_east(self.lov);
        let theta1 = n * (lon1.to_radians() - lov.to_radians());
        let rho1 = self.rho(self.lat1, n, f);
        let xr = x as f64 * self.dx;
        let yr = y as f64 * self.dy;
        let x_abs = xr + rho1 * theta1.sin();
        let y_abs = rho1 * theta1.cos() - yr;
        let rho = (x_abs * x_abs + y_abs * y_abs).sqrt();
        let theta = x_abs.atan2(y_abs);
        let lat = 2.0 * (self.earth_radius_m * f / rho).powf(1.0 / n).atan()
            - std::f64::consts::FRAC_PI_2;
        let lon = lov.to_radians() + theta / n;
        (lat.to_degrees(), normalize_lon(lon.to_degrees()))
    }

    fn normalize_lon_east(lon: f64) -> f64 {
        if lon < 0.0 { lon + 360.0 } else { lon }
    }

    fn n(&self) -> f64 {
        let phi1 = self.latin1.to_radians();
        let phi2 = self.latin2.to_radians();
        if (self.latin1 - self.latin2).abs() < 1e-9 {
            phi1.sin()
        } else {
            (phi1.cos() / phi2.cos()).ln()
                / (((std::f64::consts::FRAC_PI_4 + phi2 / 2.0).tan())
                    / ((std::f64::consts::FRAC_PI_4 + phi1 / 2.0).tan()))
                .ln()
        }
    }

    fn f(&self, n: f64) -> f64 {
        let phi1 = self.latin1.to_radians();
        (phi1.cos() * (std::f64::consts::FRAC_PI_4 + phi1 / 2.0).tan().powf(n)) / n
    }

    fn rho(&self, lat: f64, n: f64, f: f64) -> f64 {
        let phi = lat.to_radians();
        self.earth_radius_m * f / (std::f64::consts::FRAC_PI_4 + phi / 2.0).tan().powf(n)
    }
}

#[allow(dead_code)]
fn _domain_from_wxa_meta(meta: &WxaDense2dMeta) -> Option<DomainSpec> {
    let bounds = meta.grid.get("bounds").and_then(value_f64_array)?;
    (bounds.len() == 4)
        .then(|| DomainSpec::new(&meta.variable, (bounds[0], bounds[2], bounds[1], bounds[3])))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regular_latlon_geometry_reconstructs_mesh() {
        let meta = WxaDense2dMeta {
            schema: "wxstore.wxa.dense2d.v1".to_string(),
            model: "gfs".to_string(),
            run: "20260506_gfs_18z".to_string(),
            member: Some("control".to_string()),
            variable: "2m_temperature".to_string(),
            units: "degF".to_string(),
            nx: 2,
            ny: 2,
            forecast_hours: vec![0],
            chunk_y: 2,
            chunk_x: 2,
            dtype: "f32_le".to_string(),
            codec: "zstd_level_1".to_string(),
            grid: serde_json::json!({
                "type": "regular_latlon",
                "lat_start": 40.0,
                "lat_step": -1.0,
                "lon_start": -101.0,
                "lon_step": 1.0,
                "bounds": [-101.0, 39.0, -100.0, 40.0]
            }),
        };
        let geometry = geometry_from_wxa_meta(&meta).unwrap();
        assert_eq!(geometry.grid.lat_deg, vec![40.0, 40.0, 39.0, 39.0]);
        assert_eq!(geometry.grid.lon_deg, vec![-101.0, -100.0, -101.0, -100.0]);
    }

    #[test]
    fn run_time_subtitle_parses_wxstore_run() {
        let subtitle = subtitle_for_wxa_time(Some(ModelId::Gfs), "20260506_gfs_18z", 3).unwrap();
        assert!(subtitle.contains("Init 05/06 18Z"));
        assert!(subtitle.contains("F003"));
    }
}
