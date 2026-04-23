use clap::{Args, Parser, Subcommand, ValueEnum};
use rustwx_core::{FieldPointSampleMethod, GeoPoint, GeoPolygon, ModelId, SourceId};
use rustwx_products::sampling::{
    ProductAreaSummaryRequest, ProductPointSamplingRequest, ProductSamplingRunRequest,
    sample_products_at_point, summarize_products_over_polygon,
};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(
    name = "product_sampling",
    about = "Machine-readable rustwx point and area sampling"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Point(PointArgs),
    Area(AreaArgs),
}

#[derive(Debug, Clone, Args)]
struct RunArgs {
    model: ModelId,
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    forecast_hour: u16,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long = "product", required = true)]
    products: Vec<String>,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

#[derive(Debug, Clone, Args)]
struct PointArgs {
    #[command(flatten)]
    run: RunArgs,
    #[arg(long)]
    lat: f64,
    #[arg(long)]
    lon: f64,
    #[arg(long, value_enum, default_value_t = PointMethodArg::InverseDistance4)]
    method: PointMethodArg,
}

#[derive(Debug, Clone, Args)]
struct AreaArgs {
    #[command(flatten)]
    run: RunArgs,
    #[arg(long)]
    polygon_file: Option<PathBuf>,
    #[arg(long)]
    polygon_json: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PointMethodArg {
    Nearest,
    InverseDistance4,
}

impl From<PointMethodArg> for FieldPointSampleMethod {
    fn from(value: PointMethodArg) -> Self {
        match value {
            PointMethodArg::Nearest => FieldPointSampleMethod::Nearest,
            PointMethodArg::InverseDistance4 => FieldPointSampleMethod::InverseDistance4,
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Point(args) => {
            let request = ProductPointSamplingRequest {
                run: build_run_request(&args.run),
                point: GeoPoint::new(args.lat, args.lon),
                method: args.method.into(),
            };
            let report = sample_products_at_point(&request)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Area(args) => {
            let polygon = load_polygon(args.polygon_file.as_deref(), args.polygon_json.as_deref())?;
            let request = ProductAreaSummaryRequest {
                run: build_run_request(&args.run),
                polygon,
            };
            let report = summarize_products_over_polygon(&request)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

fn build_run_request(args: &RunArgs) -> ProductSamplingRunRequest {
    ProductSamplingRunRequest {
        model: args.model,
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args
            .source
            .unwrap_or_else(|| rustwx_models::model_summary(args.model).sources[0].id),
        cache_root: args.cache_dir.clone().unwrap_or_else(default_cache_dir),
        use_cache: !args.no_cache,
        product_slugs: args.products.clone(),
    }
}

fn default_cache_dir() -> PathBuf {
    std::env::temp_dir().join("rustwx-product-sampling-cache")
}

fn load_polygon(
    polygon_file: Option<&Path>,
    polygon_json: Option<&str>,
) -> Result<GeoPolygon, Box<dyn std::error::Error>> {
    match (polygon_file, polygon_json) {
        (Some(_), Some(_)) => Err("choose either --polygon-file or --polygon-json".into()),
        (None, None) => Err("one of --polygon-file or --polygon-json is required".into()),
        (Some(path), None) => parse_polygon_json(&std::fs::read_to_string(path)?),
        (None, Some(json)) => parse_polygon_json(json),
    }
}

fn parse_polygon_json(json: &str) -> Result<GeoPolygon, Box<dyn std::error::Error>> {
    if let Ok(polygon) = serde_json::from_str::<GeoPolygon>(json) {
        return Ok(polygon);
    }

    let value = serde_json::from_str::<serde_json::Value>(json)?;
    parse_geojson_like_polygon(&value)
}

fn parse_geojson_like_polygon(
    value: &serde_json::Value,
) -> Result<GeoPolygon, Box<dyn std::error::Error>> {
    let Some(kind) = value.get("type").and_then(serde_json::Value::as_str) else {
        return Err("polygon JSON must be a GeoPolygon schema or GeoJSON object".into());
    };
    match kind {
        "Polygon" => polygon_from_geojson_geometry(value),
        "Feature" => polygon_from_geojson_geometry(
            value
                .get("geometry")
                .ok_or("GeoJSON feature is missing a geometry object")?,
        ),
        "FeatureCollection" => {
            let features = value
                .get("features")
                .and_then(serde_json::Value::as_array)
                .ok_or("GeoJSON feature collection is missing a features array")?;
            let feature = features
                .iter()
                .find(|feature| {
                    feature
                        .get("geometry")
                        .and_then(|geometry| geometry.get("type"))
                        .and_then(serde_json::Value::as_str)
                        == Some("Polygon")
                })
                .ok_or("GeoJSON feature collection did not contain a Polygon geometry")?;
            polygon_from_geojson_geometry(
                feature
                    .get("geometry")
                    .ok_or("GeoJSON feature is missing a geometry object")?,
            )
        }
        other => Err(format!("unsupported polygon JSON type '{other}'").into()),
    }
}

fn polygon_from_geojson_geometry(
    geometry: &serde_json::Value,
) -> Result<GeoPolygon, Box<dyn std::error::Error>> {
    if geometry.get("type").and_then(serde_json::Value::as_str) != Some("Polygon") {
        return Err("GeoJSON geometry must be of type Polygon".into());
    }
    let rings = geometry
        .get("coordinates")
        .and_then(serde_json::Value::as_array)
        .ok_or("GeoJSON polygon is missing coordinates")?;
    let mut parsed_rings = Vec::<Vec<GeoPoint>>::new();
    for ring in rings {
        let coordinates = ring
            .as_array()
            .ok_or("GeoJSON polygon rings must be arrays of [lon, lat] pairs")?;
        let mut points = Vec::with_capacity(coordinates.len());
        for coordinate in coordinates {
            let pair = coordinate
                .as_array()
                .ok_or("GeoJSON polygon coordinates must be [lon, lat] arrays")?;
            if pair.len() < 2 {
                return Err("GeoJSON polygon coordinates must contain lon and lat".into());
            }
            let lon = pair[0]
                .as_f64()
                .ok_or("GeoJSON polygon lon must be numeric")?;
            let lat = pair[1]
                .as_f64()
                .ok_or("GeoJSON polygon lat must be numeric")?;
            points.push(GeoPoint::new(lat, lon));
        }
        parsed_rings.push(points);
    }
    let exterior = parsed_rings
        .first()
        .cloned()
        .ok_or("GeoJSON polygon must contain at least one ring")?;
    let holes = if parsed_rings.len() > 1 {
        parsed_rings[1..].to_vec()
    } else {
        Vec::new()
    };
    Ok(GeoPolygon::new(exterior, holes))
}
