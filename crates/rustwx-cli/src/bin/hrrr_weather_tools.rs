use clap::{Args, Parser, Subcommand, ValueEnum};
use rustwx_cli::cross_section_proof::{
    PressureCrossSectionRequest, RoutePreset, prepare_pressure_cross_section_scene,
    run_pressure_cross_section_with_loaded,
};
use rustwx_core::{FieldPointSampleMethod, GeoPoint, SourceId};
use rustwx_cross_section::{CrossSectionProduct, GeoPoint as SectionGeoPoint};
use rustwx_products::artifact_bundle::{
    ArtifactBundleArtifact, ArtifactBundleManifest, ArtifactBundleRole, ArtifactBundleRunContext,
    default_artifact_bundle_manifest_path, publish_artifact_bundle_manifest,
};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::derived::{
    NativeContourRenderMode, build_hrrr_live_derived_artifact_profiled,
};
use rustwx_products::direct::build_projected_map_with_projection;
use rustwx_products::gridded::load_model_timestep_from_parts;
use rustwx_products::intelligence::{
    AreaQueryResult, PointQueryResult, bounds_from_named_asset, compare_query_fields_over_bounds,
    resolve_query_field, sample_query_field_point, summarize_query_field_bounds,
};
use rustwx_products::named_geometry::{
    NamedGeometryAsset, NamedGeometryCatalog, NamedGeometryKind, NamedGeometrySelector,
};
use rustwx_products::publication_provenance::capture_default_build_provenance;
use rustwx_products::shared_context::DomainSpec;
use rustwx_render::{PngCompressionMode, PngWriteOptions, save_png_profile_with_options};
use serde::Serialize;
use serde_json::json;
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-weather-tools",
    about = "Structured HRRR backend tools for named assets, field queries, comparisons, and artifact bundles"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    ListAssets(ListAssetsArgs),
    PointSample(PointSampleArgs),
    AreaSummary(AreaSummaryArgs),
    CompareArea(CompareAreaArgs),
    RouteSummary(RouteSummaryArgs),
    BundleDerivedMap(BundleDerivedMapArgs),
    BundleCrossSection(BundleCrossSectionArgs),
}

#[derive(Debug, Clone, Args)]
struct RunArgs {
    #[arg(long, default_value = "20260422")]
    date: String,
    #[arg(long, default_value_t = 7)]
    cycle: u8,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: SourceId,
    #[arg(long, default_value = "target\\weather_tools")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

impl RunArgs {
    fn cache_root(&self) -> PathBuf {
        self.cache_dir
            .clone()
            .unwrap_or_else(|| default_proof_cache_dir(&self.out_dir))
    }

    fn use_cache(&self) -> bool {
        !self.no_cache
    }
}

#[derive(Debug, Clone, Args)]
struct CatalogArgs {
    #[arg(long)]
    catalog_json: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct AssetSelectorArgs {
    #[arg(long, value_enum)]
    kind: Option<KindArg>,
    #[arg(long)]
    group: Option<String>,
    #[arg(long = "tag", value_delimiter = ',', num_args = 1..)]
    tags: Vec<String>,
    #[arg(long = "slug", value_delimiter = ',', num_args = 1..)]
    slugs: Vec<String>,
}

#[derive(Debug, Clone, Args)]
struct ListAssetsArgs {
    #[command(flatten)]
    catalog: CatalogArgs,
    #[command(flatten)]
    selector: AssetSelectorArgs,
}

#[derive(Debug, Clone, Args)]
struct PointSampleArgs {
    #[command(flatten)]
    run: RunArgs,
    #[arg(long)]
    recipe: String,
    #[arg(long)]
    lat: f64,
    #[arg(long)]
    lon: f64,
    #[arg(long, value_enum, default_value_t = SampleMethodArg::Nearest)]
    sample_method: SampleMethodArg,
}

#[derive(Debug, Clone, Args)]
struct AreaSummaryArgs {
    #[command(flatten)]
    run: RunArgs,
    #[command(flatten)]
    catalog: CatalogArgs,
    #[arg(long)]
    recipe: String,
    #[arg(long)]
    asset: String,
}

#[derive(Debug, Clone, Args)]
struct CompareAreaArgs {
    #[command(flatten)]
    run: RunArgs,
    #[command(flatten)]
    catalog: CatalogArgs,
    #[arg(long)]
    recipe: String,
    #[arg(long)]
    asset: String,
    #[arg(long)]
    compare_date: Option<String>,
    #[arg(long)]
    compare_cycle: Option<u8>,
    #[arg(long)]
    compare_forecast_hour: u16,
    #[arg(long)]
    compare_source: Option<SourceId>,
}

#[derive(Debug, Clone, Args)]
struct RouteSummaryArgs {
    #[command(flatten)]
    run: RunArgs,
    #[command(flatten)]
    catalog: CatalogArgs,
    #[arg(long)]
    route_asset: String,
    #[arg(long, value_enum, default_value_t = CrossSectionProductArg::FireWeather)]
    product: CrossSectionProductArg,
    #[arg(long, default_value_t = 181)]
    sample_count: usize,
}

#[derive(Debug, Clone, Args)]
struct BundleDerivedMapArgs {
    #[command(flatten)]
    run: RunArgs,
    #[command(flatten)]
    catalog: CatalogArgs,
    #[arg(long)]
    recipe: String,
    #[arg(long)]
    asset: String,
}

#[derive(Debug, Clone, Args)]
struct BundleCrossSectionArgs {
    #[command(flatten)]
    run: RunArgs,
    #[command(flatten)]
    catalog: CatalogArgs,
    #[arg(long)]
    route_asset: String,
    #[arg(long, value_enum, default_value_t = CrossSectionProductArg::FireWeather)]
    product: CrossSectionProductArg,
    #[arg(long, default_value_t = 181)]
    sample_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum KindArg {
    Metro,
    Region,
    WatchArea,
    Route,
    Other,
}

impl From<KindArg> for NamedGeometryKind {
    fn from(value: KindArg) -> Self {
        match value {
            KindArg::Metro => Self::Metro,
            KindArg::Region => Self::Region,
            KindArg::WatchArea => Self::WatchArea,
            KindArg::Route => Self::Route,
            KindArg::Other => Self::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SampleMethodArg {
    Nearest,
    InverseDistance4,
}

impl From<SampleMethodArg> for FieldPointSampleMethod {
    fn from(value: SampleMethodArg) -> Self {
        match value {
            SampleMethodArg::Nearest => Self::Nearest,
            SampleMethodArg::InverseDistance4 => Self::InverseDistance4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CrossSectionProductArg {
    Temperature,
    RelativeHumidity,
    SpecificHumidity,
    ThetaE,
    WindSpeed,
    WetBulb,
    VaporPressureDeficit,
    DewpointDepression,
    MoistureTransport,
    FireWeather,
}

impl From<CrossSectionProductArg> for CrossSectionProduct {
    fn from(value: CrossSectionProductArg) -> Self {
        match value {
            CrossSectionProductArg::Temperature => Self::Temperature,
            CrossSectionProductArg::RelativeHumidity => Self::RelativeHumidity,
            CrossSectionProductArg::SpecificHumidity => Self::SpecificHumidity,
            CrossSectionProductArg::ThetaE => Self::ThetaE,
            CrossSectionProductArg::WindSpeed => Self::WindSpeed,
            CrossSectionProductArg::WetBulb => Self::WetBulb,
            CrossSectionProductArg::VaporPressureDeficit => Self::VaporPressureDeficit,
            CrossSectionProductArg::DewpointDepression => Self::DewpointDepression,
            CrossSectionProductArg::MoistureTransport => Self::MoistureTransport,
            CrossSectionProductArg::FireWeather => Self::FireWeather,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct CrossSectionRouteSummary {
    route_asset: NamedGeometryAsset,
    facts: rustwx_products::cross_section::PressureCrossSectionFacts,
}

#[derive(Debug, Clone, Serialize)]
struct DerivedBundleSummary {
    bundle_manifest: PathBuf,
    image_path: PathBuf,
    summary_json: PathBuf,
    area_summary: AreaQueryResult,
}

#[derive(Debug, Clone, Serialize)]
struct CrossSectionBundleSummary {
    bundle_manifest: PathBuf,
    image_path: PathBuf,
    summary_json: PathBuf,
    facts: rustwx_products::cross_section::PressureCrossSectionFacts,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::ListAssets(args) => print_json(&run_list_assets(&args)?),
        Command::PointSample(args) => print_json(&run_point_sample(&args)?),
        Command::AreaSummary(args) => print_json(&run_area_summary(&args)?),
        Command::CompareArea(args) => print_json(&run_compare_area(&args)?),
        Command::RouteSummary(args) => print_json(&run_route_summary(&args)?),
        Command::BundleDerivedMap(args) => print_json(&run_bundle_derived_map(&args)?),
        Command::BundleCrossSection(args) => print_json(&run_bundle_cross_section(&args)?),
    }
}

fn run_list_assets(
    args: &ListAssetsArgs,
) -> Result<Vec<NamedGeometryAsset>, Box<dyn std::error::Error>> {
    let catalog = load_catalog(args.catalog.catalog_json.as_deref())?;
    Ok(catalog
        .select(&build_selector(&args.selector))
        .into_iter()
        .cloned()
        .collect())
}

fn run_point_sample(
    args: &PointSampleArgs,
) -> Result<PointQueryResult, Box<dyn std::error::Error>> {
    ensure_dir(&args.run.out_dir)?;
    let field = resolve_query_field(
        rustwx_core::ModelId::Hrrr,
        &args.run.date,
        Some(args.run.cycle),
        args.run.forecast_hour,
        args.run.source,
        &args.recipe,
        &args.run.cache_root(),
        args.run.use_cache(),
    )?;
    let point = GeoPoint::new(args.lat, args.lon);
    Ok(sample_query_field_point(
        &field,
        point,
        args.sample_method.into(),
    ))
}

fn run_area_summary(args: &AreaSummaryArgs) -> Result<AreaQueryResult, Box<dyn std::error::Error>> {
    ensure_dir(&args.run.out_dir)?;
    let catalog = load_catalog(args.catalog.catalog_json.as_deref())?;
    let asset = resolve_asset(&catalog, &args.asset)?;
    let bounds = bounds_from_named_asset(&asset)?;
    let field = resolve_query_field(
        rustwx_core::ModelId::Hrrr,
        &args.run.date,
        Some(args.run.cycle),
        args.run.forecast_hour,
        args.run.source,
        &args.recipe,
        &args.run.cache_root(),
        args.run.use_cache(),
    )?;
    Ok(summarize_query_field_bounds(&field, bounds, Some(asset)))
}

fn run_compare_area(
    args: &CompareAreaArgs,
) -> Result<rustwx_products::intelligence::AreaComparisonResult, Box<dyn std::error::Error>> {
    ensure_dir(&args.run.out_dir)?;
    let catalog = load_catalog(args.catalog.catalog_json.as_deref())?;
    let asset = resolve_asset(&catalog, &args.asset)?;
    let bounds = bounds_from_named_asset(&asset)?;
    let left = resolve_query_field(
        rustwx_core::ModelId::Hrrr,
        &args.run.date,
        Some(args.run.cycle),
        args.run.forecast_hour,
        args.run.source,
        &args.recipe,
        &args.run.cache_root(),
        args.run.use_cache(),
    )?;
    let right = resolve_query_field(
        rustwx_core::ModelId::Hrrr,
        args.compare_date.as_deref().unwrap_or(&args.run.date),
        Some(args.compare_cycle.unwrap_or(args.run.cycle)),
        args.compare_forecast_hour,
        args.compare_source.unwrap_or(args.run.source),
        &args.recipe,
        &args.run.cache_root(),
        args.run.use_cache(),
    )?;
    compare_query_fields_over_bounds(&left, &right, bounds, Some(asset))
}

fn run_route_summary(
    args: &RouteSummaryArgs,
) -> Result<CrossSectionRouteSummary, Box<dyn std::error::Error>> {
    ensure_dir(&args.run.out_dir)?;
    let catalog = load_catalog(args.catalog.catalog_json.as_deref())?;
    let asset = resolve_asset(&catalog, &args.route_asset)?;
    let route = route_from_asset(&asset)?;
    let loaded = load_model_timestep_from_parts(
        rustwx_core::ModelId::Hrrr,
        &args.run.date,
        Some(args.run.cycle),
        args.run.forecast_hour,
        args.run.source,
        None,
        None,
        &args.run.cache_root(),
        args.run.use_cache(),
    )?;
    let request = PressureCrossSectionRequest {
        model: rustwx_core::ModelId::Hrrr,
        date: args.run.date.clone(),
        cycle: args.run.cycle,
        forecast_hour: args.run.forecast_hour,
        source: args.run.source,
        route,
        product: args.product.into(),
        palette: None,
        sample_count: args.sample_count,
        out_dir: args.run.out_dir.clone(),
        cache_dir: Some(args.run.cache_root()),
        use_cache: args.run.use_cache(),
        show_wind_overlay: true,
        surface_product_override: None,
        pressure_product_override: None,
    };
    let scene = prepare_pressure_cross_section_scene(&request, &loaded)?;
    Ok(CrossSectionRouteSummary {
        route_asset: asset,
        facts: scene.facts,
    })
}

fn run_bundle_derived_map(
    args: &BundleDerivedMapArgs,
) -> Result<DerivedBundleSummary, Box<dyn std::error::Error>> {
    ensure_dir(&args.run.out_dir)?;
    let catalog = load_catalog(args.catalog.catalog_json.as_deref())?;
    let asset = resolve_asset(&catalog, &args.asset)?;
    let bounds = bounds_from_named_asset(&asset)?;
    let cache_root = args.run.cache_root();
    let loaded = load_model_timestep_from_parts(
        rustwx_core::ModelId::Hrrr,
        &args.run.date,
        Some(args.run.cycle),
        args.run.forecast_hour,
        args.run.source,
        None,
        None,
        &cache_root,
        args.run.use_cache(),
    )?;
    let domain = DomainSpec::new(asset.slug.clone(), bounds.as_tuple());
    let projected = build_projected_map_with_projection(
        &loaded.grid.lat_deg,
        &loaded.grid.lon_deg,
        loaded.surface_decode.value.projection.as_ref(),
        domain.bounds,
        1200.0 / 900.0,
    )?;
    let profiled = build_hrrr_live_derived_artifact_profiled(
        &args.recipe,
        &loaded.surface_decode.value,
        &loaded.pressure_decode.value,
        &loaded.grid,
        &projected,
        domain.bounds,
        &args.run.date,
        args.run.cycle,
        args.run.forecast_hour,
        args.run.source,
        NativeContourRenderMode::LegacyRaster,
    )?;
    let image_path = args.run.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_{}.png",
        args.run.date, args.run.cycle, args.run.forecast_hour, asset.slug, args.recipe
    ));
    save_png_profile_with_options(
        &profiled.artifact.request,
        &image_path,
        &PngWriteOptions {
            compression: PngCompressionMode::Default,
        },
    )?;
    let resolved = resolve_query_field(
        rustwx_core::ModelId::Hrrr,
        &args.run.date,
        Some(args.run.cycle),
        args.run.forecast_hour,
        args.run.source,
        &args.recipe,
        &cache_root,
        args.run.use_cache(),
    )?;
    let area_summary = summarize_query_field_bounds(&resolved, bounds, Some(asset.clone()));
    let summary_path = args.run.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_{}_bundle_summary.json",
        args.run.date, args.run.cycle, args.run.forecast_hour, asset.slug, args.recipe
    ));
    std::fs::write(&summary_path, serde_json::to_vec_pretty(&area_summary)?)?;

    let bundle_path = default_artifact_bundle_manifest_path(
        &args.run.out_dir,
        &format!("{}_{}", asset.slug, args.recipe),
    );
    let mut manifest = ArtifactBundleManifest::new(
        "hrrr_weather_tools_derived_bundle",
        format!("{} {}", asset.label, args.recipe),
        &args.run.out_dir,
    )
    .with_build_provenance(capture_default_build_provenance())
    .with_run_context(
        ArtifactBundleRunContext::new("hrrr_weather_tools")
            .with_model("hrrr")
            .with_cycle_metadata(
                args.run.date.clone(),
                args.run.cycle,
                args.run.forecast_hour,
            )
            .with_source(args.run.source.to_string())
            .with_domain_slug(asset.slug.clone()),
    );
    manifest.insert_metadata_value("asset_slug", json!(asset.slug.as_str()));
    manifest.insert_metadata_value("asset_label", json!(asset.label.as_str()));
    manifest.insert_metadata_value("recipe_slug", json!(args.recipe.as_str()));

    let mut image_artifact = ArtifactBundleArtifact::from_existing_path(
        format!("map:{}:{}", asset.slug, args.recipe),
        ArtifactBundleRole::PrimaryImage,
        "image/png",
        &args.run.out_dir,
        &image_path,
    )?;
    image_artifact.insert_metadata_value("recipe_slug", json!(args.recipe.as_str()));
    image_artifact.insert_metadata_value("asset_slug", json!(asset.slug.as_str()));
    image_artifact.insert_stat_value(
        "included_cell_count",
        json!(area_summary.summary.included_cell_count),
    );
    image_artifact.insert_stat_value(
        "valid_cell_count",
        json!(area_summary.summary.valid_cell_count),
    );
    image_artifact.insert_stat_value("mean", json!(area_summary.summary.mean));
    manifest.push_artifact(image_artifact);

    let mut summary_artifact = ArtifactBundleArtifact::from_existing_path(
        format!("map:{}:{}:summary", asset.slug, args.recipe),
        ArtifactBundleRole::Stats,
        "application/json",
        &args.run.out_dir,
        &summary_path,
    )?;
    summary_artifact.insert_metadata_value("recipe_slug", json!(args.recipe.as_str()));
    summary_artifact.insert_metadata_value("asset_slug", json!(asset.slug.as_str()));
    manifest.push_artifact(summary_artifact);
    publish_artifact_bundle_manifest(&bundle_path, &manifest)?;

    Ok(DerivedBundleSummary {
        bundle_manifest: bundle_path,
        image_path,
        summary_json: summary_path,
        area_summary,
    })
}

fn run_bundle_cross_section(
    args: &BundleCrossSectionArgs,
) -> Result<CrossSectionBundleSummary, Box<dyn std::error::Error>> {
    ensure_dir(&args.run.out_dir)?;
    let catalog = load_catalog(args.catalog.catalog_json.as_deref())?;
    let asset = resolve_asset(&catalog, &args.route_asset)?;
    let route = route_from_asset(&asset)?;
    let cache_root = args.run.cache_root();
    let loaded = load_model_timestep_from_parts(
        rustwx_core::ModelId::Hrrr,
        &args.run.date,
        Some(args.run.cycle),
        args.run.forecast_hour,
        args.run.source,
        None,
        None,
        &cache_root,
        args.run.use_cache(),
    )?;
    let request = PressureCrossSectionRequest {
        model: rustwx_core::ModelId::Hrrr,
        date: args.run.date.clone(),
        cycle: args.run.cycle,
        forecast_hour: args.run.forecast_hour,
        source: args.run.source,
        route,
        product: args.product.into(),
        palette: None,
        sample_count: args.sample_count,
        out_dir: args.run.out_dir.clone(),
        cache_dir: Some(cache_root),
        use_cache: args.run.use_cache(),
        show_wind_overlay: true,
        surface_product_override: None,
        pressure_product_override: None,
    };
    let output = run_pressure_cross_section_with_loaded(&request, &loaded)?;
    let bundle_path = default_artifact_bundle_manifest_path(
        &args.run.out_dir,
        &format!("{}_{}", asset.slug, output.summary.product_slug),
    );
    let mut manifest = ArtifactBundleManifest::new(
        "hrrr_weather_tools_cross_section_bundle",
        format!("{} {}", asset.label, output.summary.product_label),
        &args.run.out_dir,
    )
    .with_build_provenance(capture_default_build_provenance())
    .with_run_context(
        ArtifactBundleRunContext::new("hrrr_weather_tools")
            .with_model("hrrr")
            .with_cycle_metadata(
                args.run.date.clone(),
                args.run.cycle,
                args.run.forecast_hour,
            )
            .with_source(args.run.source.to_string())
            .with_domain_slug(asset.slug.clone()),
    );
    manifest.insert_metadata_value("route_asset_slug", json!(asset.slug.as_str()));
    manifest.insert_metadata_value("route_asset_label", json!(asset.label.as_str()));
    manifest.insert_metadata_value(
        "cross_section_product",
        json!(output.summary.product_slug.as_str()),
    );

    let mut image_artifact = ArtifactBundleArtifact::from_existing_path(
        format!(
            "cross_section:{}:{}",
            asset.slug, output.summary.product_slug
        ),
        ArtifactBundleRole::PrimaryImage,
        "image/png",
        &args.run.out_dir,
        &output.output_path,
    )?;
    image_artifact.insert_metadata_value("route_asset_slug", json!(asset.slug.as_str()));
    image_artifact.insert_metadata_value(
        "cross_section_product",
        json!(output.summary.product_slug.as_str()),
    );
    manifest.push_artifact(image_artifact);

    let mut summary_artifact = ArtifactBundleArtifact::from_existing_path(
        format!(
            "cross_section:{}:{}:summary",
            asset.slug, output.summary.product_slug
        ),
        ArtifactBundleRole::Stats,
        "application/json",
        &args.run.out_dir,
        &output.summary_path,
    )?;
    summary_artifact.insert_metadata_value("route_asset_slug", json!(asset.slug.as_str()));
    summary_artifact.insert_metadata_value(
        "cross_section_product",
        json!(output.summary.product_slug.as_str()),
    );
    manifest.push_artifact(summary_artifact);
    publish_artifact_bundle_manifest(&bundle_path, &manifest)?;

    Ok(CrossSectionBundleSummary {
        bundle_manifest: bundle_path,
        image_path: output.output_path,
        summary_json: output.summary_path,
        facts: output.summary.facts,
    })
}

fn load_catalog(path: Option<&Path>) -> Result<NamedGeometryCatalog, Box<dyn std::error::Error>> {
    match path {
        Some(path) => NamedGeometryCatalog::load_json(path),
        None => Ok(NamedGeometryCatalog::built_in()),
    }
}

fn build_selector(args: &AssetSelectorArgs) -> NamedGeometrySelector {
    let mut selector = NamedGeometrySelector::new();
    if let Some(kind) = args.kind {
        selector = selector.with_kind(kind.into());
    }
    if let Some(group) = args.group.as_deref() {
        selector = selector.with_group(group);
    }
    for tag in &args.tags {
        selector = selector.with_tag(tag);
    }
    for slug in &args.slugs {
        selector = selector.with_slug(slug);
    }
    selector
}

fn resolve_asset(
    catalog: &NamedGeometryCatalog,
    slug: &str,
) -> Result<NamedGeometryAsset, Box<dyn std::error::Error>> {
    catalog
        .find(slug)
        .cloned()
        .ok_or_else(|| format!("named asset '{}' was not found", slug).into())
}

fn route_from_asset(asset: &NamedGeometryAsset) -> Result<RoutePreset, Box<dyn std::error::Error>> {
    let points = asset
        .path_points()
        .ok_or_else(|| format!("named asset '{}' does not carry a route path", asset.slug))?;
    let start = points
        .first()
        .ok_or_else(|| format!("named route '{}' does not contain points", asset.slug))?;
    let end = points
        .last()
        .ok_or_else(|| format!("named route '{}' does not contain points", asset.slug))?;
    RoutePreset::new(
        asset.slug.clone(),
        asset.label.clone(),
        SectionGeoPoint::new(start.lat_deg, start.lon_deg)?,
        SectionGeoPoint::new(end.lat_deg, end.lon_deg)?,
    )
}

fn print_json<T: Serialize>(value: &T) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_builder_keeps_kind_group_tags_and_slugs() {
        let selector = build_selector(&AssetSelectorArgs {
            kind: Some(KindArg::WatchArea),
            group: Some("enterprise_watch".to_string()),
            tags: vec!["fire".to_string()],
            slugs: vec!["foothill_watch".to_string()],
        });

        assert_eq!(selector.kind, Some(NamedGeometryKind::WatchArea));
        assert_eq!(selector.group.as_deref(), Some("enterprise_watch"));
        assert_eq!(selector.tags, vec!["fire"]);
        assert_eq!(selector.slugs, vec!["foothill_watch"]);
    }

    #[test]
    fn built_in_route_asset_converts_to_cross_section_route() {
        let catalog = NamedGeometryCatalog::built_in();
        let asset = resolve_asset(&catalog, "sacramento_reno").unwrap();
        let route = route_from_asset(&asset).unwrap();

        assert_eq!(route.slug(), "sacramento_reno");
        assert!(route.path().unwrap().total_distance_km() > 100.0);
    }
}
