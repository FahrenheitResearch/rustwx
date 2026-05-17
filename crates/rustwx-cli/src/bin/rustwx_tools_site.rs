use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use rustwx_products::volume_store::VolumeStore;
use rustwx_products::wxstore_wxa::{WxaRenderedPlot, WxaStaticPlotRequest};
use rustwx_products::wxstore_wxa::{
    available_wxa_products, read_wxa_dense2d_metadata, render_wxa_static_plot, wxa_product_path,
};
use rustwx_render::{PngCompressionMode, StaticPlotStyle};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashSet, hash_map::DefaultHasher};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Parser)]
#[command(
    name = "rustwx-tools-site",
    about = "Serve a polished local RustWX tools website for WXA static plots, cross sections, and soundings"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 8790)]
    port: u16,
    #[arg(long, default_value = "proof/rustwx_tools_site")]
    out_dir: PathBuf,
    #[arg(
        long = "wxa-dataset",
        help = "Dataset spec: label|spatial_root|model|run|member. Can be repeated."
    )]
    wxa_datasets: Vec<String>,
    #[arg(
        long = "volume-store",
        help = "VolumeStore spec: label|store_path. Can be repeated."
    )]
    volume_stores: Vec<String>,
    #[arg(long, default_value = "/free")]
    public_base_path: String,
    #[arg(long, default_value = "127.0.0.1:8910")]
    wxstore_proxy_addr: String,
    #[arg(
        long,
        default_value = "/home/drew/weather/wrf_1974_hourly_refphys_live/plots_full/wrfout/d02"
    )]
    super1974_plot_root: PathBuf,
}

#[derive(Debug, Clone)]
struct AppState {
    out_dir: PathBuf,
    artifact_root: PathBuf,
    wxa_datasets: Vec<WxaDataset>,
    volume_stores: Vec<VolumeStoreDataset>,
    public_base_path: String,
    wxstore_proxy_addr: String,
    catalog_cache: Arc<Mutex<Option<CatalogCache>>>,
    super1974_plot_root: PathBuf,
    super1974_plot_cache: Arc<Mutex<Option<Arc<SuperPlotCache>>>>,
}

#[derive(Debug, Clone)]
struct CatalogCache {
    refreshed: Instant,
    body: Value,
}

#[derive(Debug, Clone)]
struct SuperPlotCache {
    refreshed: Instant,
    catalog: Value,
    records: Vec<SuperPlotRecord>,
}

#[derive(Debug, Clone)]
struct SuperPlotRecord {
    product: String,
    title: String,
    forecast_hour: u32,
    lead_code: u32,
    lead_minutes: u32,
    valid_utc: String,
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct WxaDataset {
    id: String,
    label: String,
    spatial_root: PathBuf,
    model: String,
    run: String,
    member: String,
    products: Vec<String>,
    forecast_hours: Vec<u32>,
    available_hours: BTreeMap<String, Vec<u32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VolumeStoreDataset {
    id: String,
    label: String,
    store_path: PathBuf,
    model: String,
    domain: String,
    cycle: String,
    forecast_hours: Vec<u16>,
    levels_hpa: Vec<u16>,
    variables: Vec<String>,
    grid_cells: usize,
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    target: String,
    path: String,
    body: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize)]
struct StaticPlotRenderRequest {
    dataset_id: String,
    product: String,
    forecast_hour: u32,
    #[serde(default = "default_static_width")]
    width: u32,
    #[serde(default = "default_static_height")]
    height: u32,
    bounds: Option<[f64; 4]>,
    plot_style: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CrossSectionRenderRequest {
    dataset_id: String,
    product: String,
    #[serde(default)]
    hour: u16,
    start_lat: f64,
    start_lon: f64,
    end_lat: f64,
    end_lon: f64,
    #[serde(default = "default_spacing_km")]
    spacing_km: f32,
    #[serde(default = "default_top_pressure_hpa")]
    top_pressure_hpa: f64,
    #[serde(default = "default_cross_width")]
    width: u32,
    #[serde(default = "default_cross_height")]
    height: u32,
}

#[derive(Debug, Deserialize)]
struct SoundingRenderRequest {
    dataset_id: String,
    #[serde(default)]
    hour: u16,
    lat: f64,
    lon: f64,
    sample_method: Option<String>,
    box_radius_lat_deg: Option<f64>,
    box_radius_lon_deg: Option<f64>,
    station_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct RenderedArtifact {
    kind: String,
    title: String,
    image_url: String,
    manifest_url: Option<String>,
    report_url: Option<String>,
    elapsed_ms: u128,
    cached: bool,
    cache_key: Option<String>,
    metadata: Value,
}

fn default_static_width() -> u32 {
    1600
}

fn default_static_height() -> u32 {
    900
}

fn default_cross_width() -> u32 {
    1400
}

fn default_cross_height() -> u32 {
    820
}

fn default_spacing_km() -> f32 {
    10.0
}

fn default_top_pressure_hpa() -> f64 {
    100.0
}

fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("create {}", args.out_dir.display()))?;
    let artifact_root = args.out_dir.join("artifacts");
    fs::create_dir_all(&artifact_root)
        .with_context(|| format!("create {}", artifact_root.display()))?;

    let wxa_datasets = load_wxa_datasets(&args)?;
    let volume_stores = load_volume_stores(&args)?;
    let state = Arc::new(AppState {
        out_dir: args.out_dir.clone(),
        artifact_root,
        wxa_datasets,
        volume_stores,
        public_base_path: normalize_base_path(&args.public_base_path),
        wxstore_proxy_addr: args.wxstore_proxy_addr.clone(),
        catalog_cache: Arc::new(Mutex::new(None)),
        super1974_plot_root: args.super1974_plot_root.clone(),
        super1974_plot_cache: Arc::new(Mutex::new(None)),
    });

    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).with_context(|| format!("bind {addr}"))?;
    println!("rustwx tools site: http://{addr}/");
    println!("out_dir: {}", args.out_dir.display());
    println!(
        "datasets: {} wxa, {} volume stores",
        state.wxa_datasets.len(),
        state.volume_stores.len()
    );

    for stream in listener.incoming() {
        let stream = stream?;
        let state = Arc::clone(&state);
        thread::spawn(move || {
            if let Err(err) = handle_connection(stream, state) {
                eprintln!("tools site request failed: {err:#}");
            }
        });
    }
    Ok(())
}

fn load_wxa_datasets(args: &Args) -> Result<Vec<WxaDataset>> {
    let mut specs = args.wxa_datasets.clone();
    if specs.is_empty() {
        specs.extend(auto_wxa_specs()?);
    }
    load_wxa_dataset_specs(specs)
}

fn load_wxa_dataset_specs(specs: Vec<String>) -> Result<Vec<WxaDataset>> {
    let mut seen = HashSet::new();
    let mut datasets = Vec::new();
    for spec in specs {
        let dataset = parse_wxa_dataset_spec(&spec)?;
        if seen.insert(dataset.id.clone()) {
            datasets.push(dataset);
        }
    }
    datasets.sort_by(|left, right| left.label.cmp(&right.label));
    Ok(datasets)
}

fn current_wxa_datasets(state: &AppState) -> Result<Vec<WxaDataset>> {
    let allowlist = tools_site_model_allowlist();
    let mut specs = state
        .wxa_datasets
        .iter()
        .map(wxa_dataset_spec)
        .collect::<Vec<_>>();
    let roots = state
        .wxa_datasets
        .iter()
        .map(|dataset| dataset.spatial_root.clone())
        .collect::<BTreeSet<_>>();
    for root in roots {
        specs.extend(scan_wxa_spatial_root(&root, None)?);
    }
    let mut datasets = load_wxa_dataset_specs(specs)?;
    datasets.retain(|dataset| model_is_allowed(&allowlist, &dataset.model));
    Ok(datasets)
}

fn wxa_dataset_spec(dataset: &WxaDataset) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        dataset.label,
        dataset.spatial_root.display(),
        dataset.model,
        dataset.run,
        dataset.member
    )
}

fn tools_site_model_allowlist() -> Option<HashSet<String>> {
    let value = std::env::var("TOOLS_SITE_MODELS").ok()?;
    let models = value
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .filter_map(|item| {
            let item = item.trim();
            (!item.is_empty()).then(|| item.to_string())
        })
        .collect::<HashSet<_>>();
    (!models.is_empty()).then_some(models)
}

fn model_is_allowed(allowlist: &Option<HashSet<String>>, model: &str) -> bool {
    allowlist
        .as_ref()
        .map_or(true, |models| models.contains(model))
}

fn auto_wxa_specs() -> Result<Vec<String>> {
    let mut specs = Vec::new();
    let proof = std::env::current_dir()?.join("proof");
    for (pointer, roots) in [
        (
            "latest_wxa_all_showcase_regen_root.txt",
            vec![("spatial", None::<&str>)],
        ),
        (
            "latest_wxa_showcases_with_global_gfs_root.txt",
            vec![("spatial", None), ("spatial_gfs_global", Some("Global"))],
        ),
    ] {
        let Some(root) = read_pointer_path(&proof.join(pointer)) else {
            continue;
        };
        for (spatial_dir, label_suffix) in &roots {
            let spatial_root = root.join(spatial_dir);
            if spatial_root.is_dir() {
                specs.extend(scan_wxa_spatial_root(&spatial_root, *label_suffix)?);
            }
        }
    }
    Ok(specs)
}

fn scan_wxa_spatial_root(spatial_root: &Path, label_suffix: Option<&str>) -> Result<Vec<String>> {
    let mut specs = Vec::new();
    if !spatial_root.is_dir() {
        return Ok(specs);
    }
    for model_entry in fs::read_dir(spatial_root)? {
        let model_entry = model_entry?;
        if !model_entry.file_type()?.is_dir() {
            continue;
        }
        let model = model_entry.file_name().to_string_lossy().to_string();
        for run_entry in fs::read_dir(model_entry.path())? {
            let run_entry = run_entry?;
            if !run_entry.file_type()?.is_dir() {
                continue;
            }
            let run = run_entry.file_name().to_string_lossy().to_string();
            let members_dir = run_entry.path().join("members");
            if !members_dir.is_dir() {
                continue;
            }
            for member_entry in fs::read_dir(members_dir)? {
                let member_entry = member_entry?;
                if !member_entry.file_type()?.is_dir() {
                    continue;
                }
                let member = member_entry.file_name().to_string_lossy().to_string();
                if contains_wxa(&member_entry.path())? {
                    let suffix = label_suffix
                        .map(|value| format!(" {value}"))
                        .unwrap_or_default();
                    let label = format!(
                        "{} {}{}",
                        model.to_ascii_uppercase(),
                        human_run_label(&run),
                        suffix
                    );
                    specs.push(format!(
                        "{}|{}|{}|{}|{}",
                        label,
                        spatial_root.display(),
                        model,
                        run,
                        member
                    ));
                }
            }
        }
    }
    Ok(specs)
}

fn contains_wxa(dir: &Path) -> Result<bool> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("wxa") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_wxa_dataset_spec(spec: &str) -> Result<WxaDataset> {
    let parts = spec.split('|').collect::<Vec<_>>();
    if parts.len() != 5 {
        bail!("invalid --wxa-dataset spec '{spec}', expected label|spatial_root|model|run|member");
    }
    let label = parts[0].trim().to_string();
    let spatial_root = PathBuf::from(parts[1].trim());
    let model = parts[2].trim().to_string();
    let run = parts[3].trim().to_string();
    let member = parts[4].trim().to_string();
    if let Some(dataset) =
        parse_wxa_dataset_from_run_manifest(&label, &spatial_root, &model, &run, &member)?
    {
        return Ok(dataset);
    }
    let products = available_wxa_products(&spatial_root, &model, &run, Some(&member))
        .map_err(|err| anyhow!(err.to_string()))
        .with_context(|| {
            format!(
                "load WXA products for {}/{}/{}",
                spatial_root.display(),
                model,
                run
            )
        })?;
    let available_hours = wxa_available_hours(&spatial_root, &model, &run, &member, &products)?;
    let forecast_hours = union_forecast_hours(&available_hours);
    let id = sanitize_id(&format!("{model}_{run}_{member}_{label}"));
    Ok(WxaDataset {
        id,
        label,
        spatial_root,
        model,
        run,
        member,
        products,
        forecast_hours,
        available_hours,
    })
}

fn parse_wxa_dataset_from_run_manifest(
    label: &str,
    spatial_root: &Path,
    model: &str,
    run: &str,
    member: &str,
) -> Result<Option<WxaDataset>> {
    let path = spatial_root.join(model).join(run).join("run-manifest.json");
    if !path.is_file() {
        return Ok(None);
    }
    let manifest: Value = serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("read {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    let mut by_product: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
    let Some(products) = manifest.get("products").and_then(Value::as_array) else {
        return Ok(None);
    };
    for product in products {
        if product
            .get("member")
            .and_then(Value::as_str)
            .is_some_and(|value| value != member)
        {
            continue;
        }
        let Some(product_slug) = product.get("product").and_then(Value::as_str) else {
            continue;
        };
        let hours = product
            .get("forecast_hours")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_u64)
            .filter_map(|value| u32::try_from(value).ok());
        by_product
            .entry(product_slug.to_string())
            .or_default()
            .extend(hours);
    }
    if by_product.is_empty() {
        return Ok(None);
    }
    let available_hours = by_product
        .into_iter()
        .map(|(product, hours)| (product, hours.into_iter().collect::<Vec<_>>()))
        .collect::<BTreeMap<_, _>>();
    let forecast_hours = union_forecast_hours(&available_hours);
    let products = available_hours.keys().cloned().collect::<Vec<_>>();
    let id = sanitize_id(&format!("{model}_{run}_{member}_{label}"));
    Ok(Some(WxaDataset {
        id,
        label: label.to_string(),
        spatial_root: spatial_root.to_path_buf(),
        model: model.to_string(),
        run: run.to_string(),
        member: member.to_string(),
        products,
        forecast_hours,
        available_hours,
    }))
}

fn wxa_available_hours(
    spatial_root: &Path,
    model: &str,
    run: &str,
    member: &str,
    products: &[String],
) -> Result<BTreeMap<String, Vec<u32>>> {
    let mut available = BTreeMap::new();
    for product in products {
        let path = wxa_product_path(spatial_root, model, run, Some(member), product);
        let (_meta, records) =
            read_wxa_dense2d_metadata(&path).map_err(|err| anyhow!(err.to_string()))?;
        let mut hours = records
            .into_iter()
            .map(|record| record.forecast_hour)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        hours.sort();
        available.insert(product.clone(), hours);
    }
    Ok(available)
}

fn union_forecast_hours(available_hours: &BTreeMap<String, Vec<u32>>) -> Vec<u32> {
    available_hours
        .values()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn load_volume_stores(args: &Args) -> Result<Vec<VolumeStoreDataset>> {
    let mut specs = args.volume_stores.clone();
    if specs.is_empty() {
        specs.extend(auto_volume_store_specs()?);
    }
    let mut seen = HashSet::new();
    let mut stores = Vec::new();
    for spec in specs {
        let dataset = parse_volume_store_spec(&spec)?;
        if seen.insert(dataset.id.clone()) {
            stores.push(dataset);
        }
    }
    stores.sort_by(|left, right| left.label.cmp(&right.label));
    Ok(stores)
}

fn current_volume_stores(state: &AppState) -> Result<Vec<VolumeStoreDataset>> {
    let allowlist = tools_site_model_allowlist();
    let mut specs = state
        .volume_stores
        .iter()
        .map(volume_store_spec)
        .collect::<Vec<_>>();
    let mut roots = state
        .volume_stores
        .iter()
        .filter_map(|dataset| volume_store_root(&dataset.store_path))
        .collect::<BTreeSet<_>>();
    for dataset in &state.wxa_datasets {
        if let Some(parent) = dataset.spatial_root.parent() {
            roots.insert(parent.join("pressure_volume"));
        }
    }
    for root in roots {
        specs.extend(scan_volume_store_root(&root)?);
    }
    let mut seen = HashSet::new();
    let mut stores = Vec::new();
    for spec in specs {
        let dataset = parse_volume_store_spec(&spec)?;
        if !model_is_allowed(&allowlist, &dataset.model) {
            continue;
        }
        if seen.insert(dataset.id.clone()) {
            stores.push(dataset);
        }
    }
    stores.sort_by(|left, right| left.label.cmp(&right.label));
    Ok(stores)
}

fn volume_store_spec(dataset: &VolumeStoreDataset) -> String {
    format!("{}|{}", dataset.label, dataset.store_path.display())
}

fn volume_store_root(store_path: &Path) -> Option<PathBuf> {
    store_path
        .parent()?
        .parent()?
        .parent()
        .map(Path::to_path_buf)
}

fn scan_volume_store_root(root: &Path) -> Result<Vec<String>> {
    let mut specs = Vec::new();
    if !root.is_dir() {
        return Ok(specs);
    }
    let allowlist = tools_site_model_allowlist();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let run = entry.file_name().to_string_lossy().to_string();
        let store_path = entry.path().join("store");
        if !is_complete_volume_store(&store_path) {
            continue;
        }
        let Ok(store) = VolumeStore::open(&store_path).map_err(|err| anyhow!(err.to_string()))
        else {
            continue;
        };
        let manifest = store.manifest();
        if !model_is_allowed(&allowlist, &manifest.model) {
            continue;
        }
        let domain = manifest.domain.to_ascii_uppercase();
        let label = format!("{} {} {}", pretty_model_label(&manifest.model), run, domain);
        specs.push(format!("{label}|{}", store_path.display()));
    }
    for model_entry in fs::read_dir(root)? {
        let model_entry = model_entry?;
        if !model_entry.file_type()?.is_dir() {
            continue;
        }
        let model = model_entry.file_name().to_string_lossy().to_string();
        if !model_is_allowed(&allowlist, &model) {
            continue;
        }
        for run_entry in fs::read_dir(model_entry.path())? {
            let run_entry = run_entry?;
            if !run_entry.file_type()?.is_dir() {
                continue;
            }
            let run = run_entry.file_name().to_string_lossy().to_string();
            if run.starts_with('.') {
                continue;
            }
            let store_path = run_entry.path().join("store");
            if !is_complete_volume_store(&store_path) {
                continue;
            }
            let Ok(store) = VolumeStore::open(&store_path).map_err(|err| anyhow!(err.to_string()))
            else {
                continue;
            };
            let domain = store.manifest().domain.to_ascii_uppercase();
            let label = format!("{} {} {}", pretty_model_label(&model), run, domain);
            specs.push(format!("{label}|{}", store_path.display()));
        }
    }
    Ok(specs)
}

fn is_complete_volume_store(store_path: &Path) -> bool {
    [
        "manifest.json",
        "index.bin",
        "chunks.bin",
        "build_stats.json",
    ]
    .into_iter()
    .all(|name| store_path.join(name).is_file())
}

fn pretty_model_label(model: &str) -> String {
    match model {
        "rrfs-a" => "RRFS-A".to_string(),
        "rrfs-public" => "RRFS Public".to_string(),
        "hrrr" => "HRRR".to_string(),
        "rap" => "RAP".to_string(),
        "gfs" => "GFS".to_string(),
        "nbm" => "NBM".to_string(),
        "nam" => "NAM".to_string(),
        other => other.to_string(),
    }
}

fn auto_volume_store_specs() -> Result<Vec<String>> {
    let proof = std::env::current_dir()?.join("proof");
    let Some(root) = read_pointer_path(&proof.join("latest_pressure_volume_benchmark_root.txt"))
    else {
        return Ok(Vec::new());
    };
    let candidates = [
        ("HRRR CONUS volume", "hrrr_conus_store\\store"),
        ("GFS CONUS volume", "gfs_conus_store\\store"),
        ("GFS global volume", "gfs_global_store_full\\store"),
    ];
    Ok(candidates
        .into_iter()
        .filter_map(|(label, rel)| {
            let path = root.join(rel);
            path.is_dir().then(|| format!("{label}|{}", path.display()))
        })
        .collect())
}

fn parse_volume_store_spec(spec: &str) -> Result<VolumeStoreDataset> {
    let parts = spec.split('|').collect::<Vec<_>>();
    if parts.len() != 2 {
        bail!("invalid --volume-store spec '{spec}', expected label|store_path");
    }
    let label = parts[0].trim().to_string();
    let store_path = PathBuf::from(parts[1].trim());
    let store = VolumeStore::open(&store_path)
        .map_err(|err| anyhow!(err.to_string()))
        .with_context(|| format!("open VolumeStore {}", store_path.display()))?;
    let manifest = store.manifest();
    let id = sanitize_id(&format!(
        "{}_{}_{}_{}",
        manifest.model, manifest.domain, manifest.cycle, label
    ));
    Ok(VolumeStoreDataset {
        id,
        label,
        store_path,
        model: manifest.model.clone(),
        domain: manifest.domain.clone(),
        cycle: manifest.cycle.clone(),
        forecast_hours: manifest.forecast_hours.clone(),
        levels_hpa: manifest.levels_hpa.clone(),
        variables: manifest
            .variables
            .iter()
            .map(|variable| variable.name.clone())
            .collect(),
        grid_cells: manifest.grid.grid_len(),
    })
}

fn read_pointer_path(path: &Path) -> Option<PathBuf> {
    fs::read_to_string(path)
        .ok()
        .map(|value| PathBuf::from(value.trim()))
        .filter(|path| path.exists())
}

fn handle_connection(mut stream: TcpStream, state: Arc<AppState>) -> Result<()> {
    let request = read_http_request(&mut stream)?;
    let path =
        strip_site_base_path(&request.path, &state.public_base_path).unwrap_or(&request.path);
    let target = strip_site_base_target(&request.target, &state.public_base_path);
    match (request.method.as_str(), path) {
        ("GET", "/") => write_response(
            &mut stream,
            200,
            "text/html; charset=utf-8",
            TOOLS_HTML.as_bytes(),
        ),
        ("GET", "/api/catalog") => {
            let catalog = catalog_response(&state);
            write_json(&mut stream, &catalog)
        }
        ("GET", "/api/1974super/plots/catalog") => {
            let catalog = super1974_plot_cache(&state)?.catalog.clone();
            write_json(&mut stream, &catalog)
        }
        ("GET", "/api/1974super/plot-image") => {
            serve_super1974_plot_image(&mut stream, &state, &target)
        }
        ("GET", "/health") => write_json(&mut stream, &json!({"ok": true})),
        ("POST", "/api/static-plot/export") => {
            let body: StaticPlotRenderRequest = serde_json::from_slice(&request.body)?;
            let artifact = render_static_plot(&state, body)?;
            write_json(&mut stream, &artifact)
        }
        ("POST", "/api/cross-section/render") => {
            let body: CrossSectionRenderRequest = serde_json::from_slice(&request.body)?;
            let artifact = render_cross_section(&state, body)?;
            write_json(&mut stream, &artifact)
        }
        ("POST", "/api/sounding/render") => {
            let body: SoundingRenderRequest = serde_json::from_slice(&request.body)?;
            let artifact = render_sounding(&state, body)?;
            write_json(&mut stream, &artifact)
        }
        ("GET", path) if path.starts_with("/artifacts/") => {
            serve_artifact(&mut stream, &state, path)
        }
        ("GET", path) if path.starts_with("/v1/") => {
            proxy_wxstore_get(&mut stream, &state, request.method.as_str(), &target)
        }
        ("HEAD", path) if path.starts_with("/v1/") => {
            proxy_wxstore_get(&mut stream, &state, request.method.as_str(), &target)
        }
        _ => write_response(
            &mut stream,
            404,
            "application/json",
            br#"{"error":"not found"}"#,
        ),
    }
}

fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let mut headers = BTreeMap::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }
    let (path, _query) = split_target(&target);
    let path = path.to_string();
    Ok(HttpRequest {
        method,
        target,
        path,
        body,
    })
}

fn render_static_plot(
    state: &AppState,
    request: StaticPlotRenderRequest,
) -> Result<RenderedArtifact> {
    let started = Instant::now();
    let datasets = current_wxa_datasets(state).unwrap_or_else(|err| {
        eprintln!("refresh WXA datasets for static plot failed: {err:#}");
        state.wxa_datasets.clone()
    });
    let dataset = datasets
        .iter()
        .find(|dataset| dataset.id == request.dataset_id)
        .ok_or_else(|| anyhow!("unknown WXA dataset '{}'", request.dataset_id))?;
    if !dataset
        .products
        .iter()
        .any(|product| product == &request.product)
    {
        bail!(
            "WXA dataset '{}' does not contain product '{}'",
            dataset.label,
            request.product
        );
    }
    let plot_style = request
        .plot_style
        .as_deref()
        .and_then(StaticPlotStyle::parse)
        .unwrap_or(StaticPlotStyle::OperationalFast);
    let cache_key = static_plot_cache_key(dataset, &request, plot_style);
    let out_dir = state.artifact_root.join("static_plots").join(&cache_key);
    let report_path = out_dir.join("rustwx_static_plot_report.json");
    if report_path.is_file() {
        let rendered: WxaRenderedPlot = serde_json::from_slice(&fs::read(&report_path)?)?;
        if rendered.output_path.is_file() {
            return rendered_static_artifact(
                state,
                started,
                rendered,
                report_path,
                true,
                Some(cache_key),
            );
        }
    }
    let wxa_path = wxa_product_path(
        &dataset.spatial_root,
        &dataset.model,
        &dataset.run,
        Some(&dataset.member),
        &request.product,
    );
    fs::create_dir_all(&out_dir)?;
    let rendered = render_wxa_static_plot(&WxaStaticPlotRequest {
        wxa_path,
        forecast_hour: request.forecast_hour,
        out_dir,
        width: request.width,
        height: request.height,
        png_compression: PngCompressionMode::Fast,
        plot_style,
        bounds_override: request
            .bounds
            .map(|bounds| (bounds[0], bounds[1], bounds[2], bounds[3])),
        title_override: None,
        subtitle_left: None,
        subtitle_right: None,
        output_suffix: Some(cache_key.clone()),
    })
    .map_err(|err| anyhow!(err.to_string()))?;
    fs::write(&report_path, serde_json::to_vec_pretty(&rendered)?)?;
    rendered_static_artifact(
        state,
        started,
        rendered,
        report_path,
        false,
        Some(cache_key),
    )
}

fn rendered_static_artifact(
    state: &AppState,
    started: Instant,
    rendered: WxaRenderedPlot,
    report_path: PathBuf,
    cached: bool,
    cache_key: Option<String>,
) -> Result<RenderedArtifact> {
    let mut metadata = serde_json::to_value(&rendered)?;
    if let Some(object) = metadata.as_object_mut() {
        object.insert("cached".to_string(), json!(cached));
        if let Some(cache_key) = &cache_key {
            object.insert("cache_key".to_string(), json!(cache_key));
        }
    }
    Ok(RenderedArtifact {
        kind: "static_plot".to_string(),
        title: format!("{} F{:03}", rendered.title, rendered.forecast_hour),
        image_url: artifact_url(state, &rendered.output_path)?,
        manifest_url: None,
        report_url: Some(artifact_url(state, &report_path)?),
        elapsed_ms: started.elapsed().as_millis(),
        cached,
        cache_key,
        metadata,
    })
}

fn static_plot_cache_key(
    dataset: &WxaDataset,
    request: &StaticPlotRenderRequest,
    plot_style: StaticPlotStyle,
) -> String {
    let mut hasher = DefaultHasher::new();
    dataset.id.hash(&mut hasher);
    dataset.model.hash(&mut hasher);
    dataset.run.hash(&mut hasher);
    dataset.member.hash(&mut hasher);
    request.product.hash(&mut hasher);
    request.forecast_hour.hash(&mut hasher);
    request.width.hash(&mut hasher);
    request.height.hash(&mut hasher);
    format!("{plot_style:?}").hash(&mut hasher);
    if let Some(bounds) = request.bounds {
        for value in bounds {
            value.to_bits().hash(&mut hasher);
        }
    }
    if let Some(style) = &request.plot_style {
        style.hash(&mut hasher);
    }
    format!(
        "{}_{}_f{:03}_{:016x}",
        sanitize_id(&dataset.model),
        sanitize_id(&request.product),
        request.forecast_hour,
        hasher.finish()
    )
}

fn cross_section_cache_key(
    dataset: &VolumeStoreDataset,
    request: &CrossSectionRenderRequest,
) -> String {
    let mut hasher = DefaultHasher::new();
    dataset.id.hash(&mut hasher);
    dataset.cycle.hash(&mut hasher);
    request.product.hash(&mut hasher);
    request.hour.hash(&mut hasher);
    request.start_lat.to_bits().hash(&mut hasher);
    request.start_lon.to_bits().hash(&mut hasher);
    request.end_lat.to_bits().hash(&mut hasher);
    request.end_lon.to_bits().hash(&mut hasher);
    request.spacing_km.to_bits().hash(&mut hasher);
    request.top_pressure_hpa.to_bits().hash(&mut hasher);
    request.width.hash(&mut hasher);
    request.height.hash(&mut hasher);
    format!(
        "{}_{}_f{:03}_{:016x}",
        sanitize_id(&dataset.model),
        sanitize_id(&request.product),
        request.hour,
        hasher.finish()
    )
}

fn sounding_cache_key(dataset: &VolumeStoreDataset, request: &SoundingRenderRequest) -> String {
    let mut hasher = DefaultHasher::new();
    dataset.id.hash(&mut hasher);
    dataset.cycle.hash(&mut hasher);
    request.hour.hash(&mut hasher);
    request.lat.to_bits().hash(&mut hasher);
    request.lon.to_bits().hash(&mut hasher);
    normalized_sounding_sample_method(request).hash(&mut hasher);
    request
        .box_radius_lat_deg
        .unwrap_or(0.0)
        .to_bits()
        .hash(&mut hasher);
    request
        .box_radius_lon_deg
        .unwrap_or(0.0)
        .to_bits()
        .hash(&mut hasher);
    request.station_id.hash(&mut hasher);
    format!(
        "{}_sounding_f{:03}_{:016x}",
        sanitize_id(&dataset.model),
        request.hour,
        hasher.finish()
    )
}

fn normalized_sounding_sample_method(request: &SoundingRenderRequest) -> &'static str {
    match request
        .sample_method
        .as_deref()
        .unwrap_or("nearest")
        .trim()
        .to_ascii_lowercase()
        .replace('_', "-")
        .as_str()
    {
        "box-mean" => "box-mean",
        _ => "nearest",
    }
}

fn sounding_title(request: &SoundingRenderRequest) -> String {
    let label = request
        .station_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            if normalized_sounding_sample_method(request) == "box-mean" {
                "Box mean"
            } else {
                "Selected point"
            }
        });
    format!("{label} sounding F{:03}", request.hour)
}

fn render_cross_section(
    state: &AppState,
    request: CrossSectionRenderRequest,
) -> Result<RenderedArtifact> {
    let started = Instant::now();
    let dataset = find_volume_store(state, &request.dataset_id)?;
    let cache_key = cross_section_cache_key(&dataset, &request);
    let out_dir = state.artifact_root.join("cross_sections").join(&cache_key);
    let report_path = out_dir.join("volume_cross_section_render_report.json");
    if report_path.is_file() {
        let report: Value = serde_json::from_slice(&fs::read(&report_path)?)?;
        if let Some(output_path) = first_report_png(&report) {
            if output_path.is_file() {
                return Ok(RenderedArtifact {
                    kind: "cross_section".to_string(),
                    title: format!("{} cross section F{:03}", request.product, request.hour),
                    image_url: artifact_url(state, &output_path)?,
                    manifest_url: None,
                    report_url: Some(artifact_url(state, &report_path)?),
                    elapsed_ms: started.elapsed().as_millis(),
                    cached: true,
                    cache_key: Some(cache_key),
                    metadata: report,
                });
            }
        }
    }
    fs::create_dir_all(&out_dir)?;
    let exe = sibling_binary("volume_store_cross_section_render")?;
    let args = vec![
        "--store".to_string(),
        dataset.store_path.display().to_string(),
        "--out-dir".to_string(),
        out_dir.display().to_string(),
        "--products".to_string(),
        request.product.clone(),
        "--hour".to_string(),
        request.hour.to_string(),
        "--spacing-km".to_string(),
        request.spacing_km.to_string(),
        "--top-pressure-hpa".to_string(),
        request.top_pressure_hpa.to_string(),
        "--width".to_string(),
        request.width.to_string(),
        "--height".to_string(),
        request.height.to_string(),
        "--route-id".to_string(),
        "interactive".to_string(),
        "--route-name".to_string(),
        "Interactive route".to_string(),
        "--start-lat".to_string(),
        request.start_lat.to_string(),
        format!("--start-lon={}", request.start_lon),
        "--end-lat".to_string(),
        request.end_lat.to_string(),
        format!("--end-lon={}", request.end_lon),
    ];
    run_child(&exe, &args)?;
    let report: Value = serde_json::from_slice(&fs::read(&report_path)?)?;
    let output_path = first_report_png(&report)
        .ok_or_else(|| anyhow!("cross-section renderer produced no PNG output"))?;
    Ok(RenderedArtifact {
        kind: "cross_section".to_string(),
        title: format!("{} cross section F{:03}", request.product, request.hour),
        image_url: artifact_url(state, &output_path)?,
        manifest_url: None,
        report_url: Some(artifact_url(state, &report_path)?),
        elapsed_ms: started.elapsed().as_millis(),
        cached: false,
        cache_key: Some(cache_key),
        metadata: report,
    })
}

fn render_sounding(state: &AppState, request: SoundingRenderRequest) -> Result<RenderedArtifact> {
    let started = Instant::now();
    let dataset = find_volume_store(state, &request.dataset_id)?;
    let cache_key = sounding_cache_key(&dataset, &request);
    let out_dir = state.artifact_root.join("soundings").join(&cache_key);
    let output_path = out_dir.join("sounding.png");
    let manifest_path = out_dir.join("sounding_manifest.json");
    if output_path.is_file() && manifest_path.is_file() {
        let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
        if let Some(object) = manifest.as_object_mut() {
            object.insert("cached".to_string(), json!(true));
            object.insert("cache_key".to_string(), json!(cache_key.clone()));
        }
        return Ok(RenderedArtifact {
            kind: "sounding".to_string(),
            title: sounding_title(&request),
            image_url: artifact_url(state, &output_path)?,
            manifest_url: Some(artifact_url(state, &manifest_path)?),
            report_url: None,
            elapsed_ms: started.elapsed().as_millis(),
            cached: true,
            cache_key: Some(cache_key),
            metadata: manifest,
        });
    }
    fs::create_dir_all(&out_dir)?;
    let exe = sibling_binary("volume_store_sounding_render")?;
    let mut args = vec![
        "--store".to_string(),
        dataset.store_path.display().to_string(),
        "--out-dir".to_string(),
        out_dir.display().to_string(),
        "--hour".to_string(),
        request.hour.to_string(),
        "--lat".to_string(),
        request.lat.to_string(),
        format!("--lon={}", request.lon),
        "--sample-method".to_string(),
        normalized_sounding_sample_method(&request).to_string(),
        "--output".to_string(),
        output_path.display().to_string(),
        "--manifest".to_string(),
        manifest_path.display().to_string(),
    ];
    if normalized_sounding_sample_method(&request) == "box-mean" {
        let radius_lat = request
            .box_radius_lat_deg
            .ok_or_else(|| anyhow!("box sounding requires box_radius_lat_deg"))?;
        let radius_lon = request
            .box_radius_lon_deg
            .ok_or_else(|| anyhow!("box sounding requires box_radius_lon_deg"))?;
        args.push("--box-radius-lat-deg".to_string());
        args.push(radius_lat.to_string());
        args.push("--box-radius-lon-deg".to_string());
        args.push(radius_lon.to_string());
    }
    if let Some(station_id) = &request.station_id {
        if !station_id.trim().is_empty() {
            args.push("--station-id".to_string());
            args.push(station_id.trim().to_string());
        }
    }
    run_child(&exe, &args)?;
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if let Some(object) = manifest.as_object_mut() {
        object.insert("cached".to_string(), json!(false));
        object.insert("cache_key".to_string(), json!(cache_key.clone()));
    }
    Ok(RenderedArtifact {
        kind: "sounding".to_string(),
        title: sounding_title(&request),
        image_url: artifact_url(state, &output_path)?,
        manifest_url: Some(artifact_url(state, &manifest_path)?),
        report_url: None,
        elapsed_ms: started.elapsed().as_millis(),
        cached: false,
        cache_key: Some(cache_key),
        metadata: manifest,
    })
}

fn catalog_response(state: &AppState) -> Value {
    let now = Instant::now();
    {
        let cache = state
            .catalog_cache
            .lock()
            .expect("catalog cache lock poisoned");
        if let Some(cached) = cache.as_ref() {
            if now.duration_since(cached.refreshed) < Duration::from_secs(300) {
                return cached.body.clone();
            }
        }
    }

    let wxa_datasets = current_wxa_datasets(state).unwrap_or_else(|err| {
        eprintln!("refresh WXA catalog failed: {err:#}");
        state.wxa_datasets.clone()
    });
    let volume_stores = current_volume_stores(state).unwrap_or_else(|err| {
        eprintln!("refresh volume catalog failed: {err:#}");
        state.volume_stores.clone()
    });
    let body = json!({
        "wxa_datasets": wxa_datasets,
        "volume_stores": volume_stores,
        "cross_section_products": cross_section_products(),
        "output_dir": state.out_dir,
    });
    *state
        .catalog_cache
        .lock()
        .expect("catalog cache lock poisoned") = Some(CatalogCache {
        refreshed: now,
        body: body.clone(),
    });
    body
}

fn super1974_plot_cache(state: &AppState) -> Result<Arc<SuperPlotCache>> {
    let now = Instant::now();
    {
        let cache = state
            .super1974_plot_cache
            .lock()
            .expect("1974 plot cache lock poisoned");
        if let Some(cached) = cache.as_ref() {
            if now.duration_since(cached.refreshed) < Duration::from_secs(120) {
                return Ok(Arc::clone(cached));
            }
        }
    }

    let refreshed = Arc::new(scan_super1974_plots(&state.super1974_plot_root, now)?);
    *state
        .super1974_plot_cache
        .lock()
        .expect("1974 plot cache lock poisoned") = Some(Arc::clone(&refreshed));
    Ok(refreshed)
}

fn scan_super1974_plots(root: &Path, now: Instant) -> Result<SuperPlotCache> {
    let mut records = Vec::new();
    if root.is_dir() {
        for entry in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("png") {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if let Some(mut record) = parse_super1974_plot_name(name) {
                record.path = path;
                records.push(record);
            }
        }
    }
    records.sort_by(|left, right| {
        left.product
            .cmp(&right.product)
            .then(left.lead_minutes.cmp(&right.lead_minutes))
    });

    let mut products: BTreeMap<String, (String, usize)> = BTreeMap::new();
    let mut leads: BTreeMap<u32, (u32, u32, String, usize)> = BTreeMap::new();
    let mut product_leads: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    for record in &records {
        let product = products
            .entry(record.product.clone())
            .or_insert_with(|| (record.title.clone(), 0));
        product.1 += 1;
        product_leads
            .entry(record.product.clone())
            .or_default()
            .push(record.lead_minutes);
        let lead = leads.entry(record.lead_minutes).or_insert_with(|| {
            (
                record.lead_code,
                record.forecast_hour,
                record.valid_utc.clone(),
                0,
            )
        });
        lead.3 += 1;
    }

    let product_values = products
        .iter()
        .map(|(id, (label, count))| {
            json!({
                "id": id,
                "label": label,
                "count": count,
            })
        })
        .collect::<Vec<_>>();
    let lead_values = leads
        .iter()
        .map(
            |(lead_minutes, (lead_code, forecast_hour, valid_utc, count))| {
                json!({
                    "lead_minutes": lead_minutes,
                    "lead_code": lead_code,
                    "forecast_hour": forecast_hour,
                    "label": super1974_lead_label(*lead_code, *lead_minutes),
                    "valid_utc": valid_utc,
                    "count": count,
                })
            },
        )
        .collect::<Vec<_>>();
    for values in product_leads.values_mut() {
        values.sort_unstable();
        values.dedup();
    }
    let product_count = products.len();
    let default_lead_minutes = leads
        .iter()
        .rev()
        .find(|(_, (_, _, _, count))| *count == product_count)
        .map(|(lead_minutes, _)| *lead_minutes)
        .or_else(|| {
            leads
                .iter()
                .max_by_key(|(lead_minutes, (_, _, _, count))| (*count, *lead_minutes))
                .map(|(lead_minutes, _)| *lead_minutes)
        })
        .unwrap_or(0);
    let default_product = [
        "sbcape",
        "composite_reflectivity",
        "1km_reflectivity",
        "sbcape_wrfout",
        "2m_dewpoint",
    ]
    .into_iter()
    .find(|product| {
        product_leads
            .get(*product)
            .is_some_and(|leads| leads.contains(&default_lead_minutes))
    })
    .or_else(|| {
        records
            .iter()
            .find(|record| record.lead_minutes == default_lead_minutes)
            .map(|record| record.product.as_str())
    })
    .or_else(|| products.keys().next().map(String::as_str))
    .unwrap_or("")
    .to_string();

    let catalog = json!({
        "ok": root.is_dir(),
        "root": root,
        "record_count": records.len(),
        "products": product_values,
        "leads": lead_values,
        "product_leads": product_leads,
        "default_product": default_product,
        "default_lead_minutes": default_lead_minutes,
        "domain_bounds": [-102.25763_f64, 28.420467_f64, -73.75076_f64, 43.9414_f64],
        "image_path": "/api/1974super/plot-image",
    });
    Ok(SuperPlotCache {
        refreshed: now,
        catalog,
        records,
    })
}

fn parse_super1974_plot_name(name: &str) -> Option<SuperPlotRecord> {
    let stem = name.strip_suffix(".png")?;
    let marker = "_super_outbreak_1974_hourly_d02_";
    let (prefix, rest) = stem.split_once(marker)?;
    let forecast_marker = prefix.rfind("_f")?;
    let forecast_hour = prefix
        .get(forecast_marker + 2..forecast_marker + 5)?
        .parse()
        .ok()?;
    let (product, tail) = rest.split_once("_wrfout_lead_p")?;
    let lead_code: u32 = tail.get(0..5)?.parse().ok()?;
    let lead_minutes = (lead_code / 100) * 60 + (lead_code % 100);
    let (_, valid_tail) = tail.split_once("_valid_")?;
    let date = valid_tail.get(0..8)?;
    let time = valid_tail.get(9..15)?;
    let valid_utc = format!(
        "{}-{}-{}T{}:{}:{}Z",
        date.get(0..4)?,
        date.get(4..6)?,
        date.get(6..8)?,
        time.get(0..2)?,
        time.get(2..4)?,
        time.get(4..6)?
    );
    Some(SuperPlotRecord {
        product: product.to_string(),
        title: pretty_product_title(product),
        forecast_hour,
        lead_code,
        lead_minutes,
        valid_utc,
        path: PathBuf::new(),
    })
}

fn pretty_product_title(slug: &str) -> String {
    slug.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| match part {
            "mb" => "mb".to_string(),
            "m" => "m".to_string(),
            "km" => "km".to_string(),
            "to" => "to".to_string(),
            "rh" => "RH".to_string(),
            "qpf" => "QPF".to_string(),
            "sbcape" => "SBCAPE".to_string(),
            "mlcape" => "MLCAPE".to_string(),
            "mucape" => "MUCAPE".to_string(),
            "sbecape" => "SBECAPE".to_string(),
            "mlecape" => "MLECAPE".to_string(),
            "muecape" => "MUECAPE".to_string(),
            "cape" => "CAPE".to_string(),
            "sbcin" => "SBCIN".to_string(),
            "mlcin" => "MLCIN".to_string(),
            "mucin" => "MUCIN".to_string(),
            "cin" => "CIN".to_string(),
            "ecape" => "ECAPE".to_string(),
            "ehi" => "EHI".to_string(),
            "scp" => "SCP".to_string(),
            "stp" => "STP".to_string(),
            "srh" => "SRH".to_string(),
            "uh" => "UH".to_string(),
            "mslp" => "MSLP".to_string(),
            other => {
                let mut chars = other.chars();
                match chars.next() {
                    Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
                    None => String::new(),
                }
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn super1974_lead_label(lead_code: u32, lead_minutes: u32) -> String {
    let hhmm_hours = lead_code / 100;
    let hhmm_minutes = lead_code % 100;
    let elapsed_hours = lead_minutes / 60;
    let elapsed_minutes = lead_minutes % 60;
    if hhmm_minutes < 60 {
        format!("+{hhmm_hours:02}:{hhmm_minutes:02}")
    } else {
        format!("+{elapsed_hours:02}:{elapsed_minutes:02}")
    }
}

fn find_volume_store(state: &AppState, id: &str) -> Result<VolumeStoreDataset> {
    if let Some(dataset) = state.volume_stores.iter().find(|dataset| dataset.id == id) {
        return Ok(dataset.clone());
    }

    {
        let cache = state
            .catalog_cache
            .lock()
            .expect("catalog cache lock poisoned");
        if let Some(cached) = cache.as_ref() {
            if let Some(stores) = cached.body.get("volume_stores").and_then(Value::as_array) {
                for store in stores {
                    if store.get("id").and_then(Value::as_str) == Some(id) {
                        return serde_json::from_value(store.clone())
                            .with_context(|| format!("decode cached VolumeStore dataset '{id}'"));
                    }
                }
            }
        }
    }

    current_volume_stores(state)?
        .into_iter()
        .find(|dataset| dataset.id == id)
        .ok_or_else(|| anyhow!("unknown VolumeStore dataset '{id}'"))
}

fn run_child(exe: &Path, args: &[String]) -> Result<()> {
    let output = Command::new(exe)
        .args(args)
        .output()
        .with_context(|| format!("run {}", exe.display()))?;
    if !output.status.success() {
        bail!(
            "{} failed with status {}\nstdout:\n{}\nstderr:\n{}",
            exe.display(),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

fn sibling_binary(name: &str) -> Result<PathBuf> {
    let exe_name = if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_string()
    };
    let current = std::env::current_exe()?;
    let candidates = [
        current
            .parent()
            .map(|parent| parent.join(&exe_name))
            .unwrap_or_else(|| PathBuf::from(&exe_name)),
        std::env::current_dir()?
            .join("target")
            .join("release")
            .join(&exe_name),
        std::env::current_dir()?
            .join("target")
            .join("debug")
            .join(&exe_name),
    ];
    for candidate in candidates {
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    bail!("could not find renderer binary '{exe_name}'. Build release binaries first.")
}

fn first_report_png(report: &Value) -> Option<PathBuf> {
    report
        .get("outputs")
        .and_then(Value::as_array)
        .and_then(|outputs| outputs.first())
        .and_then(|output| output.get("png_path"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
}

fn serve_artifact(stream: &mut TcpStream, state: &AppState, path: &str) -> Result<()> {
    let rel = percent_decode(path.trim_start_matches("/artifacts/"));
    if rel.contains('\0') {
        return write_response(stream, 400, "application/json", br#"{"error":"bad path"}"#);
    }
    let mut safe_rel = PathBuf::new();
    for part in rel.split(|ch| ch == '/' || ch == '\\') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return write_response(stream, 400, "application/json", br#"{"error":"bad path"}"#);
        }
        safe_rel.push(part);
    }
    let requested = state.artifact_root.join(safe_rel);
    let artifact_root = fs::canonicalize(&state.artifact_root)?;
    let requested = fs::canonicalize(&requested)?;
    if !requested.starts_with(&artifact_root) || !requested.is_file() {
        return write_response(stream, 404, "application/json", br#"{"error":"not found"}"#);
    }
    let body = fs::read(&requested)?;
    let content_type = content_type_for_path(&requested);
    write_response(stream, 200, content_type, &body)
}

fn serve_super1974_plot_image(
    stream: &mut TcpStream,
    state: &AppState,
    target: &str,
) -> Result<()> {
    let Some(product) = query_param(target, "product") else {
        return write_response(
            stream,
            400,
            "application/json",
            br#"{"error":"missing product"}"#,
        );
    };
    let Some(lead) = query_param(target, "lead").and_then(|value| value.parse::<u32>().ok()) else {
        return write_response(
            stream,
            400,
            "application/json",
            br#"{"error":"missing lead"}"#,
        );
    };
    let cache = super1974_plot_cache(state)?;
    let Some(record) = cache
        .records
        .iter()
        .find(|record| record.product == product && record.lead_minutes == lead)
    else {
        return write_response(
            stream,
            404,
            "application/json",
            br#"{"error":"plot not found"}"#,
        );
    };
    let root = fs::canonicalize(&state.super1974_plot_root)
        .with_context(|| format!("canonicalize {}", state.super1974_plot_root.display()))?;
    let requested = fs::canonicalize(&record.path)
        .with_context(|| format!("canonicalize {}", record.path.display()))?;
    if !requested.starts_with(&root) || !requested.is_file() {
        return write_response(
            stream,
            404,
            "application/json",
            br#"{"error":"plot not found"}"#,
        );
    }
    let body = fs::read(&requested).with_context(|| format!("read {}", requested.display()))?;
    write_response(stream, 200, "image/png", &body)
}

fn artifact_url(state: &AppState, path: &Path) -> Result<String> {
    let root = fs::canonicalize(&state.artifact_root)?;
    let path = fs::canonicalize(path)?;
    let rel = path
        .strip_prefix(&root)
        .with_context(|| format!("{} is outside {}", path.display(), root.display()))?;
    Ok(format!(
        "/artifacts/{}",
        rel.to_string_lossy().replace('\\', "/")
    ))
}

fn content_type_for_path(path: &Path) -> &'static str {
    match path.extension().and_then(|ext| ext.to_str()).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "json" => "application/json",
        "png" => "image/png",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    }
}

fn write_json<T: Serialize>(stream: &mut TcpStream, value: &T) -> Result<()> {
    let body = serde_json::to_vec(value)?;
    write_response(stream, 200, "application/json", &body)
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        502 => "Bad Gateway",
        500 => "Internal Server Error",
        _ => "OK",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    Ok(())
}

fn proxy_wxstore_get(
    stream: &mut TcpStream,
    state: &AppState,
    method: &str,
    target: &str,
) -> Result<()> {
    if target.bytes().any(|byte| byte == b'\r' || byte == b'\n') {
        return write_response(
            stream,
            400,
            "application/json",
            br#"{"error":"bad target"}"#,
        );
    }
    let mut origin = match TcpStream::connect(&state.wxstore_proxy_addr) {
        Ok(origin) => origin,
        Err(err) => {
            eprintln!(
                "wxstore proxy connect failed addr={} err={err}",
                state.wxstore_proxy_addr
            );
            return write_response(
                stream,
                502,
                "application/json",
                br#"{"error":"wxstore unavailable"}"#,
            );
        }
    };
    origin.set_read_timeout(Some(Duration::from_secs(60))).ok();
    origin.set_write_timeout(Some(Duration::from_secs(10))).ok();
    write!(
        origin,
        "{method} {target} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nAccept: */*\r\nUser-Agent: rustwx-tools-site-proxy\r\n\r\n",
        state.wxstore_proxy_addr
    )?;
    let mut buf = [0u8; 32768];
    loop {
        let n = origin.read(&mut buf)?;
        if n == 0 {
            break;
        }
        stream.write_all(&buf[..n])?;
    }
    Ok(())
}

fn normalize_base_path(path: &str) -> String {
    let trimmed = path.trim().trim_end_matches('/');
    if trimmed.is_empty() || trimmed == "/" {
        String::new()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn strip_base_path<'a>(path: &'a str, base: &str) -> Option<&'a str> {
    if base.is_empty() {
        return None;
    }
    if path == base {
        return Some("/");
    }
    path.strip_prefix(base).filter(|rest| rest.starts_with('/'))
}

fn strip_site_base_path<'a>(path: &'a str, public_base: &str) -> Option<&'a str> {
    strip_base_path(path, "/1974super").or_else(|| strip_base_path(path, public_base))
}

fn strip_site_base_target(target: &str, public_base: &str) -> String {
    let (path, query) = split_target(target);
    let stripped = strip_site_base_path(path, public_base).unwrap_or(path);
    if query.is_empty() {
        stripped.to_string()
    } else {
        format!("{stripped}?{query}")
    }
}

fn split_target(target: &str) -> (&str, &str) {
    target
        .split_once('?')
        .map(|(path, query)| (path, query))
        .unwrap_or((target, ""))
}

fn query_param(target: &str, name: &str) -> Option<String> {
    let (_, query) = split_target(target);
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (percent_decode(key) == name).then(|| percent_decode(value))
    })
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                if let Ok(decoded) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
                    out.push(decoded);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn render_id(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!("{prefix}_{millis}")
}

fn sanitize_id(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_underscore = false;
    for ch in value.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '_'
        };
        if mapped == '_' {
            if !last_underscore {
                out.push(mapped);
            }
            last_underscore = true;
        } else {
            out.push(mapped);
            last_underscore = false;
        }
    }
    out.trim_matches('_').to_string()
}

fn human_run_label(run: &str) -> String {
    run.replace('_', " ").replace(" z", "Z")
}

fn cross_section_products() -> Vec<Value> {
    [
        ("temperature", "Temperature"),
        ("wind_speed", "Wind Speed"),
        ("theta_e", "Theta-E"),
        ("rh", "Relative Humidity"),
        ("q", "Specific Humidity"),
        ("omega", "Omega"),
        ("vorticity", "Absolute Vorticity"),
        ("shear", "Wind Shear"),
        ("lapse_rate", "Lapse Rate"),
        ("cloud", "Cloud Water"),
        ("cloud_total", "Total Condensate"),
        ("wetbulb", "Wet Bulb"),
        ("icing", "Icing"),
        ("frontogenesis", "Frontogenesis"),
        ("vpd", "Vapor Pressure Deficit"),
        ("dewpoint_dep", "Dewpoint Depression"),
        ("moisture_transport", "Moisture Transport"),
        ("pv", "Potential Vorticity"),
        ("fire_wx", "Fire Weather"),
    ]
    .into_iter()
    .map(|(slug, label)| json!({ "slug": slug, "label": label }))
    .collect()
}

const TOOLS_HTML: &str = include_str!("rustwx_tools_site.html");

#[allow(dead_code)]
const TOOLS_HTML_LEGACY: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>RustWX Tools</title>
  <style>
    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0;
      font-family: Inter, Segoe UI, Arial, sans-serif;
      color: #12171c;
      background: #f3f5f7;
    }
    button, input, select {
      font: inherit;
    }
    button {
      height: 34px;
      border: 1px solid #b6bec7;
      border-radius: 6px;
      background: #ffffff;
      color: #18212a;
      padding: 0 12px;
      cursor: pointer;
    }
    button.primary {
      background: #155e63;
      border-color: #155e63;
      color: #ffffff;
      font-weight: 700;
    }
    button.active {
      background: #17212b;
      color: #ffffff;
      border-color: #17212b;
    }
    button:disabled {
      opacity: .55;
      cursor: wait;
    }
    select, input {
      height: 34px;
      border: 1px solid #b8c0c9;
      border-radius: 6px;
      background: #ffffff;
      color: #111820;
      padding: 0 10px;
      min-width: 0;
    }
    header {
      height: 58px;
      display: flex;
      align-items: center;
      gap: 18px;
      padding: 0 20px;
      background: #111820;
      color: #f8fafc;
      border-bottom: 1px solid #2b3744;
    }
    .brand {
      display: flex;
      flex-direction: column;
      gap: 2px;
      min-width: 160px;
    }
    .brand strong { font-size: 17px; }
    .brand span { font-size: 12px; color: #b8c3ce; }
    .topMetrics {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-left: auto;
    }
    .metric {
      border: 1px solid #384756;
      background: #17222d;
      border-radius: 6px;
      padding: 6px 9px;
      font-size: 12px;
      color: #dbe4ec;
    }
    main {
      height: calc(100vh - 58px);
      display: grid;
      grid-template-columns: minmax(520px, 1.2fr) minmax(430px, .8fr);
      gap: 12px;
      padding: 12px;
    }
    .mapPanel, .workPanel {
      min-height: 0;
      border: 1px solid #c8d0d8;
      border-radius: 8px;
      background: #ffffff;
      overflow: hidden;
    }
    .mapToolbar {
      height: 48px;
      display: grid;
      grid-template-columns: auto auto auto 1fr auto;
      align-items: center;
      gap: 8px;
      padding: 7px 9px;
      border-bottom: 1px solid #d8dee4;
      background: #fbfcfd;
    }
    #map {
      position: relative;
      height: calc(100% - 48px);
      min-height: 420px;
      overflow: hidden;
      background: #d8dde1;
      cursor: crosshair;
      user-select: none;
    }
    .tile {
      position: absolute;
      width: 256px;
      height: 256px;
    }
    #overlay {
      position: absolute;
      inset: 0;
      pointer-events: none;
    }
    .coord {
      font-size: 12px;
      color: #46525e;
      text-align: right;
      white-space: nowrap;
    }
    .workPanel {
      display: grid;
      grid-template-rows: auto 1fr;
    }
    .toolGrid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
      padding: 10px;
      border-bottom: 1px solid #d8dee4;
      background: #fbfcfd;
    }
    .toolTile {
      border: 1px solid #cbd3dc;
      border-radius: 8px;
      padding: 10px;
      background: #ffffff;
      cursor: pointer;
      min-height: 82px;
    }
    .toolTile.active {
      border-color: #155e63;
      box-shadow: inset 0 0 0 2px #155e63;
    }
    .toolTile strong {
      display: block;
      font-size: 14px;
      margin-bottom: 8px;
    }
    .toolTile span {
      display: block;
      font-size: 12px;
      color: #53606b;
      line-height: 1.35;
    }
    .toolBody {
      min-height: 0;
      overflow: auto;
      padding: 12px;
    }
    .formGrid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }
    label {
      display: flex;
      flex-direction: column;
      gap: 5px;
      min-width: 0;
      font-size: 12px;
      color: #48545f;
      font-weight: 700;
    }
    label.wide { grid-column: 1 / -1; }
    .actionRow {
      display: flex;
      gap: 8px;
      align-items: center;
      margin-bottom: 12px;
      flex-wrap: wrap;
    }
    .status {
      color: #48545f;
      font-size: 13px;
    }
    .result {
      border-top: 1px solid #d8dee4;
      padding-top: 12px;
    }
    .result img {
      width: 100%;
      height: auto;
      display: block;
      border: 1px solid #c9d1d9;
      background: #eef1f4;
    }
    .resultMeta {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin: 10px 0;
    }
    .pill {
      border: 1px solid #c8d0d8;
      border-radius: 999px;
      padding: 4px 8px;
      font-size: 12px;
      background: #ffffff;
      color: #3e4a55;
    }
    .links {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      font-size: 13px;
    }
    a { color: #0f5e79; text-decoration: none; }
    a:hover { text-decoration: underline; }
    @media (max-width: 980px) {
      header { height: auto; min-height: 58px; align-items: flex-start; padding: 10px 14px; }
      main { height: auto; min-height: calc(100vh - 58px); grid-template-columns: 1fr; }
      .mapPanel { min-height: 520px; }
      .toolGrid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <strong>RustWX Tools</strong>
      <span>Native plots, profiles, and volume diagnostics</span>
    </div>
    <div class="topMetrics" id="topMetrics"></div>
  </header>
  <main>
    <section class="mapPanel">
      <div class="mapToolbar">
        <button id="pickPoint" class="active">Point</button>
        <button id="pickRoute">Route</button>
        <button id="zoomOut">-</button>
        <div class="coord" id="coordReadout">loading...</div>
        <button id="zoomIn">+</button>
      </div>
      <div id="map">
        <div id="tiles"></div>
        <svg id="overlay"></svg>
      </div>
    </section>
    <section class="workPanel">
      <div class="toolGrid">
        <div class="toolTile active" data-tool="static"><strong>Static plot export</strong><span>WXA dense2d to operational PNG</span></div>
        <div class="toolTile" data-tool="cross"><strong>Cross section</strong><span>Pressure-volume route render</span></div>
        <div class="toolTile" data-tool="sounding"><strong>Sounding</strong><span>Profile sample to SHARP-style PNG</span></div>
      </div>
      <div class="toolBody">
        <div class="formGrid" id="formGrid"></div>
        <div class="actionRow" id="actionRow"></div>
        <div class="status" id="status">initializing</div>
        <div class="result" id="result"></div>
      </div>
    </section>
  </main>
<script>
const app = {
  catalog: null,
  tool: "static",
  map: { center: { lat: 37.8, lon: -96.0 }, zoom: 4, point: { lat: 35.4676, lon: -97.5164 }, route: [{ lat: 34.05, lon: -118.25 }, { lat: 39.32, lon: -120.18 }] },
  busy: false
};
const mapEl = document.getElementById("map");
const tilesEl = document.getElementById("tiles");
const overlayEl = document.getElementById("overlay");
const coordEl = document.getElementById("coordReadout");
const statusEl = document.getElementById("status");
const resultEl = document.getElementById("result");
const formGrid = document.getElementById("formGrid");
const actionRow = document.getElementById("actionRow");

function lonToX(lon, z) { return (lon + 180) / 360 * 256 * Math.pow(2, z); }
function latToY(lat, z) {
  const rad = lat * Math.PI / 180;
  return (1 - Math.log(Math.tan(rad) + 1 / Math.cos(rad)) / Math.PI) / 2 * 256 * Math.pow(2, z);
}
function xToLon(x, z) { return x / (256 * Math.pow(2, z)) * 360 - 180; }
function yToLat(y, z) {
  const n = Math.PI - 2 * Math.PI * y / (256 * Math.pow(2, z));
  return 180 / Math.PI * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)));
}
function project(lat, lon) {
  const cx = lonToX(app.map.center.lon, app.map.zoom);
  const cy = latToY(app.map.center.lat, app.map.zoom);
  return { x: lonToX(lon, app.map.zoom) - cx + mapEl.clientWidth / 2, y: latToY(lat, app.map.zoom) - cy + mapEl.clientHeight / 2 };
}
function unproject(x, y) {
  const cx = lonToX(app.map.center.lon, app.map.zoom);
  const cy = latToY(app.map.center.lat, app.map.zoom);
  return { lat: yToLat(y - mapEl.clientHeight / 2 + cy, app.map.zoom), lon: xToLon(x - mapEl.clientWidth / 2 + cx, app.map.zoom) };
}
function renderMap() {
  tilesEl.innerHTML = "";
  const z = app.map.zoom;
  const cx = lonToX(app.map.center.lon, z);
  const cy = latToY(app.map.center.lat, z);
  const left = cx - mapEl.clientWidth / 2;
  const top = cy - mapEl.clientHeight / 2;
  const x0 = Math.floor(left / 256) - 1;
  const x1 = Math.floor((left + mapEl.clientWidth) / 256) + 1;
  const y0 = Math.floor(top / 256) - 1;
  const y1 = Math.floor((top + mapEl.clientHeight) / 256) + 1;
  const max = Math.pow(2, z);
  for (let x = x0; x <= x1; x++) {
    for (let y = y0; y <= y1; y++) {
      if (y < 0 || y >= max) continue;
      const img = document.createElement("img");
      img.className = "tile";
      img.alt = "";
      img.src = `https://tile.openstreetmap.org/${z}/${((x % max) + max) % max}/${y}.png`;
      img.style.left = `${x * 256 - left}px`;
      img.style.top = `${y * 256 - top}px`;
      tilesEl.appendChild(img);
    }
  }
  renderOverlay();
}
function renderOverlay() {
  overlayEl.setAttribute("width", mapEl.clientWidth);
  overlayEl.setAttribute("height", mapEl.clientHeight);
  overlayEl.innerHTML = "";
  const point = project(app.map.point.lat, app.map.point.lon);
  overlayEl.insertAdjacentHTML("beforeend", `<circle cx="${point.x}" cy="${point.y}" r="7" fill="#b9352f" stroke="#ffffff" stroke-width="2"/>`);
  const pts = app.map.route.map(p => project(p.lat, p.lon));
  if (pts.length === 2) {
    overlayEl.insertAdjacentHTML("beforeend", `<line x1="${pts[0].x}" y1="${pts[0].y}" x2="${pts[1].x}" y2="${pts[1].y}" stroke="#155e63" stroke-width="3"/>`);
  }
  for (const p of pts) overlayEl.insertAdjacentHTML("beforeend", `<circle cx="${p.x}" cy="${p.y}" r="6" fill="#155e63" stroke="#ffffff" stroke-width="2"/>`);
  coordEl.textContent = `point ${app.map.point.lat.toFixed(3)}, ${app.map.point.lon.toFixed(3)} | route ${app.map.route.length}/2`;
}
function setTool(tool) {
  app.tool = tool;
  document.querySelectorAll(".toolTile").forEach(tile => tile.classList.toggle("active", tile.dataset.tool === tool));
  resultEl.innerHTML = "";
  renderForm();
}
function setPickMode(mode) {
  document.getElementById("pickPoint").classList.toggle("active", mode === "point");
  document.getElementById("pickRoute").classList.toggle("active", mode === "route");
  mapEl.dataset.pick = mode;
}
function optionList(items, valueKey, labelKey) {
  return items.map(item => `<option value="${item[valueKey]}">${item[labelKey]}</option>`).join("");
}
function renderForm() {
  const wxa = app.catalog.wxa_datasets;
  const volumes = app.catalog.volume_stores;
  if (app.tool === "static") {
    const ds = wxa[0];
    const products = ds ? ds.products : [];
    const hours = ds ? ds.forecast_hours : [];
    formGrid.innerHTML = `
      <label class="wide">WXA dataset<select id="wxaDataset">${optionList(wxa, "id", "label")}</select></label>
      <label>Product<select id="staticProduct">${products.map(p => `<option value="${p}">${p}</option>`).join("")}</select></label>
      <label>Forecast hour<select id="staticHour">${hours.map(h => `<option value="${h}">F${String(h).padStart(3, "0")}</option>`).join("")}</select></label>
      <label>Width<input id="staticWidth" type="number" value="1600"></label>
      <label>Height<input id="staticHeight" type="number" value="900"></label>`;
    actionRow.innerHTML = `<button class="primary" id="runStatic">Export PNG</button><span class="status">native WXA render</span>`;
    document.getElementById("wxaDataset").addEventListener("change", renderForm);
    document.getElementById("runStatic").addEventListener("click", runStatic);
  } else if (app.tool === "cross") {
    const ds = volumes[0];
    const hours = ds ? ds.forecast_hours : [];
    formGrid.innerHTML = `
      <label class="wide">Volume store<select id="volumeDataset">${optionList(volumes, "id", "label")}</select></label>
      <label>Product<select id="crossProduct">${optionList(app.catalog.cross_section_products, "slug", "label")}</select></label>
      <label>Forecast hour<select id="crossHour">${hours.map(h => `<option value="${h}">F${String(h).padStart(3, "0")}</option>`).join("")}</select></label>
      <label>Spacing km<input id="spacingKm" type="number" value="10"></label>
      <label>Top hPa<input id="topPressure" type="number" value="100"></label>`;
    actionRow.innerHTML = `<button class="primary" id="runCross">Render Cross Section</button><span class="status">uses selected route</span>`;
    document.getElementById("volumeDataset").addEventListener("change", renderForm);
    document.getElementById("runCross").addEventListener("click", runCross);
  } else {
    const ds = volumes[0];
    const hours = ds ? ds.forecast_hours : [];
    formGrid.innerHTML = `
      <label class="wide">Volume store<select id="soundingDataset">${optionList(volumes, "id", "label")}</select></label>
      <label>Forecast hour<select id="soundingHour">${hours.map(h => `<option value="${h}">F${String(h).padStart(3, "0")}</option>`).join("")}</select></label>
      <label>Station label<input id="stationId" value="KOKC"></label>
      <label>Sample lon<input id="sampleLon" type="number" value="${app.map.point.lon.toFixed(4)}"></label>
      <label>Sample lat<input id="sampleLat" type="number" value="${app.map.point.lat.toFixed(4)}"></label>`;
    actionRow.innerHTML = `<button class="primary" id="runSounding">Render Sounding</button><span class="status">uses selected point</span>`;
    document.getElementById("soundingDataset").addEventListener("change", renderForm);
    document.getElementById("runSounding").addEventListener("click", runSounding);
  }
}
async function postJson(url, body) {
  const res = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}
function setBusy(value) {
  app.busy = value;
  document.querySelectorAll("button").forEach(btn => btn.disabled = value);
}
function showArtifact(artifact) {
  const links = [];
  if (artifact.report_url) links.push(`<a href="${artifact.report_url}" target="_blank">report</a>`);
  if (artifact.manifest_url) links.push(`<a href="${artifact.manifest_url}" target="_blank">manifest</a>`);
  links.push(`<a href="${artifact.image_url}" target="_blank">png</a>`);
  resultEl.innerHTML = `
    <div class="resultMeta">
      <span class="pill">${artifact.kind}</span>
      <span class="pill">${artifact.elapsed_ms} ms</span>
      <span class="pill">${artifact.title}</span>
    </div>
    <img src="${artifact.image_url}?v=${Date.now()}" alt="${artifact.title}">
    <div class="links">${links.join("")}</div>`;
}
async function runStatic() {
  const ds = document.getElementById("wxaDataset").value;
  setBusy(true); statusEl.textContent = "exporting static PNG...";
  try {
    const artifact = await postJson("/api/static-plot/export", {
      dataset_id: ds,
      product: document.getElementById("staticProduct").value,
      forecast_hour: Number(document.getElementById("staticHour").value),
      width: Number(document.getElementById("staticWidth").value),
      height: Number(document.getElementById("staticHeight").value)
    });
    showArtifact(artifact);
    statusEl.textContent = "static PNG exported";
  } catch (err) {
    statusEl.textContent = err.message;
  } finally {
    setBusy(false);
  }
}
async function runCross() {
  if (app.map.route.length !== 2) { statusEl.textContent = "select a complete route"; return; }
  setBusy(true); statusEl.textContent = "rendering cross section...";
  try {
    const [a, b] = app.map.route;
    const artifact = await postJson("/api/cross-section/render", {
      dataset_id: document.getElementById("volumeDataset").value,
      product: document.getElementById("crossProduct").value,
      hour: Number(document.getElementById("crossHour").value),
      start_lat: a.lat,
      start_lon: a.lon,
      end_lat: b.lat,
      end_lon: b.lon,
      spacing_km: Number(document.getElementById("spacingKm").value),
      top_pressure_hpa: Number(document.getElementById("topPressure").value)
    });
    showArtifact(artifact);
    statusEl.textContent = "cross section rendered";
  } catch (err) {
    statusEl.textContent = err.message;
  } finally {
    setBusy(false);
  }
}
async function runSounding() {
  setBusy(true); statusEl.textContent = "rendering sounding...";
  try {
    const artifact = await postJson("/api/sounding/render", {
      dataset_id: document.getElementById("soundingDataset").value,
      hour: Number(document.getElementById("soundingHour").value),
      lat: Number(document.getElementById("sampleLat").value),
      lon: Number(document.getElementById("sampleLon").value),
      station_id: document.getElementById("stationId").value
    });
    showArtifact(artifact);
    statusEl.textContent = "sounding rendered";
  } catch (err) {
    statusEl.textContent = err.message;
  } finally {
    setBusy(false);
  }
}
async function init() {
  app.catalog = await fetch("/api/catalog").then(res => res.json());
  document.getElementById("topMetrics").innerHTML = `
    <div class="metric">${app.catalog.wxa_datasets.length} WXA datasets</div>
    <div class="metric">${app.catalog.volume_stores.length} volume stores</div>
    <div class="metric">${app.catalog.cross_section_products.length} cross-section products</div>`;
  renderForm();
  renderMap();
  statusEl.textContent = "ready";
}
document.querySelectorAll(".toolTile").forEach(tile => tile.addEventListener("click", () => setTool(tile.dataset.tool)));
document.getElementById("pickPoint").addEventListener("click", () => setPickMode("point"));
document.getElementById("pickRoute").addEventListener("click", () => setPickMode("route"));
document.getElementById("zoomIn").addEventListener("click", () => { app.map.zoom = Math.min(app.map.zoom + 1, 10); renderMap(); });
document.getElementById("zoomOut").addEventListener("click", () => { app.map.zoom = Math.max(app.map.zoom - 1, 2); renderMap(); });
mapEl.dataset.pick = "point";
mapEl.addEventListener("click", evt => {
  if (app.busy) return;
  const rect = mapEl.getBoundingClientRect();
  const point = unproject(evt.clientX - rect.left, evt.clientY - rect.top);
  if (mapEl.dataset.pick === "point") {
    app.map.point = point;
    const lat = document.getElementById("sampleLat");
    const lon = document.getElementById("sampleLon");
    if (lat && lon) { lat.value = point.lat.toFixed(4); lon.value = point.lon.toFixed(4); }
  } else {
    if (app.map.route.length >= 2) app.map.route = [];
    app.map.route.push(point);
  }
  renderOverlay();
});
window.addEventListener("resize", renderMap);
init().catch(err => { statusEl.textContent = err.message; });
</script>
</body>
</html>
"##;
