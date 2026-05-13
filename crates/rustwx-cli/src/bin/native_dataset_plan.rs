use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustwx_products::native_dataset::{
    NativeDatasetBounds, NativeDatasetBuildConfig, NativeDatasetCase, NativeDatasetShardSpec,
    NativeDatasetSource, NativeDatasetTile, plan_native_dataset,
};
use std::collections::BTreeSet;
use std::path::PathBuf;

const GOES_CORE_CHANNELS: &[&str] = &["C01", "C02", "C03", "C07", "C08", "C09", "C10", "C13"];
const GOES_ALL_CHANNELS: &[&str] = &[
    "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12", "C13",
    "C14", "C15", "C16",
];
const GOES_DERIVED_FIELDS: &[&str] = &[
    "btd_c13_c15",
    "btd_c08_c10",
    "btd_c10_c13",
    "btd_c07_c13",
    "ndiff_c02_c01",
];
const LEVEL2_CORE_PRODUCTS: &[&str] = &["reflectivity", "velocity"];
const LEVEL2_ALL_PRODUCTS: &[&str] = &[
    "reflectivity",
    "velocity",
    "spectrum_width",
    "zdr",
    "cc",
    "phi",
    "kdp",
    "hca",
    "srv",
    "vil",
    "echo_tops",
];

#[derive(Debug, Parser)]
#[command(
    name = "native-dataset-plan",
    about = "Write a Rust-native multisource dataset orchestration plan"
)]
struct Args {
    #[arg(long, default_value = "rustwx_hrrr_multisource_v1")]
    dataset_name: String,
    #[arg(
        long = "case",
        value_name = "ID,START_UTC,HOURS",
        default_value = "20240506_ok_ks,2024-05-06T12:00:00Z,24"
    )]
    cases: Vec<String>,
    #[arg(
        long,
        value_name = "WEST,EAST,SOUTH,NORTH,ROWS,COLS",
        allow_hyphen_values = true,
        help = "Generate a regular tile grid. Example: -104,-88,30,40,4,6"
    )]
    tile_grid: Option<String>,
    #[arg(
        long = "tile",
        value_name = "ID,LAT,LON,WEST,EAST,SOUTH,NORTH[,RADAR]",
        allow_hyphen_values = true,
        help = "Add an explicit tile; may be repeated"
    )]
    tiles: Vec<String>,
    #[arg(long, default_value_t = 0)]
    shard_index: usize,
    #[arg(long, default_value_t = 1)]
    shard_count: usize,
    #[arg(long, default_value_t = 512)]
    grid_size: u16,
    #[arg(long, default_value_t = 3)]
    history_steps: u16,
    #[arg(long, default_value_t = 1)]
    forecast_step_frames: u16,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "t2m,d2m,u10,v10,cape,cin,refc,mslp,terrain,pwat",
        help = "Comma-separated HRRR native dataset fields"
    )]
    hrrr_fields: Vec<String>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "refc,llz,prate",
        help = "Comma-separated MRMS fields/products"
    )]
    mrms_fields: Vec<String>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "C01,C02,C03,C07,C08,C09,C10,C13",
        help = "Comma-separated GOES ABI channels. Supports core, all, and ranges like C01-C16"
    )]
    goes_channels: Vec<String>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "",
        help = "Comma-separated derived GOES fields. Supports all. Current derived fields: btd_c13_c15,btd_c08_c10,btd_c10_c13,btd_c07_c13,ndiff_c02_c01"
    )]
    goes_derived: Vec<String>,
    #[arg(
        long,
        default_value = "ABI-L2-MCMIPC",
        help = "GOES ABI multichannel product family: ABI-L2-MCMIPC, ABI-L2-MCMIPF, ABI-L2-MCMIPM, ABI-L2-MCMIPM1, or ABI-L2-MCMIPM2"
    )]
    goes_product_family: String,
    #[arg(
        long,
        help = "Shortcut for GOES multichannel sector: conus, full_disk, meso1, or meso2"
    )]
    goes_sector: Option<String>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "reflectivity,velocity",
        help = "Comma-separated NEXRAD Level-II products. Supports core or all."
    )]
    level2_products: Vec<String>,
    #[arg(long, default_value = "target/native_dataset_plan/dataset_plan.json")]
    out: PathBuf,
    #[arg(long, default_value_t = false)]
    print: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cases = args
        .cases
        .iter()
        .map(|value| parse_case(value))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut tiles = Vec::new();
    if let Some(grid) = args.tile_grid.as_deref() {
        tiles.extend(parse_tile_grid(grid)?);
    }
    for tile in &args.tiles {
        tiles.push(parse_tile(tile)?);
    }
    if tiles.is_empty() {
        bail!("provide --tile-grid or at least one --tile");
    }

    let mut config = NativeDatasetBuildConfig::hrrr_multisource_v1(args.dataset_name, cases, tiles);
    config.grid_size = args.grid_size;
    config.history_steps = args.history_steps;
    config.forecast_step_frames = args.forecast_step_frames;
    let goes_product_family =
        resolve_goes_product_family(&args.goes_product_family, args.goes_sector.as_deref())?;
    config.sources = vec![
        NativeDatasetSource::hrrr_surface(clean_list(args.hrrr_fields)),
        NativeDatasetSource::mrms(clean_list(args.mrms_fields)),
        NativeDatasetSource::goes_abi_product(goes_product_family, {
            let mut fields = expand_goes_channels(args.goes_channels)?;
            fields.extend(expand_named_preset_list(
                args.goes_derived,
                &[],
                GOES_DERIVED_FIELDS,
            ));
            dedupe_preserve_order(fields)
        }),
        NativeDatasetSource::nexrad_level2_products(expand_named_preset_list(
            args.level2_products,
            LEVEL2_CORE_PRODUCTS,
            LEVEL2_ALL_PRODUCTS,
        )),
    ];
    let shard = NativeDatasetShardSpec::new(args.shard_index, args.shard_count)
        .map_err(anyhow::Error::msg)?;
    let plan = plan_native_dataset(config, shard).map_err(anyhow::Error::msg)?;
    plan.write_json(&args.out)
        .map_err(|err| anyhow::anyhow!("{err}"))?;
    if args.print {
        println!("{}", serde_json::to_string_pretty(&plan)?);
    } else {
        println!(
            "{}",
            serde_json::json!({
                "plan": args.out,
                "schema_version": plan.schema_version,
                "dataset_name": plan.dataset_name,
                "shard_index": plan.shard.shard_index,
                "shard_count": plan.shard.shard_count,
                "tiles": plan.shard.tiles.len(),
                "frame_jobs": plan.expected_frame_jobs,
                "sample_windows": plan.expected_samples,
                "sources": plan.required_source_ids().into_iter().collect::<Vec<_>>(),
            })
        );
    }
    Ok(())
}

fn clean_list(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect()
}

fn expand_named_preset_list(values: Vec<String>, core: &[&str], all: &[&str]) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for raw in values {
        let value = raw.trim();
        if value.is_empty() {
            continue;
        }
        let expanded = match value.to_ascii_lowercase().as_str() {
            "core" | "default" => core
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>(),
            "all" | "*" => all
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>(),
            _ => vec![value.to_string()],
        };
        for item in expanded {
            let key = item.to_ascii_lowercase();
            if seen.insert(key) {
                out.push(item);
            }
        }
    }
    out
}

fn dedupe_preserve_order(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let key = value.to_ascii_lowercase();
        if seen.insert(key) {
            out.push(value);
        }
    }
    out
}

fn expand_goes_channels(values: Vec<String>) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for raw in values {
        let value = raw.trim();
        if value.is_empty() {
            continue;
        }
        let expanded = match value.to_ascii_uppercase().as_str() {
            "CORE" | "DEFAULT" => GOES_CORE_CHANNELS
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>(),
            "ALL" | "*" => GOES_ALL_CHANNELS
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>(),
            _ => expand_goes_channel_range(value)?,
        };
        for item in expanded {
            let key = item.to_ascii_uppercase();
            if seen.insert(key.clone()) {
                out.push(key);
            }
        }
    }
    Ok(out)
}

fn expand_goes_channel_range(value: &str) -> anyhow::Result<Vec<String>> {
    let upper = value.trim().to_ascii_uppercase();
    let Some((start, end)) = upper.split_once('-') else {
        return Ok(vec![upper]);
    };
    let start = parse_goes_channel_number(start)?;
    let end = parse_goes_channel_number(end)?;
    if start > end || start == 0 || end > 16 {
        bail!("GOES channel range must be C01-C16 with start <= end: {value}");
    }
    Ok((start..=end)
        .map(|channel| format!("C{channel:02}"))
        .collect())
}

fn parse_goes_channel_number(value: &str) -> anyhow::Result<u8> {
    let trimmed = value.trim().strip_prefix('C').unwrap_or(value.trim());
    trimmed
        .parse::<u8>()
        .with_context(|| format!("invalid GOES channel: {value}"))
}

fn parse_case(value: &str) -> anyhow::Result<NativeDatasetCase> {
    let parts = value.split(',').collect::<Vec<_>>();
    if parts.len() != 3 {
        bail!("case must be ID,START_UTC,HOURS: {value}");
    }
    let start = DateTime::parse_from_rfc3339(parts[1])
        .with_context(|| format!("invalid case start time: {}", parts[1]))?
        .with_timezone(&Utc);
    let hours = parts[2]
        .parse::<u16>()
        .with_context(|| format!("invalid case hours: {}", parts[2]))?;
    Ok(NativeDatasetCase::new(parts[0], start, hours))
}

fn parse_tile_grid(value: &str) -> anyhow::Result<Vec<NativeDatasetTile>> {
    let parts = value.split(',').collect::<Vec<_>>();
    if parts.len() != 6 {
        bail!("tile grid must be WEST,EAST,SOUTH,NORTH,ROWS,COLS: {value}");
    }
    let west = parse_f64(parts[0], "west")?;
    let east = parse_f64(parts[1], "east")?;
    let south = parse_f64(parts[2], "south")?;
    let north = parse_f64(parts[3], "north")?;
    let rows = parts[4]
        .parse::<usize>()
        .context("invalid tile grid rows")?;
    let cols = parts[5]
        .parse::<usize>()
        .context("invalid tile grid cols")?;
    if rows == 0 || cols == 0 {
        bail!("tile grid rows and cols must be >= 1");
    }
    if !(west < east && south < north) {
        bail!("tile grid bounds must satisfy west < east and south < north");
    }
    let dlat = (north - south) / rows as f64;
    let dlon = (east - west) / cols as f64;
    let mut tiles = Vec::with_capacity(rows * cols);
    for row in 0..rows {
        for col in 0..cols {
            let tile_south = south + row as f64 * dlat;
            let tile_north = tile_south + dlat;
            let tile_west = west + col as f64 * dlon;
            let tile_east = tile_west + dlon;
            tiles.push(NativeDatasetTile::new(
                format!("r{row:02}-c{col:02}"),
                0.5 * (tile_south + tile_north),
                0.5 * (tile_west + tile_east),
                NativeDatasetBounds::new(tile_west, tile_east, tile_south, tile_north),
            ));
        }
    }
    Ok(tiles)
}

fn parse_tile(value: &str) -> anyhow::Result<NativeDatasetTile> {
    let parts = value.split(',').collect::<Vec<_>>();
    if !(7..=8).contains(&parts.len()) {
        bail!("tile must be ID,LAT,LON,WEST,EAST,SOUTH,NORTH[,RADAR]: {value}");
    }
    let mut tile = NativeDatasetTile::new(
        parts[0],
        parse_f64(parts[1], "tile lat")?,
        parse_f64(parts[2], "tile lon")?,
        NativeDatasetBounds::new(
            parse_f64(parts[3], "tile west")?,
            parse_f64(parts[4], "tile east")?,
            parse_f64(parts[5], "tile south")?,
            parse_f64(parts[6], "tile north")?,
        ),
    );
    if let Some(site) = parts.get(7).filter(|site| !site.trim().is_empty()) {
        tile = tile.with_radar_site(*site);
    }
    Ok(tile)
}

fn parse_f64(value: &str, label: &str) -> anyhow::Result<f64> {
    value
        .parse::<f64>()
        .with_context(|| format!("invalid {label}: {value}"))
}

fn resolve_goes_product_family(
    product_family: &str,
    sector: Option<&str>,
) -> anyhow::Result<String> {
    if let Some(sector) = sector.map(str::trim).filter(|value| !value.is_empty()) {
        let normalized = sector
            .to_ascii_lowercase()
            .replace('-', "_")
            .replace(' ', "_");
        let suffix = match normalized.as_str() {
            "conus" | "continental_us" | "continental_united_states" | "c" => "C",
            "full" | "full_disk" | "fulldisk" | "full_disc" | "fulldisc" | "fd" | "f" => "F",
            "meso" | "mesoscale" => "M",
            "meso1" | "mesoscale1" | "mesoscale_1" | "m1" => "M1",
            "meso2" | "mesoscale2" | "mesoscale_2" | "m2" => "M2",
            _ => bail!(
                "unsupported GOES sector '{sector}', expected conus, full_disk, meso1, or meso2"
            ),
        };
        return Ok(format!("ABI-L2-MCMIP{suffix}"));
    }

    let trimmed = product_family.trim();
    if trimmed.is_empty() {
        bail!("GOES product family cannot be empty");
    }
    Ok(trimmed.to_ascii_uppercase())
}
