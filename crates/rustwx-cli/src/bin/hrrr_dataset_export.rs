use clap::{Parser, ValueEnum};

#[path = "../region.rs"]
mod region;

use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::default_proof_cache_dir;
use rustwx_products::dataset_export::{
    MlChannelPreset, MlDatasetExportRequest, MlDatasetSplit, export_model_dataset_bundle,
};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-dataset-export",
    about = "Export a wxtrain-compatible NPY dataset bundle from rustwx for HRRR and verified RRFS-A profiles"
)]
struct Cli {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,
    #[arg(long, default_value = "rustwx_hrrr_mesoconvective")]
    dataset_name: String,
    #[arg(long, default_value = "20260422")]
    date: String,
    #[arg(long, default_value_t = 7)]
    cycle: u8,
    #[arg(long = "forecast-hour", value_delimiter = ',', num_args = 1.., default_values_t = [0u16])]
    forecast_hours: Vec<u16>,
    #[arg(long, default_value = "nomads")]
    source: SourceId,
    #[arg(long, value_enum)]
    region: Option<RegionPreset>,
    #[arg(long, value_enum, default_value_t = SplitArg::Train)]
    split: SplitArg,
    #[arg(long, value_enum, default_value_t = PresetArg::MesoconvectiveV1)]
    preset: PresetArg,
    #[arg(long, default_value_t = false)]
    no_ecape: bool,
    #[arg(long, default_value = "target\\hrrr_dataset_export")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SplitArg {
    Train,
    Validation,
    Test,
}

impl From<SplitArg> for MlDatasetSplit {
    fn from(value: SplitArg) -> Self {
        match value {
            SplitArg::Train => Self::Train,
            SplitArg::Validation => Self::Validation,
            SplitArg::Test => Self::Test,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PresetArg {
    MesoconvectiveV1,
    HybridColumnV1,
}

impl From<PresetArg> for MlChannelPreset {
    fn from(value: PresetArg) -> Self {
        match value {
            PresetArg::MesoconvectiveV1 => Self::MesoconvectiveV1,
            PresetArg::HybridColumnV1 => Self::HybridColumnV1,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let cache_root = cli
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&cli.out_dir));
    let request = MlDatasetExportRequest {
        model: cli.model,
        dataset_name: cli.dataset_name,
        date_yyyymmdd: cli.date,
        cycle_utc: cli.cycle,
        forecast_hours: cli.forecast_hours,
        source: cli.source,
        split: cli.split.into(),
        out_dir: cli.out_dir,
        cache_root,
        use_cache: !cli.no_cache,
        preset: cli.preset.into(),
        include_ecape: !cli.no_ecape,
        requested_domain_id: cli.region.map(|region| region.slug().to_string()),
        crop_bounds: cli.region.map(RegionPreset::bounds),
    };
    let report = export_model_dataset_bundle(&request)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
