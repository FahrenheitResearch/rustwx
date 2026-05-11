use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use rustwx_products::agent_evidence::{
    RecoverySample, TeleconnectionIndex, TimedGeoPoint, WindVector, agent_evidence_capabilities,
    cold_pool_recovery, compute_feature_translation, decompose_ehi, effective_fixed_stp,
    line_relative_shear, parse_metar_line, parse_rmm_table, parse_sounding_text,
    parse_spc_storm_reports_csv, parse_teleconnection_table,
};

#[derive(Debug, Parser)]
#[command(
    name = "agent-evidence",
    about = "All-Rust agent evidence ingest and severe-weather diagnostics"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Emit the all-Rust evidence capability catalog.
    Catalog,
    /// Parse a daily teleconnection index table from a local text/CSV file.
    Teleconnection(ParseTeleconnectionArgs),
    /// Parse an RMM/MJO year-month-day table from a local text/CSV file.
    Rmm(ParseFileArgs),
    /// Parse an SPC-style storm report CSV file.
    SpcReports(ParseFileArgs),
    /// Parse fixed-width observed sounding text.
    Sounding(ParseFileArgs),
    /// Parse one raw METAR line.
    Metar(MetarArgs),
    /// Decompose layer shear relative to a storm line or boundary.
    LineShear(LineShearArgs),
    /// Compute feature translation from JSON point samples.
    JetTranslation(JetTranslationArgs),
    /// Decompose EHI into CAPE and SRH terms.
    Ehi(EhiArgs),
    /// Compute a fixed-layer STP-style term breakdown.
    Stp(StpArgs),
    /// Compare before/after cold-pool recovery samples.
    ColdPoolRecovery(ColdPoolRecoveryArgs),
}

#[derive(Debug, Args)]
struct ParseFileArgs {
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug, Args)]
struct ParseTeleconnectionArgs {
    #[arg(long)]
    index: String,
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug, Args)]
struct MetarArgs {
    #[arg(long)]
    raw: String,
}

#[derive(Debug, Args)]
struct LineShearArgs {
    #[arg(long)]
    line_azimuth_deg: f64,
    #[arg(long)]
    bottom_u_ms: f64,
    #[arg(long)]
    bottom_v_ms: f64,
    #[arg(long)]
    top_u_ms: f64,
    #[arg(long)]
    top_v_ms: f64,
}

#[derive(Debug, Args)]
struct JetTranslationArgs {
    /// JSON array of {time_epoch_seconds,lat,lon} feature points.
    #[arg(long)]
    points_json: String,
}

#[derive(Debug, Args)]
struct EhiArgs {
    #[arg(long)]
    cape_j_kg: f64,
    #[arg(long)]
    srh_m2_s2: f64,
}

#[derive(Debug, Args)]
struct StpArgs {
    #[arg(long)]
    mlcape_j_kg: f64,
    #[arg(long)]
    srh01_m2_s2: f64,
    #[arg(long)]
    bulk06_ms: f64,
    #[arg(long)]
    mllcl_m: f64,
    #[arg(long)]
    mlcin_j_kg: f64,
}

#[derive(Debug, Args)]
struct ColdPoolRecoveryArgs {
    #[arg(long)]
    before_json: String,
    #[arg(long)]
    after_json: String,
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
        Command::Catalog => print_json(&agent_evidence_capabilities())?,
        Command::Teleconnection(args) => {
            let content = fs::read_to_string(args.input)?;
            let index = TeleconnectionIndex::parse(&args.index);
            print_json(&parse_teleconnection_table(index, &content)?)?;
        }
        Command::Rmm(args) => {
            let content = fs::read_to_string(args.input)?;
            print_json(&parse_rmm_table(&content)?)?;
        }
        Command::SpcReports(args) => {
            let content = fs::read_to_string(args.input)?;
            print_json(&parse_spc_storm_reports_csv(&content)?)?;
        }
        Command::Sounding(args) => {
            let content = fs::read_to_string(args.input)?;
            print_json(&parse_sounding_text(&content)?)?;
        }
        Command::Metar(args) => print_json(&parse_metar_line(&args.raw)?)?,
        Command::LineShear(args) => print_json(&line_relative_shear(
            args.line_azimuth_deg,
            WindVector {
                u_ms: args.bottom_u_ms,
                v_ms: args.bottom_v_ms,
            },
            WindVector {
                u_ms: args.top_u_ms,
                v_ms: args.top_v_ms,
            },
        ))?,
        Command::JetTranslation(args) => {
            let points = serde_json::from_str::<Vec<TimedGeoPoint>>(&args.points_json)?;
            print_json(&compute_feature_translation(&points)?)?;
        }
        Command::Ehi(args) => print_json(&decompose_ehi(args.cape_j_kg, args.srh_m2_s2))?,
        Command::Stp(args) => print_json(&effective_fixed_stp(
            args.mlcape_j_kg,
            args.srh01_m2_s2,
            args.bulk06_ms,
            args.mllcl_m,
            args.mlcin_j_kg,
        ))?,
        Command::ColdPoolRecovery(args) => {
            let before = serde_json::from_str::<RecoverySample>(&args.before_json)?;
            let after = serde_json::from_str::<RecoverySample>(&args.after_json)?;
            print_json(&cold_pool_recovery(before, after))?;
        }
    }
    Ok(())
}

fn print_json(value: &impl serde::Serialize) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
