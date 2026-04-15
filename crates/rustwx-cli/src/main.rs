use clap::{Parser, Subcommand};
use rustwx_core::{CycleSpec, ModelId, ModelRunRequest, SourceId};
use rustwx_io::{
    FetchRequest, available_forecast_hours, fetch_bytes, fetch_bytes_with_cache, probe_sources,
};
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(name = "rustwx", about = "Rust-first weather model registry and tools")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    List,
    Show {
        model: ModelId,
    },
    Url {
        model: ModelId,
        date: String,
        hour: u8,
        forecast_hour: u16,
        #[arg(default_value = "default")]
        product: String,
    },
    Latest {
        model: ModelId,
        date: String,
        #[arg(long)]
        source: Option<SourceId>,
    },
    Hours {
        model: ModelId,
        date: String,
        hour: u8,
        #[arg(default_value = "default")]
        product: String,
        #[arg(long)]
        source: Option<SourceId>,
    },
    Probe {
        model: ModelId,
        date: String,
        hour: u8,
        forecast_hour: u16,
        #[arg(default_value = "default")]
        product: String,
        #[arg(long)]
        source: Option<SourceId>,
        #[arg(long = "var")]
        variable_patterns: Vec<String>,
    },
    Fetch {
        model: ModelId,
        date: String,
        hour: u8,
        forecast_hour: u16,
        #[arg(default_value = "default")]
        product: String,
        #[arg(long)]
        source: Option<SourceId>,
        #[arg(long = "var")]
        variable_patterns: Vec<String>,
        #[arg(long)]
        output: Option<String>,
        #[arg(long)]
        cache_dir: Option<String>,
        #[arg(long, default_value_t = false)]
        no_cache: bool,
    },
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
        Command::List => {
            for model in rustwx_models::built_in_models() {
                println!("{model}");
            }
            Ok(())
        }
        Command::Show { model } => {
            let summary = rustwx_models::model_summary(model);
            println!("{}", serde_json::to_string_pretty(summary)?);
            Ok(())
        }
        Command::Url {
            model,
            date,
            hour,
            forecast_hour,
            product,
        } => {
            let summary = rustwx_models::model_summary(model);
            let product = if product == "default" {
                summary.default_product.to_string()
            } else {
                product
            };
            let cycle = CycleSpec::new(date, hour)?;
            let request = ModelRunRequest::new(model, cycle, forecast_hour, product)?;
            let urls = rustwx_models::resolve_urls(&request)?;
            println!("{}", serde_json::to_string_pretty(&urls)?);
            Ok(())
        }
        Command::Latest {
            model,
            date,
            source,
        } => {
            let latest = rustwx_models::latest_available_run(model, source, &date)?;
            println!("{}", serde_json::to_string_pretty(&latest)?);
            Ok(())
        }
        Command::Hours {
            model,
            date,
            hour,
            product,
            source,
        } => {
            let product = resolve_product(model, product);
            let hours = available_forecast_hours(model, &date, hour, &product, source)?;
            println!("{}", serde_json::to_string_pretty(&hours)?);
            Ok(())
        }
        Command::Probe {
            model,
            date,
            hour,
            forecast_hour,
            product,
            source,
            variable_patterns,
        } => {
            let request = build_fetch_request(
                model,
                date,
                hour,
                forecast_hour,
                product,
                source,
                variable_patterns,
            )?;
            let results = probe_sources(&request)?;
            println!("{}", serde_json::to_string_pretty(&results)?);
            Ok(())
        }
        Command::Fetch {
            model,
            date,
            hour,
            forecast_hour,
            product,
            source,
            variable_patterns,
            output,
            cache_dir,
            no_cache,
        } => {
            let request = build_fetch_request(
                model,
                date,
                hour,
                forecast_hour,
                product,
                source,
                variable_patterns,
            )?;
            let cached = match cache_dir {
                Some(cache_dir) if !no_cache => Some(fetch_bytes_with_cache(
                    &request,
                    std::path::Path::new(&cache_dir),
                    true,
                )?),
                _ => None,
            };
            let result = cached
                .as_ref()
                .map(|cached| &cached.result)
                .cloned()
                .unwrap_or(fetch_bytes(&request)?);

            if let Some(output) = output {
                std::fs::write(&output, &result.bytes)?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "source": result.source,
                        "url": result.url,
                        "bytes": result.bytes.len(),
                        "output": output,
                        "cache_hit": cached.as_ref().map(|item| item.cache_hit),
                        "cache_path": cached.as_ref().map(|item| item.bytes_path.display().to_string()),
                    }))?
                );
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "source": result.source,
                        "url": result.url,
                        "bytes": result.bytes.len(),
                        "cache_hit": cached.as_ref().map(|item| item.cache_hit),
                        "cache_path": cached.as_ref().map(|item| item.bytes_path.display().to_string()),
                    }))?
                );
            }
            Ok(())
        }
    }
}

fn resolve_product(model: ModelId, product: String) -> String {
    if product == "default" {
        rustwx_models::model_summary(model)
            .default_product
            .to_string()
    } else {
        product
    }
}

fn build_fetch_request(
    model: ModelId,
    date: String,
    hour: u8,
    forecast_hour: u16,
    product: String,
    source: Option<SourceId>,
    variable_patterns: Vec<String>,
) -> Result<FetchRequest, Box<dyn std::error::Error>> {
    let product = resolve_product(model, product);
    let cycle = CycleSpec::new(date, hour)?;
    let request = ModelRunRequest::new(model, cycle, forecast_hour, product)?;
    Ok(FetchRequest {
        request,
        source_override: source,
        variable_patterns,
    })
}
