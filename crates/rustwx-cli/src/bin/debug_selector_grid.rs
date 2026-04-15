use clap::Parser;
use grib_core::grib2::Grib2File;
use rustwx_core::{CanonicalField, CycleSpec, FieldSelector, ModelId, ModelRunRequest, SourceId};
use rustwx_io::{FetchRequest, extract_field_from_grib2, fetch_bytes};
use rustwx_models::latest_available_run;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    model: ModelId,
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: u8,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long, default_value_t = 500)]
    level_hpa: u16,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let latest = rustwx_models::LatestRun {
        model: args.model,
        cycle: CycleSpec::new(&args.date, args.cycle)?,
        source: args
            .source
            .or_else(|| {
                latest_available_run(args.model, args.source, &args.date)
                    .ok()
                    .map(|r| r.source)
            })
            .unwrap_or(rustwx_models::model_summary(args.model).sources[0].id),
    };
    let request =
        ModelRunRequest::new(args.model, latest.cycle.clone(), args.forecast_hour, "oper")?;
    let fetch = FetchRequest {
        request,
        source_override: Some(latest.source),
        variable_patterns: Vec::new(),
    };
    let fetched = fetch_bytes(&fetch)?;
    let grib = Grib2File::from_bytes(&fetched.bytes)?;

    let selector = FieldSelector::isobaric(CanonicalField::Temperature, args.level_hpa);
    let field = extract_field_from_grib2(&grib, selector)?;
    let message = grib
        .messages
        .iter()
        .find(|message| {
            message.discipline == 0
                && message.product.parameter_category == 0
                && message.product.parameter_number == 0
                && message.product.level_type == 100
                && ((if message.product.level_value > 2_000.0 {
                    message.product.level_value / 100.0
                } else {
                    message.product.level_value
                }) - f64::from(args.level_hpa))
                .abs()
                    < 0.25
        })
        .ok_or("matching TMP message not found")?;

    println!(
        "model={} cycle={}Z source={} template={} nx={} ny={} scan_mode={} lat1={} lat2={} lon1={} lon2={}",
        args.model,
        args.cycle,
        latest.source,
        message.grid.template,
        message.grid.nx,
        message.grid.ny,
        message.grid.scan_mode,
        message.grid.lat1,
        message.grid.lat2,
        message.grid.lon1,
        message.grid.lon2
    );

    let nx = field.grid.shape.nx;
    let ny = field.grid.shape.ny;
    for row in [0usize, ny / 2, ny.saturating_sub(1)] {
        let start = row * nx;
        let end = start + nx.min(12);
        println!(
            "row {} lat {:?}",
            row,
            &field.grid.lat_deg[start..end.min(field.grid.lat_deg.len())]
        );
        println!(
            "row {} lon {:?}",
            row,
            &field.grid.lon_deg[start..end.min(field.grid.lon_deg.len())]
        );
        println!(
            "row {} values {:?}",
            row,
            &field.values[start..end.min(field.values.len())]
        );
    }

    Ok(())
}
