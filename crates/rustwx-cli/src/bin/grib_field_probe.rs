use clap::Parser;
use grib_core::grib2::{Grib2File, unpack_message};
use std::path::PathBuf;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    grib_path: PathBuf,

    #[arg(long)]
    message: Option<usize>,

    #[arg(long, default_value_t = 40)]
    list_limit: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let bytes = std::fs::read(&args.grib_path)?;
    let grib = Grib2File::from_bytes(&bytes)?;
    println!("messages={}", grib.messages.len());

    for (idx, message) in grib.messages.iter().enumerate().take(args.list_limit) {
        println!(
            "#{idx:03} disc={} cat={} num={} level_type={} level_value={} pdt={} drt={} grid_template={} nx={} ny={} scan_mode={} scan={}",
            message.discipline,
            message.product.parameter_category,
            message.product.parameter_number,
            message.product.level_type,
            message.product.level_value,
            message.product.template,
            message.data_rep.template,
            message.grid.template,
            message.grid.nx,
            message.grid.ny,
            message.grid.scan_mode,
            scan_bits(message.grid.scan_mode),
        );
        println!(
            "      lat1={} lon1={} lat2={} lon2={} lov={} dx={} dy={} latin1={} latin2={}",
            message.grid.lat1,
            message.grid.lon1,
            message.grid.lat2,
            message.grid.lon2,
            message.grid.lov,
            message.grid.dx,
            message.grid.dy,
            message.grid.latin1,
            message.grid.latin2,
        );
    }

    if let Some(index) = args.message {
        let message = grib
            .messages
            .get(index)
            .ok_or_else(|| format!("message index {index} out of range"))?;
        let values = unpack_message(message)?;
        let nx = message.grid.nx as usize;
        let ny = message.grid.ny as usize;
        println!(
            "selected #{index}: values={} expected={} nx={} ny={} scan_mode={} scan={}",
            values.len(),
            nx * ny,
            nx,
            ny,
            message.grid.scan_mode,
            scan_bits(message.grid.scan_mode),
        );
        println!(
            "all finite={} nan={} minmax={:?}",
            values.iter().filter(|value| value.is_finite()).count(),
            values.iter().filter(|value| value.is_nan()).count(),
            minmax(&values),
        );
        for row in sample_rows(ny) {
            let start = row * nx;
            let end = (start + nx).min(values.len());
            let row_values = &values[start..end];
            println!(
                "row={row} finite={} nan={} minmax={:?} first12={:?}",
                row_values.iter().filter(|value| value.is_finite()).count(),
                row_values.iter().filter(|value| value.is_nan()).count(),
                minmax(row_values),
                &row_values[..row_values.len().min(12)],
            );
        }
    }

    Ok(())
}

fn scan_bits(scan_mode: u8) -> String {
    format!(
        "i_neg={} j_pos={} j_consecutive={} adjacent_rows_opposite={} reserved=0x{:02x}",
        scan_mode & 0x80 != 0,
        scan_mode & 0x40 != 0,
        scan_mode & 0x20 != 0,
        scan_mode & 0x10 != 0,
        scan_mode & 0x0f,
    )
}

fn minmax(values: &[f64]) -> Option<(f64, f64)> {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut any = false;
    for value in values.iter().copied().filter(|value| value.is_finite()) {
        min = min.min(value);
        max = max.max(value);
        any = true;
    }
    any.then_some((min, max))
}

fn sample_rows(ny: usize) -> Vec<usize> {
    let mut rows = vec![
        0,
        1,
        2,
        ny / 4,
        ny / 2,
        ny.saturating_mul(3) / 4,
        ny.saturating_sub(3),
        ny.saturating_sub(2),
        ny.saturating_sub(1),
    ];
    rows.sort_unstable();
    rows.dedup();
    rows.into_iter().filter(|row| *row < ny).collect()
}
