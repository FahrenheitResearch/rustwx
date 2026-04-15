use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_calc::{
    EcapeVolumeInputs, GridShape, SupportedSevereFields, SurfaceInputs, VolumeShape,
    compute_supported_severe_fields,
};
use rustwx_io::artifact_cache_dir;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::hrrr::{
    HrrrPressureFields, HrrrSurfaceFields, PRESSURE_PATTERNS, SURFACE_PATTERNS, Solar07PanelField,
    Solar07PanelHeader, Solar07PanelLayout, broadcast_levels_pa, build_projected_map,
    decode_cache_path, fetch_hrrr_subset, load_or_decode_pressure, load_or_decode_surface,
    render_two_by_four_solar07_panel, resolve_hrrr_run,
};
use rustwx_render::Solar07Product;
use serde_json::json;

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-severe-proof",
    about = "Generate a RustWX HRRR severe proof panel from supported fixed-depth diagnostics"
)]
struct Args {
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: rustwx_core::SourceId,
    #[arg(long, value_enum, default_value_t = RegionPreset::Midwest)]
    region: RegionPreset,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

#[derive(Debug, Clone)]
struct Timing {
    fetch_surface_ms: u128,
    fetch_pressure_ms: u128,
    decode_surface_ms: u128,
    decode_pressure_ms: u128,
    project_ms: u128,
    compute_ms: u128,
    render_ms: u128,
    total_ms: u128,
    fetch_surface_cache_hit: bool,
    fetch_pressure_cache_hit: bool,
    decode_surface_cache_hit: bool,
    decode_pressure_cache_hit: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    fs::create_dir_all(&args.out_dir)?;
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    let total_start = Instant::now();
    let latest = resolve_hrrr_run(&args.date, args.cycle, args.source)?;
    let cycle = latest.cycle.hour_utc;

    let fetch_surface_start = Instant::now();
    let surface_subset = fetch_hrrr_subset(
        latest.cycle.clone(),
        args.forecast_hour,
        args.source,
        "sfc",
        SURFACE_PATTERNS,
        &cache_root,
        !args.no_cache,
    )?;
    let fetch_surface_ms = fetch_surface_start.elapsed().as_millis();

    let fetch_pressure_start = Instant::now();
    let pressure_subset = fetch_hrrr_subset(
        latest.cycle.clone(),
        args.forecast_hour,
        args.source,
        "prs",
        PRESSURE_PATTERNS,
        &cache_root,
        !args.no_cache,
    )?;
    let fetch_pressure_ms = fetch_pressure_start.elapsed().as_millis();

    let decode_surface_start = Instant::now();
    let surface_decode = load_or_decode_surface(
        &decode_cache_path(&cache_root, &surface_subset.request, "surface"),
        &surface_subset.bytes,
        !args.no_cache,
    )?;
    let decode_surface_ms = decode_surface_start.elapsed().as_millis();

    let decode_pressure_start = Instant::now();
    let pressure_decode = load_or_decode_pressure(
        &decode_cache_path(&cache_root, &pressure_subset.request, "pressure"),
        &pressure_subset.bytes,
        surface_decode.value.nx,
        surface_decode.value.ny,
        !args.no_cache,
    )?;
    let decode_pressure_ms = decode_pressure_start.elapsed().as_millis();

    let layout = Solar07PanelLayout {
        top_padding: 86,
        ..Default::default()
    };
    let project_start = Instant::now();
    let projected = build_projected_map(
        &surface_decode.value,
        args.region.bounds(),
        layout.target_aspect_ratio(),
    )?;
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let fields = compute_panel_fields(&surface_decode.value, &pressure_decode.value)?;
    let compute_ms = compute_start.elapsed().as_millis();

    let render_start = Instant::now();
    let panel_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_{}_severe_proof_panel.png",
        args.date,
        cycle,
        args.forecast_hour,
        args.region.slug()
    ));
    let header = Solar07PanelHeader::new(format!(
        "HRRR Severe Proof Panel  Run: {} {:02}:00 UTC  Forecast Hour: F{:02}",
        args.date, cycle, args.forecast_hour
    ))
    .with_subtitle_line(
        "STP is fixed-layer only: sbCAPE + sbLCL + 0-1 km SRH + 0-6 km bulk shear.",
    )
    .with_subtitle_line(
        "SCP and EHI are fixed-depth proxies here: SCP uses muCAPE + 0-3 km SRH + 0-6 km shear. EHI uses sbCAPE + 0-1 km SRH. Effective-layer derivation is not wired yet.",
    );
    render_two_by_four_solar07_panel(
        &panel_path,
        &surface_decode.value.core_grid()?,
        &projected,
        &fields,
        &header,
        layout,
    )?;
    let render_ms = render_start.elapsed().as_millis();

    let timing = Timing {
        fetch_surface_ms,
        fetch_pressure_ms,
        decode_surface_ms,
        decode_pressure_ms,
        project_ms,
        compute_ms,
        render_ms,
        total_ms: total_start.elapsed().as_millis(),
        fetch_surface_cache_hit: surface_subset.fetched.cache_hit,
        fetch_pressure_cache_hit: pressure_subset.fetched.cache_hit,
        decode_surface_cache_hit: surface_decode.cache_hit,
        decode_pressure_cache_hit: pressure_decode.cache_hit,
    };
    let timing_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_{}_severe_proof_timing.json",
        args.date,
        cycle,
        args.forecast_hour,
        args.region.slug()
    ));
    fs::write(
        &timing_path,
        serde_json::to_vec_pretty(&json!({
            "date": args.date,
            "cycle_utc": cycle,
            "forecast_hour": args.forecast_hour,
            "region": args.region.slug(),
            "source": args.source.as_str(),
            "assumptions": {
                "stp": "fixed-layer Thompson-style STP using sbCAPE, sbLCL, 0-1 km SRH, and 0-6 km bulk shear",
                "scp": "fixed-depth proxy using muCAPE, 0-3 km SRH, and 0-6 km bulk shear",
                "ehi": "fixed-depth proxy using sbCAPE and 0-1 km SRH",
                "effective_layer": "not derived in this proof path"
            },
            "cache": {
                "root": cache_root,
                "surface_fetch_hit": timing.fetch_surface_cache_hit,
                "pressure_fetch_hit": timing.fetch_pressure_cache_hit,
                "surface_decode_hit": timing.decode_surface_cache_hit,
                "pressure_decode_hit": timing.decode_pressure_cache_hit,
                "surface_fetch_bytes_path": surface_subset.fetched.bytes_path,
                "surface_fetch_meta_path": surface_subset.fetched.metadata_path,
                "pressure_fetch_bytes_path": pressure_subset.fetched.bytes_path,
                "pressure_fetch_meta_path": pressure_subset.fetched.metadata_path,
                "surface_decode_path": surface_decode.path,
                "pressure_decode_path": pressure_decode.path,
                "surface_decode_artifact_root": artifact_cache_dir(&cache_root, &surface_subset.request),
                "pressure_decode_artifact_root": artifact_cache_dir(&cache_root, &pressure_subset.request),
            },
            "panel_path": panel_path,
            "timing_ms": {
                "fetch_surface": timing.fetch_surface_ms,
                "fetch_pressure": timing.fetch_pressure_ms,
                "decode_surface": timing.decode_surface_ms,
                "decode_pressure": timing.decode_pressure_ms,
                "project": timing.project_ms,
                "compute": timing.compute_ms,
                "render": timing.render_ms,
                "total": timing.total_ms,
            }
        }))?,
    )?;

    println!("{}", panel_path.display());
    println!("{}", timing_path.display());
    Ok(())
}

fn compute_panel_fields(
    surface: &HrrrSurfaceFields,
    pressure: &HrrrPressureFields,
) -> Result<Vec<Solar07PanelField>, Box<dyn std::error::Error>> {
    let grid = GridShape::new(surface.nx, surface.ny)?;
    let shape = VolumeShape::new(grid, pressure.pressure_levels_hpa.len())?;

    let mut height_agl_3d = pressure
        .gh_m_3d
        .iter()
        .enumerate()
        .map(|(idx, &value)| {
            let ij = idx % grid.len();
            (value - surface.orog_m[ij]).max(0.0)
        })
        .collect::<Vec<_>>();
    for k in 1..shape.nz {
        let level_offset = k * grid.len();
        let prev_offset = (k - 1) * grid.len();
        for ij in 0..grid.len() {
            let min_height = height_agl_3d[prev_offset + ij] + 1.0;
            if height_agl_3d[level_offset + ij] < min_height {
                height_agl_3d[level_offset + ij] = min_height;
            }
        }
    }

    let pressure_3d_pa = broadcast_levels_pa(&pressure.pressure_levels_hpa, grid.len());
    let fields = compute_supported_severe_fields(
        grid,
        EcapeVolumeInputs {
            pressure_pa: &pressure_3d_pa,
            temperature_c: &pressure.temperature_c_3d,
            qvapor_kgkg: &pressure.qvapor_kgkg_3d,
            height_agl_m: &height_agl_3d,
            u_ms: &pressure.u_ms_3d,
            v_ms: &pressure.v_ms_3d,
            nz: shape.nz,
        },
        SurfaceInputs {
            psfc_pa: &surface.psfc_pa,
            t2_k: &surface.t2_k,
            q2_kgkg: &surface.q2_kgkg,
            u10_ms: &surface.u10_ms,
            v10_ms: &surface.v10_ms,
        },
    )?;
    Ok(panel_fields_from_supported(fields))
}

fn panel_fields_from_supported(fields: SupportedSevereFields) -> Vec<Solar07PanelField> {
    vec![
        Solar07PanelField::new(Solar07Product::Sbcape, "J/kg", fields.sbcape_jkg),
        Solar07PanelField::new(Solar07Product::Mlcin, "J/kg", fields.mlcin_jkg),
        Solar07PanelField::new(Solar07Product::Mucape, "J/kg", fields.mucape_jkg),
        Solar07PanelField::new(Solar07Product::Srh01km, "m^2/s^2", fields.srh_01km_m2s2),
        Solar07PanelField::new(Solar07Product::Srh03km, "m^2/s^2", fields.srh_03km_m2s2),
        Solar07PanelField::new(Solar07Product::StpFixed, "dimensionless", fields.stp_fixed),
        Solar07PanelField::new(
            Solar07Product::Scp,
            "dimensionless",
            fields.scp_mu_03km_06km_proxy,
        )
        .with_title_override("SCP (MU / 0-3 KM / 0-6 KM PROXY)"),
        Solar07PanelField::new(
            Solar07Product::Ehi,
            "dimensionless",
            fields.ehi_sb_01km_proxy,
        )
        .with_title_override("EHI (SB / 0-1 KM PROXY)"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severe_panel_products_keep_fixed_and_proxy_labels_explicit() {
        let fields = panel_fields_from_supported(SupportedSevereFields {
            sbcape_jkg: vec![1.0],
            mlcin_jkg: vec![-25.0],
            mucape_jkg: vec![2.0],
            srh_01km_m2s2: vec![100.0],
            srh_03km_m2s2: vec![200.0],
            shear_06km_ms: vec![20.0],
            stp_fixed: vec![1.5],
            scp_mu_03km_06km_proxy: vec![5.0],
            ehi_sb_01km_proxy: vec![2.0],
        });

        assert_eq!(fields.len(), 8);
        assert_eq!(fields[5].product, Solar07Product::StpFixed);
        assert_eq!(
            fields[6].title_override.as_deref(),
            Some("SCP (MU / 0-3 KM / 0-6 KM PROXY)")
        );
        assert_eq!(
            fields[7].title_override.as_deref(),
            Some("EHI (SB / 0-1 KM PROXY)")
        );
    }
}
