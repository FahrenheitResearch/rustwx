use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser;
use rustwx_core::{ModelId, ModelRunRequest};
use rustwx_cross_section::ALL_CROSS_SECTION_PRODUCTS;
use rustwx_models::{
    latest_available_run_for_products_at_forecast_hour, model_summary, resolve_urls,
};
use rustwx_products::catalog::{
    ProductCatalogEntry, ProductTargetStatus, SupportedProductsCatalog,
    build_supported_products_catalog,
};
use rustwx_products::cross_section::{
    SUPPORTED_PRESSURE_CROSS_SECTION_PRODUCTS, supports_pressure_cross_section_product,
};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-capability-inventory",
    about = "Inventory current HRRR map/cross-section capability and probe smoke-capable native files"
)]
struct Args {
    #[arg(long)]
    date: String,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "proof/inventory")]
    out_dir: PathBuf,
}

#[derive(Debug, Serialize)]
struct InventoryReport {
    model: &'static str,
    date_yyyymmdd: String,
    forecast_hour: u16,
    smoke_probe: SmokeProbeReport,
    map_inventory: MapInventoryReport,
    cross_sections: CrossSectionInventoryReport,
    wxsection_map_candidates: Vec<WxsectionMapCandidate>,
}

#[derive(Debug, Serialize)]
struct SmokeProbeReport {
    planned_product: &'static str,
    native_file_family: &'static str,
    notes: Vec<String>,
    sources: Vec<SmokeSourceProbe>,
}

#[derive(Debug, Serialize)]
struct SmokeSourceProbe {
    source: String,
    source_priority: u8,
    idx_available: bool,
    latest_cycle_yyyymmdd: Option<String>,
    latest_cycle_hour_utc: Option<u8>,
    grib_url: Option<String>,
    idx_url: Option<String>,
    availability_probe_url: Option<String>,
    available: bool,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct MapInventoryReport {
    direct: LaneInventoryReport,
    derived: LaneInventoryReport,
    heavy: LaneInventoryReport,
    windowed: LaneInventoryReport,
}

#[derive(Debug, Serialize)]
struct LaneInventoryReport {
    available_count: usize,
    blocked_count: usize,
    available: Vec<LaneProductEntry>,
    blocked: Vec<LaneProductEntry>,
}

#[derive(Debug, Serialize)]
struct LaneProductEntry {
    slug: String,
    title: String,
    render_style: Option<String>,
    source_routes: Vec<String>,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CrossSectionInventoryReport {
    declared_count: usize,
    pressure_builder_count: usize,
    declared: Vec<CrossSectionEntry>,
}

#[derive(Debug, Serialize)]
struct CrossSectionEntry {
    slug: String,
    display_name: String,
    group: String,
    units: String,
    builder_supported: bool,
}

#[derive(Debug, Serialize, Clone)]
struct WxsectionMapCandidate {
    slug: &'static str,
    title: &'static str,
    priority: &'static str,
    upstream_basis: &'static str,
    likely_inputs: &'static [&'static str],
    note: &'static str,
}

const WXSECTION_MAP_CANDIDATES: &[WxsectionMapCandidate] = &[
    WxsectionMapCandidate {
        slug: "smoke_pm25_native",
        title: "PM2.5 Smoke",
        priority: "high",
        upstream_basis: "wxsection smoke style + HRRR wrfnat MASSDEN on hybrid levels",
        likely_inputs: &[
            "wrfnat MASSDEN (disc 0 / cat 20 / num 0)",
            "hybrid pressure",
        ],
        note: "Highest-value smoke add. Enables both plan-view smoke maps and true native-level smoke cross sections.",
    },
    WxsectionMapCandidate {
        slug: "smoke_column",
        title: "Column-Integrated Smoke",
        priority: "high",
        upstream_basis: "wxsection COLMD extraction path",
        likely_inputs: &["wrfnat COLMD entire atmosphere"],
        note: "Cheaper overview smoke map if the column field is present in the native file.",
    },
    WxsectionMapCandidate {
        slug: "vpd_2m",
        title: "2m Vapor Pressure Deficit",
        priority: "high",
        upstream_basis: "wxsection fire-weather style family",
        likely_inputs: &["2m temperature", "2m RH or dewpoint"],
        note: "Useful fire-weather scalar and straightforward from current surface thermo fields.",
    },
    WxsectionMapCandidate {
        slug: "dewpoint_depression_2m",
        title: "2m Dewpoint Depression",
        priority: "medium",
        upstream_basis: "wxsection dry-layer diagnostics",
        likely_inputs: &["2m temperature", "2m dewpoint"],
        note: "Cheap diagnostic that pairs well with cloud-base / dryline style maps.",
    },
    WxsectionMapCandidate {
        slug: "fire_weather_composite",
        title: "Fire Weather Composite",
        priority: "high",
        upstream_basis: "wxsection fire_wx composite",
        likely_inputs: &["2m RH", "10m wind", "VPD"],
        note: "Public-facing fire-weather composite candidate; likely best implemented as a clean derived lane product.",
    },
    WxsectionMapCandidate {
        slug: "omega_700mb",
        title: "700mb Omega",
        priority: "medium",
        upstream_basis: "wxsection omega style",
        likely_inputs: &["pressure vertical velocity", "700mb pressure level"],
        note: "Straight synoptic add once the vertical-velocity field is wired through grib extraction.",
    },
    WxsectionMapCandidate {
        slug: "wetbulb_2m",
        title: "2m Wet-Bulb Temperature",
        priority: "medium",
        upstream_basis: "wxsection wetbulb style",
        likely_inputs: &["2m temperature", "2m RH or dewpoint"],
        note: "Strong winter-weather / fire-weather crossover field and likely cheaper than many severe diagnostics.",
    },
    WxsectionMapCandidate {
        slug: "frontogenesis_700_850mb",
        title: "700-850mb Frontogenesis",
        priority: "medium",
        upstream_basis: "wxsection frontogenesis style",
        likely_inputs: &["temperature gradient", "wind deformation/confluence"],
        note: "Good winter-weather add, but needs a defensible derived implementation rather than a placeholder.",
    },
    WxsectionMapCandidate {
        slug: "moisture_transport_850mb",
        title: "850mb Moisture Transport",
        priority: "medium",
        upstream_basis: "wxsection moisture_transport style",
        likely_inputs: &["specific humidity", "wind speed"],
        note: "Simple, meteorologist-friendly plume diagnostic once specific humidity is exposed cleanly.",
    },
    WxsectionMapCandidate {
        slug: "potential_vorticity",
        title: "Potential Vorticity",
        priority: "low",
        upstream_basis: "wxsection pv style",
        likely_inputs: &["theta", "vorticity", "pressure derivatives"],
        note: "More specialized and costlier, but valuable later for jet/tropopause work.",
    },
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    fs::create_dir_all(&args.out_dir)?;

    let report = build_inventory_report(&args)?;

    let json_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_f{:03}_capability_inventory.json",
        args.date, args.forecast_hour
    ));
    let md_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_f{:03}_capability_inventory.md",
        args.date, args.forecast_hour
    ));

    fs::write(&json_path, serde_json::to_vec_pretty(&report)?)?;
    fs::write(&md_path, render_markdown(&report))?;

    println!("wrote {}", display_path(&json_path));
    println!("wrote {}", display_path(&md_path));
    Ok(())
}

fn build_inventory_report(args: &Args) -> Result<InventoryReport, Box<dyn std::error::Error>> {
    let catalog = build_supported_products_catalog();
    Ok(InventoryReport {
        model: "hrrr",
        date_yyyymmdd: args.date.clone(),
        forecast_hour: args.forecast_hour,
        smoke_probe: build_smoke_probe(&args.date, args.forecast_hour)?,
        map_inventory: build_map_inventory(&catalog),
        cross_sections: build_cross_section_inventory(),
        wxsection_map_candidates: WXSECTION_MAP_CANDIDATES.to_vec(),
    })
}

fn build_smoke_probe(
    date_yyyymmdd: &str,
    forecast_hour: u16,
) -> Result<SmokeProbeReport, Box<dyn std::error::Error>> {
    let mut sources = Vec::new();
    for descriptor in model_summary(ModelId::Hrrr).sources {
        let availability = latest_available_run_for_products_at_forecast_hour(
            ModelId::Hrrr,
            Some(descriptor.id),
            date_yyyymmdd,
            &["nat"],
            forecast_hour,
        );
        match availability {
            Ok(latest) => {
                let request = ModelRunRequest::new(
                    ModelId::Hrrr,
                    latest.cycle.clone(),
                    forecast_hour,
                    "nat",
                )?;
                let resolved = resolve_urls(&request)?
                    .into_iter()
                    .find(|url| url.source == descriptor.id);
                sources.push(SmokeSourceProbe {
                    source: descriptor.id.as_str().to_string(),
                    source_priority: descriptor.priority,
                    idx_available: descriptor.idx_available,
                    latest_cycle_yyyymmdd: Some(latest.cycle.date_yyyymmdd.clone()),
                    latest_cycle_hour_utc: Some(latest.cycle.hour_utc),
                    grib_url: resolved.as_ref().map(|url| url.grib_url.clone()),
                    idx_url: resolved.as_ref().and_then(|url| url.idx_url.clone()),
                    availability_probe_url: resolved
                        .as_ref()
                        .map(|url| url.availability_probe_url().to_string()),
                    available: true,
                    error: None,
                });
            }
            Err(err) => {
                sources.push(SmokeSourceProbe {
                    source: descriptor.id.as_str().to_string(),
                    source_priority: descriptor.priority,
                    idx_available: descriptor.idx_available,
                    latest_cycle_yyyymmdd: None,
                    latest_cycle_hour_utc: None,
                    grib_url: None,
                    idx_url: None,
                    availability_probe_url: None,
                    available: false,
                    error: Some(err.to_string()),
                });
            }
        }
    }

    Ok(SmokeProbeReport {
        planned_product: "nat",
        native_file_family: "wrfnat",
        notes: vec![
            "rustwx-models resolves HRRR product `nat` to `wrfnat` URLs.".to_string(),
            "wxsection_ref smoke loading uses MASSDEN on hybrid levels from wrfnat, with hybrid pressure for vertical placement.".to_string(),
            "wxsection_ref also has a COLMD extraction path for column-integrated smoke.".to_string(),
        ],
        sources,
    })
}

fn build_map_inventory(catalog: &SupportedProductsCatalog) -> MapInventoryReport {
    MapInventoryReport {
        direct: build_lane_inventory(&catalog.direct),
        derived: build_lane_inventory(&catalog.derived),
        heavy: build_lane_inventory(&catalog.heavy),
        windowed: build_lane_inventory(&catalog.windowed),
    }
}

fn build_lane_inventory(entries: &[ProductCatalogEntry]) -> LaneInventoryReport {
    let mut available = Vec::new();
    let mut blocked = Vec::new();
    for entry in entries {
        let Some(hrrr) = entry
            .support
            .iter()
            .find(|target| target.model == Some(ModelId::Hrrr))
        else {
            continue;
        };
        let lane_entry = LaneProductEntry {
            slug: entry.slug.clone(),
            title: entry.title.clone(),
            render_style: entry.render_style.clone(),
            source_routes: hrrr
                .source_routes
                .iter()
                .map(|route| route.as_str().to_string())
                .collect(),
            notes: entry.notes.clone(),
        };
        match hrrr.status {
            ProductTargetStatus::Supported => available.push(lane_entry),
            ProductTargetStatus::Blocked => blocked.push(lane_entry),
        }
    }
    available.sort_by(|left, right| left.slug.cmp(&right.slug));
    blocked.sort_by(|left, right| left.slug.cmp(&right.slug));
    LaneInventoryReport {
        available_count: available.len(),
        blocked_count: blocked.len(),
        available,
        blocked,
    }
}

fn build_cross_section_inventory() -> CrossSectionInventoryReport {
    let declared = ALL_CROSS_SECTION_PRODUCTS
        .iter()
        .copied()
        .map(|product| CrossSectionEntry {
            slug: product.slug().to_string(),
            display_name: product.display_name().to_string(),
            group: product.group().display_name().to_string(),
            units: product.units().to_string(),
            builder_supported: supports_pressure_cross_section_product(product),
        })
        .collect::<Vec<_>>();
    CrossSectionInventoryReport {
        declared_count: ALL_CROSS_SECTION_PRODUCTS.len(),
        pressure_builder_count: SUPPORTED_PRESSURE_CROSS_SECTION_PRODUCTS.len(),
        declared,
    }
}

fn render_markdown(report: &InventoryReport) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# HRRR Capability Inventory\n\nDate: `{}`  Forecast Hour: `F{:03}`\n",
        report.date_yyyymmdd, report.forecast_hour
    );

    let _ = writeln!(out, "## Smoke Source Probe\n");
    let _ = writeln!(
        out,
        "- Planned product: `{}`\n- Native file family: `{}`\n",
        report.smoke_probe.planned_product, report.smoke_probe.native_file_family
    );
    for note in &report.smoke_probe.notes {
        let _ = writeln!(out, "- {}", note);
    }
    let _ = writeln!(
        out,
        "\n| Source | Available | Latest Cycle | Probe URL |\n| --- | --- | --- | --- |"
    );
    for source in &report.smoke_probe.sources {
        let latest = match (&source.latest_cycle_yyyymmdd, source.latest_cycle_hour_utc) {
            (Some(date), Some(hour)) => format!("{date} {:02}Z", hour),
            _ => "n/a".to_string(),
        };
        let probe = source
            .availability_probe_url
            .as_deref()
            .or(source.grib_url.as_deref())
            .unwrap_or("n/a");
        let status = if source.available { "yes" } else { "no" };
        let _ = writeln!(
            out,
            "| {} | {} | {} | `{}` |",
            source.source, status, latest, probe
        );
        if let Some(error) = &source.error {
            let _ = writeln!(out, "\n  source `{}` error: `{}`\n", source.source, error);
        }
    }

    write_lane_section(&mut out, "Direct Maps", &report.map_inventory.direct);
    write_lane_section(&mut out, "Derived Maps", &report.map_inventory.derived);
    write_lane_section(&mut out, "Heavy Map Sets", &report.map_inventory.heavy);
    write_lane_section(&mut out, "Windowed Maps", &report.map_inventory.windowed);

    let _ = writeln!(out, "## Cross Sections\n");
    let _ = writeln!(
        out,
        "- Declared styles: `{}`\n- Pressure-section builder wired now: `{}`\n",
        report.cross_sections.declared_count, report.cross_sections.pressure_builder_count
    );
    let _ = writeln!(
        out,
        "| Product | Group | Units | Builder Wired |\n| --- | --- | --- | --- |"
    );
    for entry in &report.cross_sections.declared {
        let _ = writeln!(
            out,
            "| `{}` | {} | `{}` | {} |",
            entry.slug,
            entry.group,
            entry.units,
            if entry.builder_supported { "yes" } else { "no" }
        );
    }

    let _ = writeln!(out, "\n## Wxsection-Inspired Missing Map Candidates\n");
    let _ = writeln!(
        out,
        "| Candidate | Priority | Upstream Basis | Inputs |\n| --- | --- | --- | --- |"
    );
    for candidate in &report.wxsection_map_candidates {
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | `{}` |",
            candidate.slug,
            candidate.priority,
            candidate.upstream_basis,
            candidate.likely_inputs.join(", ")
        );
        let _ = writeln!(out, "\n  {}\n", candidate.note);
    }

    out
}

fn write_lane_section(out: &mut String, title: &str, lane: &LaneInventoryReport) {
    let _ = writeln!(
        out,
        "\n## {}\n\n- Available now: `{}`\n- Blocked on HRRR: `{}`\n",
        title, lane.available_count, lane.blocked_count
    );
    for entry in &lane.available {
        let routes = if entry.source_routes.is_empty() {
            "n/a".to_string()
        } else {
            entry.source_routes.join(", ")
        };
        let _ = writeln!(out, "- `{}`: {}  (`{}`)", entry.slug, entry.title, routes);
    }
    if !lane.blocked.is_empty() {
        let _ = writeln!(out, "\nBlocked:");
        for entry in &lane.blocked {
            let _ = writeln!(out, "- `{}`: {}", entry.slug, entry.title);
        }
    }
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
