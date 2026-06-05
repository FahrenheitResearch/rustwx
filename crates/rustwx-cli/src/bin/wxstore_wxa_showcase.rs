use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use rayon::prelude::*;
use rustwx_products::wxstore_wxa::{
    WxaCompositePanelRequest, WxaRenderedPlot, WxaStaticPlotRequest, available_wxa_products,
    read_wxa_dense2d_metadata, render_wxa_composite_panel, render_wxa_static_plot,
    wxa_composite_panel_component_products, wxa_product_path,
};
use rustwx_render::{PngCompressionMode, StaticPlotStyle};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum PngCompressionArg {
    Default,
    Fast,
    Fastest,
}

impl From<PngCompressionArg> for PngCompressionMode {
    fn from(value: PngCompressionArg) -> Self {
        match value {
            PngCompressionArg::Default => Self::Default,
            PngCompressionArg::Fast => Self::Fast,
            PngCompressionArg::Fastest => Self::Fastest,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "wxstore-wxa-showcase",
    about = "Render static plot PNGs directly from WxStore .wxa dense2d files"
)]
struct Args {
    #[arg(long)]
    spatial_root: PathBuf,
    #[arg(long)]
    model: String,
    #[arg(long)]
    run: String,
    #[arg(long, default_value = "control")]
    member: String,
    #[arg(long = "product", value_delimiter = ',', num_args = 0..)]
    products: Vec<String>,
    #[arg(long, alias = "forecast-hour", alias = "hours", default_value = "0-2")]
    forecast_hour: String,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 1600)]
    width: u32,
    #[arg(long, default_value_t = 900)]
    height: u32,
    #[arg(long, default_value_t = 1)]
    jobs: usize,
    #[arg(long, allow_hyphen_values = true)]
    bounds: Option<String>,
    #[arg(long)]
    max_products: Option<usize>,
    #[arg(long, value_enum, default_value_t = PngCompressionArg::Default)]
    png_compression: PngCompressionArg,
    #[arg(long, default_value = "operational-fast", value_parser = parse_plot_style)]
    plot_style: StaticPlotStyle,
}

#[derive(Debug, Clone, Serialize)]
struct ShowcaseReport {
    schema: String,
    spatial_root: PathBuf,
    model: String,
    run: String,
    member: String,
    products_requested: Vec<String>,
    hours_requested: Vec<u32>,
    plot_style: StaticPlotStyle,
    output_dir: PathBuf,
    html_path: PathBuf,
    rendered_count: usize,
    blocker_count: usize,
    total_ms: u128,
    rendered: Vec<WxaRenderedPlot>,
    blockers: Vec<WxaShowcaseBlocker>,
}

#[derive(Debug, Clone, Serialize)]
struct WxaShowcaseBlocker {
    product_slug: String,
    forecast_hour: Option<u32>,
    reason: String,
}

#[derive(Debug, Clone)]
enum ShowcaseTask {
    Single {
        product_slug: String,
        forecast_hour: u32,
        wxa_path: PathBuf,
    },
    Composite {
        product_slug: String,
        forecast_hour: u32,
    },
}

impl ShowcaseTask {
    fn product_slug(&self) -> &str {
        match self {
            Self::Single { product_slug, .. } | Self::Composite { product_slug, .. } => {
                product_slug
            }
        }
    }

    fn forecast_hour(&self) -> u32 {
        match self {
            Self::Single { forecast_hour, .. } | Self::Composite { forecast_hour, .. } => {
                *forecast_hour
            }
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let started = Instant::now();
    let products = select_products(&args)?;
    if products.is_empty() {
        bail!("no WXA products selected");
    }
    let hours = parse_hours(&args.forecast_hour)?;
    if hours.is_empty() {
        bail!("no forecast hours selected");
    }
    let bounds_override = args.bounds.as_deref().map(parse_bounds).transpose()?;
    fs::create_dir_all(&args.out_dir)?;
    let plot_dir = args
        .out_dir
        .join("plots")
        .join(&args.model)
        .join(&args.run)
        .join(&args.member);
    fs::create_dir_all(&plot_dir)?;

    let mut jobs = args.jobs;
    if jobs == 0 {
        jobs = std::thread::available_parallelism()
            .map(|n| (n.get() / 2).clamp(1, 8))
            .unwrap_or(4);
    }
    jobs = jobs.max(1);

    let tasks = build_tasks(&args, &products, &hours)?;
    let png_compression: PngCompressionMode = args.png_compression.into();
    let plot_style = args.plot_style;
    let render_task = |task: &ShowcaseTask| {
        let result = match task {
            ShowcaseTask::Single {
                forecast_hour,
                wxa_path,
                ..
            } => {
                let request = WxaStaticPlotRequest {
                    wxa_path: wxa_path.clone(),
                    forecast_hour: *forecast_hour,
                    out_dir: plot_dir.clone(),
                    width: args.width,
                    height: args.height,
                    png_compression,
                    plot_style,
                    bounds_override,
                    title_override: None,
                    subtitle_left: None,
                    subtitle_right: None,
                    output_suffix: None,
                };
                render_wxa_static_plot(&request)
            }
            ShowcaseTask::Composite {
                product_slug,
                forecast_hour,
            } => {
                let request = WxaCompositePanelRequest {
                    spatial_root: args.spatial_root.clone(),
                    model: args.model.clone(),
                    run: args.run.clone(),
                    member: Some(args.member.clone()),
                    product_slug: product_slug.clone(),
                    forecast_hour: *forecast_hour,
                    out_dir: plot_dir.clone(),
                    width: args.width,
                    height: args.height,
                    png_compression,
                    bounds_override,
                    title_override: None,
                    subtitle_left: None,
                    subtitle_right: None,
                    output_suffix: None,
                };
                render_wxa_composite_panel(&request)
            }
        };
        result.map_err(|err| WxaShowcaseBlocker {
            product_slug: task.product_slug().to_string(),
            forecast_hour: Some(task.forecast_hour()),
            reason: err.to_string(),
        })
    };

    let outcomes = if jobs == 1 {
        tasks.iter().map(render_task).collect::<Vec<_>>()
    } else {
        rayon::ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build()
            .context("build rayon thread pool")?
            .install(|| tasks.par_iter().map(render_task).collect::<Vec<_>>())
    };

    let mut rendered = Vec::new();
    let mut blockers = Vec::new();
    for outcome in outcomes {
        match outcome {
            Ok(plot) => rendered.push(plot),
            Err(blocker) => blockers.push(blocker),
        }
    }
    rendered.sort_by(|a, b| {
        a.model
            .cmp(&b.model)
            .then(a.run.cmp(&b.run))
            .then(a.product_slug.cmp(&b.product_slug))
            .then(a.forecast_hour.cmp(&b.forecast_hour))
    });
    blockers.sort_by(|a, b| {
        a.product_slug
            .cmp(&b.product_slug)
            .then(a.forecast_hour.cmp(&b.forecast_hour))
            .then(a.reason.cmp(&b.reason))
    });

    let html_path = args.out_dir.join("html").join("index.html");
    write_html(&html_path, &args, &rendered)?;
    let report = ShowcaseReport {
        schema: "rustwx.wxstore_wxa_showcase.v1".to_string(),
        spatial_root: args.spatial_root.clone(),
        model: args.model.clone(),
        run: args.run.clone(),
        member: args.member.clone(),
        products_requested: products,
        hours_requested: hours,
        plot_style: args.plot_style,
        output_dir: args.out_dir.clone(),
        html_path: html_path.clone(),
        rendered_count: rendered.len(),
        blocker_count: blockers.len(),
        total_ms: started.elapsed().as_millis(),
        rendered,
        blockers,
    };
    fs::write(
        args.out_dir.join("wxstore_wxa_showcase_report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{}", html_path.display());
    println!(
        "rendered {} PNGs in {} ms",
        report.rendered_count, report.total_ms
    );
    if report.blocker_count > 0 {
        println!("{} blockers", report.blocker_count);
    }
    Ok(())
}

fn parse_plot_style(value: &str) -> Result<StaticPlotStyle, String> {
    StaticPlotStyle::parse(value).ok_or_else(|| format!("unknown plot style '{value}'"))
}

fn select_products(args: &Args) -> Result<Vec<String>> {
    let mut products = if args.products.is_empty() {
        available_wxa_products(
            &args.spatial_root,
            &args.model,
            &args.run,
            Some(args.member.as_str()),
        )
        .map_err(|err| {
            anyhow!(
                "list WXA products under {}: {}",
                args.spatial_root
                    .join(&args.model)
                    .join(&args.run)
                    .join("members")
                    .join(&args.member)
                    .display(),
                err
            )
        })?
    } else {
        args.products.clone()
    };
    products.sort();
    products.dedup();
    if let Some(max_products) = args.max_products {
        products.truncate(max_products);
    }
    Ok(products)
}

fn build_tasks(args: &Args, products: &[String], hours: &[u32]) -> Result<Vec<ShowcaseTask>> {
    let wanted_hours = hours.iter().copied().collect::<BTreeSet<_>>();
    let mut tasks = Vec::new();
    for product in products {
        if let Some(component_products) = wxa_composite_panel_component_products(product) {
            let mut common_hours: Option<BTreeSet<u32>> = None;
            let mut all_components_present = true;
            for component in component_products {
                let path = wxa_product_path(
                    &args.spatial_root,
                    &args.model,
                    &args.run,
                    Some(args.member.as_str()),
                    &component,
                );
                if !path.is_file() {
                    all_components_present = false;
                    break;
                }
                let (meta, _) = read_wxa_dense2d_metadata(&path)
                    .map_err(|err| anyhow!("read WXA metadata {}: {}", path.display(), err))?;
                let available = meta.forecast_hours.into_iter().collect::<BTreeSet<_>>();
                common_hours = Some(match common_hours {
                    Some(existing) => existing.intersection(&available).copied().collect(),
                    None => available,
                });
            }
            if all_components_present {
                if let Some(available) = common_hours {
                    for hour in wanted_hours.intersection(&available) {
                        tasks.push(ShowcaseTask::Composite {
                            product_slug: product.clone(),
                            forecast_hour: *hour,
                        });
                    }
                }
                continue;
            }
        }

        let path = wxa_product_path(
            &args.spatial_root,
            &args.model,
            &args.run,
            Some(args.member.as_str()),
            product,
        );
        if !path.is_file() {
            continue;
        }
        let (meta, _) = read_wxa_dense2d_metadata(&path)
            .map_err(|err| anyhow!("read WXA metadata {}: {}", path.display(), err))?;
        let available = meta.forecast_hours.into_iter().collect::<BTreeSet<_>>();
        for hour in wanted_hours.intersection(&available) {
            tasks.push(ShowcaseTask::Single {
                product_slug: product.clone(),
                forecast_hour: *hour,
                wxa_path: path.clone(),
            });
        }
    }
    Ok(tasks)
}

fn parse_hours(value: &str) -> Result<Vec<u32>> {
    let mut hours = BTreeSet::new();
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        if let Some((a, b)) = part.split_once('-') {
            let start: u32 = a.trim().parse()?;
            let end: u32 = b.trim().parse()?;
            if end < start {
                bail!("invalid hour range '{part}'");
            }
            for hour in start..=end {
                hours.insert(hour);
            }
        } else {
            hours.insert(part.parse()?);
        }
    }
    Ok(hours.into_iter().collect())
}

fn parse_bounds(value: &str) -> Result<(f64, f64, f64, f64)> {
    let parts = value
        .split(',')
        .map(|part| part.trim().parse::<f64>())
        .collect::<Result<Vec<_>, _>>()?;
    if parts.len() != 4 {
        bail!("--bounds must be west,east,south,north");
    }
    Ok((parts[0], parts[1], parts[2], parts[3]))
}

fn write_html(path: &Path, args: &Args, rendered: &[WxaRenderedPlot]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut html = String::new();
    html.push_str("<!doctype html><html><head><meta charset=\"utf-8\"><title>WxStore WXA Showcase</title><style>");
    html.push_str("body{font-family:Arial,sans-serif;margin:0;background:#f4f5f7;color:#14171a}header{padding:18px 22px;background:#fff;border-bottom:1px solid #d8dde3;position:sticky;top:0;z-index:2}h1{font-size:20px;margin:0 0 4px}p{margin:0;color:#4e5964}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(520px,1fr));gap:14px;padding:14px}.card{background:#fff;border:1px solid #d8dde3;border-radius:6px;overflow:hidden}.meta{display:flex;justify-content:space-between;gap:10px;padding:9px 11px;font-size:13px}.meta b{font-size:14px}img{display:block;width:100%;height:auto;background:#fff}");
    html.push_str("</style></head><body>");
    html.push_str(&format!(
        "<header><h1>WxStore WXA Showcase</h1><p>{} / {} / {} - {} plots rendered directly from .wxa</p></header><main class=\"grid\">",
        escape_html(&args.model),
        escape_html(&args.run),
        escape_html(&args.member),
        rendered.len()
    ));
    for plot in rendered {
        let rel = relative_url(
            path.parent().unwrap_or_else(|| Path::new(".")),
            &plot.output_path,
        );
        html.push_str("<article class=\"card\">");
        html.push_str(&format!(
            "<div class=\"meta\"><b>{}</b><span>F{:03} | {} | {}x{}</span></div>",
            escape_html(&plot.title),
            plot.forecast_hour,
            escape_html(&plot.units),
            plot.nx,
            plot.ny
        ));
        html.push_str(&format!(
            "<img src=\"{}\" loading=\"lazy\">",
            escape_html(&rel)
        ));
        html.push_str("</article>");
    }
    html.push_str("</main></body></html>");
    fs::write(path, html)?;
    Ok(())
}

fn relative_url(from_dir: &Path, target: &Path) -> String {
    let relative = target
        .strip_prefix(from_dir.parent().unwrap_or(from_dir))
        .map(Path::to_path_buf)
        .unwrap_or_else(|_| target.to_path_buf());
    let mut out = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("/");
    if !out.starts_with("..") && !out.contains(':') {
        out = format!("../{out}");
    }
    out
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
