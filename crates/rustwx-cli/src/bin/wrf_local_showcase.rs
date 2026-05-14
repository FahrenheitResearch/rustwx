#[cfg(not(feature = "wrf"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("wrf_local_showcase requires `cargo build --release --features wrf`".into())
}

#[cfg(feature = "wrf")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    app::run()
}

#[cfg(feature = "wrf")]
mod app {
    use chrono::{Duration, NaiveDate, NaiveDateTime};
    use clap::{Parser, ValueEnum};
    use rayon::prelude::*;
    use rustwx_core::{CycleSpec, FieldSelector, ModelId, SourceId};
    use rustwx_models::{LatestRun, plot_recipe_fetch_plan};
    use rustwx_products::DomainSpec;
    use rustwx_products::derived::NativeContourRenderMode;
    use rustwx_products::direct::{
        DirectBatchRequest, render_direct_recipes_from_selected_fields,
        supported_direct_recipe_slugs,
    };
    use rustwx_products::places::{PlaceLabelDensityTier, default_place_label_overlay_for_domain};
    use rustwx_render::PngCompressionMode;
    use serde::Serialize;
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Instant;

    const DEFAULT_RECIPES: &str = "2m_temperature_10m_winds,2m_dewpoint_10m_winds,mslp_10m_winds,10m_wind_gusts,total_qpf,composite_reflectivity,composite_reflectivity_uh,uh_2to5km";

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
        name = "wrf-local-showcase",
        about = "Render high-resolution direct recipe plots from local WRF wrfout/auxhist NetCDF files"
    )]
    struct Args {
        #[arg(long)]
        input_dir: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
        #[arg(long, default_value = "20250621")]
        cycle_date: String,
        #[arg(long, default_value_t = 0)]
        cycle: u8,
        #[arg(long, value_delimiter = ',', default_value = "d01,d02,d03")]
        domains: Vec<String>,
        #[arg(long, value_delimiter = ',', default_value = "wrfout")]
        kinds: Vec<String>,
        #[arg(long, value_delimiter = ',', default_value = DEFAULT_RECIPES)]
        recipes: Vec<String>,
        #[arg(long, default_value_t = false)]
        all_supported: bool,
        #[arg(long, allow_hyphen_values = true)]
        bounds: Option<String>,
        #[arg(long, default_value = "enderlin_ef5")]
        domain_slug_prefix: String,
        #[arg(long, default_value_t = 0.02)]
        domain_pad_deg: f64,
        #[arg(long)]
        max_files_per_domain: Option<usize>,
        #[arg(long)]
        valid_start: Option<String>,
        #[arg(long)]
        valid_end: Option<String>,
        #[arg(long, default_value_t = 1)]
        stride: usize,
        #[arg(long, default_value_t = 2400)]
        width: u32,
        #[arg(long, default_value_t = 1600)]
        height: u32,
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u8).range(0..=3))]
        place_label_density: u8,
        #[arg(long = "png-compression", value_enum, default_value_t = PngCompressionArg::Fast)]
        png_compression: PngCompressionArg,
        /// Number of WRF files to extract/render concurrently. Use 0 for auto.
        #[arg(long, default_value_t = 0)]
        jobs: usize,
    }

    #[derive(Debug, Clone)]
    struct InputFile {
        path: PathBuf,
        kind: String,
        domain: String,
        valid: NaiveDateTime,
        lead_minutes: i64,
    }

    #[derive(Debug, Clone, Serialize)]
    struct RenderRecord {
        kind: String,
        domain: String,
        valid_utc: String,
        lead_label: String,
        recipe_slug: String,
        title: String,
        output_path: PathBuf,
        render_ms: u128,
    }

    #[derive(Debug, Clone, Serialize)]
    struct FileBlocker {
        path: PathBuf,
        reason: String,
    }

    #[derive(Debug, Clone, Serialize)]
    struct RecipeBlocker {
        path: PathBuf,
        kind: String,
        domain: String,
        valid_utc: String,
        recipe_slug: String,
        missing_selectors: Vec<String>,
    }

    #[derive(Debug, Serialize)]
    struct ShowcaseReport {
        input_dir: PathBuf,
        out_dir: PathBuf,
        cycle_date: String,
        cycle_utc: u8,
        width: u32,
        height: u32,
        domains: Vec<String>,
        kinds: Vec<String>,
        recipes_requested: Vec<String>,
        files_considered: usize,
        jobs: usize,
        rendered_count: usize,
        file_blockers: Vec<FileBlocker>,
        recipe_blockers: Vec<RecipeBlocker>,
        total_ms: u128,
        html_path: PathBuf,
        records: Vec<RenderRecord>,
        command: Vec<String>,
    }

    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args = Args::parse();
        let start = Instant::now();
        fs::create_dir_all(&args.out_dir)?;
        let html_dir = args.out_dir.join("html");
        fs::create_dir_all(&html_dir)?;

        let recipes = resolve_recipes(&args)?;
        let selectors_by_recipe = selectors_by_recipe(&recipes)?;
        let mut all_selectors = Vec::<FieldSelector>::new();
        for selector in selectors_by_recipe.values().flatten().copied() {
            if !all_selectors.contains(&selector) {
                all_selectors.push(selector);
            }
        }

        let cycle_dt = parse_cycle(&args.cycle_date, args.cycle)?;
        let discovered_files = discover_files(&args, cycle_dt)?;
        let discovered_file_count = discovered_files.len();
        let mut file_blockers = Vec::<FileBlocker>::new();
        let files = preflight_openable_groups(discovered_files, &mut file_blockers);
        let jobs = effective_jobs(&args, &files);
        println!(
            "wrf_local_showcase: {} files, {} recipes, {} unique selectors, {} preflight blockers, {} job(s)",
            files.len(),
            recipes.len(),
            all_selectors.len(),
            file_blockers.len(),
            jobs
        );

        let latest = LatestRun {
            model: ModelId::WrfGdex,
            cycle: CycleSpec::new(&args.cycle_date, args.cycle)?,
            source: SourceId::Gdex,
        };
        let explicit_bounds = args.bounds.as_deref().map(parse_bounds).transpose()?;
        let mut records = Vec::<RenderRecord>::new();
        let mut recipe_blockers = Vec::<RecipeBlocker>::new();
        let html_path = html_dir.join("index.html");
        write_report_snapshot(
            &args,
            &html_path,
            &recipes,
            discovered_file_count,
            &records,
            &file_blockers,
            &recipe_blockers,
            jobs,
            start.elapsed().as_millis(),
        )?;

        if jobs <= 1 {
            for (index, input) in files.iter().enumerate() {
                println!(
                    "[{}/{}] extracting {}",
                    index + 1,
                    files.len(),
                    input.path.display()
                );
                let output = process_input_file(
                    &args,
                    &recipes,
                    &selectors_by_recipe,
                    &all_selectors,
                    &latest,
                    explicit_bounds,
                    input,
                );
                records.extend(output.records);
                file_blockers.extend(output.file_blockers);
                recipe_blockers.extend(output.recipe_blockers);
                write_report_snapshot(
                    &args,
                    &html_path,
                    &recipes,
                    discovered_file_count,
                    &records,
                    &file_blockers,
                    &recipe_blockers,
                    jobs,
                    start.elapsed().as_millis(),
                )?;
            }
        } else {
            let completed = AtomicUsize::new(0);
            let pool = rayon::ThreadPoolBuilder::new().num_threads(jobs).build()?;
            let outputs = pool.install(|| {
                files
                    .par_iter()
                    .map(|input| {
                        let output = process_input_file(
                            &args,
                            &recipes,
                            &selectors_by_recipe,
                            &all_selectors,
                            &latest,
                            explicit_bounds,
                            input,
                        );
                        let done = completed.fetch_add(1, Ordering::SeqCst) + 1;
                        println!("[{done}/{}] done {}", files.len(), input.path.display());
                        output
                    })
                    .collect::<Vec<_>>()
            });
            for output in outputs {
                records.extend(output.records);
                file_blockers.extend(output.file_blockers);
                recipe_blockers.extend(output.recipe_blockers);
            }
        }

        write_report_snapshot(
            &args,
            &html_path,
            &recipes,
            discovered_file_count,
            &records,
            &file_blockers,
            &recipe_blockers,
            jobs,
            start.elapsed().as_millis(),
        )?;
        println!("{}", html_path.display());
        println!(
            "rendered {} PNGs in {} ms",
            records.len(),
            start.elapsed().as_millis()
        );
        Ok(())
    }

    #[derive(Debug, Default)]
    struct ProcessOutput {
        records: Vec<RenderRecord>,
        file_blockers: Vec<FileBlocker>,
        recipe_blockers: Vec<RecipeBlocker>,
    }

    fn process_input_file(
        args: &Args,
        recipes: &[String],
        selectors_by_recipe: &HashMap<String, Vec<FieldSelector>>,
        all_selectors: &[FieldSelector],
        latest: &LatestRun,
        explicit_bounds: Option<(f64, f64, f64, f64)>,
        input: &InputFile,
    ) -> ProcessOutput {
        let mut output = ProcessOutput::default();
        if input.path.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
            output.file_blockers.push(FileBlocker {
                path: input.path.clone(),
                reason: "zero-byte WRF file".to_string(),
            });
            return output;
        }

        let partial =
            match rustwx_wrf::extract_selectors_partial_from_path(&input.path, all_selectors) {
                Ok(partial) => partial,
                Err(err) => {
                    output.file_blockers.push(FileBlocker {
                        path: input.path.clone(),
                        reason: err.to_string(),
                    });
                    return output;
                }
            };
        let extracted = partial
            .extracted
            .into_iter()
            .map(|field| (field.selector, field))
            .collect::<HashMap<_, _>>();
        if extracted.is_empty() {
            output.file_blockers.push(FileBlocker {
                path: input.path.clone(),
                reason: "no requested recipe fields were extracted".to_string(),
            });
            return output;
        }

        let bounds = match explicit_bounds {
            Some(bounds) => bounds,
            None => match bounds_from_extracted(&extracted, args.domain_pad_deg) {
                Ok(bounds) => bounds,
                Err(err) => {
                    output.file_blockers.push(FileBlocker {
                        path: input.path.clone(),
                        reason: err.to_string(),
                    });
                    return output;
                }
            },
        };
        let domain = DomainSpec::new(
            format!(
                "{}_{}",
                sanitize_slug(&args.domain_slug_prefix),
                input.domain
            ),
            bounds,
        );
        let forecast_hour = (input.lead_minutes.max(0) / 60).min(u16::MAX as i64) as u16;
        let lead_label = lead_label(input.lead_minutes);
        let subtitle_left = format!(
            "Init {} {:02}Z | {} | Valid {}Z | {}",
            format_date_for_title(&args.cycle_date),
            args.cycle,
            lead_label,
            input.valid.format("%Y-%m-%d %H:%M"),
            input.domain
        );
        let suffix = format!(
            "{}_{}_valid_{}",
            input.kind,
            lead_slug(input.lead_minutes),
            input.valid.format("%Y%m%d_%H%M%S")
        );
        let request = DirectBatchRequest {
            model: ModelId::WrfGdex,
            date_yyyymmdd: args.cycle_date.clone(),
            cycle_override_utc: Some(args.cycle),
            forecast_hour,
            source: SourceId::Gdex,
            domain: domain.clone(),
            out_dir: args.out_dir.join(&input.kind).join(&input.domain),
            cache_root: args.out_dir.join("cache"),
            use_cache: false,
            recipe_slugs: recipes.to_vec(),
            product_overrides: HashMap::new(),
            contour_mode: NativeContourRenderMode::Automatic,
            native_fill_level_multiplier: 1,
            output_width: args.width,
            output_height: args.height,
            png_compression: args.png_compression.into(),
            custom_poi_overlay: None,
            place_label_overlay: default_place_label_overlay_for_domain(
                &domain,
                PlaceLabelDensityTier::from_numeric(args.place_label_density),
            ),
            output_suffix: Some(suffix),
            subtitle_left_override: Some(subtitle_left),
            subtitle_right_override: Some("source: local WRF NetCDF".to_string()),
            earth2_ensemble: None,
        };

        let mut renderable_recipes = Vec::<String>::new();
        for recipe in recipes {
            let selectors = selectors_by_recipe
                .get(recipe)
                .expect("recipe selector map was prebuilt");
            let missing = selectors
                .iter()
                .filter(|selector| !extracted.contains_key(selector))
                .copied()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                output.recipe_blockers.push(RecipeBlocker {
                    path: input.path.clone(),
                    kind: input.kind.clone(),
                    domain: input.domain.clone(),
                    valid_utc: input.valid.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    recipe_slug: recipe.clone(),
                    missing_selectors: missing.iter().map(ToString::to_string).collect(),
                });
                continue;
            }
            renderable_recipes.push(recipe.clone());
        }

        if renderable_recipes.is_empty() {
            return output;
        }

        let render_start = Instant::now();
        let mut render_request = request.clone();
        render_request.recipe_slugs = renderable_recipes.clone();
        match render_direct_recipes_from_selected_fields(
            &render_request,
            latest,
            &renderable_recipes,
            &extracted,
            input.kind.clone(),
            input.path.display().to_string(),
            input.path.display().to_string(),
        ) {
            Ok(rendered) => {
                let batch_ms = render_start.elapsed().as_millis();
                for (recipe, rendered) in renderable_recipes.iter().zip(rendered) {
                    output.records.push(RenderRecord {
                        kind: input.kind.clone(),
                        domain: input.domain.clone(),
                        valid_utc: input.valid.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        lead_label: lead_label.clone(),
                        recipe_slug: recipe.clone(),
                        title: rendered.title,
                        output_path: rendered.output_path,
                        render_ms: batch_ms,
                    });
                }
            }
            Err(err) => {
                let message = err.to_string();
                for recipe in &renderable_recipes {
                    output.recipe_blockers.push(RecipeBlocker {
                        path: input.path.clone(),
                        kind: input.kind.clone(),
                        domain: input.domain.clone(),
                        valid_utc: input.valid.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        recipe_slug: recipe.clone(),
                        missing_selectors: vec![message.clone()],
                    });
                }
            }
        }
        output
    }

    fn write_report_snapshot(
        args: &Args,
        html_path: &Path,
        recipes: &[String],
        files_considered: usize,
        records: &[RenderRecord],
        file_blockers: &[FileBlocker],
        recipe_blockers: &[RecipeBlocker],
        jobs: usize,
        total_ms: u128,
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_html(
            html_path,
            &args.out_dir,
            records,
            recipe_blockers,
            file_blockers,
        )?;
        let report = ShowcaseReport {
            input_dir: args.input_dir.clone(),
            out_dir: args.out_dir.clone(),
            cycle_date: args.cycle_date.clone(),
            cycle_utc: args.cycle,
            width: args.width,
            height: args.height,
            domains: args.domains.clone(),
            kinds: args.kinds.clone(),
            recipes_requested: recipes.to_vec(),
            files_considered,
            jobs,
            rendered_count: records.len(),
            file_blockers: file_blockers.to_vec(),
            recipe_blockers: recipe_blockers.to_vec(),
            total_ms,
            html_path: html_path.to_path_buf(),
            records: records.to_vec(),
            command: std::env::args().collect(),
        };
        fs::write(
            args.out_dir.join("wrf_local_showcase_report.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;
        Ok(())
    }

    fn resolve_recipes(args: &Args) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let recipes = if args.all_supported {
            supported_direct_recipe_slugs(ModelId::WrfGdex)
        } else {
            args.recipes.clone()
        };
        if recipes.is_empty() {
            return Err("no recipes requested".into());
        }
        Ok(recipes)
    }

    fn selectors_by_recipe(
        recipes: &[String],
    ) -> Result<HashMap<String, Vec<FieldSelector>>, Box<dyn std::error::Error>> {
        let mut out = HashMap::new();
        for recipe in recipes {
            let plan = plot_recipe_fetch_plan(recipe, ModelId::WrfGdex)?;
            out.insert(recipe.clone(), plan.selectors());
        }
        Ok(out)
    }

    fn effective_jobs(args: &Args, files: &[InputFile]) -> usize {
        if args.jobs > 0 {
            return args.jobs;
        }
        if files.iter().any(|file| file.kind == "wrfout") {
            return 1;
        }
        let available = thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);
        (available / 2).clamp(1, 6)
    }

    fn discover_files(
        args: &Args,
        cycle_dt: NaiveDateTime,
    ) -> Result<Vec<InputFile>, Box<dyn std::error::Error>> {
        let wanted_domains = args
            .domains
            .iter()
            .map(|value| normalize_domain(value))
            .collect::<BTreeSet<_>>();
        let wanted_kinds = args
            .kinds
            .iter()
            .map(|value| value.trim().to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let valid_start = args
            .valid_start
            .as_deref()
            .map(parse_valid_filter)
            .transpose()?;
        let valid_end = args
            .valid_end
            .as_deref()
            .map(parse_valid_filter)
            .transpose()?;
        let mut grouped = BTreeMap::<(String, String), Vec<InputFile>>::new();
        for entry in fs::read_dir(&args.input_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some((kind, domain, valid)) = parse_wrf_filename(name) else {
                continue;
            };
            if !wanted_kinds.contains(&kind) || !wanted_domains.contains(&domain) {
                continue;
            }
            if valid_start.is_some_and(|start| valid < start)
                || valid_end.is_some_and(|end| valid > end)
            {
                continue;
            }
            let lead_minutes = valid.signed_duration_since(cycle_dt).num_minutes();
            grouped
                .entry((kind.clone(), domain.clone()))
                .or_default()
                .push(InputFile {
                    path: entry.path(),
                    kind,
                    domain,
                    valid,
                    lead_minutes,
                });
        }

        let stride = args.stride.max(1);
        let mut files = Vec::new();
        for (_, mut group) in grouped {
            group.sort_by_key(|file| file.valid);
            let selected = group.into_iter().step_by(stride);
            if let Some(max) = args.max_files_per_domain {
                files.extend(selected.take(max));
            } else {
                files.extend(selected);
            }
        }
        files.sort_by_key(|file| (file.kind.clone(), file.domain.clone(), file.valid));
        Ok(files)
    }

    fn preflight_openable_groups(
        files: Vec<InputFile>,
        file_blockers: &mut Vec<FileBlocker>,
    ) -> Vec<InputFile> {
        let mut grouped = BTreeMap::<(String, String), Vec<InputFile>>::new();
        for file in files {
            grouped
                .entry((file.kind.clone(), file.domain.clone()))
                .or_default()
                .push(file);
        }

        let mut out = Vec::new();
        for ((kind, domain), mut group) in grouped {
            group.sort_by_key(|file| file.valid);
            let Some(first) = group.first() else {
                continue;
            };
            match rustwx_wrf::WrfFile::open(&first.path) {
                Ok(_) => out.extend(group),
                Err(err) => {
                    let reason = format!(
                        "skipped {kind}/{domain}: representative file failed WRF open: {err}"
                    );
                    for file in group {
                        file_blockers.push(FileBlocker {
                            path: file.path,
                            reason: reason.clone(),
                        });
                    }
                }
            }
        }
        out.sort_by_key(|file| (file.kind.clone(), file.domain.clone(), file.valid));
        out
    }

    fn parse_valid_filter(value: &str) -> Result<NaiveDateTime, Box<dyn std::error::Error>> {
        let trimmed = value.trim();
        for format in [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d_%H_%M_%S",
            "%Y%m%d_%H%M%S",
        ] {
            if let Ok(parsed) = NaiveDateTime::parse_from_str(trimmed, format) {
                return Ok(parsed);
            }
        }
        Err(format!(
            "invalid valid time `{trimmed}`; expected YYYY-MM-DDTHH:MM or YYYY-MM-DD_HH_MM_SS"
        )
        .into())
    }

    fn parse_wrf_filename(name: &str) -> Option<(String, String, NaiveDateTime)> {
        for kind in ["wrfout", "auxhist2"] {
            if let Some(rest) = name.strip_prefix(&format!("{kind}_")) {
                let (domain, stamp) = rest.split_once('_')?;
                if !domain.starts_with('d') || domain.len() != 3 {
                    return None;
                }
                let valid = NaiveDateTime::parse_from_str(stamp, "%Y-%m-%d_%H_%M_%S").ok()?;
                return Some((kind.to_string(), normalize_domain(domain), valid));
            }
        }
        None
    }

    fn parse_cycle(date: &str, hour: u8) -> Result<NaiveDateTime, Box<dyn std::error::Error>> {
        let date = NaiveDate::parse_from_str(date, "%Y%m%d")?;
        date.and_hms_opt(hour as u32, 0, 0)
            .ok_or_else(|| format!("invalid cycle hour {hour}").into())
    }

    fn parse_bounds(value: &str) -> Result<(f64, f64, f64, f64), Box<dyn std::error::Error>> {
        let parts = value
            .split(',')
            .map(|part| part.trim().parse::<f64>())
            .collect::<Result<Vec<_>, _>>()?;
        if parts.len() != 4 {
            return Err("--bounds expects WEST,EAST,SOUTH,NORTH".into());
        }
        let (west, east, south, north) = (parts[0], parts[1], parts[2], parts[3]);
        if !(west.is_finite()
            && east.is_finite()
            && south.is_finite()
            && north.is_finite()
            && west < east
            && south < north)
        {
            return Err("invalid --bounds; expected finite WEST<EAST and SOUTH<NORTH".into());
        }
        Ok((west, east, south, north))
    }

    fn bounds_from_extracted(
        extracted: &HashMap<FieldSelector, rustwx_core::SelectedField2D>,
        pad_deg: f64,
    ) -> Result<(f64, f64, f64, f64), Box<dyn std::error::Error>> {
        let field = extracted
            .values()
            .next()
            .ok_or("cannot derive bounds without extracted fields")?;
        let mut west = f64::INFINITY;
        let mut east = f64::NEG_INFINITY;
        let mut south = f64::INFINITY;
        let mut north = f64::NEG_INFINITY;
        for (&lat, &lon) in field.grid.lat_deg.iter().zip(field.grid.lon_deg.iter()) {
            let lat = lat as f64;
            let lon = lon as f64;
            if lat.is_finite() && lon.is_finite() {
                west = west.min(lon);
                east = east.max(lon);
                south = south.min(lat);
                north = north.max(lat);
            }
        }
        if !(west.is_finite() && east.is_finite() && south.is_finite() && north.is_finite()) {
            return Err("extracted WRF field has no finite lat/lon bounds".into());
        }
        let pad = pad_deg.max(0.0);
        Ok((west - pad, east + pad, south - pad, north + pad))
    }

    fn lead_label(minutes: i64) -> String {
        let sign = if minutes < 0 { "-" } else { "+" };
        let duration = Duration::minutes(minutes.abs());
        let hours = duration.num_hours();
        let mins = duration.num_minutes() % 60;
        format!("Lead {sign}{hours:02}:{mins:02}")
    }

    fn lead_slug(minutes: i64) -> String {
        let prefix = if minutes < 0 { "lead_m" } else { "lead_p" };
        let duration = Duration::minutes(minutes.abs());
        let hours = duration.num_hours();
        let mins = duration.num_minutes() % 60;
        format!("{prefix}{hours:03}{mins:02}")
    }

    fn format_date_for_title(date: &str) -> String {
        if date.len() == 8 {
            format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8])
        } else {
            date.to_string()
        }
    }

    fn normalize_domain(value: &str) -> String {
        let value = value.trim().to_ascii_lowercase();
        if let Some(rest) = value.strip_prefix('d') {
            format!("d{:0>2}", rest)
        } else {
            value
        }
    }

    fn sanitize_slug(value: &str) -> String {
        let mut out = String::new();
        for ch in value.chars() {
            if ch.is_ascii_alphanumeric() {
                out.push(ch.to_ascii_lowercase());
            } else if !out.ends_with('_') {
                out.push('_');
            }
        }
        out.trim_matches('_').to_string()
    }

    fn write_html(
        html_path: &Path,
        out_dir: &Path,
        records: &[RenderRecord],
        recipe_blockers: &[RecipeBlocker],
        file_blockers: &[FileBlocker],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut by_group = BTreeMap::<String, Vec<&RenderRecord>>::new();
        for record in records {
            by_group
                .entry(format!("{} / {}", record.kind, record.domain))
                .or_default()
                .push(record);
        }
        let mut html = String::new();
        html.push_str("<!doctype html><html><head><meta charset=\"utf-8\"><title>WRF Local Showcase</title><style>");
        html.push_str("body{font-family:Arial,Helvetica,sans-serif;margin:0;background:#f5f5f5;color:#111}header{position:sticky;top:0;background:white;border-bottom:1px solid #ccc;padding:12px 18px;z-index:3}h1{font-size:20px;margin:0 0 4px}h2{font-size:17px;margin:22px 18px 10px}.meta{font-size:13px;color:#444}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(420px,1fr));gap:14px;padding:0 18px 20px}.card{background:white;border:1px solid #bbb}.card img{width:100%;display:block}.cap{font-size:12px;line-height:1.35;padding:8px 10px;border-top:1px solid #ddd}.warn{margin:12px 18px;padding:10px;background:#fff5d6;border:1px solid #d8b94f;font-size:13px}code{font-family:Consolas,monospace}");
        html.push_str("</style></head><body><header>");
        html.push_str(&format!(
            "<h1>WRF Local Showcase</h1><div class=\"meta\">{} PNGs",
            records.len()
        ));
        if !recipe_blockers.is_empty() || !file_blockers.is_empty() {
            html.push_str(&format!(
                " | {} recipe blockers | {} file blockers",
                recipe_blockers.len(),
                file_blockers.len()
            ));
        }
        html.push_str("</div></header>");
        if !file_blockers.is_empty() {
            html.push_str("<div class=\"warn\"><b>File blockers</b><br>");
            for blocker in file_blockers.iter().take(20) {
                html.push_str(&format!(
                    "<code>{}</code>: {}<br>",
                    escape_html(&blocker.path.display().to_string()),
                    escape_html(&blocker.reason)
                ));
            }
            html.push_str("</div>");
        }
        if !recipe_blockers.is_empty() {
            html.push_str("<div class=\"warn\"><b>Recipe blockers</b><br>");
            for blocker in recipe_blockers.iter().take(30) {
                html.push_str(&format!(
                    "{} {} {} <code>{}</code>: {}<br>",
                    escape_html(&blocker.kind),
                    escape_html(&blocker.domain),
                    escape_html(&blocker.valid_utc),
                    escape_html(&blocker.recipe_slug),
                    escape_html(&blocker.missing_selectors.join(", "))
                ));
            }
            html.push_str("</div>");
        }
        for (group, mut group_records) in by_group {
            group_records
                .sort_by_key(|record| (record.valid_utc.clone(), record.recipe_slug.clone()));
            html.push_str(&format!(
                "<h2>{}</h2><div class=\"grid\">",
                escape_html(&group)
            ));
            for record in group_records {
                let src = image_src(out_dir, &record.output_path);
                html.push_str("<div class=\"card\">");
                html.push_str(&format!(
                    "<a href=\"{src}\"><img loading=\"lazy\" src=\"{src}\"></a>",
                    src = escape_html(&src)
                ));
                html.push_str(&format!(
                    "<div class=\"cap\"><b>{}</b><br>{} | {} | {} ms<br><code>{}</code></div>",
                    escape_html(&record.recipe_slug),
                    escape_html(&record.valid_utc),
                    escape_html(&record.lead_label),
                    record.render_ms,
                    escape_html(&record.output_path.display().to_string())
                ));
                html.push_str("</div>");
            }
            html.push_str("</div>");
        }
        html.push_str("</body></html>");
        fs::write(html_path, html)?;
        Ok(())
    }

    fn image_src(out_dir: &Path, path: &Path) -> String {
        path.strip_prefix(out_dir)
            .map(|relative| format!("../{}", relative.to_string_lossy().replace('\\', "/")))
            .unwrap_or_else(|_| path.to_string_lossy().replace('\\', "/"))
    }

    fn escape_html(value: &str) -> String {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    }
}
