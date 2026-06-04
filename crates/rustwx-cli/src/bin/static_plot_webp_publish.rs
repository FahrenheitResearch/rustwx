use anyhow::{Context, Result, bail};
use clap::Parser;
use image::ImageFormat;
use rayon::prelude::*;
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, artifact_identity_from_path,
    atomic_write_json,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(
    name = "static-plot-webp-publish",
    about = "Generate WebP companions for static plot PNG artifacts and publish them in run manifests"
)]
struct Args {
    #[arg(long)]
    root: PathBuf,
    #[arg(long, default_value_t = false)]
    overwrite: bool,
    #[arg(long, default_value_t = true)]
    update_manifests: bool,
    #[arg(long, default_value_t = 0)]
    jobs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebpPublishReport {
    schema: String,
    root: PathBuf,
    manifest_count: usize,
    png_artifact_count: usize,
    converted_count: usize,
    skipped_count: usize,
    manifest_updated_count: usize,
    failed_count: usize,
    elapsed_ms: u128,
    failures: Vec<WebpFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebpFailure {
    path: PathBuf,
    reason: String,
}

#[derive(Debug, Clone)]
struct ManifestTask {
    path: PathBuf,
    output_root: PathBuf,
    png_artifacts: Vec<PngArtifact>,
}

#[derive(Debug, Clone)]
struct PngArtifact {
    artifact_key: String,
    relative_path: PathBuf,
    input_fetch_keys: Vec<String>,
}

#[derive(Debug, Clone)]
struct ConvertTask {
    source: PathBuf,
    target: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestValue {
    #[serde(default)]
    output_root: PathBuf,
    #[serde(default)]
    artifacts: Vec<PublishedArtifactRecord>,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.root.is_dir() {
        bail!("static plot root does not exist: {}", args.root.display());
    }

    let started = Instant::now();
    let manifests = find_run_manifests(&args.root)?;
    let mut tasks = Vec::new();
    let mut failures = Vec::new();
    for manifest_path in manifests {
        match load_manifest_task(&manifest_path) {
            Ok(Some(task)) => tasks.push(task),
            Ok(None) => {}
            Err(err) => failures.push(WebpFailure {
                path: manifest_path,
                reason: err.to_string(),
            }),
        }
    }

    let convert_tasks = tasks
        .iter()
        .flat_map(|task| {
            task.png_artifacts.iter().filter_map(|artifact| {
                let source = task.output_root.join(&artifact.relative_path);
                if !source.is_file() {
                    return None;
                }
                Some(ConvertTask {
                    target: source.with_extension("webp"),
                    source,
                })
            })
        })
        .collect::<Vec<_>>();

    let jobs = if args.jobs == 0 {
        std::thread::available_parallelism()
            .map(|n| (n.get() / 2).clamp(1, 16))
            .unwrap_or(4)
    } else {
        args.jobs.max(1)
    };
    let converted = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let convert_failures = rayon::ThreadPoolBuilder::new()
        .num_threads(jobs)
        .build()
        .context("build WebP conversion thread pool")?
        .install(|| {
            convert_tasks
                .par_iter()
                .filter_map(|task| match convert_png_to_webp(task, args.overwrite) {
                    Ok(ConvertOutcome::Converted) => {
                        converted.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                    Ok(ConvertOutcome::Skipped) => {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                    Err(err) => Some(WebpFailure {
                        path: task.source.clone(),
                        reason: err.to_string(),
                    }),
                })
                .collect::<Vec<_>>()
        });
    failures.extend(convert_failures);

    let mut manifest_updated_count = 0usize;
    if args.update_manifests {
        for task in &tasks {
            match update_manifest_with_webp_artifacts(task) {
                Ok(true) => manifest_updated_count += 1,
                Ok(false) => {}
                Err(err) => failures.push(WebpFailure {
                    path: task.path.clone(),
                    reason: err.to_string(),
                }),
            }
        }
    }

    let report = WebpPublishReport {
        schema: "rustwx.static_plot_webp_publish.v1".to_string(),
        root: args.root,
        manifest_count: tasks.len(),
        png_artifact_count: tasks
            .iter()
            .map(|task| task.png_artifacts.len())
            .sum::<usize>(),
        converted_count: converted.load(Ordering::Relaxed),
        skipped_count: skipped.load(Ordering::Relaxed),
        manifest_updated_count,
        failed_count: failures.len(),
        elapsed_ms: started.elapsed().as_millis(),
        failures,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.failed_count > 0 {
        bail!("{} WebP publish failures", report.failed_count);
    }
    Ok(())
}

fn find_run_manifests(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    visit_dirs(root, &mut |path| {
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with("_run_manifest.json"))
        {
            out.push(path.to_path_buf());
        }
        Ok(())
    })?;
    out.sort();
    Ok(out)
}

fn visit_dirs(dir: &Path, f: &mut dyn FnMut(&Path) -> Result<()>) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            visit_dirs(&path, f)?;
        } else if file_type.is_file() {
            f(&path)?;
        }
    }
    Ok(())
}

fn load_manifest_task(path: &Path) -> Result<Option<ManifestTask>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let manifest: ManifestValue =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    if manifest.artifacts.is_empty() {
        return Ok(None);
    }
    let output_root = if manifest.output_root.as_os_str().is_empty() {
        path.parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    } else {
        manifest.output_root
    };
    let png_artifacts = manifest
        .artifacts
        .iter()
        .filter(|artifact| is_complete_or_cache_hit(artifact.state))
        .filter(|artifact| is_png_path(&artifact.relative_path))
        .map(|artifact| PngArtifact {
            artifact_key: artifact.artifact_key.clone(),
            relative_path: artifact.relative_path.clone(),
            input_fetch_keys: artifact.input_fetch_keys.clone(),
        })
        .collect::<Vec<_>>();
    if png_artifacts.is_empty() {
        return Ok(None);
    }
    Ok(Some(ManifestTask {
        path: path.to_path_buf(),
        output_root,
        png_artifacts,
    }))
}

fn is_complete_or_cache_hit(state: ArtifactPublicationState) -> bool {
    matches!(
        state,
        ArtifactPublicationState::Complete | ArtifactPublicationState::CacheHit
    )
}

fn is_png_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("png"))
}

enum ConvertOutcome {
    Converted,
    Skipped,
}

fn convert_png_to_webp(task: &ConvertTask, overwrite: bool) -> Result<ConvertOutcome> {
    if task.target.is_file() && !overwrite {
        return Ok(ConvertOutcome::Skipped);
    }
    let image = image::open(&task.source)
        .with_context(|| format!("decode PNG {}", task.source.display()))?;
    if let Some(parent) = task.target.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp = task.target.with_extension("webp.tmp");
    image
        .save_with_format(&tmp, ImageFormat::WebP)
        .with_context(|| format!("write WebP {}", tmp.display()))?;
    fs::rename(&tmp, &task.target)
        .with_context(|| format!("publish WebP {}", task.target.display()))?;
    Ok(ConvertOutcome::Converted)
}

fn update_manifest_with_webp_artifacts(task: &ManifestTask) -> Result<bool> {
    let bytes = fs::read(&task.path).with_context(|| format!("read {}", task.path.display()))?;
    let mut manifest: ManifestValue =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", task.path.display()))?;
    let existing = manifest
        .artifacts
        .iter()
        .map(|artifact| artifact.artifact_key.clone())
        .collect::<BTreeSet<_>>();
    let mut changed = false;
    for png in &task.png_artifacts {
        let webp_key = format!("{}__webp", png.artifact_key);
        if existing.contains(&webp_key) {
            continue;
        }
        let source = task.output_root.join(&png.relative_path);
        let webp_path = source.with_extension("webp");
        if !webp_path.is_file() {
            continue;
        }
        let relative_path = png.relative_path.with_extension("webp");
        let mut record = PublishedArtifactRecord::planned(webp_key, relative_path)
            .with_state(ArtifactPublicationState::Complete)
            .with_detail("WebP companion generated from PNG static plot")
            .with_input_fetch_keys(png.input_fetch_keys.clone());
        record.content_identity = Some(
            artifact_identity_from_path(&webp_path)
                .map_err(|err| anyhow::anyhow!("hash {}: {}", webp_path.display(), err))?,
        );
        manifest.artifacts.push(record);
        changed = true;
    }
    if changed {
        atomic_write_json(&task.path, &manifest)
            .map_err(|err| anyhow::anyhow!("write {}: {}", task.path.display(), err))?;
    }
    Ok(changed)
}
