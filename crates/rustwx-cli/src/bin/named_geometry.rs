use clap::{Args, Parser, Subcommand, ValueEnum};
use rustwx_products::named_geometry::{
    NamedGeometryCatalog, NamedGeometryKind, NamedGeometrySelector,
};
use serde::Serialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(
    name = "named-geometry",
    about = "List or query built-in or JSON-loaded named geometry assets"
)]
struct Cli {
    #[arg(long, help = "Load a custom named geometry catalog from JSON")]
    catalog_json: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    List {
        #[command(flatten)]
        selector: SelectorArgs,
    },
    Show {
        slug: String,
    },
    Domains {
        #[command(flatten)]
        selector: SelectorArgs,
    },
}

#[derive(Debug, Clone, Args, Default)]
struct SelectorArgs {
    #[arg(long, value_enum)]
    kind: Option<KindArg>,
    #[arg(long)]
    group: Option<String>,
    #[arg(long = "tag", value_delimiter = ',', num_args = 1..)]
    tags: Vec<String>,
    #[arg(long = "slug", value_delimiter = ',', num_args = 1..)]
    slugs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum KindArg {
    Metro,
    Region,
    WatchArea,
    Route,
    Other,
}

impl From<KindArg> for NamedGeometryKind {
    fn from(value: KindArg) -> Self {
        match value {
            KindArg::Metro => Self::Metro,
            KindArg::Region => Self::Region,
            KindArg::WatchArea => Self::WatchArea,
            KindArg::Route => Self::Route,
            KindArg::Other => Self::Other,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    run(&cli)
}

fn run(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = load_catalog(cli.catalog_json.as_deref())?;
    match &cli.command {
        Command::List { selector } => {
            let selector = build_selector(selector);
            let assets = catalog
                .select(&selector)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            print_json(&assets)
        }
        Command::Show { slug } => {
            let asset = catalog.find(slug).cloned().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("named geometry asset '{slug}' was not found"),
                )
            })?;
            print_json(&asset)
        }
        Command::Domains { selector } => {
            let selector = build_selector(selector);
            let domains = catalog.domain_specs(&selector);
            print_json(&domains)
        }
    }
}

fn load_catalog(path: Option<&Path>) -> Result<NamedGeometryCatalog, Box<dyn std::error::Error>> {
    match path {
        Some(path) => NamedGeometryCatalog::load_json(path),
        None => Ok(NamedGeometryCatalog::built_in()),
    }
}

fn build_selector(args: &SelectorArgs) -> NamedGeometrySelector {
    let mut selector = NamedGeometrySelector::new();
    if let Some(kind) = args.kind {
        selector = selector.with_kind(kind.into());
    }
    if let Some(group) = args.group.as_deref() {
        selector = selector.with_group(group);
    }
    for tag in &args.tags {
        selector = selector.with_tag(tag);
    }
    for slug in &args.slugs {
        selector = selector.with_slug(slug);
    }
    selector
}

fn print_json<T: Serialize>(value: &T) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_builder_preserves_all_filters() {
        let selector = build_selector(&SelectorArgs {
            kind: Some(KindArg::WatchArea),
            group: Some("enterprise_watch".to_string()),
            tags: vec!["fire".to_string(), "priority".to_string()],
            slugs: vec!["foothill_watch".to_string()],
        });

        assert_eq!(selector.kind, Some(NamedGeometryKind::WatchArea));
        assert_eq!(selector.group.as_deref(), Some("enterprise_watch"));
        assert_eq!(selector.tags, vec!["fire", "priority"]);
        assert_eq!(selector.slugs, vec!["foothill_watch"]);
    }

    #[test]
    fn built_in_domains_query_returns_bounds_assets_only() {
        let catalog = NamedGeometryCatalog::built_in();
        let selector = build_selector(&SelectorArgs {
            kind: Some(KindArg::Route),
            ..SelectorArgs::default()
        });

        assert!(catalog.domain_specs(&selector).is_empty());
    }
}
