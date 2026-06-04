use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

const BIN_README: &str = include_str!("../src/bin/README.md");

#[test]
fn bin_taxonomy_documents_every_src_bin_file() {
    let documented = documented_bin_names();
    let actual = actual_bin_names();

    assert_eq!(
        documented, actual,
        "src/bin inventory changed; update crates/rustwx-cli/src/bin/README.md with the lane and notes"
    );
}

fn documented_bin_names() -> BTreeSet<String> {
    BIN_README
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("| `")?;
            Some(rest.split('`').next()?.to_string())
        })
        .collect()
}

fn actual_bin_names() -> BTreeSet<String> {
    let bin_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/bin");
    fs::read_dir(bin_dir)
        .expect("src/bin should be readable")
        .map(|entry| entry.expect("src/bin entry should be readable").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .map(|path| {
            path.file_stem()
                .expect("bin file should have a stem")
                .to_string_lossy()
                .to_string()
        })
        .collect()
}
