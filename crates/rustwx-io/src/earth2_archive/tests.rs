use super::*;
use rustwx_core::{CycleSpec, ModelRunRequest};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_archive_root(name: &str) -> PathBuf {
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("rustwx-earth2-{name}-{id}"))
}

#[test]
fn archive_path_uses_canonical_layout() {
    let request = ModelRunRequest::new(
        ModelId::Aifs,
        CycleSpec::new("20160822", 0).unwrap(),
        24,
        "oper",
    )
    .unwrap();
    let path = archive_path_with_root("X:/archive", &request).unwrap();
    let as_text = path.to_string_lossy().replace('\\', "/");
    assert!(as_text.ends_with("X:/archive/aifs/20160822T00Z/lead024.nc"));
}

#[test]
fn archive_path_selects_flat_aifs_member_file() {
    let root = temp_archive_root("flat-member-path");
    fs::create_dir_all(&root).unwrap();
    let file = root.join("aifs_long_20260513T060000Z_m02_lead00006.nc");
    fs::write(&file, []).unwrap();
    let request = ModelRunRequest::new(
        ModelId::Aifs,
        CycleSpec::new("20260513", 6).unwrap(),
        6,
        "oper",
    )
    .unwrap();

    let selected = archive_path_with_root_and_selector(
        &root,
        &request,
        Some(Earth2EnsembleSelector::Member(2)),
    )
    .unwrap();
    assert_eq!(selected, file);

    let product_request = ModelRunRequest::new(
        ModelId::Aifs,
        CycleSpec::new("20260513", 6).unwrap(),
        6,
        "m02",
    )
    .unwrap();
    let selected = archive_path_with_root_and_selector(&root, &product_request, None).unwrap();
    assert_eq!(selected, file);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn archive_path_and_lead_scan_allow_multi_year_leads() {
    let request = ModelRunRequest::new(
        ModelId::Aifs,
        CycleSpec::new("20160822", 0).unwrap(),
        8640,
        "oper",
    )
    .unwrap();
    let path = archive_path_with_root("X:/archive", &request).unwrap();
    let as_text = path.to_string_lossy().replace('\\', "/");
    assert!(as_text.ends_with("X:/archive/aifs/20160822T00Z/lead8640.nc"));

    let root = temp_archive_root("long-leads");
    let cycle_dir = root.join("aifs").join("20160822T00Z");
    fs::create_dir_all(&cycle_dir).unwrap();
    fs::write(cycle_dir.join("lead8640.nc"), []).unwrap();
    fs::write(cycle_dir.join("leadabc.nc"), []).unwrap();
    fs::write(cycle_dir.join("lead12.nc"), []).unwrap();
    assert_eq!(
        available_leads_for_cycle_with_root(
            &root,
            ModelId::Aifs,
            &CycleSpec::new("20160822", 0).unwrap()
        ),
        vec![8640]
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn flat_aifs_member_files_participate_in_lead_discovery() {
    let root = temp_archive_root("flat-member-leads");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("aifs_long_20260513T060000Z_m00_lead00006.nc"), []).unwrap();
    fs::write(root.join("aifs_long_20260513T060000Z_m01_lead00012.nc"), []).unwrap();
    fs::write(root.join("aifs_long_20260513T120000Z_m00_lead00018.nc"), []).unwrap();

    assert_eq!(
        available_leads_for_cycle_with_root(
            &root,
            ModelId::Aifs,
            &CycleSpec::new("20260513", 6).unwrap()
        ),
        vec![6, 12]
    );
    assert_eq!(
        default_forecast_hour_for_archive_with_root(&root, ModelId::Aifs, "20260513", Some(6)),
        Some(6)
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn default_forecast_hour_uses_first_lead_from_latest_cycle() {
    let root = temp_archive_root("leads");
    let old_cycle = root.join("aifs").join("20160822T00Z");
    let new_cycle = root.join("aifs").join("20160822T06Z");
    fs::create_dir_all(&old_cycle).unwrap();
    fs::create_dir_all(&new_cycle).unwrap();
    fs::write(old_cycle.join("lead024.nc"), []).unwrap();
    fs::write(new_cycle.join("lead012.nc"), []).unwrap();
    fs::write(new_cycle.join("lead024.nc"), []).unwrap();

    assert_eq!(
        default_forecast_hour_for_archive_with_root(&root, ModelId::Aifs, "20160822", None),
        Some(12)
    );
    assert_eq!(
        available_leads_for_cycle_with_root(
            &root,
            ModelId::Aifs,
            &CycleSpec::new("20160822", 6).unwrap()
        ),
        vec![12, 24]
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn dewpoint_q_roundtrip_is_reasonable() {
    let pressure = 100_000.0;
    let dewpoint = 293.15;
    let q = specific_humidity_from_dewpoint(pressure, dewpoint);
    let decoded = dewpoint_from_specific_humidity(pressure, q);
    assert!((decoded - dewpoint).abs() < 0.01);
}

#[test]
fn earth2_total_precipitation_tp_units_are_render_ready() {
    assert_eq!(Transform::MetersToMillimeters.apply(0.012), 12.0);
}

#[test]
fn ensemble_member_stats_are_computed_cellwise() {
    let values = vec![
        1.0, 10.0, 100.0, //
        3.0, 14.0, 106.0,
    ];
    assert_eq!(
        compute_member_stat(&values, 2, 3, Earth2EnsembleStat::Mean),
        vec![2.0, 12.0, 103.0]
    );
    assert_eq!(
        compute_member_stat(&values, 2, 3, Earth2EnsembleStat::Std),
        vec![1.0, 2.0, 3.0]
    );
    assert_eq!(
        compute_member_stat(&values, 2, 3, Earth2EnsembleStat::Min),
        vec![1.0, 10.0, 100.0]
    );
    assert_eq!(
        compute_member_stat(&values, 2, 3, Earth2EnsembleStat::Max),
        vec![3.0, 14.0, 106.0]
    );
}

#[test]
fn percentile_stat_uses_nearest_rank_on_sorted_members() {
    let mut values = vec![30.0, 10.0, 20.0, 40.0, 50.0];
    assert_eq!(percentile_nearest_rank(&mut values, 0.10), 10.0);
    let mut values = vec![30.0, 10.0, 20.0, 40.0, 50.0];
    assert_eq!(percentile_nearest_rank(&mut values, 0.50), 30.0);
    let mut values = vec![30.0, 10.0, 20.0, 40.0, 50.0];
    assert_eq!(percentile_nearest_rank(&mut values, 0.90), 50.0);
}
