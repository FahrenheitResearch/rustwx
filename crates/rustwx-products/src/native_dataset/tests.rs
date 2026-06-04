use super::*;

fn case() -> NativeDatasetCase {
    NativeDatasetCase::new(
        "case",
        DateTime::parse_from_rfc3339("2024-05-06T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        24,
    )
}

fn tiles(n: usize) -> Vec<NativeDatasetTile> {
    (0..n)
        .map(|idx| {
            NativeDatasetTile::new(
                format!("tile_{idx:02}"),
                35.0,
                -97.0 + idx as f64,
                NativeDatasetBounds::new(-98.0, -96.0, 34.0, 36.0),
            )
        })
        .collect()
}

#[test]
fn case_frame_times_include_both_endpoints() {
    let times = case().frame_times(1);
    assert_eq!(times.len(), 25);
    assert_eq!(
        times.first().unwrap().to_rfc3339(),
        "2024-05-06T12:00:00+00:00"
    );
    assert_eq!(
        times.last().unwrap().to_rfc3339(),
        "2024-05-07T12:00:00+00:00"
    );
}

#[test]
fn one_day_three_history_one_target_yields_twenty_two_samples_per_tile() {
    let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(1));
    assert_eq!(config.samples_per_complete_tile_case(&config.cases[0]), 22);
    let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
    assert_eq!(plan.expected_frame_jobs, 25);
    assert_eq!(plan.expected_samples, 22);
    assert_eq!(
        plan.shard.sample_windows[0].history_frame_times_utc.len(),
        3
    );
    assert_eq!(
        plan.shard.sample_windows[0].valid_time_utc.to_rfc3339(),
        "2024-05-06T14:00:00+00:00"
    );
    assert_eq!(
        plan.shard.sample_windows[0].target_time_utc.to_rfc3339(),
        "2024-05-06T15:00:00+00:00"
    );
}

#[test]
fn shard_spec_assigns_disjoint_tile_subsets() {
    let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(5));
    let shard0 =
        plan_native_dataset(config.clone(), NativeDatasetShardSpec::new(0, 2).unwrap()).unwrap();
    let shard1 = plan_native_dataset(config, NativeDatasetShardSpec::new(1, 2).unwrap()).unwrap();
    let ids0 = shard0
        .shard
        .tiles
        .iter()
        .map(|tile| tile.tile_id.as_str())
        .collect::<Vec<_>>();
    let ids1 = shard1
        .shard
        .tiles
        .iter()
        .map(|tile| tile.tile_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids0, vec!["tile_00", "tile_02", "tile_04"]);
    assert_eq!(ids1, vec!["tile_01", "tile_03"]);
    assert_eq!(shard0.expected_samples + shard1.expected_samples, 5 * 22);
}

#[test]
fn default_hrrr_multisource_plan_declares_real_source_families() {
    let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(1));
    let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
    let sources = plan.required_source_ids();
    assert!(sources.contains("hrrr_wrfsfc"));
    assert!(sources.contains("mrms"));
    assert!(sources.contains("goes_abi"));
    assert!(sources.contains("nexrad_level2"));
}

#[test]
fn goes_abi_source_can_select_full_disk_or_mesoscale_family() {
    let full_disk = NativeDatasetSource::goes_abi_product("ABI-L2-MCMIPF", ["C08", "C13"]);
    assert_eq!(full_disk.product_family.as_deref(), Some("ABI-L2-MCMIPF"));
    assert_eq!(full_disk.fields, vec!["C08", "C13"]);

    let meso = NativeDatasetSource::goes_abi_product("ABI-L2-MCMIPM1", ["C13"]);
    assert_eq!(meso.product_family.as_deref(), Some("ABI-L2-MCMIPM1"));
}

#[test]
fn hour_jobs_group_frame_jobs_by_case_and_valid_time() {
    let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(2));
    let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
    let hour_jobs = build_native_dataset_hour_jobs(&plan).unwrap();

    assert_eq!(hour_jobs.len(), 25);
    assert_eq!(
        hour_jobs[0].valid_time_utc.to_rfc3339(),
        "2024-05-06T12:00:00+00:00"
    );
    assert_eq!(hour_jobs[0].tile_jobs.len(), 2);
    assert_eq!(hour_jobs[0].bundle.source_keys.len(), 4);
    let tile_ids = hour_jobs[0]
        .tile_jobs
        .iter()
        .map(|job| job.tile_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(tile_ids, vec!["tile_00", "tile_01"]);
}

#[test]
fn dry_run_hour_runner_reports_progress_counters() {
    let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(2));
    let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
    let mut processor = NativeDryRunHourProcessor;
    let mut progress_events = Vec::new();
    let report = run_native_dataset_hour_plan_with_progress(
        &plan,
        &NativeDatasetRunnerConfig {
            max_attempts: 1,
            continue_on_error: true,
        },
        &mut processor,
        |progress| progress_events.push(progress.clone()),
    )
    .unwrap();

    assert_eq!(progress_events.len(), 26);
    assert_eq!(progress_events[0].hours_completed, 0);
    assert_eq!(report.progress.hours_total, 25);
    assert_eq!(report.progress.hours_completed, 25);
    assert_eq!(report.progress.tile_frames_total, 50);
    assert_eq!(report.progress.tile_frames_completed, 50);
    assert_eq!(report.progress.samples_emitted, 44);
    assert_eq!(report.progress.failed_hours, 0);
    assert_eq!(report.progress.failed_tile_frames, 0);
}

#[derive(Default)]
struct FlakyHourProcessor {
    fail_first_hour_once: bool,
    calls: usize,
}

impl NativeHourProcessor for FlakyHourProcessor {
    fn process_hour(
        &mut self,
        _plan: &NativeDatasetPlan,
        job: &NativeDatasetHourJob,
    ) -> Result<NativeHourOutput, String> {
        self.calls += 1;
        if self.fail_first_hour_once && job.job_id.ends_with("20240506_1200") {
            self.fail_first_hour_once = false;
            return Err("transient hour decode error".to_string());
        }
        Ok(NativeHourOutput::empty())
    }
}

#[test]
fn hour_runner_retries_transient_processor_failures() {
    let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(1));
    let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
    let mut processor = FlakyHourProcessor {
        fail_first_hour_once: true,
        calls: 0,
    };
    let report = run_native_dataset_hour_plan(
        &plan,
        &NativeDatasetRunnerConfig {
            max_attempts: 2,
            continue_on_error: true,
        },
        &mut processor,
    )
    .unwrap();

    assert_eq!(report.progress.hours_completed, 25);
    assert_eq!(report.progress.failed_hours, 0);
    assert_eq!(report.jobs[0].attempts, 2);
    assert_eq!(processor.calls, 26);
}

#[derive(Default)]
struct FlakyProcessor {
    fail_first_job_once: bool,
    calls: usize,
}

impl NativeFrameProcessor for FlakyProcessor {
    fn process_frame(
        &mut self,
        _plan: &NativeDatasetPlan,
        job: &NativeDatasetFrameJob,
    ) -> Result<NativeFrameOutput, String> {
        self.calls += 1;
        if self.fail_first_job_once && job.job_id.ends_with("20240506_1200") {
            self.fail_first_job_once = false;
            return Err("transient source error".to_string());
        }
        Ok(NativeFrameOutput {
            artifacts: vec![format!("{}.npz", job.job_id)],
        })
    }
}

#[test]
fn runner_retries_transient_processor_failures() {
    let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(1));
    let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
    let mut processor = FlakyProcessor {
        fail_first_job_once: true,
        calls: 0,
    };
    let report = run_native_dataset_plan(
        &plan,
        &NativeDatasetRunnerConfig {
            max_attempts: 2,
            continue_on_error: true,
        },
        &mut processor,
    )
    .unwrap();
    assert_eq!(report.attempted_frame_jobs, 25);
    assert_eq!(report.succeeded_frame_jobs, 25);
    assert_eq!(report.failed_frame_jobs, 0);
    assert_eq!(report.jobs[0].attempts, 2);
    assert_eq!(processor.calls, 26);
}

struct AlwaysFail;

impl NativeFrameProcessor for AlwaysFail {
    fn process_frame(
        &mut self,
        _plan: &NativeDatasetPlan,
        _job: &NativeDatasetFrameJob,
    ) -> Result<NativeFrameOutput, String> {
        Err("permanent decode failure".to_string())
    }
}

#[test]
fn runner_can_abort_on_first_failure() {
    let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(1));
    let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
    let mut processor = AlwaysFail;
    let err = run_native_dataset_plan(
        &plan,
        &NativeDatasetRunnerConfig {
            max_attempts: 1,
            continue_on_error: false,
        },
        &mut processor,
    )
    .unwrap_err();
    assert!(err.contains("permanent decode failure"));
}
