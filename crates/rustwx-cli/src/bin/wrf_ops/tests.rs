use super::*;

fn common_args() -> WrfPlanCommonArgs {
    WrfPlanCommonArgs {
        project_name: "ops-test".to_string(),
        init: WrfInitSource::Hrrr,
        start_utc: "1974-04-03T09:00:00Z".to_string(),
        end_utc: "1974-04-03T12:00:00Z".to_string(),
        west: Some(-99.0),
        east: Some(-91.0),
        south: Some(33.0),
        north: Some(39.0),
        center_lat: None,
        center_lon: None,
        radius_km: None,
        width_km: None,
        height_km: None,
        resolution: ResolutionArg::Default3km,
        inner_dx_m: None,
        parent_ratio: 3,
        nested: true,
        history_interval_minutes: 1,
        output_3d_interval_minutes: Some(5),
        num_cores: 4,
        physics: PhysicsArg::SevereConvection,
        plot_preset: PlotPresetArg::FullDerived,
        num_metgrid_levels: None,
        num_metgrid_soil_levels: None,
        wps_products: None,
        geog_data_path: "/home/drew/weather/wrf/WRF_BUILD/WPS_GEOG".to_string(),
        wrf_build_path: "/home/drew/weather/wrf/WRF_BUILD".to_string(),
        rustwx_bin_dir: "/home/drew/weather/apps/rustwx/target/release".to_string(),
    }
}

fn temp_project_dir(name: &str) -> PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{name}-{}-{stamp}", std::process::id()))
}

#[test]
fn create_project_writes_operational_scripts() {
    let args = common_args();
    let plan = build_plan(&args).unwrap();
    let dir = temp_project_dir("rustwx-wrf-ops");
    write_project_files(&dir, &plan, &args).unwrap();

    let pipeline = fs::read_to_string(dir.join("run_pipeline.sh")).unwrap();
    assert!(pipeline.contains("\"$RUSTWX_BIN/wrf_ops\" stage-gribs"));
    assert!(pipeline.contains("STAGE_SOURCE"));
    assert!(pipeline.contains("--forecast-hours"));
    assert!(pipeline.contains("run_wps.sh"));
    assert!(pipeline.contains("run_real.sh"));
    assert!(pipeline.contains("run_wrf.sh"));
    assert!(pipeline.contains("plot_wrfout.sh"));

    let status = fs::read_to_string(dir.join("status.sh")).unwrap();
    assert!(status.contains("wrf_ops_plan.json"));
    assert!(status.contains("wrfout"));
    assert!(status.contains("wrf_ops\" status"));
    let dashboard = fs::read_to_string(dir.join("dashboard.sh")).unwrap();
    assert!(dashboard.contains("wrf_ops\" dashboard"));
    let plot = fs::read_to_string(dir.join("plot_wrfout.sh")).unwrap();
    assert!(plot.contains("PLOT_PRESET"));
    assert!(plot.contains("full-derived"));
    assert!(plot.contains(DEFAULT_NON_ECAPE_SEVERE_DERIVED_RECIPES));

    for script in ["run_wps.sh", "run_real.sh", "run_wrf.sh"] {
        let contents = fs::read_to_string(dir.join(script)).unwrap();
        assert!(contents.contains("/opt/intel/oneapi/setvars.sh"));
    }
    let wps = fs::read_to_string(dir.join("run_wps.sh")).unwrap();
    assert!(wps.contains("prefix                 = 'PRES'"));
    assert!(wps.contains("prefix                 = 'SOIL'"));
    assert!(wps.contains("fg_name                = 'PRES', 'SOIL'"));
    assert!(wps.contains("Vtable.raphrrr"));
    for script in ["run_real.sh", "run_wrf.sh"] {
        let contents = fs::read_to_string(dir.join(script)).unwrap();
        assert!(contents.contains("rm -f namelist.input"));
        assert!(contents.contains("cp \"$PROJECT_DIR/namelist.input\" namelist.input"));
        assert!(contents.contains("ulimit -s unlimited"));
        assert!(contents.contains("KMP_STACKSIZE"));
    }

    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn common_args_can_build_centered_domain() {
    let mut args = common_args();
    args.west = None;
    args.east = None;
    args.south = None;
    args.north = None;
    args.center_lat = Some(40.1);
    args.center_lon = Some(-95.6);
    args.radius_km = Some(350.0);
    let plan = build_plan(&args).unwrap();

    assert!((plan.request.bounds.center_lat() - 40.1).abs() < 1e-9);
    assert!((plan.request.bounds.center_lon() + 95.6).abs() < 1e-9);
    assert!((plan.request.bounds.width_m() / 1_000.0 - 700.0).abs() < 1.0);
}

#[test]
fn common_args_can_override_to_custom_nested_resolution() {
    let mut args = common_args();
    args.inner_dx_m = Some(750);
    args.parent_ratio = 3;
    let plan = build_plan(&args).unwrap();

    assert_eq!(plan.domains[1].dx_m, 750.0);
    assert_eq!(plan.domains[1].parent_grid_ratio, 3);
    assert_eq!((plan.domains[1].e_we - 1) % 3, 0);
    assert_eq!((plan.domains[1].e_sn - 1) % 3, 0);
}

#[test]
fn doctor_checks_report_required_and_optional_paths() {
    let dir = temp_project_dir("rustwx-wrf-doctor");
    let wrf = dir.join("WRF/main");
    let wps_tables = dir.join("WPS/ungrib/Variable_Tables");
    let geog = dir.join("WPS_GEOG/topo_gmted2010_30s");
    let bin = dir.join("bin");
    fs::create_dir_all(&wrf).unwrap();
    fs::create_dir_all(&wps_tables).unwrap();
    fs::create_dir_all(&geog).unwrap();
    fs::create_dir_all(&bin).unwrap();
    fs::create_dir_all(dir.join("projects")).unwrap();
    for path in [
        wrf.join("wrf.exe"),
        wrf.join("real.exe"),
        wrf.join("ndown.exe"),
        dir.join("WPS/geogrid.exe"),
        dir.join("WPS/ungrib.exe"),
        dir.join("WPS/metgrid.exe"),
        dir.join("WPS/link_grib.csh"),
        wps_tables.join("Vtable.GFS"),
        wps_tables.join("Vtable.ECMWF"),
        geog.join("index"),
        bin.join("wrf_ops"),
    ] {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, "").unwrap();
    }

    let args = DoctorArgs {
        wrf_build_path: dir.clone(),
        geog_data_path: dir.join("WPS_GEOG"),
        rustwx_bin_dir: bin,
        projects_dir: dir.join("projects"),
        json: true,
    };
    let checks = doctor_checks(&args);

    assert!(checks.iter().any(|check| {
        check.kind == "rustwx-bin" && check.target.ends_with("wrf_ops") && check.ok
    }));
    assert!(checks.iter().any(|check| {
        check.kind == "rustwx-bin"
            && check.target.ends_with("wrf_local_showcase")
            && !check.required
            && !check.ok
    }));
    assert!(checks.iter().any(|check| {
        check.kind == "wps-ungrib-runtime" && check.target.contains("libgfortran") && check.required
    }));
    assert!(checks.iter().any(|check| {
        check.kind == "wrf-runtime" && check.target.contains("libimf") && check.required
    }));

    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn launch_env_pairs_make_dry_run_and_skip_modes_explicit() {
    let args = LaunchArgs {
        project_dir: PathBuf::from("/tmp/ops-test"),
        tmux_session: None,
        foreground: false,
        dry_run: true,
        skip_stage_gribs: true,
        skip_wps: true,
        skip_real: false,
        skip_wrf: true,
        plot: true,
        plot_preset: Some(PlotPresetArg::ReflOnly),
        overwrite_gribs: true,
        source: Some(SourceId::Aws),
        cache_dir: Some(PathBuf::from("/tmp/rustwx-cache")),
        products: vec!["prs".to_string(), "sfc".to_string()],
        forecast_hours: vec![0, 1],
        cycle_utc: Some("2026-05-17T15:00:00Z".to_string()),
    };
    let pairs = pipeline_env_pairs(&args);

    assert!(pairs.contains(&("DRY_RUN", "1".to_string())));
    assert!(pairs.contains(&("STAGE_GRIBS", "0".to_string())));
    assert!(pairs.contains(&("RUN_WPS", "0".to_string())));
    assert!(pairs.contains(&("RUN_REAL", "1".to_string())));
    assert!(pairs.contains(&("RUN_WRF", "0".to_string())));
    assert!(pairs.contains(&("PLOT", "1".to_string())));
    assert!(pairs.contains(&("PLOT_PRESET", "refl-only".to_string())));
    assert!(pairs.contains(&("OVERWRITE_GRIBS", "1".to_string())));
    assert!(pairs.contains(&("STAGE_SOURCE", "aws".to_string())));
    assert!(pairs.contains(&("STAGE_CACHE_DIR", "/tmp/rustwx-cache".to_string())));
    assert!(pairs.contains(&("STAGE_PRODUCTS", "prs,sfc".to_string())));
    assert!(pairs.contains(&("STAGE_FORECAST_HOURS", "0,1".to_string())));
    assert!(pairs.contains(&("STAGE_CYCLE_UTC", "2026-05-17T15:00:00Z".to_string())));
}

#[test]
fn recipe_command_contains_create_launch_and_status_steps() {
    let args = common_args();
    let command = recipe_create_command(
        "/home/drew/weather/apps/rustwx/target/release/wrf_ops",
        &args,
        Path::new("/home/drew/weather/wrf/projects/ops-test"),
    )
    .unwrap();

    assert!(command.contains(" create "));
    assert!(command.contains("--project-name 'ops-test'"));
    assert!(command.contains("--init 'hrrr'"));
    assert!(command.contains("--resolution default3km"));
    assert!(command.contains("--output-3d-interval-minutes 5"));
    assert!(command.contains("--plot-preset full-derived"));
    assert!(command.contains("--project-dir '/home/drew/weather/wrf/projects/ops-test'"));
}

#[test]
fn bootstrap_script_prepares_rust_and_rustwx_binaries() {
    let script = normalize_script_lf(&intel_bootstrap_script(
        "/home/drew/weather/wrf",
        "4.6.1",
        "4.6.0",
    ));

    assert!(!script.contains('\r'));
    assert!(script.contains("rustup.rs"));
    assert!(script.contains("RUSTWX_DIR"));
    assert!(script.contains("cargo build --release -p rustwx-cli --bin wrf_ops"));
    assert!(script.contains("cargo build --release -p rustwx-cli --bin wrf_local_showcase"));
    assert!(script.contains("wrf_ops\" doctor"));
}
