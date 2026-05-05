// This file is concatenated onto main.rs at build time via include!().

fn upload_conus(
    ctx: &ContextHandle,
    vol: &ConusVolume,
    slab: &ConusSlab,
) -> Result<ConusDevice> {
    let n = CONUS_NX * CONUS_NY;
    Ok(ConusDevice {
        pressure:    upload_pinned(ctx, &vol.pressure)?,
        t_3d:        upload_pinned(ctx, &vol.t)?,
        t850:        upload_pinned(ctx, &slab.t850)?,
        t700:        upload_pinned(ctx, &slab.t700)?,
        t500:        upload_pinned(ctx, &slab.t500)?,
        td850:       upload_pinned(ctx, &slab.td850)?,
        td700:       upload_pinned(ctx, &slab.td700)?,
        u500:        upload_pinned(ctx, &slab.u500)?,
        v500:        upload_pinned(ctx, &slab.v500)?,
        height500:   upload_pinned(ctx, &slab.height500)?,
        sbcape:      upload_pinned(ctx, &slab.sbcape)?,
        mucape:      upload_pinned(ctx, &slab.mucape)?,
        sblcl:       upload_pinned(ctx, &slab.sblcl)?,
        srh_1km:     upload_pinned(ctx, &slab.srh_1km)?,
        srh_3km:     upload_pinned(ctx, &slab.srh_3km)?,
        shear_06:    upload_pinned(ctx, &slab.shear_06)?,
        z1000:       upload_pinned(ctx, &slab.z1000)?,
        z700:        upload_pinned(ctx, &slab.z700)?,
        dx:          upload_pinned(ctx, &slab.dx)?,
        dy:          upload_pinned(ctx, &slab.dy)?,
        lat:         upload_pinned(ctx, &slab.lat)?,
        p500_const:  upload_pinned(ctx, &vec![500.0_f64; n])?,
        p850_const:  upload_pinned(ctx, &vec![850.0_f64; n])?,
        mr_const:    upload_pinned(ctx, &vec![0.001_f64; n])?,
    })
}

struct RegionDevice {
    nx: usize, ny: usize,
    t_3d: CudaSlice<f64>,
    pressure: CudaSlice<f64>,
    t850: CudaSlice<f64>, t700: CudaSlice<f64>, t500: CudaSlice<f64>,
    td850: CudaSlice<f64>, td700: CudaSlice<f64>,
    u500: CudaSlice<f64>, v500: CudaSlice<f64>,
    height500: CudaSlice<f64>,
    sbcape: CudaSlice<f64>, mucape: CudaSlice<f64>, sblcl: CudaSlice<f64>,
    srh_1km: CudaSlice<f64>, srh_3km: CudaSlice<f64>,
    shear_06: CudaSlice<f64>,
    z1000: CudaSlice<f64>, z700: CudaSlice<f64>,
    dx: CudaSlice<f64>, dy: CudaSlice<f64>,
    lat: CudaSlice<f64>,
    p500_const: CudaSlice<f64>, p850_const: CudaSlice<f64>, mr_const: CudaSlice<f64>,
    out_a: CudaSlice<f64>, out_b: CudaSlice<f64>,
}

fn alloc_region(ctx: &ContextHandle, r: &Region) -> Result<RegionDevice> {
    let n = r.nx * r.ny;
    let alloc_n = || ctx.stream().alloc_zeros::<f64>(n);
    Ok(RegionDevice {
        nx: r.nx, ny: r.ny,
        t_3d: ctx.stream().alloc_zeros::<f64>(NZ * n)?,
        pressure: ctx.stream().alloc_zeros::<f64>(NZ)?,
        t850: alloc_n()?, t700: alloc_n()?, t500: alloc_n()?,
        td850: alloc_n()?, td700: alloc_n()?,
        u500: alloc_n()?, v500: alloc_n()?,
        height500: alloc_n()?,
        sbcape: alloc_n()?, mucape: alloc_n()?, sblcl: alloc_n()?,
        srh_1km: alloc_n()?, srh_3km: alloc_n()?,
        shear_06: alloc_n()?,
        z1000: alloc_n()?, z700: alloc_n()?,
        dx: alloc_n()?, dy: alloc_n()?,
        lat: alloc_n()?,
        p500_const: alloc_n()?, p850_const: alloc_n()?, mr_const: alloc_n()?,
        out_a: alloc_n()?, out_b: alloc_n()?,
    })
}

fn crop_2d(
    ctx: &ContextHandle,
    src: &CudaSlice<f64>,
    dst: &mut CudaSlice<f64>,
    r: &Region,
) -> Result<()> {
    rustwx_cuda_grid::crop_2d::launch_device(
        ctx, src, dst, CONUS_NX, r.nx, r.ny, r.off_x, r.off_y,
    )
}

fn crop_all_to_region(
    ctx: &ContextHandle,
    conus: &ConusDevice,
    rd: &mut RegionDevice,
    r: &Region,
) -> Result<()> {
    crop_2d(ctx, &conus.t850,      &mut rd.t850,      r)?;
    crop_2d(ctx, &conus.t700,      &mut rd.t700,      r)?;
    crop_2d(ctx, &conus.t500,      &mut rd.t500,      r)?;
    crop_2d(ctx, &conus.td850,     &mut rd.td850,     r)?;
    crop_2d(ctx, &conus.td700,     &mut rd.td700,     r)?;
    crop_2d(ctx, &conus.u500,      &mut rd.u500,      r)?;
    crop_2d(ctx, &conus.v500,      &mut rd.v500,      r)?;
    crop_2d(ctx, &conus.height500, &mut rd.height500, r)?;
    crop_2d(ctx, &conus.sbcape,    &mut rd.sbcape,    r)?;
    crop_2d(ctx, &conus.mucape,    &mut rd.mucape,    r)?;
    crop_2d(ctx, &conus.sblcl,     &mut rd.sblcl,     r)?;
    crop_2d(ctx, &conus.srh_1km,   &mut rd.srh_1km,   r)?;
    crop_2d(ctx, &conus.srh_3km,   &mut rd.srh_3km,   r)?;
    crop_2d(ctx, &conus.shear_06,  &mut rd.shear_06,  r)?;
    crop_2d(ctx, &conus.z1000,     &mut rd.z1000,     r)?;
    crop_2d(ctx, &conus.z700,      &mut rd.z700,      r)?;
    crop_2d(ctx, &conus.dx,        &mut rd.dx,        r)?;
    crop_2d(ctx, &conus.dy,        &mut rd.dy,        r)?;
    crop_2d(ctx, &conus.lat,       &mut rd.lat,       r)?;
    crop_2d(ctx, &conus.p500_const,&mut rd.p500_const,r)?;
    crop_2d(ctx, &conus.p850_const,&mut rd.p850_const,r)?;
    crop_2d(ctx, &conus.mr_const,  &mut rd.mr_const,  r)?;
    rustwx_cuda_grid::crop_2d::launch_device_3d(
        ctx, &conus.t_3d, &mut rd.t_3d,
        CONUS_NX, CONUS_NY, r.nx, r.ny, NZ, r.off_x, r.off_y,
    )?;
    ctx.stream().memcpy_dtod(&conus.pressure, &mut rd.pressure)?;
    Ok(())
}

struct RegionCpu {
    nx: usize, ny: usize,
    t_3d: Vec<f64>,
    pressure: Vec<f64>,
    t850: Vec<f64>, t700: Vec<f64>, t500: Vec<f64>,
    td850: Vec<f64>, td700: Vec<f64>,
    u500: Vec<f64>, v500: Vec<f64>,
    height500: Vec<f64>,
    sbcape: Vec<f64>, mucape: Vec<f64>, sblcl: Vec<f64>,
    srh_1km: Vec<f64>, srh_3km: Vec<f64>,
    shear_06: Vec<f64>,
    z1000: Vec<f64>, z700: Vec<f64>,
    lat: Vec<f64>,
    p500_const: Vec<f64>, p850_const: Vec<f64>, mr_const: Vec<f64>,
}

fn build_region_cpu(vol: &ConusVolume, slab: &ConusSlab, r: &Region) -> RegionCpu {
    let n = r.nx * r.ny;
    let mut t_3d = Vec::with_capacity(NZ * n);
    let conus_slab_size = CONUS_NX * CONUS_NY;
    for k in 0..NZ {
        let lvl = &vol.t[k * conus_slab_size .. (k + 1) * conus_slab_size];
        for j in 0..r.ny {
            let row = (j + r.off_y) * CONUS_NX + r.off_x;
            t_3d.extend_from_slice(&lvl[row .. row + r.nx]);
        }
    }
    RegionCpu {
        nx: r.nx, ny: r.ny,
        t_3d,
        pressure: vol.pressure.clone(),
        t850: cpu_crop_2d(&slab.t850, r),
        t700: cpu_crop_2d(&slab.t700, r),
        t500: cpu_crop_2d(&slab.t500, r),
        td850: cpu_crop_2d(&slab.td850, r),
        td700: cpu_crop_2d(&slab.td700, r),
        u500: cpu_crop_2d(&slab.u500, r),
        v500: cpu_crop_2d(&slab.v500, r),
        height500: cpu_crop_2d(&slab.height500, r),
        sbcape: cpu_crop_2d(&slab.sbcape, r),
        mucape: cpu_crop_2d(&slab.mucape, r),
        sblcl: cpu_crop_2d(&slab.sblcl, r),
        srh_1km: cpu_crop_2d(&slab.srh_1km, r),
        srh_3km: cpu_crop_2d(&slab.srh_3km, r),
        shear_06: cpu_crop_2d(&slab.shear_06, r),
        z1000: cpu_crop_2d(&slab.z1000, r),
        z700: cpu_crop_2d(&slab.z700, r),
        lat: cpu_crop_2d(&slab.lat, r),
        p500_const: vec![500.0_f64; n],
        p850_const: vec![850.0_f64; n],
        mr_const: vec![0.001_f64; n],
    }
}

struct PipelineOpts {
    save_maps: bool,
    map_dir: PathBuf,
}

fn run_pipeline(
    ctx: &ContextHandle,
    cpu_in: &RegionCpu,
    rd: &mut RegionDevice,
    opts: &PipelineOpts,
    out: &mut Vec<Record>,
) -> Result<()> {
    let nx = rd.nx;
    let ny = rd.ny;
    let n = nx * ny;

    let mut save_a = |ctx: &ContextHandle, rd: &RegionDevice, name: &str| -> Result<()> {
        if opts.save_maps {
            let v = ctx.stream().memcpy_dtov(&rd.out_a)?;
            let path = opts.map_dir.join(format!("{name}.png"));
            let _ = viz::save_png(&path, &v, nx, ny);
        }
        Ok(())
    };

    macro_rules! gpu {
        ($call:expr) => {{
            let t = Instant::now();
            { $call }
            ctx.synchronize()?;
            t.elapsed()
        }};
    }

    // ----- vertical interpolation -----
    let levels = &cpu_in.pressure;
    for &(name, target) in &[
        ("vinterp_t850", 850.0_f64),
        ("vinterp_t700", 700.0),
        ("vinterp_t500", 500.0),
        ("vinterp_t250", 250.0),
    ] {
        let (cpu, _) = time_cpu(|| cpu_regrid::interpolate_vertical(
            &cpu_in.t_3d, levels, target, cpu_in.nx, cpu_in.ny, NZ, true,
        ));
        let bracket = (1..NZ).find(|&k| levels[k] <= target).map(|k| (k - 1, k));
        let gpu_dt = if let Some((k0, k1)) = bracket {
            let l0 = levels[k0]; let l1 = levels[k1];
            let weight = if l0 > 0.0 && l1 > 0.0 && target > 0.0 {
                (target.ln() - l0.ln()) / (l1.ln() - l0.ln())
            } else { (target - l0) / (l1 - l0) };
            gpu!(rustwx_cuda_grid::interpolate_vertical::launch_device(
                ctx, &rd.t_3d, &mut rd.out_a, n, k0 * n, k1 * n, weight,
            )?)
        } else { Duration::ZERO };
        save_a(ctx, rd, name)?;
        out.push(Record { product: name, category: Cat::VInterp, cpu, gpu: gpu_dt });
    }

    // ----- severe -----
    let (cpu, _) = time_cpu(|| cpu_comp::compute_stp(&cpu_in.sbcape, &cpu_in.sblcl, &cpu_in.srh_1km, &cpu_in.shear_06));
    let g = gpu!(rustwx_cuda_severe::stp::launch_device(ctx, &rd.sbcape, &rd.sblcl, &rd.srh_1km, &rd.shear_06, &mut rd.out_a, n)?);
    save_a(ctx, rd, "stp")?;
    out.push(Record { product: "stp", category: Cat::Severe, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| cpu_comp::compute_scp(&cpu_in.mucape, &cpu_in.srh_3km, &cpu_in.shear_06));
    let g = gpu!(rustwx_cuda_severe::scp::launch_device(ctx, &rd.mucape, &rd.srh_3km, &rd.shear_06, &mut rd.out_a, n)?);
    save_a(ctx, rd, "scp")?;
    out.push(Record { product: "scp", category: Cat::Severe, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| cpu_comp::compute_ehi(&cpu_in.sbcape, &cpu_in.srh_1km));
    let g = gpu!(rustwx_cuda_severe::ehi::launch_device(ctx, &rd.sbcape, &rd.srh_1km, &mut rd.out_a, n)?);
    save_a(ctx, rd, "ehi")?;
    out.push(Record { product: "ehi", category: Cat::Severe, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| {
        (0..n).into_par_iter()
            .map(|i| cpu_comp::k_index(cpu_in.t850[i], cpu_in.t700[i], cpu_in.t500[i], cpu_in.td850[i], cpu_in.td700[i]))
            .collect::<Vec<_>>()
    });
    let g = gpu!(rustwx_cuda_severe::k_index::launch_device(ctx, &rd.t850, &rd.t700, &rd.t500, &rd.td850, &rd.td700, &mut rd.out_a, n)?);
    save_a(ctx, rd, "k_index")?;
    out.push(Record { product: "k_index", category: Cat::Severe, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| {
        (0..n).into_par_iter()
            .map(|i| cpu_comp::total_totals(cpu_in.t850[i], cpu_in.t500[i], cpu_in.td850[i]))
            .collect::<Vec<_>>()
    });
    let g = gpu!(rustwx_cuda_severe::total_totals::launch_device(ctx, &rd.t850, &rd.t500, &rd.td850, &mut rd.out_a, n)?);
    save_a(ctx, rd, "total_totals")?;
    out.push(Record { product: "total_totals", category: Cat::Severe, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| {
        (0..n).into_par_iter()
            .map(|i| cpu_comp::cross_totals(cpu_in.td850[i], cpu_in.t500[i]))
            .collect::<Vec<_>>()
    });
    let g = gpu!(rustwx_cuda_severe::cross_totals::launch_device(ctx, &rd.td850, &rd.t500, &mut rd.out_a, n)?);
    save_a(ctx, rd, "cross_totals")?;
    out.push(Record { product: "cross_totals", category: Cat::Severe, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| {
        (0..n).into_par_iter()
            .map(|i| cpu_comp::vertical_totals(cpu_in.t850[i], cpu_in.t500[i]))
            .collect::<Vec<_>>()
    });
    let g = gpu!(rustwx_cuda_severe::vertical_totals::launch_device(ctx, &rd.t850, &rd.t500, &mut rd.out_a, n)?);
    save_a(ctx, rd, "vertical_totals")?;
    out.push(Record { product: "vertical_totals", category: Cat::Severe, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| {
        (0..n).into_par_iter()
            .map(|i| cpu_comp::boyden_index(cpu_in.z1000[i], cpu_in.z700[i], cpu_in.t700[i]))
            .collect::<Vec<_>>()
    });
    let g = gpu!(rustwx_cuda_severe::boyden_index::launch_device(ctx, &rd.z1000, &rd.z700, &rd.t700, &mut rd.out_a, n)?);
    save_a(ctx, rd, "boyden_index")?;
    out.push(Record { product: "boyden_index", category: Cat::Severe, cpu, gpu: g });

    // ----- stencils -----
    let (cpu, _) = time_cpu(|| cpu_dyn::vorticity(&cpu_in.u500, &cpu_in.v500, nx, ny, HRRR_DX_M, HRRR_DX_M));
    let g = gpu!(rustwx_cuda_grid::vorticity::launch_device(ctx, &rd.u500, &rd.v500, &rd.dx, &rd.dy, &mut rd.out_a, nx, ny)?);
    save_a(ctx, rd, "vorticity")?;
    out.push(Record { product: "vorticity", category: Cat::Stencil, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| cpu_dyn::divergence(&cpu_in.u500, &cpu_in.v500, nx, ny, HRRR_DX_M, HRRR_DX_M));
    let g = gpu!(rustwx_cuda_grid::divergence::launch_device(ctx, &rd.u500, &rd.v500, &rd.dx, &rd.dy, &mut rd.out_a, nx, ny)?);
    save_a(ctx, rd, "divergence")?;
    out.push(Record { product: "divergence", category: Cat::Stencil, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| cpu_dyn::absolute_vorticity(&cpu_in.u500, &cpu_in.v500, &cpu_in.lat, nx, ny, HRRR_DX_M, HRRR_DX_M));
    let g = gpu!(rustwx_cuda_grid::absolute_vorticity::launch_device(ctx, &rd.u500, &rd.v500, &rd.dx, &rd.dy, &rd.lat, &mut rd.out_a, nx, ny)?);
    save_a(ctx, rd, "absolute_vorticity")?;
    out.push(Record { product: "absolute_vorticity", category: Cat::Stencil, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| cpu_dyn::total_deformation(&cpu_in.u500, &cpu_in.v500, nx, ny, HRRR_DX_M, HRRR_DX_M));
    let g = gpu!(rustwx_cuda_grid::total_deformation::launch_device(ctx, &rd.u500, &rd.v500, &rd.dx, &rd.dy, &mut rd.out_a, nx, ny)?);
    save_a(ctx, rd, "total_deformation")?;
    out.push(Record { product: "total_deformation", category: Cat::Stencil, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| cpu_dyn::laplacian(&cpu_in.height500, nx, ny, HRRR_DX_M, HRRR_DX_M));
    let g = gpu!(rustwx_cuda_grid::laplacian::launch_device(ctx, &rd.height500, &rd.dx, &rd.dy, &mut rd.out_a, nx, ny)?);
    save_a(ctx, rd, "laplacian")?;
    out.push(Record { product: "laplacian", category: Cat::Stencil, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| cpu_dyn::advection(&cpu_in.t500, &cpu_in.u500, &cpu_in.v500, nx, ny, HRRR_DX_M, HRRR_DX_M));
    let g = gpu!(rustwx_cuda_grid::advection::launch_device(ctx, &rd.t500, &rd.u500, &rd.v500, &rd.dx, &rd.dy, &mut rd.out_a, nx, ny)?);
    save_a(ctx, rd, "advection")?;
    out.push(Record { product: "advection", category: Cat::Stencil, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| cpu_dyn::frontogenesis_2d(&cpu_in.t500, &cpu_in.u500, &cpu_in.v500, nx, ny, HRRR_DX_M, HRRR_DX_M));
    let g = gpu!(rustwx_cuda_grid::frontogenesis::launch_device(ctx, &rd.t500, &rd.u500, &rd.v500, &rd.dx, &rd.dy, &mut rd.out_a, nx, ny)?);
    save_a(ctx, rd, "frontogenesis")?;
    out.push(Record { product: "frontogenesis", category: Cat::Stencil, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| cpu_dyn::q_vector(&cpu_in.t500, &cpu_in.u500, &cpu_in.v500, 500.0, nx, ny, HRRR_DX_M, HRRR_DX_M));
    let g = gpu!(rustwx_cuda_grid::q_vector::launch_device(ctx, &rd.t500, &rd.u500, &rd.v500, 500.0, &rd.dx, &rd.dy, &mut rd.out_a, &mut rd.out_b, nx, ny)?);
    save_a(ctx, rd, "q_vector_q1")?;
    if opts.save_maps {
        let v = ctx.stream().memcpy_dtov(&rd.out_b)?;
        let path = opts.map_dir.join("q_vector_q2.png");
        let _ = viz::save_png(&path, &v, nx, ny);
    }
    out.push(Record { product: "q_vector", category: Cat::Stencil, cpu, gpu: g });

    // ----- thermo -----
    let (cpu, _) = time_cpu(|| {
        (0..n).into_par_iter()
            .map(|i| cpu_thermo::potential_temperature(cpu_in.p500_const[i], cpu_in.t500[i]))
            .collect::<Vec<_>>()
    });
    let g = gpu!(rustwx_cuda_thermo::potential_temperature::launch_device(ctx, &rd.p500_const, &rd.t500, &mut rd.out_a, n)?);
    save_a(ctx, rd, "theta_500")?;
    out.push(Record { product: "theta_500", category: Cat::Thermo, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| {
        (0..n).into_par_iter()
            .map(|i| cpu_thermo::saturation_mixing_ratio(cpu_in.p850_const[i], cpu_in.t850[i]))
            .collect::<Vec<_>>()
    });
    let g = gpu!(rustwx_cuda_thermo::saturation_mixing_ratio::launch_device(ctx, &rd.p850_const, &rd.t850, &mut rd.out_a, n)?);
    save_a(ctx, rd, "sat_mixr_850")?;
    out.push(Record { product: "sat_mixr_850", category: Cat::Thermo, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| {
        (0..n).into_par_iter()
            .map(|i| cpu_thermo::rh_from_dewpoint(cpu_in.t850[i], cpu_in.td850[i]))
            .collect::<Vec<_>>()
    });
    let g = gpu!(rustwx_cuda_thermo::relative_humidity_from_dewpoint::launch_device(ctx, &rd.t850, &rd.td850, &mut rd.out_a, n)?);
    save_a(ctx, rd, "rh_850")?;
    out.push(Record { product: "rh_850", category: Cat::Thermo, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| {
        (0..n).into_par_iter()
            .map(|i| cpu_thermo::exner_function(cpu_in.p500_const[i]))
            .collect::<Vec<_>>()
    });
    let g = gpu!(rustwx_cuda_thermo::exner_function::launch_device(ctx, &rd.p500_const, &mut rd.out_a, n)?);
    save_a(ctx, rd, "exner_500")?;
    out.push(Record { product: "exner_500", category: Cat::Thermo, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| {
        (0..n).into_par_iter()
            .map(|i| cpu_thermo::density(cpu_in.p500_const[i], cpu_in.t500[i], cpu_in.mr_const[i]))
            .collect::<Vec<_>>()
    });
    let g = gpu!(rustwx_cuda_thermo::density::launch_device(ctx, &rd.p500_const, &rd.t500, &rd.mr_const, &mut rd.out_a, n)?);
    save_a(ctx, rd, "density_500")?;
    out.push(Record { product: "density_500", category: Cat::Thermo, cpu, gpu: g });

    // ----- wind -----
    let (cpu, _) = time_cpu(|| cpu_dyn::wind_speed(&cpu_in.u500, &cpu_in.v500));
    let g = gpu!(rustwx_cuda_wind::wind_speed::launch_device(ctx, &rd.u500, &rd.v500, &mut rd.out_a, n)?);
    save_a(ctx, rd, "wind_speed")?;
    out.push(Record { product: "wind_speed", category: Cat::Wind, cpu, gpu: g });

    let (cpu, _) = time_cpu(|| cpu_dyn::wind_direction(&cpu_in.u500, &cpu_in.v500));
    let g = gpu!(rustwx_cuda_wind::wind_direction::launch_device(ctx, &rd.u500, &rd.v500, &mut rd.out_a, n)?);
    save_a(ctx, rd, "wind_direction")?;
    out.push(Record { product: "wind_direction", category: Cat::Wind, cpu, gpu: g });

    Ok(())
}

fn fmt_ms(d: Duration) -> String { format!("{:>9.1}", d.as_secs_f64() * 1000.0) }
fn fmt_s(d: Duration)  -> String { format!("{:>7.2}s", d.as_secs_f64()) }
fn speedup(cpu: Duration, gpu: Duration) -> f64 {
    if gpu.is_zero() { 0.0 } else { cpu.as_secs_f64() / gpu.as_secs_f64() }
}

fn main() -> Result<()> {
    let ctx = global()?;
    println!("device:  {}", ctx.cuda().name().unwrap_or_else(|_| "unknown".into()));
    println!("threads (rayon): {}", rayon::current_num_threads());
    println!("forecast hours:  {HOURS}");
    println!("vertical levels: {NZ}");

    let map_root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent().unwrap().parent().unwrap()
        .join("bench_output");
    let _ = std::fs::create_dir_all(&map_root);
    println!("maps:            {}", map_root.display());
    println!();

    {
        let n = 64 * 64;
        let scratch: Vec<f64> = vec![1.0; n];
        let _ = rustwx_cuda_severe::stp::host(&ctx, &scratch, &scratch, &scratch, &scratch);
        let _ = rustwx_cuda_grid::vorticity::host(&ctx, &scratch, &scratch, &scratch, &scratch, 64, 64);
        let _ = rustwx_cuda_thermo::potential_temperature::host(&ctx, &scratch, &scratch);
        let _ = rustwx_cuda_wind::wind_speed::host(&ctx, &scratch, &scratch);
        ctx.synchronize().ok();
    }

    let mut all_records: Vec<Record> = Vec::new();
    let mut upload_total = Duration::ZERO;
    let mut crop_total = Duration::ZERO;

    println!("{:<28} {:>10} {:>10} {:>10}  {:>9}",
             "region (nx×ny)", "cells", "cpu_ms", "gpu_ms", "speedup");
    println!("{}", "-".repeat(74));

    for hour in 0..HOURS {
        let vol = build_conus_volume(hour);
        let slab = build_conus_slab(hour);

        let t_upload = Instant::now();
        let conus = upload_conus(&ctx, &vol, &slab)?;
        ctx.synchronize()?;
        upload_total += t_upload.elapsed();

        for r in regions() {
            let mut rd = alloc_region(&ctx, &r)?;

            let t_crop = Instant::now();
            crop_all_to_region(&ctx, &conus, &mut rd, &r)?;
            ctx.synchronize()?;
            transport_total += t_crop.elapsed();

            let cpu_in = build_region_cpu(&vol, &slab, &r);

            let map_dir = map_root.join(format!("h{hour}")).join(r.name);
            let opts = PipelineOpts { save_maps: hour == 0, map_dir };

            let mut records = Vec::with_capacity(32);
            run_pipeline(&ctx, &cpu_in, &mut rd, &opts, &mut records)?;

            let cpu_total: Duration = records.iter().map(|x| x.cpu).sum();
            let gpu_total: Duration = records.iter().map(|x| x.gpu).sum();
            println!(
                "h{} {:<25} {:>10} {} {}  {:>8.1}x",
                hour,
                format!("{} ({}×{})", r.name, r.nx, r.ny),
                r.nx * r.ny,
                fmt_ms(cpu_total),
                fmt_ms(gpu_total),
                speedup(cpu_total, gpu_total),
            );
            all_records.extend(records);
        }
    }

    all_records.push(Record {
        product: "transport (upload + crops)",
        category: Cat::Transport,
        cpu: Duration::ZERO,
        gpu: transport_total,
    });

    let mut by_product: HashMap<&'static str, (Cat, Duration, Duration)> = HashMap::new();
    for r in &all_records {
        let e = by_product.entry(r.product).or_insert((r.category, Duration::ZERO, Duration::ZERO));
        e.1 += r.cpu;
        e.2 += r.gpu;
    }
    let total_cpu: Duration = by_product.values().map(|v| v.1).sum();
    let total_gpu: Duration = by_product.values().map(|v| v.2).sum();

    println!("{}", "-".repeat(74));
    println!(
        "{:<28} {:>10} {} {}  {:>8.1}x",
        "TOTAL",
        format!("{} hrs", HOURS),
        fmt_s(total_cpu),
        fmt_s(total_gpu),
        speedup(total_cpu, total_gpu),
    );

    let mut by_product_vec: Vec<_> = by_product.into_iter()
        .map(|(name, (cat, c, g))| (name, cat, c, g))
        .collect();
    by_product_vec.sort_by(|a, b| b.2.cmp(&a.2));

    println!();
    println!("Per-product breakdown (totaled across 8 regions × {HOURS} hours, sorted by GPU cost):");
    println!("{:<28} {:<14} {:>10} {:>10} {:>9} {:>8}",
             "product", "category", "cpu_ms", "gpu_ms", "speedup", "%cpu");
    println!("{}", "-".repeat(86));
    for (name, cat, cpu, gpu) in &by_product_vec {
        let pct = if total_cpu.is_zero() { 0.0 } else { 100.0 * cpu.as_secs_f64() / total_cpu.as_secs_f64() };
        println!("{:<28} {:<14} {} {}  {:>7.1}x  {:>6.1}%",
            name, cat.name(), fmt_ms(*cpu), fmt_ms(*gpu), speedup(*cpu, *gpu), pct);
    }

    let mut by_cat: HashMap<&'static str, (Duration, Duration)> = HashMap::new();
    for (name, cat, cpu, gpu) in &by_product_vec {
        let _ = name;
        let e = by_cat.entry(cat.name()).or_insert((Duration::ZERO, Duration::ZERO));
        e.0 += *cpu; e.1 += *gpu;
    }
    let mut by_cat_vec: Vec<_> = by_cat.into_iter().collect();
    by_cat_vec.sort_by(|a, b| b.1.0.cmp(&a.1.0));

    println!();
    println!("Per-category breakdown:");
    println!("{:<18} {:>10} {:>10} {:>9}", "category", "cpu_ms", "gpu_ms", "speedup");
    println!("{}", "-".repeat(54));
    for (cat, (cpu, gpu)) in &by_cat_vec {
        println!("{:<18} {} {}  {:>7.1}x", cat, fmt_ms(*cpu), fmt_ms(*gpu), speedup(*cpu, *gpu));
    }

    println!();
    println!("maps written to: {}", map_root.display());
    Ok(())
}
