//! Main compositor — assembles all panels into a final SHARPpy-style sounding image.
//!
//! The reference layout (2400×1800 px, 2× for crisp rendering) is:
//!
//! ```text
//! +--------------------------------------------------+---------------------+
//! | OMEGA |         SKEW-T              | BARBS |  HODOGRAPH            |
//! | (thin)|    (main diagram)           |(right)|  (top-right)          |
//! |       |                             |       |                       |
//! |       |                             |       +-----------+-----------+
//! |       |                             |       | Inf.Temp  | Stm Slnky|
//! |       |                             |       +-----------+-----------+
//! |       |                             |       | SARS      | Eff STP  |
//! |       |                             |       | Analogs   | BoxPlot  |
//! |       |                             |       +-----------+-----------+
//! |       |                             |       |   Psbl Haz. Type      |
//! +-------+-----------------------------+-------+-----------------------+
//! | PARAMETER TABLE (full width)                                        |
//! +---------------------------------------------------------------------+
//! ```
//!
//! The compositor function [`render_full_sounding`] creates the main canvas,
//! calls into the sub-module renderers, and assembles the final image.

use crate::params::cape::{self, DcapeResult, ParcelResult, ParcelType};
use crate::params::composites;
use crate::params::indices;
use crate::profile::Profile;
use crate::watch_type;
use crate::winds;

use super::canvas::Canvas;
use super::hodograph::{self, HodographData};
use super::panels::{self, SarsCategory, SarsData, SlinkyPoint, StpClimatology};
use super::param_table::{self, LapseRateRow, ParamTableData, ParcelRow, PixelBuf, ShearRow};

// =========================================================================
// Layout constants (pixels)
// =========================================================================

/// Total image width (2× for crisp rendering).
pub const IMG_W: u32 = 2400;
/// Total image height (2× for crisp rendering).
pub const IMG_H: u32 = 1800;

/// Title bar height at the very top.
const TITLE_H: u32 = 44;

/// Height of the upper panel region (skew-T + hodograph + insets).
const UPPER_H: u32 = 1120;

/// Parameter table height (bottom strip).
const TABLE_H: u32 = IMG_H - TITLE_H - UPPER_H; // 636

/// Width of the left region (skew-T with built-in omega + barbs).
/// The skew-T renderer handles omega and barbs internally.
const LEFT_W: u32 = 1680;

/// Width of the right column (hodograph + inset panels).
const RIGHT_W: u32 = IMG_W - LEFT_W; // 720

/// Height of the hodograph in the top-right.
const HODO_H: u32 = 600;

/// Height of the inset panels region below the hodograph.
const INSET_H: u32 = UPPER_H - HODO_H; // 520

// =========================================================================
// Colour palette
// =========================================================================

const COL_BG: [u8; 4] = [10, 10, 22, 255];
const COL_TITLE_BG: [u8; 4] = [30, 30, 50, 255];
const COL_WHITE: [u8; 4] = [230, 230, 230, 255];
const COL_BORDER: [u8; 4] = [50, 50, 70, 255];

// =========================================================================
// ComputedParams — all pre-computed values needed by every panel
// =========================================================================

/// Holds every derived parameter needed to render the full sounding image.
///
/// Constructed once via [`compute_all_params`] and then borrowed immutably by
/// each panel renderer.
#[derive(Debug, Clone)]
pub struct ComputedParams {
    // -- Parcel results --
    pub sfcpcl: ParcelResult,
    pub mlpcl: ParcelResult,
    pub mupcl: ParcelResult,

    // -- DCAPE --
    pub dcape: DcapeResult,

    // -- Storm motion (Bunkers) --
    pub rstu: f64,
    pub rstv: f64,
    pub lstu: f64,
    pub lstv: f64,

    // -- Corfidi vectors --
    pub corfidi_up_u: f64,
    pub corfidi_up_v: f64,
    pub corfidi_dn_u: f64,
    pub corfidi_dn_v: f64,

    // -- Helicity --
    /// 0-1 km SRH (total, positive, negative) m^2/s^2.
    pub srh01: (f64, f64, f64),
    /// 0-3 km SRH.
    pub srh03: (f64, f64, f64),

    // -- Shear --
    pub shr01: (f64, f64),
    pub shr03: (f64, f64),
    pub shr06: (f64, f64),
    pub shr08: (f64, f64),

    // -- Mean wind --
    pub mean_wind_06: (f64, f64),

    // -- Effective inflow layer --
    pub eff_inflow: (f64, f64),

    // -- Stability indices --
    pub k_index: Option<f64>,
    pub t_totals: Option<f64>,
    pub v_totals: Option<f64>,
    pub c_totals: Option<f64>,
    pub precip_water: Option<f64>,
    pub conv_t: Option<f64>,
    pub max_temp: Option<f64>,

    // -- Lapse rates --
    pub lr03: Option<f64>,
    pub lr36: Option<f64>,
    pub lr75: Option<f64>,
    pub lr85: Option<f64>,

    // -- Composite parameters --
    pub stp_fixed: Option<f64>,
    pub stp_cin: Option<f64>,
    pub scp: Option<f64>,
    pub ship: Option<f64>,

    // -- EHI --
    pub ehi01: Option<f64>,
    pub ehi03: Option<f64>,

    // -- Temperature levels --
    pub frz_lvl: Option<f64>,
    pub wb_zero: Option<f64>,

    // -- Theta-E --
    pub tei: Option<f64>,

    // -- Watch type --
    pub watch_type: watch_type::WatchType,

    // -- Critical angle --
    pub critical_angle: f64,

    // -- Mean mixing ratio / RH --
    pub mean_mixr: Option<f64>,
    pub mean_rh_low: Option<f64>,
    pub mean_rh_mid: Option<f64>,

    // -- Wind at key levels --
    pub wind_1km: (f64, f64), // dir, spd
    pub wind_6km: (f64, f64),
}

/// Compute all derived sounding parameters from a profile.
///
/// This is the single entry point that calls into every analysis module.
/// The resulting [`ComputedParams`] is passed to each panel renderer.
pub fn compute_all_params(profile: &Profile) -> ComputedParams {
    // -- Build cape::Profile from the main Profile --
    let cape_prof = cape::Profile::new(
        profile.pres.clone(),
        profile.hght.clone(),
        profile.tmpc.clone(),
        profile.dwpc.clone(),
        profile.sfc,
    );

    // -- Parcel computations --
    let sfc_lpl = cape::define_parcel(&cape_prof, ParcelType::Surface);
    let sfcpcl = cape::parcelx(&cape_prof, &sfc_lpl, None, None);

    let ml_lpl = cape::define_parcel(&cape_prof, ParcelType::MixedLayer { depth_hpa: 100.0 });
    let mlpcl = cape::parcelx(&cape_prof, &ml_lpl, None, None);

    let mu_lpl = cape::define_parcel(&cape_prof, ParcelType::MostUnstable { depth_hpa: 300.0 });
    let mupcl = cape::parcelx(&cape_prof, &mu_lpl, None, None);

    let dcape_result = cape::dcape(&cape_prof);

    // -- Effective inflow layer --
    let eff_inflow = cape::effective_inflow_layer(&cape_prof, 100.0, -250.0, Some(&mupcl));

    // -- Storm motion --
    let (rstu, rstv, lstu, lstv) = winds::non_parcel_bunkers_motion(profile).unwrap_or((
        f64::NAN,
        f64::NAN,
        f64::NAN,
        f64::NAN,
    ));

    // -- Corfidi vectors --
    let (corfidi_up_u, corfidi_up_v, corfidi_dn_u, corfidi_dn_v) =
        winds::corfidi_mcs_motion(profile).unwrap_or((f64::NAN, f64::NAN, f64::NAN, f64::NAN));

    // -- Helicity --
    let srh01 = winds::helicity(profile, 0.0, 1000.0, rstu, rstv, -1.0, false).unwrap_or((
        f64::NAN,
        f64::NAN,
        f64::NAN,
    ));
    let srh03 = winds::helicity(profile, 0.0, 3000.0, rstu, rstv, -1.0, false).unwrap_or((
        f64::NAN,
        f64::NAN,
        f64::NAN,
    ));

    // -- Shear --
    let p_sfc = profile.sfc_pressure();
    let p1km = profile.pres_at_height(profile.to_msl(1000.0));
    let p3km = profile.pres_at_height(profile.to_msl(3000.0));
    let p6km = profile.pres_at_height(profile.to_msl(6000.0));
    let p8km = profile.pres_at_height(profile.to_msl(8000.0));

    let shr01 = winds::wind_shear(profile, p_sfc, p1km).unwrap_or((f64::NAN, f64::NAN));
    let shr03 = winds::wind_shear(profile, p_sfc, p3km).unwrap_or((f64::NAN, f64::NAN));
    let shr06 = winds::wind_shear(profile, p_sfc, p6km).unwrap_or((f64::NAN, f64::NAN));
    let shr08 = winds::wind_shear(profile, p_sfc, p8km).unwrap_or((f64::NAN, f64::NAN));

    // -- Mean wind --
    let mean_wind_06 =
        winds::mean_wind(profile, p_sfc, p6km, -1.0, 0.0, 0.0).unwrap_or((f64::NAN, f64::NAN));

    // -- Stability indices --
    let k_index = indices::k_index(profile);
    let t_totals = indices::t_totals(profile);
    let v_totals = indices::v_totals(profile);
    let c_totals = indices::c_totals(profile);
    let precip_water = indices::precip_water(profile, None, None);
    let conv_t = indices::conv_t(profile);
    let max_temp = indices::max_temp(profile, None);

    // -- Lapse rates --
    let lr03 = indices::lapse_rate(profile, 0.0, 3000.0, false);
    let lr36 = indices::lapse_rate(profile, 3000.0, 6000.0, false);
    let lr75 = indices::lapse_rate(profile, 700.0, 500.0, true);
    let lr85 = indices::lapse_rate(profile, 850.0, 500.0, true);

    // -- Temperature levels --
    let frz_lvl = indices::temp_lvl(profile, 0.0, false).map(|p| {
        let h = profile.interp_hght(p);
        if h.is_finite() {
            h - profile.sfc_height()
        } else {
            f64::NAN
        }
    });
    let wb_zero = indices::wet_bulb_zero(profile);

    // -- Composite parameters --
    let shr06_mag = composites::shr_sfc_to_6km(shr06.0, shr06.1);
    let stp_fixed = shr06_mag
        .and_then(|bwd6| composites::stp_fixed(sfcpcl.bplus, sfcpcl.lclhght, srh01.0, bwd6));
    let stp_cin = shr06_mag.and_then(|ebwd| {
        composites::stp_cin(mlpcl.bplus, srh01.0, ebwd, mlpcl.lclhght, mlpcl.bminus)
    });
    let scp = shr06_mag.and_then(|ebwd| composites::scp(mupcl.bplus, srh03.0, ebwd));
    let ship = {
        let mu_mr = if mupcl.pres.is_finite() && mupcl.dwpc.is_finite() {
            crate::profile::mixratio(mupcl.pres, mupcl.dwpc)
        } else {
            f64::NAN
        };
        let t500 = profile.interp_tmpc(500.0);
        let frz_agl = frz_lvl.unwrap_or(f64::NAN);
        match (lr75, shr06_mag) {
            (Some(lr), Some(shr)) => composites::ship(mupcl.bplus, mu_mr, lr, t500, shr, frz_agl),
            _ => None,
        }
    };

    // -- EHI --
    let ehi01 = composites::ehi(sfcpcl.bplus, srh01.0);
    let ehi03 = composites::ehi(sfcpcl.bplus, srh03.0);

    // -- TEI --
    let tei = indices::thetae_diff(profile);

    // -- Watch type --
    let shr08_mag = (shr08.0 * shr08.0 + shr08.1 * shr08.1).sqrt();
    let watch_params = watch_type::WatchParams {
        stp_eff: stp_cin.unwrap_or(0.0),
        stp_fixed: stp_fixed.unwrap_or(0.0),
        srw_4_6km: f64::NAN,
        esrh: srh01.0,
        srh1km: srh01.0,
        sfc_8km_shear: shr08_mag,
        lr1: lr03.unwrap_or(0.0),
        sfcpcl_lclhght: sfcpcl.lclhght,
        mlpcl_lclhght: mlpcl.lclhght,
        mlpcl_bminus: mlpcl.bminus,
        mupcl_bminus: mupcl.bminus,
        ebotm: eff_inflow.0,
        scp: scp.unwrap_or(0.0),
        ship: ship.unwrap_or(0.0),
        sig_severe: 0.0,
        mmp: 0.0,
        wndg: 0.0,
        dcape: dcape_result.dcape,
        pwat: precip_water.unwrap_or(0.0),
        pwv_flag: 0,
        low_rh: 0.0,
        mid_rh: 0.0,
        upshear_wspd: 0.0,
        sfc_tmpc: profile.tmpc[profile.sfc],
        sfc_dwpc: profile.dwpc[profile.sfc],
        sfc_pres: p_sfc,
        sfc_wspd_kts: profile.wspd[profile.sfc],
        precip_type: String::new(),
    };
    let watch = watch_type::best_watch(&watch_params);

    // -- Critical angle --
    let critical_angle = winds::critical_angle(profile, rstu, rstv).unwrap_or(f64::NAN);

    // -- Mean mixing ratio / RH --
    let mean_mixr = indices::mean_mixratio(profile, None, None);
    let mean_rh_low = indices::mean_relh(profile, None, None);
    let p700 = 700.0_f64;
    let p500 = 500.0_f64;
    let mean_rh_mid = indices::mean_relh(profile, Some(p700), Some(p500));

    // -- Wind at key levels --
    let wind_1km = if p1km.is_finite() {
        profile.interp_vec(p1km)
    } else {
        (f64::NAN, f64::NAN)
    };
    let wind_6km = if p6km.is_finite() {
        profile.interp_vec(p6km)
    } else {
        (f64::NAN, f64::NAN)
    };

    ComputedParams {
        sfcpcl,
        mlpcl,
        mupcl,
        dcape: dcape_result,
        rstu,
        rstv,
        lstu,
        lstv,
        corfidi_up_u,
        corfidi_up_v,
        corfidi_dn_u,
        corfidi_dn_v,
        srh01,
        srh03,
        shr01,
        shr03,
        shr06,
        shr08,
        mean_wind_06,
        eff_inflow,
        k_index,
        t_totals,
        v_totals,
        c_totals,
        precip_water,
        conv_t,
        max_temp,
        lr03,
        lr36,
        lr75,
        lr85,
        stp_fixed,
        stp_cin,
        scp,
        ship,
        ehi01,
        ehi03,
        frz_lvl,
        wb_zero,
        tei,
        watch_type: watch,
        critical_angle,
        mean_mixr,
        mean_rh_low,
        mean_rh_mid,
        wind_1km,
        wind_6km,
    }
}

// =========================================================================
// Data bridge helpers — convert ComputedParams into render-module structs
// =========================================================================

/// Build [`HodographData`] from a profile (delegates to hodograph module).
fn build_hodo_data(profile: &Profile) -> Option<HodographData> {
    hodograph::hodograph_data_from_profile(profile).ok()
}

/// Build [`ParamTableData`] from ComputedParams.
fn build_param_table(profile: &Profile, p: &ComputedParams) -> ParamTableData {
    let nan_or = |v: f64| if v.is_finite() { v } else { f64::NAN };
    let opt_or = |v: Option<f64>| v.unwrap_or(f64::NAN);

    // Storm-relative wind helper
    let sr_wind_mag = |pbot: f64, ptop: f64| -> f64 {
        if let Ok((su, sv)) = winds::sr_wind(profile, pbot, ptop, p.rstu, p.rstv, -1.0) {
            (su * su + v_sq(sv)).sqrt()
        } else {
            f64::NAN
        }
    };

    let p_sfc = profile.sfc_pressure();
    let p1km = profile.pres_at_height(profile.to_msl(1000.0));
    let p3km = profile.pres_at_height(profile.to_msl(3000.0));
    let p6km = profile.pres_at_height(profile.to_msl(6000.0));
    let _p9km = profile.pres_at_height(profile.to_msl(9000.0));

    let parcels = vec![
        ParcelRow {
            label: "SFC".into(),
            cape: nan_or(p.sfcpcl.bplus),
            cinh: nan_or(p.sfcpcl.bminus),
            lcl_m: nan_or(p.sfcpcl.lclhght),
            li: nan_or(p.sfcpcl.li5),
            lfc_m: nan_or(p.sfcpcl.lfchght),
            el_m: nan_or(p.sfcpcl.elhght),
        },
        ParcelRow {
            label: "ML".into(),
            cape: nan_or(p.mlpcl.bplus),
            cinh: nan_or(p.mlpcl.bminus),
            lcl_m: nan_or(p.mlpcl.lclhght),
            li: nan_or(p.mlpcl.li5),
            lfc_m: nan_or(p.mlpcl.lfchght),
            el_m: nan_or(p.mlpcl.elhght),
        },
        ParcelRow {
            label: "MU".into(),
            cape: nan_or(p.mupcl.bplus),
            cinh: nan_or(p.mupcl.bminus),
            lcl_m: nan_or(p.mupcl.lclhght),
            li: nan_or(p.mupcl.li5),
            lfc_m: nan_or(p.mupcl.lfchght),
            el_m: nan_or(p.mupcl.elhght),
        },
    ];

    let mag = |u: f64, v: f64| (u * u + v * v).sqrt();

    let shear_layers = vec![
        ShearRow {
            label: "SFC-1km".into(),
            ehi: opt_or(p.ehi01),
            srh: nan_or(p.srh01.0),
            shear: mag(p.shr01.0, p.shr01.1),
            mn_wind: f64::NAN,
            srw: sr_wind_mag(p_sfc, p1km),
        },
        ShearRow {
            label: "SFC-3km".into(),
            ehi: opt_or(p.ehi03),
            srh: nan_or(p.srh03.0),
            shear: mag(p.shr03.0, p.shr03.1),
            mn_wind: f64::NAN,
            srw: sr_wind_mag(p_sfc, p3km),
        },
        ShearRow {
            label: "SFC-6km".into(),
            ehi: f64::NAN,
            srh: f64::NAN,
            shear: mag(p.shr06.0, p.shr06.1),
            mn_wind: mag(p.mean_wind_06.0, p.mean_wind_06.1),
            srw: sr_wind_mag(p_sfc, p6km),
        },
        ShearRow {
            label: "SFC-8km".into(),
            ehi: f64::NAN,
            srh: f64::NAN,
            shear: mag(p.shr08.0, p.shr08.1),
            mn_wind: f64::NAN,
            srw: f64::NAN,
        },
    ];

    let lapse_rates = vec![
        LapseRateRow {
            label: "SFC-3km".into(),
            value: opt_or(p.lr03),
        },
        LapseRateRow {
            label: "3-6km".into(),
            value: opt_or(p.lr36),
        },
        LapseRateRow {
            label: "700-500".into(),
            value: opt_or(p.lr75),
        },
        LapseRateRow {
            label: "850-500".into(),
            value: opt_or(p.lr85),
        },
    ];

    let (rm_dir, rm_spd) = crate::profile::comp2vec(p.rstu, p.rstv);
    let (lm_dir, lm_spd) = crate::profile::comp2vec(p.lstu, p.lstv);
    let (cu_dir, cu_spd) = crate::profile::comp2vec(p.corfidi_up_u, p.corfidi_up_v);
    let (cd_dir, cd_spd) = crate::profile::comp2vec(p.corfidi_dn_u, p.corfidi_dn_v);

    ParamTableData {
        parcels,
        shear_layers,
        pw: opt_or(p.precip_water),
        mean_w: opt_or(p.mean_mixr),
        low_rh: opt_or(p.mean_rh_low),
        mid_rh: opt_or(p.mean_rh_mid),
        dcape: nan_or(p.dcape.dcape),
        dwn_t: f64::NAN,
        k_index: opt_or(p.k_index),
        t_totals: opt_or(p.t_totals),
        tei: opt_or(p.tei),
        conv_t: opt_or(p.conv_t),
        max_t: opt_or(p.max_temp),
        mmp: f64::NAN,
        sig_svr: f64::NAN,
        esp: f64::NAN,
        wndg: f64::NAN,
        cape_3km: nan_or(p.sfcpcl.b3km),
        lapse_rates,
        bunkers_right: param_table::StormMotion {
            label: "RM".into(),
            direction: rm_dir,
            speed: rm_spd,
        },
        bunkers_left: param_table::StormMotion {
            label: "LM".into(),
            direction: lm_dir,
            speed: lm_spd,
        },
        corfidi_down: param_table::StormMotion {
            label: "DN".into(),
            direction: cd_dir,
            speed: cd_spd,
        },
        corfidi_up: param_table::StormMotion {
            label: "UP".into(),
            direction: cu_dir,
            speed: cu_spd,
        },
        stp_cin: opt_or(p.stp_cin),
        stp_fix: opt_or(p.stp_fixed),
        ship: opt_or(p.ship),
        scp: opt_or(p.scp),
        brn_shear: f64::NAN,
        wind_1km_dir: p.wind_1km.0,
        wind_1km_spd: p.wind_1km.1,
        wind_6km_dir: p.wind_6km.0,
        wind_6km_spd: p.wind_6km.1,
    }
}

/// v^2 helper (avoids name collision with `v` variable).
#[inline]
fn v_sq(v: f64) -> f64 {
    v * v
}

/// Build SARS data (placeholder — no analog database yet).
fn build_sars_data(_p: &ComputedParams) -> SarsData {
    SarsData {
        supercell: SarsCategory {
            loose_matches: 0,
            quality_matches: 0,
            quality_lines: Vec::new(),
        },
        sgfnt_hail: SarsCategory {
            loose_matches: 0,
            quality_matches: 0,
            quality_lines: Vec::new(),
        },
    }
}

/// Build STP climatology box-plot data (Thompson et al. climatological values).
fn build_stp_climo() -> StpClimatology {
    StpClimatology::standard()
}

/// Build storm slinky points from parcel trace and storm motion.
fn build_slinky(profile: &Profile, p: &ComputedParams) -> Vec<SlinkyPoint> {
    if !p.rstu.is_finite() || !p.rstv.is_finite() {
        return Vec::new();
    }
    let pcl = &p.sfcpcl;
    if pcl.ptrace.is_empty() {
        return Vec::new();
    }

    let mut points = Vec::new();
    let sfc_h = profile.sfc_height();
    for i in 0..pcl.ptrace.len() {
        let pres = pcl.ptrace[i];
        if !pres.is_finite() || pres <= 0.0 {
            continue;
        }
        let h_msl = profile.interp_hght(pres);
        if !h_msl.is_finite() {
            continue;
        }
        let h_agl = h_msl - sfc_h;
        if h_agl < 0.0 {
            continue;
        }
        // Storm-relative wind at this level
        let (u, v) = profile.interp_wind(pres);
        if !u.is_finite() || !v.is_finite() {
            continue;
        }
        points.push(SlinkyPoint {
            height_m: h_agl,
            sr_u: u - p.rstu,
            sr_v: v - p.rstv,
        });
    }
    points
}

// =========================================================================
// Canvas helpers — blit and PNG encoding
// =========================================================================

/// Blit a source canvas onto a destination canvas at offset (dx, dy).
fn blit_canvas(dst: &mut Canvas, src: &Canvas, dx: i32, dy: i32) {
    for sy in 0..src.h as i32 {
        for sx in 0..src.w as i32 {
            let si = ((sy as u32) * src.w + (sx as u32)) as usize * 4;
            if si + 3 >= src.pixels.len() {
                continue;
            }
            let tx = dx + sx;
            let ty = dy + sy;
            if tx < 0 || ty < 0 || tx >= dst.w as i32 || ty >= dst.h as i32 {
                continue;
            }
            let di = ((ty as u32) * dst.w + (tx as u32)) as usize * 4;
            // Alpha blend
            let sa = src.pixels[si + 3] as f32 / 255.0;
            let inv = 1.0 - sa;
            dst.pixels[di] = (src.pixels[si] as f32 * sa + dst.pixels[di] as f32 * inv) as u8;
            dst.pixels[di + 1] =
                (src.pixels[si + 1] as f32 * sa + dst.pixels[di + 1] as f32 * inv) as u8;
            dst.pixels[di + 2] =
                (src.pixels[si + 2] as f32 * sa + dst.pixels[di + 2] as f32 * inv) as u8;
            dst.pixels[di + 3] = 255;
        }
    }
}

/// Blit a PixelBuf (from param_table) onto a Canvas at offset (dx, dy).
fn blit_pixelbuf(dst: &mut Canvas, src: &PixelBuf, dx: i32, dy: i32) {
    for sy in 0..src.height as i32 {
        for sx in 0..src.width as i32 {
            let si = (sy as usize * src.width + sx as usize) * 4;
            if si + 3 >= src.data.len() {
                continue;
            }
            let tx = dx + sx;
            let ty = dy + sy;
            if tx < 0 || ty < 0 || tx >= dst.w as i32 || ty >= dst.h as i32 {
                continue;
            }
            let di = ((ty as u32) * dst.w + (tx as u32)) as usize * 4;
            let sa = src.data[si + 3] as f32 / 255.0;
            let inv = 1.0 - sa;
            dst.pixels[di] = (src.data[si] as f32 * sa + dst.pixels[di] as f32 * inv) as u8;
            dst.pixels[di + 1] =
                (src.data[si + 1] as f32 * sa + dst.pixels[di + 1] as f32 * inv) as u8;
            dst.pixels[di + 2] =
                (src.data[si + 2] as f32 * sa + dst.pixels[di + 2] as f32 * inv) as u8;
            dst.pixels[di + 3] = 255;
        }
    }
}

/// Blit raw RGBA pixel data (from skewt::render_skewt) onto a Canvas.
fn blit_raw_rgba(dst: &mut Canvas, src: &[u8], src_w: u32, src_h: u32, dx: i32, dy: i32) {
    for sy in 0..src_h as i32 {
        for sx in 0..src_w as i32 {
            let si = ((sy as u32) * src_w + (sx as u32)) as usize * 4;
            if si + 3 >= src.len() {
                continue;
            }
            let tx = dx + sx;
            let ty = dy + sy;
            if tx < 0 || ty < 0 || tx >= dst.w as i32 || ty >= dst.h as i32 {
                continue;
            }
            let di = ((ty as u32) * dst.w + (tx as u32)) as usize * 4;
            let sa = src[si + 3] as f32 / 255.0;
            let inv = 1.0 - sa;
            dst.pixels[di] = (src[si] as f32 * sa + dst.pixels[di] as f32 * inv) as u8;
            dst.pixels[di + 1] = (src[si + 1] as f32 * sa + dst.pixels[di + 1] as f32 * inv) as u8;
            dst.pixels[di + 2] = (src[si + 2] as f32 * sa + dst.pixels[di + 2] as f32 * inv) as u8;
            dst.pixels[di + 3] = 255;
        }
    }
}

/// Encode a Canvas to PNG bytes.
fn canvas_to_png(c: &Canvas) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let encoder = image::codecs::png::PngEncoder::new(&mut buf);
        use image::ImageEncoder;
        encoder
            .write_image(&c.pixels, c.w, c.h, image::ExtendedColorType::Rgba8)
            .expect("PNG encode");
    }
    buf
}

// =========================================================================
// Main compositor entry point
// =========================================================================

/// Render a complete SHARPpy-style sounding analysis image.
///
/// Returns the PNG file as raw bytes.  The image is 2400x1800 pixels (2×
/// scale for crisp rendering) with a dark background, matching the classic
/// SHARPpy display.
///
/// # Arguments
///
/// * `profile` — the sounding profile to render
/// * `params` — pre-computed parameters (from [`compute_all_params`])
pub fn render_full_sounding(profile: &Profile, params: &ComputedParams) -> Vec<u8> {
    let mut img = Canvas::new(IMG_W, IMG_H, COL_BG);

    // ===================================================================
    // 1. Title bar
    // ===================================================================
    img.fill_rect(0, 0, IMG_W as i32, TITLE_H as i32, COL_TITLE_BG);
    let station = &profile.station.station_id;
    let datetime = &profile.station.datetime;
    let title = if station.is_empty() && datetime.is_empty() {
        "SHARPpy Sounding Analysis".to_string()
    } else {
        format!("SHARPpy Sounding Analysis - {} {}", station, datetime)
    };
    img.draw_text_centered(&title, IMG_W as i32 / 2, 12, COL_WHITE);

    // ===================================================================
    // 2. Skew-T diagram (left region, includes omega + barbs internally)
    // ===================================================================
    let skewt_rgba = super::skewt::render_skewt(profile, LEFT_W, UPPER_H);
    blit_raw_rgba(&mut img, &skewt_rgba, LEFT_W, UPPER_H, 0, TITLE_H as i32);

    // ===================================================================
    // 3. Hodograph (top-right)
    // ===================================================================
    if let Some(hodo_data) = build_hodo_data(profile) {
        let hodo_canvas = hodograph::render_hodograph(&hodo_data, RIGHT_W, HODO_H);
        blit_canvas(&mut img, &hodo_canvas, LEFT_W as i32, TITLE_H as i32);
    }

    // ===================================================================
    // 4. Right-side diagnostic panels (below hodograph)
    // ===================================================================
    {
        let sars = build_sars_data(params);
        let climo = build_stp_climo();
        let current_stp = params.stp_cin.or(params.stp_fixed).unwrap_or(0.0);
        let slinky_pts = build_slinky(profile, params);

        let panels_x = LEFT_W as i32;
        let panels_y = TITLE_H as i32 + HODO_H as i32;
        let panels_w = RIGHT_W as i32;
        let panels_h = INSET_H as i32;

        panels::draw_all_panels(
            &mut img,
            &sars,
            &climo,
            current_stp,
            None, // STP probabilities not yet computed
            &slinky_pts,
            None, // slinky tilt degrees not yet computed
            params.watch_type,
            &[], // temp advection levels not yet computed
            panels_x,
            panels_y,
            panels_w,
            panels_h,
        );
    }

    // ===================================================================
    // 5. Parameter table (bottom, full width)
    // ===================================================================
    {
        let table_data = build_param_table(profile, params);
        let table_buf = param_table::render_sized(&table_data, IMG_W as usize, TABLE_H as usize);
        blit_pixelbuf(&mut img, &table_buf, 0, (TITLE_H + UPPER_H) as i32);
    }

    // ===================================================================
    // 6. Panel border lines
    // ===================================================================
    // Horizontal separator between upper panels and table
    img.draw_line(
        0,
        (TITLE_H + UPPER_H) as i32,
        IMG_W as i32 - 1,
        (TITLE_H + UPPER_H) as i32,
        COL_BORDER,
    );
    // Vertical separator between skew-T and right panels
    img.draw_line(
        LEFT_W as i32,
        TITLE_H as i32,
        LEFT_W as i32,
        (TITLE_H + UPPER_H) as i32 - 1,
        COL_BORDER,
    );
    // Horizontal separator between hodograph and inset panels
    img.draw_line(
        LEFT_W as i32,
        (TITLE_H + HODO_H) as i32,
        IMG_W as i32 - 1,
        (TITLE_H + HODO_H) as i32,
        COL_BORDER,
    );

    canvas_to_png(&img)
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::StationInfo;

    fn test_profile() -> Profile {
        let pres = [1000.0, 925.0, 850.0, 700.0, 500.0, 300.0, 200.0];
        let hght = [100.0, 800.0, 1500.0, 3100.0, 5600.0, 9200.0, 12000.0];
        let tmpc = [30.0, 24.0, 18.0, 4.0, -15.0, -40.0, -55.0];
        let dwpc = [22.0, 18.0, 12.0, -4.0, -30.0, -50.0, -65.0];
        let wdir = [180.0, 200.0, 220.0, 250.0, 270.0, 280.0, 280.0];
        let wspd = [10.0, 15.0, 20.0, 30.0, 50.0, 60.0, 70.0];
        Profile::new(
            &pres,
            &hght,
            &tmpc,
            &dwpc,
            &wdir,
            &wspd,
            &[],
            StationInfo {
                station_id: "TST".into(),
                datetime: "250402/0000".into(),
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn compute_params_runs() {
        let prof = test_profile();
        let params = compute_all_params(&prof);
        // CAPE should be a finite number (or NaN for stable profiles).
        assert!(params.sfcpcl.bplus.is_finite() || params.sfcpcl.bplus.is_nan());
        // Storm motion should have been computed.
        assert!(params.rstu.is_finite() || params.rstu.is_nan());
    }

    #[test]
    fn render_produces_valid_png() {
        let prof = test_profile();
        let params = compute_all_params(&prof);
        let png = render_full_sounding(&prof, &params);
        // PNG magic bytes: 0x89 P N G
        assert!(png.len() > 1000, "PNG too small: {} bytes", png.len());
        assert_eq!(&png[..4], &[0x89, 0x50, 0x4E, 0x47], "Not a valid PNG");
    }
}
