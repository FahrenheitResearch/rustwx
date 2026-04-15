mod ecape;
mod error;
mod severe;

pub use ecape::{
    EcapeFields, EcapeFieldsWithFailureMask, EcapeGridInputs, EcapeOptions, EcapeTripletFields,
    EcapeTripletFieldsWithFailureMask, EcapeTripletOptions, EcapeVolumeInputs, SurfaceInputs,
    VolumeShape, compute_ecape, compute_ecape_from_parts, compute_ecape_triplet,
    compute_ecape_triplet_from_parts, compute_ecape_triplet_with_failure_mask,
    compute_ecape_triplet_with_failure_mask_from_parts, compute_ecape_with_failure_mask,
    compute_ecape_with_failure_mask_from_parts,
};
pub use error::CalcError;
pub use rustwx_core::GridShape;
pub use severe::{
    BulkRichardsonInputs, CapeCinOutputs, EffectiveScpInputs, EffectiveSevereInputs,
    EffectiveSevereOutputs, EffectiveStpInputs, FixedStpInputs, ScpEhiInputs, ScpEhiOutputs,
    ShipInputs, SupportedSevereFields, WindGridInputs, compute_bri, compute_cape_cin,
    compute_effective_severe, compute_ehi, compute_scp, compute_scp_effective, compute_scp_ehi,
    compute_shear, compute_ship, compute_srh, compute_stp, compute_stp_effective,
    compute_stp_fixed, compute_supported_severe_fields, critical_angle,
    significant_tornado_parameter, supercell_composite_parameter,
};

#[cfg(test)]
mod tests {
    use crate::{
        BulkRichardsonInputs, CalcError, EcapeGridInputs, EcapeOptions, EffectiveSevereInputs,
        GridShape, ScpEhiInputs, ShipInputs, VolumeShape, WindGridInputs, compute_bri,
        compute_ecape, compute_effective_severe, compute_scp_ehi, compute_ship, compute_stp,
        significant_tornado_parameter,
    };

    #[test]
    fn ecape_wrapper_rejects_bad_lengths() {
        let inputs = EcapeGridInputs {
            shape: VolumeShape::new(GridShape::new(1, 1).unwrap(), 2).unwrap(),
            pressure_3d_pa: &[100000.0],
            temperature_3d_c: &[20.0],
            qvapor_3d_kgkg: &[0.01],
            height_agl_3d_m: &[50.0],
            u_3d_ms: &[10.0],
            v_3d_ms: &[5.0],
            psfc_pa: &[100500.0],
            t2_k: &[298.0],
            q2_kgkg: &[0.012],
            u10_ms: &[10.0],
            v10_ms: &[5.0],
        };

        let err = compute_ecape(inputs, &EcapeOptions::default()).unwrap_err();
        assert!(matches!(
            err,
            CalcError::LengthMismatch {
                field: "pressure_pa",
                ..
            }
        ));
    }

    #[test]
    fn stp_grid_wrapper_matches_expected_length() {
        let grid = GridShape::new(2, 1).unwrap();
        let stp = compute_stp(
            grid,
            &[1500.0, 2000.0],
            &[1000.0, 1200.0],
            &[150.0, 200.0],
            &[20.0, 25.0],
        )
        .unwrap();
        assert_eq!(stp.len(), 2);
        assert!(stp[1] > stp[0]);
    }

    #[test]
    fn point_stp_wrapper_is_positive_for_favorable_environment() {
        let stp = significant_tornado_parameter(2500.0, 900.0, 250.0, 22.0);
        assert!(stp > 0.0);
    }

    #[test]
    fn wind_wrapper_fixture_is_shape_consistent() {
        let wind = WindGridInputs {
            shape: VolumeShape::new(GridShape::new(2, 1).unwrap(), 2).unwrap(),
            u_3d_ms: &[10.0, 12.0, 18.0, 20.0],
            v_3d_ms: &[5.0, 6.0, 12.0, 14.0],
            height_agl_3d_m: &[50.0, 50.0, 1500.0, 1500.0],
        };
        assert_eq!(wind.shape.len3d(), wind.u_3d_ms.len());
    }

    #[test]
    fn ship_grid_wrapper_is_positive_for_favorable_hail_environment() {
        let grid = GridShape::new(1, 1).unwrap();
        let ship = compute_ship(ShipInputs {
            grid,
            mucape_jkg: &[2000.0],
            shear_6km_ms: &[20.0],
            temperature_500c: &[-15.0],
            lapse_rate_700_500_cpkm: &[7.0],
            mixing_ratio_500_gkg: &[10.0],
        })
        .unwrap();

        assert_eq!(ship, vec![1.0]);
    }

    #[test]
    fn bri_grid_wrapper_zeroes_degenerate_brn_shear() {
        let grid = GridShape::new(1, 1).unwrap();
        let bri = compute_bri(BulkRichardsonInputs {
            grid,
            cape_jkg: &[1000.0],
            brn_shear_ms: &[0.1],
        })
        .unwrap();

        assert_eq!(bri, vec![0.0]);
    }

    #[test]
    fn effective_severe_wrapper_returns_both_effective_products() {
        let grid = GridShape::new(1, 1).unwrap();
        let outputs = compute_effective_severe(EffectiveSevereInputs {
            grid,
            mlcape_jkg: &[1500.0],
            mlcin_jkg: &[-50.0],
            ml_lcl_m: &[1000.0],
            mucape_jkg: &[3000.0],
            effective_srh_m2s2: &[150.0],
            effective_bulk_wind_difference_ms: &[20.0],
        })
        .unwrap();

        assert_eq!(outputs.stp_effective, vec![1.0]);
        assert_eq!(outputs.scp_effective, vec![9.0]);
    }

    #[test]
    fn scp_ehi_wrapper_returns_both_products() {
        let grid = GridShape::new(1, 1).unwrap();
        let outputs = compute_scp_ehi(ScpEhiInputs {
            grid,
            scp_cape_jkg: &[3000.0],
            scp_srh_m2s2: &[150.0],
            scp_bulk_wind_difference_ms: &[20.0],
            ehi_cape_jkg: &[2000.0],
            ehi_srh_m2s2: &[200.0],
        })
        .unwrap();

        assert_eq!(outputs.scp, vec![9.0]);
        assert_eq!(outputs.ehi, vec![2.5]);
    }
}
