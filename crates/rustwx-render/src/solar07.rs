use crate::request::{Color, DiscreteColorScale, ExtendMode, ProductSemantics};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Solar07Palette {
    Cape,
    ThreeCape,
    Ehi,
    Srh,
    Stp,
    LapseRate,
    Uh,
    MlMetric,
    Reflectivity,
    Winds,
    Temperature,
    Dewpoint,
    Rh,
    RelVort,
    Advection,
    SimIr,
    GeopotAnomaly,
    Precip,
    ShadedOverlay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Solar07Preset {
    Cape,
    ThreeCape,
    Cin,
    Lcl,
    Lfc,
    El,
    Srh,
    Stp,
    Scp,
    Ehi,
    Uh,
    LapseRate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DerivedScalePreset {
    LiftedIndex,
    TemperatureAdvection,
    BulkShear,
    SurfaceComfort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DerivedProductStyle {
    LiftedIndex,
    TemperatureAdvection700mb,
    TemperatureAdvection850mb,
    BulkShear01km,
    BulkShear06km,
    ApparentTemperature,
    HeatIndex,
    WindChill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Solar07Product {
    Sbcape,
    Mlcape,
    Mucape,
    Sbecape,
    Mlecape,
    Muecape,
    Sbncape,
    Mlncape,
    Muncape,
    Sbcin,
    Mlcin,
    Mucin,
    Sbecin,
    Mlecin,
    Muecin,
    EcapeCape,
    EcapeCin,
    Lcl,
    Lfc,
    El,
    EcapeLfc,
    EcapeEl,
    Srh01km,
    Srh03km,
    Stp,
    StpFixed,
    StpEffective,
    Scp,
    Ehi,
    Uh,
    EcapeScpExperimental,
    EcapeEhiExperimental,
}

pub const SEVERE_CLASSIC_PANEL_PRODUCTS: [Solar07Product; 8] = [
    Solar07Product::Sbcape,
    Solar07Product::Mlcape,
    Solar07Product::Mucape,
    Solar07Product::Mlcin,
    Solar07Product::Srh01km,
    Solar07Product::Srh03km,
    Solar07Product::Stp,
    Solar07Product::Scp,
];

pub const ECAPE_SEVERE_PANEL_PRODUCTS: [Solar07Product; 8] = [
    Solar07Product::Sbecape,
    Solar07Product::Mlecape,
    Solar07Product::Muecape,
    Solar07Product::Sbncape,
    Solar07Product::Sbecin,
    Solar07Product::Mlecin,
    Solar07Product::EcapeScpExperimental,
    Solar07Product::EcapeEhiExperimental,
];

impl Solar07Product {
    pub fn from_product_name(name: &str) -> Option<Self> {
        match normalize(name).as_str() {
            "sbcape" => Some(Self::Sbcape),
            "mlcape" => Some(Self::Mlcape),
            "mucape" => Some(Self::Mucape),
            "sbecape" => Some(Self::Sbecape),
            "mlecape" => Some(Self::Mlecape),
            "muecape" => Some(Self::Muecape),
            "sbncape" => Some(Self::Sbncape),
            "mlncape" => Some(Self::Mlncape),
            "muncape" => Some(Self::Muncape),
            "sbcin" => Some(Self::Sbcin),
            "mlcin" => Some(Self::Mlcin),
            "mucin" => Some(Self::Mucin),
            "sbecin" => Some(Self::Sbecin),
            "mlecin" => Some(Self::Mlecin),
            "muecin" => Some(Self::Muecin),
            "ecape_cape" => Some(Self::EcapeCape),
            "ecape_cin" => Some(Self::EcapeCin),
            "lcl" => Some(Self::Lcl),
            "lfc" => Some(Self::Lfc),
            "el" => Some(Self::El),
            "ecape_lfc" => Some(Self::EcapeLfc),
            "ecape_el" => Some(Self::EcapeEl),
            "srh1" | "srh_0_1km" | "srh01km" => Some(Self::Srh01km),
            "srh3" | "srh_0_3km" | "srh03km" => Some(Self::Srh03km),
            "stp" => Some(Self::Stp),
            "stp_fixed" => Some(Self::StpFixed),
            "stp_effective" => Some(Self::StpEffective),
            "scp" => Some(Self::Scp),
            "ehi" => Some(Self::Ehi),
            "uhel" | "uh" => Some(Self::Uh),
            "ecape_scp" => Some(Self::EcapeScpExperimental),
            "ecape_ehi" => Some(Self::EcapeEhiExperimental),
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::Sbcape => "sbcape",
            Self::Mlcape => "mlcape",
            Self::Mucape => "mucape",
            Self::Sbecape => "sbecape",
            Self::Mlecape => "mlecape",
            Self::Muecape => "muecape",
            Self::Sbncape => "sbncape",
            Self::Mlncape => "mlncape",
            Self::Muncape => "muncape",
            Self::Sbcin => "sbcin",
            Self::Mlcin => "mlcin",
            Self::Mucin => "mucin",
            Self::Sbecin => "sbecin",
            Self::Mlecin => "mlecin",
            Self::Muecin => "muecin",
            Self::EcapeCape => "ecape_cape",
            Self::EcapeCin => "ecape_cin",
            Self::Lcl => "lcl",
            Self::Lfc => "lfc",
            Self::El => "el",
            Self::EcapeLfc => "ecape_lfc",
            Self::EcapeEl => "ecape_el",
            Self::Srh01km => "srh1",
            Self::Srh03km => "srh3",
            Self::Stp => "stp",
            Self::StpFixed => "stp_fixed",
            Self::StpEffective => "stp_effective",
            Self::Scp => "scp",
            Self::Ehi => "ehi",
            Self::Uh => "uhel",
            Self::EcapeScpExperimental => "ecape_scp",
            Self::EcapeEhiExperimental => "ecape_ehi",
        }
    }

    pub fn display_title(self) -> &'static str {
        match self {
            Self::Sbcape => "SBCAPE",
            Self::Mlcape => "MLCAPE",
            Self::Mucape => "MUCAPE",
            Self::Sbecape => "SBECAPE",
            Self::Mlecape => "MLECAPE",
            Self::Muecape => "MUECAPE",
            Self::Sbncape => "SBNCAPE",
            Self::Mlncape => "MLNCAPE",
            Self::Muncape => "MUNCAPE",
            Self::Sbcin => "SBCIN",
            Self::Mlcin => "MLCIN",
            Self::Mucin => "MUCIN",
            Self::Sbecin => "SBECIN",
            Self::Mlecin => "MLECIN",
            Self::Muecin => "MUECIN",
            Self::EcapeCape => "ECAPE CAPE",
            Self::EcapeCin => "ECAPE CIN",
            Self::Lcl => "LCL",
            Self::Lfc => "LFC",
            Self::El => "EL",
            Self::EcapeLfc => "ECAPE LFC",
            Self::EcapeEl => "ECAPE EL",
            Self::Srh01km => "0-1 KM SRH",
            Self::Srh03km => "0-3 KM SRH",
            Self::Stp => "STP",
            Self::StpFixed => "STP (FIXED)",
            Self::StpEffective => "STP (EFFECTIVE)",
            Self::Scp => "SCP",
            Self::Ehi => "EHI",
            Self::Uh => "UH",
            Self::EcapeScpExperimental => "ECAPE SCP (EXP)",
            Self::EcapeEhiExperimental => "ECAPE EHI (EXP)",
        }
    }

    pub fn scale_preset(self) -> Solar07Preset {
        match self {
            Self::Sbcape
            | Self::Mlcape
            | Self::Mucape
            | Self::Sbecape
            | Self::Mlecape
            | Self::Muecape
            | Self::Sbncape
            | Self::Mlncape
            | Self::Muncape
            | Self::EcapeCape => Solar07Preset::Cape,
            Self::Sbcin
            | Self::Mlcin
            | Self::Mucin
            | Self::Sbecin
            | Self::Mlecin
            | Self::Muecin
            | Self::EcapeCin => Solar07Preset::Cin,
            Self::Lcl => Solar07Preset::Lcl,
            Self::Lfc | Self::EcapeLfc => Solar07Preset::Lfc,
            Self::El | Self::EcapeEl => Solar07Preset::El,
            Self::Srh01km | Self::Srh03km => Solar07Preset::Srh,
            Self::Stp | Self::StpFixed | Self::StpEffective => Solar07Preset::Stp,
            Self::Scp | Self::EcapeScpExperimental => Solar07Preset::Scp,
            Self::Ehi | Self::EcapeEhiExperimental => Solar07Preset::Ehi,
            Self::Uh => Solar07Preset::Uh,
        }
    }

    pub fn default_tick_step(self) -> Option<f64> {
        match self.scale_preset() {
            Solar07Preset::Cape => Some(500.0),
            Solar07Preset::ThreeCape => Some(500.0),
            Solar07Preset::Cin => Some(50.0),
            Solar07Preset::Lcl => Some(500.0),
            Solar07Preset::Lfc => Some(500.0),
            Solar07Preset::El => Some(1000.0),
            Solar07Preset::Srh => Some(50.0),
            Solar07Preset::Stp => Some(1.0),
            Solar07Preset::Scp => Some(1.0),
            Solar07Preset::Ehi => Some(0.5),
            Solar07Preset::Uh => Some(20.0),
            Solar07Preset::LapseRate => Some(0.5),
        }
    }

    pub fn semantics(self) -> ProductSemantics {
        if self.is_experimental() {
            ProductSemantics::experimental()
        } else {
            ProductSemantics::operational()
        }
    }

    pub fn is_experimental(self) -> bool {
        matches!(
            self,
            Self::EcapeScpExperimental | Self::EcapeEhiExperimental
        )
    }
}

impl From<Solar07Product> for Solar07Preset {
    fn from(value: Solar07Product) -> Self {
        value.scale_preset()
    }
}

impl DerivedProductStyle {
    pub fn from_product_name(name: &str) -> Option<Self> {
        match normalize(name).as_str() {
            "lifted_index" | "li" | "surface_based_lifted_index" | "sbli" => {
                Some(Self::LiftedIndex)
            }
            "temperature_advection_700mb" | "temp_advection_700mb" | "tadv700" => {
                Some(Self::TemperatureAdvection700mb)
            }
            "temperature_advection_850mb" | "temp_advection_850mb" | "tadv850" => {
                Some(Self::TemperatureAdvection850mb)
            }
            "bulk_shear_0_1km" | "bulk_shear_01km" | "shear_01km" | "shear01km" => {
                Some(Self::BulkShear01km)
            }
            "bulk_shear_0_6km" | "bulk_shear_06km" | "shear_06km" | "shear06km" => {
                Some(Self::BulkShear06km)
            }
            "apparent_temperature" | "apparent_temp" => Some(Self::ApparentTemperature),
            "heat_index" => Some(Self::HeatIndex),
            "wind_chill" => Some(Self::WindChill),
            _ => None,
        }
    }

    pub fn display_title(self) -> &'static str {
        match self {
            Self::LiftedIndex => "LIFTED INDEX",
            Self::TemperatureAdvection700mb => "700 MB TEMPERATURE ADVECTION",
            Self::TemperatureAdvection850mb => "850 MB TEMPERATURE ADVECTION",
            Self::BulkShear01km => "0-1 KM BULK SHEAR",
            Self::BulkShear06km => "0-6 KM BULK SHEAR",
            Self::ApparentTemperature => "APPARENT TEMPERATURE",
            Self::HeatIndex => "HEAT INDEX",
            Self::WindChill => "WIND CHILL",
        }
    }

    pub fn scale_preset(self) -> DerivedScalePreset {
        match self {
            Self::LiftedIndex => DerivedScalePreset::LiftedIndex,
            Self::TemperatureAdvection700mb | Self::TemperatureAdvection850mb => {
                DerivedScalePreset::TemperatureAdvection
            }
            Self::BulkShear01km | Self::BulkShear06km => DerivedScalePreset::BulkShear,
            Self::ApparentTemperature | Self::HeatIndex | Self::WindChill => {
                DerivedScalePreset::SurfaceComfort
            }
        }
    }

    pub fn scale(self) -> DiscreteColorScale {
        self.scale_preset().scale()
    }

    pub fn default_tick_step(self) -> Option<f64> {
        self.scale_preset().default_tick_step()
    }

    pub fn semantics(self) -> ProductSemantics {
        match self {
            Self::ApparentTemperature | Self::HeatIndex | Self::WindChill => {
                ProductSemantics::operational()
            }
            Self::LiftedIndex
            | Self::TemperatureAdvection700mb
            | Self::TemperatureAdvection850mb
            | Self::BulkShear01km
            | Self::BulkShear06km => ProductSemantics::operational(),
        }
    }
}

impl From<DerivedProductStyle> for DerivedScalePreset {
    fn from(value: DerivedProductStyle) -> Self {
        value.scale_preset()
    }
}

impl Solar07Preset {
    pub fn from_product_name(name: &str) -> Option<Self> {
        if let Some(product) = Solar07Product::from_product_name(name) {
            return Some(product.scale_preset());
        }

        match normalize(name).as_str() {
            "sbcape" | "mlcape" | "mucape" | "cape" | "effective_cape" | "ecape" | "sbecape"
            | "mlecape" | "muecape" | "ecape_cape" | "sbncape" | "mlncape" | "muncape" => {
                Some(Self::Cape)
            }
            "cape3d" | "three_cape" => Some(Self::ThreeCape),
            "sbcin" | "mlcin" | "mucin" | "cin" | "ecape_cin" | "sbecin" | "mlecin" | "muecin" => {
                Some(Self::Cin)
            }
            "lcl" => Some(Self::Lcl),
            "lfc" | "ecape_lfc" => Some(Self::Lfc),
            "el" | "ecape_el" => Some(Self::El),
            "srh" | "srh1" | "srh3" | "effective_srh" => Some(Self::Srh),
            "stp" | "stp_fixed" | "stp_effective" => Some(Self::Stp),
            "scp" | "ecape_scp" => Some(Self::Scp),
            "ehi" | "ecape_ehi" => Some(Self::Ehi),
            "uhel" | "uh" => Some(Self::Uh),
            "lapse_rate" | "lapse_rate_700_500" | "lapse_rate_0_3km" => Some(Self::LapseRate),
            _ => None,
        }
    }

    pub fn scale(self) -> DiscreteColorScale {
        match self {
            Self::Cape => DiscreteColorScale {
                levels: range_step(0.0, 4250.0, 250.0),
                colors: solar07_palette(Solar07Palette::Cape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::ThreeCape => DiscreteColorScale {
                levels: range_step(0.0, 4250.0, 250.0),
                colors: solar07_palette(Solar07Palette::ThreeCape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Cin => DiscreteColorScale {
                levels: range_step(-300.0, 1.0, 25.0),
                colors: solar07_palette(Solar07Palette::Cape),
                extend: ExtendMode::Min,
                mask_below: None,
            },
            Self::Lcl => DiscreteColorScale {
                levels: range_step(0.0, 4200.0, 200.0),
                colors: solar07_palette(Solar07Palette::Cape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Lfc => DiscreteColorScale {
                levels: range_step(0.0, 5500.0, 500.0),
                colors: solar07_palette(Solar07Palette::Cape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::El => DiscreteColorScale {
                levels: range_step(0.0, 16000.0, 1000.0),
                colors: solar07_palette(Solar07Palette::Cape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Srh => DiscreteColorScale {
                levels: range_step(0.0, 525.0, 25.0),
                colors: solar07_palette(Solar07Palette::Srh),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Stp => DiscreteColorScale {
                levels: range_step(0.0, 11.0, 1.0),
                colors: solar07_palette(Solar07Palette::Stp),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Scp => DiscreteColorScale {
                levels: range_step(0.0, 11.0, 1.0),
                colors: solar07_palette(Solar07Palette::Cape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Ehi => DiscreteColorScale {
                levels: range_step(0.0, 5.5, 0.5),
                colors: solar07_palette(Solar07Palette::Ehi),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Uh => DiscreteColorScale {
                levels: range_step(0.0, 210.0, 10.0),
                colors: solar07_palette(Solar07Palette::Uh),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::LapseRate => DiscreteColorScale {
                levels: range_step(4.0, 10.5, 0.5),
                colors: solar07_palette(Solar07Palette::LapseRate),
                extend: ExtendMode::Both,
                mask_below: None,
            },
        }
    }
}

impl DerivedScalePreset {
    pub fn scale(self) -> DiscreteColorScale {
        match self {
            Self::LiftedIndex => {
                let mut colors = solar07_palette(Solar07Palette::Advection);
                colors.reverse();
                DiscreteColorScale {
                    levels: range_step(-12.0, 14.0, 2.0),
                    colors,
                    extend: ExtendMode::Both,
                    mask_below: None,
                }
            }
            Self::TemperatureAdvection => DiscreteColorScale {
                levels: range_step(-12.0, 14.0, 2.0),
                colors: solar07_palette(Solar07Palette::Advection),
                extend: ExtendMode::Both,
                mask_below: None,
            },
            Self::BulkShear => DiscreteColorScale {
                levels: range_step(0.0, 65.0, 5.0),
                colors: solar07_palette(Solar07Palette::Winds),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::SurfaceComfort => DiscreteColorScale {
                levels: range_step(-30.0, 50.0, 5.0),
                colors: solar07_palette(Solar07Palette::Temperature),
                extend: ExtendMode::Both,
                mask_below: None,
            },
        }
    }

    pub fn default_tick_step(self) -> Option<f64> {
        match self {
            Self::LiftedIndex => Some(2.0),
            Self::TemperatureAdvection => Some(2.0),
            Self::BulkShear => Some(5.0),
            Self::SurfaceComfort => Some(5.0),
        }
    }
}

pub fn solar07_palette(palette: Solar07Palette) -> Vec<Color> {
    use wrf_render::colormaps;

    let colors = match palette {
        Solar07Palette::Cape => colormaps::cape(),
        Solar07Palette::ThreeCape => colormaps::three_cape(),
        Solar07Palette::Ehi => colormaps::ehi(),
        Solar07Palette::Srh => colormaps::srh(),
        Solar07Palette::Stp => colormaps::stp(),
        Solar07Palette::LapseRate => colormaps::lapse_rate(),
        Solar07Palette::Uh => colormaps::uh(),
        Solar07Palette::MlMetric => colormaps::ml_metric(),
        Solar07Palette::Reflectivity => colormaps::reflectivity(),
        Solar07Palette::Winds => colormaps::winds(60),
        Solar07Palette::Temperature => colormaps::temperature(180),
        Solar07Palette::Dewpoint => colormaps::dewpoint(80, 50),
        Solar07Palette::Rh => colormaps::rh(),
        Solar07Palette::RelVort => colormaps::relvort(100),
        Solar07Palette::Advection => advection_palette(),
        Solar07Palette::SimIr => colormaps::sim_ir(),
        Solar07Palette::GeopotAnomaly => colormaps::geopot_anomaly(100),
        Solar07Palette::Precip => colormaps::precip_in(),
        Solar07Palette::ShadedOverlay => colormaps::shaded_overlay(),
    };

    colors.into_iter().map(Into::into).collect()
}

pub fn palette_scale(
    palette: Solar07Palette,
    levels: Vec<f64>,
    extend: ExtendMode,
    mask_below: Option<f64>,
) -> DiscreteColorScale {
    DiscreteColorScale {
        levels,
        colors: solar07_palette(palette),
        extend,
        mask_below,
    }
}

fn advection_palette() -> Vec<wrf_render::Rgba> {
    const ADVECTION_HEX: [&str; 9] = [
        "#0b3c5d", "#328cc1", "#74b3ce", "#d9ecf2", "#f7f7f7", "#f3d9ca", "#e39b7b", "#c75d43",
        "#8f2d1f",
    ];

    ADVECTION_HEX
        .into_iter()
        .map(rgba_from_hex)
        .collect::<Vec<_>>()
}

fn rgba_from_hex(value: &str) -> wrf_render::Rgba {
    let trimmed = value.trim_start_matches('#');
    let red = u8::from_str_radix(&trimmed[0..2], 16).expect("valid red component");
    let green = u8::from_str_radix(&trimmed[2..4], 16).expect("valid green component");
    let blue = u8::from_str_radix(&trimmed[4..6], 16).expect("valid blue component");
    wrf_render::Rgba {
        r: red,
        g: green,
        b: blue,
        a: u8::MAX,
    }
}

fn normalize(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace(['-', ' '], "_")
}

fn range_step(start: f64, stop: f64, step: f64) -> Vec<f64> {
    let mut values = Vec::new();
    let mut current = start;
    while current < stop - step * 1.0e-9 {
        values.push(current);
        current += step;
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{ProductMaturity, ProductSemanticFlag};

    #[test]
    fn explicit_ecape_panel_products_have_expected_titles_and_experimental_flags() {
        assert_eq!(Solar07Product::Sbecape.display_title(), "SBECAPE");
        assert_eq!(Solar07Product::Mlecin.display_title(), "MLECIN");
        assert!(Solar07Product::EcapeScpExperimental.is_experimental());
        assert!(Solar07Product::EcapeEhiExperimental.is_experimental());
        assert!(!Solar07Product::Muecape.is_experimental());
        assert_eq!(
            Solar07Product::EcapeScpExperimental.semantics().maturity,
            ProductMaturity::Experimental
        );
        assert_eq!(
            Solar07Product::Sbcape.semantics().maturity,
            ProductMaturity::Operational
        );
    }

    #[test]
    fn ecape_panel_defaults_match_requested_operational_layout() {
        assert_eq!(
            ECAPE_SEVERE_PANEL_PRODUCTS,
            [
                Solar07Product::Sbecape,
                Solar07Product::Mlecape,
                Solar07Product::Muecape,
                Solar07Product::Sbncape,
                Solar07Product::Sbecin,
                Solar07Product::Mlecin,
                Solar07Product::EcapeScpExperimental,
                Solar07Product::EcapeEhiExperimental,
            ]
        );
    }

    #[test]
    fn severe_panel_defaults_cover_classic_severe_suite() {
        assert_eq!(
            SEVERE_CLASSIC_PANEL_PRODUCTS,
            [
                Solar07Product::Sbcape,
                Solar07Product::Mlcape,
                Solar07Product::Mucape,
                Solar07Product::Mlcin,
                Solar07Product::Srh01km,
                Solar07Product::Srh03km,
                Solar07Product::Stp,
                Solar07Product::Scp,
            ]
        );
    }

    #[test]
    fn product_name_resolution_covers_parcel_explicit_ecape_fields() {
        assert_eq!(
            Solar07Product::from_product_name("mlecin"),
            Some(Solar07Product::Mlecin)
        );
        assert_eq!(
            Solar07Product::from_product_name("ecape_scp"),
            Some(Solar07Product::EcapeScpExperimental)
        );
        assert_eq!(
            Solar07Preset::from_product_name("ecape_ehi"),
            Some(Solar07Preset::Ehi)
        );
    }

    #[test]
    fn palette_scale_wraps_palette_and_levels_into_discrete_scale() {
        let scale = palette_scale(
            Solar07Palette::Reflectivity,
            vec![5.0, 15.0, 25.0, 35.0],
            ExtendMode::Max,
            Some(5.0),
        );

        assert_eq!(scale.levels, vec![5.0, 15.0, 25.0, 35.0]);
        assert_eq!(scale.extend, ExtendMode::Max);
        assert_eq!(scale.mask_below, Some(5.0));
        assert!(!scale.colors.is_empty());
    }

    #[test]
    fn derived_product_styles_cover_new_helper_tranche() {
        assert_eq!(
            DerivedProductStyle::from_product_name("lifted_index"),
            Some(DerivedProductStyle::LiftedIndex)
        );
        assert_eq!(
            DerivedProductStyle::from_product_name("temperature_advection_850mb"),
            Some(DerivedProductStyle::TemperatureAdvection850mb)
        );
        assert_eq!(
            DerivedProductStyle::from_product_name("bulk_shear_0_6km"),
            Some(DerivedProductStyle::BulkShear06km)
        );
        assert_eq!(
            DerivedProductStyle::from_product_name("apparent_temperature"),
            Some(DerivedProductStyle::ApparentTemperature)
        );
    }

    #[test]
    fn lifted_index_and_advection_scales_use_diverging_advection_helper() {
        let li = DerivedScalePreset::LiftedIndex.scale();
        let advection = DerivedScalePreset::TemperatureAdvection.scale();

        assert_eq!(li.levels, range_step(-12.0, 14.0, 2.0));
        assert_eq!(advection.levels, range_step(-12.0, 14.0, 2.0));
        assert_eq!(li.extend, ExtendMode::Both);
        assert_eq!(advection.extend, ExtendMode::Both);
        assert_eq!(li.colors.first(), advection.colors.last());
        assert_eq!(li.colors.last(), advection.colors.first());
    }

    #[test]
    fn bulk_shear_and_surface_comfort_have_sane_tick_steps() {
        assert_eq!(DerivedScalePreset::BulkShear.default_tick_step(), Some(5.0));
        assert_eq!(
            DerivedProductStyle::ApparentTemperature.default_tick_step(),
            Some(5.0)
        );
        assert_eq!(
            DerivedProductStyle::TemperatureAdvection700mb.display_title(),
            "700 MB TEMPERATURE ADVECTION"
        );
    }

    #[test]
    fn semantic_flags_stay_narrow_in_render_presets() {
        let severe = Solar07Product::Scp.semantics();
        assert_eq!(severe.maturity, ProductMaturity::Operational);
        assert!(!severe.has_flag(ProductSemanticFlag::Proxy));

        let ecape = Solar07Product::EcapeEhiExperimental.semantics();
        assert_eq!(ecape.maturity, ProductMaturity::Experimental);
        assert!(!ecape.has_flag(ProductSemanticFlag::ProofOriented));
    }
}
