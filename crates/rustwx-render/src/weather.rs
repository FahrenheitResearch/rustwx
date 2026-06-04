use crate::presentation::ProductVisualMode;
use crate::request::{Color, DiscreteColorScale, ExtendMode, ProductSemantics};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeatherPalette {
    Cape,
    ThreeCape,
    Ehi,
    Srh,
    Stp,
    LapseRate,
    Uh,
    EcapeRatio,
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
pub enum WeatherPreset {
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
    EcapeCapeRatio,
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
pub enum WeatherProduct {
    Sbcape,
    Mlcape,
    Mucape,
    Sbecape,
    Mlecape,
    Muecape,
    SbEcapeDerivedCapeRatio,
    MlEcapeDerivedCapeRatio,
    MuEcapeDerivedCapeRatio,
    SbEcapeNativeCapeRatio,
    MlEcapeNativeCapeRatio,
    MuEcapeNativeCapeRatio,
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
    Tehi,
    Tts,
    VtpMod,
    Uh,
    EcapeScpExperimental,
    EcapeEhi01kmExperimental,
    EcapeEhi03kmExperimental,
    EcapeStpExperimental,
}

pub const SEVERE_CLASSIC_PANEL_PRODUCTS: [WeatherProduct; 8] = [
    WeatherProduct::Sbcape,
    WeatherProduct::Mlcape,
    WeatherProduct::Mucape,
    WeatherProduct::Mlcin,
    WeatherProduct::Srh01km,
    WeatherProduct::Srh03km,
    WeatherProduct::Stp,
    WeatherProduct::Scp,
];

pub const ECAPE_SEVERE_PANEL_PRODUCTS: [WeatherProduct; 16] = [
    WeatherProduct::Sbecape,
    WeatherProduct::Mlecape,
    WeatherProduct::Muecape,
    WeatherProduct::SbEcapeDerivedCapeRatio,
    WeatherProduct::MlEcapeDerivedCapeRatio,
    WeatherProduct::MuEcapeDerivedCapeRatio,
    WeatherProduct::SbEcapeNativeCapeRatio,
    WeatherProduct::MlEcapeNativeCapeRatio,
    WeatherProduct::MuEcapeNativeCapeRatio,
    WeatherProduct::Sbncape,
    WeatherProduct::Sbecin,
    WeatherProduct::Mlecin,
    WeatherProduct::EcapeScpExperimental,
    WeatherProduct::EcapeEhi01kmExperimental,
    WeatherProduct::EcapeEhi03kmExperimental,
    WeatherProduct::EcapeStpExperimental,
];

impl WeatherProduct {
    pub fn from_product_name(name: &str) -> Option<Self> {
        match normalize(name).as_str() {
            "sbcape" => Some(Self::Sbcape),
            "mlcape" => Some(Self::Mlcape),
            "mucape" => Some(Self::Mucape),
            "sbecape" => Some(Self::Sbecape),
            "mlecape" => Some(Self::Mlecape),
            "muecape" => Some(Self::Muecape),
            "sb_ecape_derived_cape_ratio" | "sbecape_derived_cape_ratio" => {
                Some(Self::SbEcapeDerivedCapeRatio)
            }
            "ml_ecape_derived_cape_ratio" | "mlecape_derived_cape_ratio" => {
                Some(Self::MlEcapeDerivedCapeRatio)
            }
            "mu_ecape_derived_cape_ratio" | "muecape_derived_cape_ratio" => {
                Some(Self::MuEcapeDerivedCapeRatio)
            }
            "sb_ecape_native_cape_ratio" | "sbecape_native_cape_ratio" => {
                Some(Self::SbEcapeNativeCapeRatio)
            }
            "ml_ecape_native_cape_ratio" | "mlecape_native_cape_ratio" => {
                Some(Self::MlEcapeNativeCapeRatio)
            }
            "mu_ecape_native_cape_ratio" | "muecape_native_cape_ratio" => {
                Some(Self::MuEcapeNativeCapeRatio)
            }
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
            "ehi" | "ehi_0_1km" | "ehi_01km" | "ehi_0_3km" | "ehi_03km" => Some(Self::Ehi),
            "tehi" | "tornadic_ehi" | "tornadic_0_1km_ehi" => Some(Self::Tehi),
            "tts" | "tornadic_tilting_stretching" => Some(Self::Tts),
            "vtp_mod" | "modified_vtp" | "vtp" => Some(Self::VtpMod),
            "uhel" | "uh" => Some(Self::Uh),
            "ecape_scp" => Some(Self::EcapeScpExperimental),
            "ecape_ehi" | "ecape_ehi_0_1km" | "ecape_ehi_01km" => {
                Some(Self::EcapeEhi01kmExperimental)
            }
            "ecape_ehi_0_3km" | "ecape_ehi_03km" => Some(Self::EcapeEhi03kmExperimental),
            "ecape_stp" => Some(Self::EcapeStpExperimental),
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
            Self::SbEcapeDerivedCapeRatio => "sb_ecape_derived_cape_ratio",
            Self::MlEcapeDerivedCapeRatio => "ml_ecape_derived_cape_ratio",
            Self::MuEcapeDerivedCapeRatio => "mu_ecape_derived_cape_ratio",
            Self::SbEcapeNativeCapeRatio => "sb_ecape_native_cape_ratio",
            Self::MlEcapeNativeCapeRatio => "ml_ecape_native_cape_ratio",
            Self::MuEcapeNativeCapeRatio => "mu_ecape_native_cape_ratio",
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
            Self::Tehi => "tehi",
            Self::Tts => "tts",
            Self::VtpMod => "vtp_mod",
            Self::Uh => "uhel",
            Self::EcapeScpExperimental => "ecape_scp",
            Self::EcapeEhi01kmExperimental => "ecape_ehi_0_1km",
            Self::EcapeEhi03kmExperimental => "ecape_ehi_0_3km",
            Self::EcapeStpExperimental => "ecape_stp",
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
            Self::SbEcapeDerivedCapeRatio => "SB ECAPE/DERIVED CAPE RATIO",
            Self::MlEcapeDerivedCapeRatio => "ML ECAPE/DERIVED CAPE RATIO",
            Self::MuEcapeDerivedCapeRatio => "MU ECAPE/DERIVED CAPE RATIO",
            Self::SbEcapeNativeCapeRatio => "SB ECAPE/NATIVE CAPE RATIO",
            Self::MlEcapeNativeCapeRatio => "ML ECAPE/NATIVE CAPE RATIO",
            Self::MuEcapeNativeCapeRatio => "MU ECAPE/NATIVE CAPE RATIO",
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
            Self::Tehi => "TEHI",
            Self::Tts => "TTS",
            Self::VtpMod => "VTP MOD",
            Self::Uh => "UH",
            Self::EcapeScpExperimental => "ECAPE SCP (EXP)",
            Self::EcapeEhi01kmExperimental => "ECAPE EHI 0-1 KM (EXP)",
            Self::EcapeEhi03kmExperimental => "ECAPE EHI 0-3 KM (EXP)",
            Self::EcapeStpExperimental => "ECAPE STP (EXP)",
        }
    }

    pub fn scale_preset(self) -> WeatherPreset {
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
            | Self::EcapeCape => WeatherPreset::Cape,
            Self::SbEcapeDerivedCapeRatio
            | Self::MlEcapeDerivedCapeRatio
            | Self::MuEcapeDerivedCapeRatio
            | Self::SbEcapeNativeCapeRatio
            | Self::MlEcapeNativeCapeRatio
            | Self::MuEcapeNativeCapeRatio => WeatherPreset::EcapeCapeRatio,
            Self::Sbcin
            | Self::Mlcin
            | Self::Mucin
            | Self::Sbecin
            | Self::Mlecin
            | Self::Muecin
            | Self::EcapeCin => WeatherPreset::Cin,
            Self::Lcl => WeatherPreset::Lcl,
            Self::Lfc | Self::EcapeLfc => WeatherPreset::Lfc,
            Self::El | Self::EcapeEl => WeatherPreset::El,
            Self::Srh01km | Self::Srh03km => WeatherPreset::Srh,
            Self::Stp
            | Self::StpFixed
            | Self::StpEffective
            | Self::Tehi
            | Self::Tts
            | Self::VtpMod
            | Self::EcapeStpExperimental => WeatherPreset::Stp,
            Self::Scp | Self::EcapeScpExperimental => WeatherPreset::Scp,
            Self::Ehi | Self::EcapeEhi01kmExperimental | Self::EcapeEhi03kmExperimental => {
                WeatherPreset::Ehi
            }
            Self::Uh => WeatherPreset::Uh,
        }
    }

    pub fn default_tick_step(self) -> Option<f64> {
        match self.scale_preset() {
            WeatherPreset::Cape => Some(500.0),
            WeatherPreset::ThreeCape => Some(50.0),
            WeatherPreset::Cin => Some(50.0),
            WeatherPreset::Lcl => Some(500.0),
            WeatherPreset::Lfc => Some(500.0),
            WeatherPreset::El => Some(1000.0),
            WeatherPreset::Srh => Some(50.0),
            WeatherPreset::Stp => Some(1.0),
            WeatherPreset::Scp => Some(1.0),
            WeatherPreset::Ehi => Some(1.0),
            WeatherPreset::EcapeCapeRatio => Some(0.25),
            WeatherPreset::Uh => Some(20.0),
            WeatherPreset::LapseRate => Some(1.0),
        }
    }

    pub fn semantics(self) -> ProductSemantics {
        if self.is_experimental() {
            ProductSemantics::experimental()
        } else {
            ProductSemantics::operational()
        }
    }

    pub fn default_visual_mode(self) -> ProductVisualMode {
        ProductVisualMode::SevereDiagnostic
    }

    pub fn is_experimental(self) -> bool {
        matches!(
            self,
            Self::EcapeScpExperimental
                | Self::SbEcapeDerivedCapeRatio
                | Self::MlEcapeDerivedCapeRatio
                | Self::MuEcapeDerivedCapeRatio
                | Self::SbEcapeNativeCapeRatio
                | Self::MlEcapeNativeCapeRatio
                | Self::MuEcapeNativeCapeRatio
                | Self::EcapeEhi01kmExperimental
                | Self::EcapeEhi03kmExperimental
                | Self::EcapeStpExperimental
        )
    }
}

impl From<WeatherProduct> for WeatherPreset {
    fn from(value: WeatherProduct) -> Self {
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

    pub fn default_visual_mode(self) -> ProductVisualMode {
        match self {
            Self::TemperatureAdvection700mb | Self::TemperatureAdvection850mb => {
                ProductVisualMode::UpperAirAnalysis
            }
            Self::ApparentTemperature | Self::HeatIndex | Self::WindChill => {
                ProductVisualMode::FilledMeteorology
            }
            Self::LiftedIndex | Self::BulkShear01km | Self::BulkShear06km => {
                ProductVisualMode::SevereDiagnostic
            }
        }
    }
}

impl From<DerivedProductStyle> for DerivedScalePreset {
    fn from(value: DerivedProductStyle) -> Self {
        value.scale_preset()
    }
}

impl WeatherPreset {
    pub fn from_product_name(name: &str) -> Option<Self> {
        if let Some(product) = WeatherProduct::from_product_name(name) {
            return Some(product.scale_preset());
        }

        match normalize(name).as_str() {
            "sbcape" | "mlcape" | "mucape" | "cape" | "effective_cape" | "ecape" | "sbecape"
            | "mlecape" | "muecape" | "ecape_cape" | "sbncape" | "mlncape" | "muncape" => {
                Some(Self::Cape)
            }
            "sb_ecape_derived_cape_ratio"
            | "ml_ecape_derived_cape_ratio"
            | "mu_ecape_derived_cape_ratio"
            | "sb_ecape_native_cape_ratio"
            | "ml_ecape_native_cape_ratio"
            | "mu_ecape_native_cape_ratio"
            | "sbecape_derived_cape_ratio"
            | "mlecape_derived_cape_ratio"
            | "muecape_derived_cape_ratio"
            | "sbecape_native_cape_ratio"
            | "mlecape_native_cape_ratio"
            | "muecape_native_cape_ratio" => Some(Self::EcapeCapeRatio),
            "cape3d" | "three_cape" => Some(Self::ThreeCape),
            "sbcin" | "mlcin" | "mucin" | "cin" | "ecape_cin" | "sbecin" | "mlecin" | "muecin" => {
                Some(Self::Cin)
            }
            "lcl" => Some(Self::Lcl),
            "lfc" | "ecape_lfc" => Some(Self::Lfc),
            "el" | "ecape_el" => Some(Self::El),
            "srh" | "srh1" | "srh3" | "effective_srh" => Some(Self::Srh),
            "stp" | "stp_fixed" | "stp_effective" | "ecape_stp" => Some(Self::Stp),
            "scp" | "ecape_scp" => Some(Self::Scp),
            "ehi" | "ehi_0_1km" | "ehi_01km" | "ehi_0_3km" | "ehi_03km" | "ecape_ehi"
            | "ecape_ehi_0_1km" | "ecape_ehi_0_3km" => Some(Self::Ehi),
            "uhel" | "uh" => Some(Self::Uh),
            "lapse_rate" | "lapse_rate_700_500" | "lapse_rate_0_3km" => Some(Self::LapseRate),
            _ => None,
        }
    }

    pub fn scale(self) -> DiscreteColorScale {
        match self {
            Self::Cape => DiscreteColorScale {
                levels: range_step(0.0, 8100.0, 100.0),
                colors: weather_palette(WeatherPalette::Cape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::ThreeCape => DiscreteColorScale {
                levels: concat_ranges(&[(0.0, 300.0, 5.0), (300.0, 501.0, 20.0)]),
                colors: weather_palette(WeatherPalette::ThreeCape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Cin => DiscreteColorScale {
                levels: range_step(-300.0, 1.0, 25.0),
                colors: weather_palette(WeatherPalette::Cape),
                extend: ExtendMode::Min,
                mask_below: None,
            },
            Self::Lcl => DiscreteColorScale {
                levels: range_step(0.0, 4200.0, 200.0),
                colors: weather_palette(WeatherPalette::Cape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Lfc => DiscreteColorScale {
                levels: range_step(0.0, 5500.0, 500.0),
                colors: weather_palette(WeatherPalette::Cape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::El => DiscreteColorScale {
                levels: range_step(0.0, 16000.0, 1000.0),
                colors: weather_palette(WeatherPalette::Cape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Srh => DiscreteColorScale {
                levels: range_step(0.0, 1001.0, 10.0),
                colors: weather_palette(WeatherPalette::Srh),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Stp => DiscreteColorScale {
                levels: concat_ranges(&[(0.0, 10.0, 0.1), (10.0, 20.1, 0.5)]),
                colors: weather_palette(WeatherPalette::Stp),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Scp => DiscreteColorScale {
                levels: range_step(0.0, 30.0, 0.5),
                colors: weather_palette(WeatherPalette::Cape),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Ehi => DiscreteColorScale {
                levels: concat_ranges(&[(0.0, 2.0, 0.1), (2.0, 20.2, 0.2)]),
                colors: weather_palette(WeatherPalette::Ehi),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::EcapeCapeRatio => DiscreteColorScale {
                levels: range_step(0.0, 1.15, 0.05),
                colors: weather_palette(WeatherPalette::EcapeRatio),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::Uh => DiscreteColorScale {
                levels: concat_ranges(&[(0.0, 200.0, 5.0), (200.0, 401.0, 10.0)]),
                colors: weather_palette(WeatherPalette::Uh),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::LapseRate => DiscreteColorScale {
                levels: range_step(3.0, 10.1, 0.1),
                colors: weather_palette(WeatherPalette::LapseRate),
                extend: ExtendMode::Both,
                mask_below: None,
            },
        }
    }

    pub fn default_tick_step(self) -> Option<f64> {
        match self {
            Self::Cape => Some(500.0),
            Self::ThreeCape => Some(50.0),
            Self::Cin => Some(50.0),
            Self::Lcl => Some(500.0),
            Self::Lfc => Some(500.0),
            Self::El => Some(1000.0),
            Self::Srh => Some(50.0),
            Self::Stp | Self::Scp | Self::Ehi => Some(1.0),
            Self::EcapeCapeRatio => Some(0.25),
            Self::Uh => Some(20.0),
            Self::LapseRate => Some(1.0),
        }
    }
}

impl DerivedScalePreset {
    pub fn scale(self) -> DiscreteColorScale {
        match self {
            Self::LiftedIndex => {
                let mut colors = weather_palette(WeatherPalette::Advection);
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
                colors: weather_palette(WeatherPalette::Advection),
                extend: ExtendMode::Both,
                mask_below: None,
            },
            Self::BulkShear => DiscreteColorScale {
                levels: range_step(0.0, 65.0, 5.0),
                colors: weather_palette(WeatherPalette::Winds),
                extend: ExtendMode::Max,
                mask_below: None,
            },
            Self::SurfaceComfort => DiscreteColorScale {
                levels: range_step(-30.0, 50.0, 5.0),
                colors: weather_palette(WeatherPalette::Temperature),
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

pub fn weather_palette(palette: WeatherPalette) -> Vec<Color> {
    use crate::colormaps;

    let colors = match palette {
        WeatherPalette::Cape => colormaps::cape(),
        WeatherPalette::ThreeCape => colormaps::three_cape(),
        WeatherPalette::Ehi => colormaps::ehi(),
        WeatherPalette::Srh => colormaps::srh(),
        WeatherPalette::Stp => colormaps::stp(),
        WeatherPalette::LapseRate => colormaps::lapse_rate(),
        WeatherPalette::Uh => colormaps::uh(),
        WeatherPalette::EcapeRatio => ecape_ratio_palette(),
        WeatherPalette::MlMetric => colormaps::ml_metric(),
        WeatherPalette::Reflectivity => colormaps::reflectivity(),
        WeatherPalette::Winds => colormaps::winds(60),
        WeatherPalette::Temperature => colormaps::temperature(180),
        WeatherPalette::Dewpoint => {
            return dewpoint_palette_fahrenheit_for_levels(&range_step(-40.0, 91.0, 1.0));
        }
        WeatherPalette::Rh => colormaps::rh(),
        WeatherPalette::RelVort => colormaps::relvort(100),
        WeatherPalette::Advection => advection_palette(),
        WeatherPalette::SimIr => colormaps::sim_ir(),
        WeatherPalette::GeopotAnomaly => colormaps::geopot_anomaly(100),
        WeatherPalette::Precip => colormaps::precip_in(),
        WeatherPalette::ShadedOverlay => colormaps::shaded_overlay(),
    };

    colors.into_iter().map(Into::into).collect()
}

pub fn winds_palette_segments(n_segments: usize) -> Vec<Color> {
    crate::colormaps::winds(n_segments)
        .into_iter()
        .map(Into::into)
        .collect()
}

pub fn temperature_palette_cropped_f(crop_f: Option<(f64, f64)>, n_segments: usize) -> Vec<Color> {
    crate::colormaps::temperature_cropped(n_segments, crop_f)
        .into_iter()
        .map(Into::into)
        .collect()
}

pub fn dewpoint_palette_params(dry_points: usize, moist_points_total: usize) -> Vec<Color> {
    crate::colormaps::dewpoint(dry_points, moist_points_total)
        .into_iter()
        .map(Into::into)
        .collect()
}

pub fn dewpoint_palette_fahrenheit_for_levels(levels_f: &[f64]) -> Vec<Color> {
    dewpoint_palette_for_levels(levels_f, |value_f| value_f)
}

pub fn dewpoint_palette_celsius_for_levels(levels_c: &[f64]) -> Vec<Color> {
    dewpoint_palette_for_levels(levels_c, |value_c| value_c * 9.0 / 5.0 + 32.0)
}

pub fn palette_scale(
    palette: WeatherPalette,
    levels: Vec<f64>,
    extend: ExtendMode,
    mask_below: Option<f64>,
) -> DiscreteColorScale {
    DiscreteColorScale {
        levels,
        colors: weather_palette(palette),
        extend,
        mask_below,
    }
}

fn advection_palette() -> Vec<crate::color::Rgba> {
    const ADVECTION_HEX: [&str; 9] = [
        "#0b3c5d", "#328cc1", "#74b3ce", "#d9ecf2", "#f7f7f7", "#f3d9ca", "#e39b7b", "#c75d43",
        "#8f2d1f",
    ];

    ADVECTION_HEX
        .into_iter()
        .map(rgba_from_hex)
        .collect::<Vec<_>>()
}

fn ecape_ratio_palette() -> Vec<crate::color::Rgba> {
    const ECAPE_RATIO_HEX: [&str; 11] = [
        "#7f1d1d", "#b91c1c", "#dc2626", "#f97316", "#f59e0b", "#facc15", "#fde047", "#bef264",
        "#84cc16", "#22c55e", "#15803d",
    ];

    ECAPE_RATIO_HEX
        .into_iter()
        .map(rgba_from_hex)
        .collect::<Vec<_>>()
}

fn dewpoint_palette_for_levels(levels: &[f64], to_fahrenheit: impl Fn(f64) -> f64) -> Vec<Color> {
    if levels.len() < 2 {
        return Vec::new();
    }

    levels
        .windows(2)
        .map(|window| {
            let midpoint = (window[0] + window[1]) * 0.5;
            let dewpoint_f = to_fahrenheit(midpoint);
            dewpoint_threshold_color_f(dewpoint_f)
        })
        .collect()
}

fn dewpoint_threshold_color_f(value_f: f64) -> Color {
    const BANDS: &[(f64, f64, &str, &str)] = &[
        (-80.0, 0.0, "#6d6758", "#8a846f"),
        (0.0, 20.0, "#8f8976", "#aaa58f"),
        (20.0, 30.0, "#a9a38e", "#bfb89e"),
        (30.0, 40.0, "#c8c0a6", "#e2dcc0"),
        (40.0, 50.0, "#e6e7d0", "#b8e0b6"),
        (50.0, 60.0, "#8bd38a", "#15951d"),
        (60.0, 70.0, "#008f1e", "#005625"),
        (70.0, 80.0, "#2a6e69", "#17284d"),
        (80.0, 90.0, "#35256f", "#76547c"),
        (90.0, 120.0, "#8f6586", "#b88f9a"),
    ];

    let (lo, hi, start, end) = BANDS
        .iter()
        .copied()
        .find(|(lo, hi, _, _)| value_f >= *lo && value_f < *hi)
        .unwrap_or_else(|| {
            if value_f < BANDS[0].0 {
                BANDS[0]
            } else {
                BANDS[BANDS.len() - 1]
            }
        });

    let span = (hi - lo).max(f64::EPSILON);
    let t = ((value_f - lo) / span).clamp(0.0, 1.0);
    lerp_color_hex(start, end, t)
}

fn lerp_color_hex(start: &str, end: &str, t: f64) -> Color {
    let start = crate::color::Rgba::from_hex(start);
    let end = crate::color::Rgba::from_hex(end);
    Color::rgba(
        (start.r as f64 + t * (end.r as f64 - start.r as f64)).round() as u8,
        (start.g as f64 + t * (end.g as f64 - start.g as f64)).round() as u8,
        (start.b as f64 + t * (end.b as f64 - start.b as f64)).round() as u8,
        u8::MAX,
    )
}

fn rgba_from_hex(value: &str) -> crate::color::Rgba {
    let trimmed = value.trim_start_matches('#');
    let red = u8::from_str_radix(&trimmed[0..2], 16).expect("valid red component");
    let green = u8::from_str_radix(&trimmed[2..4], 16).expect("valid green component");
    let blue = u8::from_str_radix(&trimmed[4..6], 16).expect("valid blue component");
    crate::color::Rgba {
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

fn concat_ranges(parts: &[(f64, f64, f64)]) -> Vec<f64> {
    let mut values: Vec<f64> = Vec::new();
    for (start, stop, step) in parts {
        let part = range_step(*start, *stop, *step);
        if let (Some(last), Some(first)) = (values.last().copied(), part.first().copied()) {
            if (last - first).abs() < 1.0e-9 {
                values.extend(part.into_iter().skip(1));
                continue;
            }
        }
        values.extend(part);
    }
    values
}

#[cfg(test)]
mod tests;
