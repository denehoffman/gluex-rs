#[derive(Copy, Clone, Default, Eq, PartialEq, Hash, Debug)]
#[allow(non_camel_case_types)]
pub enum DetectorSystem {
    #[default]
    NULL,
    CDC,
    FDC,
    BCAL,
    TOF,
    CHERENKOV,
    FCAL,
    UPV,
    TAGM,
    START,
    DIRC,
    CCAL,
    CCAL_REF,
    ECAL,
    ECAL_REF,
    TAGH,
    RF,
    PS,
    PSC,
    FMWPC,
    TPOL,
    TAC,
    TRD,
    CTOF,
    HELI,
    ECAL_FCAL,
}

impl std::fmt::Display for DetectorSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::NULL => "NULL_DETECTOR",
                Self::CDC => "CDC",
                Self::FDC => "FDC",
                Self::BCAL => "BCAL",
                Self::TOF => "TOF",
                Self::CHERENKOV => "Cherenkov",
                Self::FCAL => "FCAL",
                Self::UPV => "UPV",
                Self::TAGM => "TAGM",
                Self::START => "ST",
                Self::DIRC => "DIRC",
                Self::CCAL => "CCAL",
                Self::CCAL_REF => "CCAL_REF",
                Self::ECAL => "ECAL",
                Self::ECAL_REF => "ECAL_REF",
                Self::TAGH => "TAGH",
                Self::RF => "RF",
                Self::PS => "PS",
                Self::PSC => "PSC",
                Self::FMWPC => "FMWPC",
                Self::TPOL => "TPOL",
                Self::TAC => "TAC",
                Self::TRD => "TRD",
                Self::CTOF => "CTOF",
                Self::HELI => "HELI",
                Self::ECAL_FCAL => "ECAL+FCAL",
            }
        )
    }
}

impl DetectorSystem {
    #[allow(clippy::match_str_case_mismatch)]
    pub fn from_string(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "CDC" => Self::CDC,
            "FDC" => Self::FDC,
            "BCAL" => Self::BCAL,
            "TOF" => Self::TOF,
            "Cherenkov" => Self::CHERENKOV,
            "FCAL" => Self::FCAL,
            "UPV" => Self::UPV,
            "TAGM" => Self::TAGM,
            "START" | "ST" | "SC" => Self::START,
            "DIRC" => Self::DIRC,
            "CCAL" => Self::CCAL,
            "CCAL_REF" => Self::CCAL_REF,
            "ECAL" => Self::ECAL,
            "ECAL_REF" => Self::ECAL_REF,
            "TAGH" => Self::TAGH,
            "RF" => Self::RF,
            "PS" => Self::PS,
            "PSC" => Self::PSC,
            "FMWPC" => Self::FMWPC,
            "TPOL" => Self::TPOL,
            "TAC" => Self::TAC,
            "TRD" => Self::TRD,
            "CTOF" => Self::CTOF,
            "HELI" => Self::HELI,
            "ECAL+FCAL" => Self::ECAL_FCAL,
            _ => Self::NULL,
        }
    }
}
