use chrono::{DateTime, Utc};
use gluex_core::{
    Charge, DetectorSystem, GlueXCoreError, Histogram, Particle, Polarization, RESTVersion,
    RESTVersionSelection, RunNumber, RunPeriod, parsers::parse_timestamp,
    run_periods::coherent_peak,
};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyDict};

/// A one-dimensional histogram with per-bin uncertainties.
#[pyclass(name = "Histogram", module = "gluex")]
pub struct PyHistogram(pub(crate) Histogram);

#[pymethods]
impl PyHistogram {
    #[new]
    #[pyo3(signature = (counts, edges, errors=None))]
    fn new(counts: Vec<f64>, edges: Vec<f64>, errors: Option<Vec<f64>>) -> PyResult<Self> {
        Histogram::new(&counts, &edges, errors.as_deref())
            .map(Self)
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }

    #[getter]
    fn counts(&self) -> Vec<f64> {
        self.0.counts.clone()
    }

    #[getter]
    fn edges(&self) -> Vec<f64> {
        self.0.edges.clone()
    }

    #[getter]
    fn errors(&self) -> Vec<f64> {
        self.0.errors.clone()
    }

    /// Return the histogram data as serializable lists.
    pub fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        histogram_to_dict(py, &self.0)
    }
}

pub(crate) fn histogram_to_dict(py: Python<'_>, histogram: &Histogram) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("counts", histogram.counts.clone())?;
    dict.set_item("edges", histogram.edges.clone())?;
    dict.set_item("errors", histogram.errors.clone())?;
    Ok(dict.unbind())
}

pub(crate) fn histogram_to_py(py: Python<'_>, histogram: &Histogram) -> PyResult<Py<PyHistogram>> {
    Py::new(py, PyHistogram(histogram.clone()))
}

#[pyclass(
    name = "RunPeriod",
    module = "gluex",
    frozen,
    eq,
    hash,
    skip_from_py_object
)]
#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct PyRunPeriod(pub(crate) RunPeriod);

#[pymethods]
impl PyRunPeriod {
    #[new]
    fn new(name: &str) -> PyResult<Self> {
        name.parse()
            .map(Self)
            .map_err(|err: GlueXCoreError| PyValueError::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_run(run: RunNumber) -> PyResult<Self> {
        RunPeriod::try_from(run)
            .map(Self)
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }

    #[classattr]
    const RP2016_02: Self = Self(RunPeriod::RP2016_02);
    #[classattr]
    const RP2017_01: Self = Self(RunPeriod::RP2017_01);
    #[classattr]
    const RP2018_01: Self = Self(RunPeriod::RP2018_01);
    #[classattr]
    const RP2018_08: Self = Self(RunPeriod::RP2018_08);
    #[classattr]
    const RP2019_01: Self = Self(RunPeriod::RP2019_01);
    #[classattr]
    const RP2019_11: Self = Self(RunPeriod::RP2019_11);
    #[classattr]
    const RP2021_08: Self = Self(RunPeriod::RP2021_08);
    #[classattr]
    const RP2021_11: Self = Self(RunPeriod::RP2021_11);
    #[classattr]
    const RP2022_05: Self = Self(RunPeriod::RP2022_05);
    #[classattr]
    const RP2022_08: Self = Self(RunPeriod::RP2022_08);
    #[classattr]
    const RP2023_01: Self = Self(RunPeriod::RP2023_01);
    #[classattr]
    const RP2025_01: Self = Self(RunPeriod::RP2025_01);

    #[getter]
    fn min_run(&self) -> RunNumber {
        self.0.min_run()
    }

    #[getter]
    fn max_run(&self) -> RunNumber {
        self.0.max_run()
    }

    #[getter]
    fn short_name(&self) -> &str {
        self.0.short_name()
    }

    fn contains(&self, run: RunNumber) -> bool {
        self.0.contains(run)
    }

    fn coherent_peak(&self) -> (f64, f64) {
        coherent_peak(self.0.min_run())
    }

    fn __str__(&self) -> &str {
        self.0.short_name()
    }

    fn __repr__(&self) -> String {
        format!("RunPeriod.{:?}", self.0)
    }
}

#[pyclass(
    name = "RESTVersionSelection",
    module = "gluex",
    frozen,
    eq,
    hash,
    skip_from_py_object
)]
#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct PyRESTVersionSelection(pub(crate) RESTVersionSelection);

#[pymethods]
impl PyRESTVersionSelection {
    #[staticmethod]
    fn current() -> Self {
        Self(RESTVersionSelection::Current)
    }

    #[staticmethod]
    fn version(run_period: &PyRunPeriod, version: RESTVersion) -> PyResult<Self> {
        RESTVersionSelection::try_new(run_period.0, version)
            .map(Self)
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }

    #[staticmethod]
    fn timestamp(timestamp: DateTime<Utc>) -> Self {
        Self(RESTVersionSelection::from_timestamp(timestamp))
    }

    fn resolve_timestamp(&self, run_period: &PyRunPeriod) -> PyResult<DateTime<Utc>> {
        self.0
            .resolve_timestamp(run_period.0)
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }
}

#[pyclass(
    name = "DetectorSystem",
    module = "gluex",
    frozen,
    eq,
    hash,
    skip_from_py_object
)]
#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct PyDetectorSystem(DetectorSystem);

#[pymethods]
impl PyDetectorSystem {
    #[new]
    fn new(name: &str) -> Self {
        Self(DetectorSystem::from_string(name))
    }

    #[classattr]
    const NULL: Self = Self(DetectorSystem::NULL);
    #[classattr]
    const CDC: Self = Self(DetectorSystem::CDC);
    #[classattr]
    const FDC: Self = Self(DetectorSystem::FDC);
    #[classattr]
    const BCAL: Self = Self(DetectorSystem::BCAL);
    #[classattr]
    const TOF: Self = Self(DetectorSystem::TOF);
    #[classattr]
    const CHERENKOV: Self = Self(DetectorSystem::CHERENKOV);
    #[classattr]
    const FCAL: Self = Self(DetectorSystem::FCAL);
    #[classattr]
    const UPV: Self = Self(DetectorSystem::UPV);
    #[classattr]
    const TAGM: Self = Self(DetectorSystem::TAGM);
    #[classattr]
    const START: Self = Self(DetectorSystem::START);
    #[classattr]
    const DIRC: Self = Self(DetectorSystem::DIRC);
    #[classattr]
    const CCAL: Self = Self(DetectorSystem::CCAL);
    #[classattr]
    const CCAL_REF: Self = Self(DetectorSystem::CCAL_REF);
    #[classattr]
    const ECAL: Self = Self(DetectorSystem::ECAL);
    #[classattr]
    const ECAL_REF: Self = Self(DetectorSystem::ECAL_REF);
    #[classattr]
    const TAGH: Self = Self(DetectorSystem::TAGH);
    #[classattr]
    const RF: Self = Self(DetectorSystem::RF);
    #[classattr]
    const PS: Self = Self(DetectorSystem::PS);
    #[classattr]
    const PSC: Self = Self(DetectorSystem::PSC);
    #[classattr]
    const FMWPC: Self = Self(DetectorSystem::FMWPC);
    #[classattr]
    const TPOL: Self = Self(DetectorSystem::TPOL);
    #[classattr]
    const TAC: Self = Self(DetectorSystem::TAC);
    #[classattr]
    const TRD: Self = Self(DetectorSystem::TRD);
    #[classattr]
    const CTOF: Self = Self(DetectorSystem::CTOF);
    #[classattr]
    const HELI: Self = Self(DetectorSystem::HELI);
    #[classattr]
    const ECAL_FCAL: Self = Self(DetectorSystem::ECAL_FCAL);

    fn __str__(&self) -> String {
        self.0.to_string()
    }
}

#[pyclass(
    name = "Polarization",
    module = "gluex",
    frozen,
    eq,
    hash,
    skip_from_py_object
)]
#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct PyPolarization(Polarization);

#[pymethods]
impl PyPolarization {
    #[classattr]
    const AMO: Self = Self(Polarization::AMO);
    #[classattr]
    const PARA0: Self = Self(Polarization::Para0);
    #[classattr]
    const PERP45: Self = Self(Polarization::Perp45);
    #[classattr]
    const PARA90: Self = Self(Polarization::Para90);
    #[classattr]
    const PERP135: Self = Self(Polarization::Perp135);
}

#[pyclass(
    name = "Charge",
    module = "gluex",
    frozen,
    eq,
    hash,
    skip_from_py_object
)]
#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct PyCharge(Charge);

#[pymethods]
#[allow(non_upper_case_globals)]
impl PyCharge {
    #[classattr]
    const Charged: Self = Self(Charge::Charged);
    #[classattr]
    const Positive: Self = Self(Charge::Positive);
    #[classattr]
    const Negative: Self = Self(Charge::Negative);
    #[classattr]
    const Neutral: Self = Self(Charge::Neutral);
    #[classattr]
    const AllCharges: Self = Self(Charge::AllCharges);
}

#[pyclass(
    name = "Particle",
    module = "gluex",
    frozen,
    eq,
    hash,
    skip_from_py_object
)]
#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct PyParticle(Particle);

#[pymethods]
#[allow(clippy::wrong_self_convention, non_upper_case_globals)]
impl PyParticle {
    #[new]
    fn new(name: &str) -> PyResult<Self> {
        name.parse()
            .map(Self)
            .map_err(|err: GlueXCoreError| PyValueError::new_err(err.to_string()))
    }

    #[classattr]
    const UnknownParticle: Self = Self(Particle::UnknownParticle);
    #[classattr]
    const Gamma: Self = Self(Particle::Gamma);
    #[classattr]
    const Positron: Self = Self(Particle::Positron);
    #[classattr]
    const Electron: Self = Self(Particle::Electron);
    #[classattr]
    const Neutrino: Self = Self(Particle::Neutrino);
    #[classattr]
    const MuonPlus: Self = Self(Particle::MuonPlus);
    #[classattr]
    const MuonMinus: Self = Self(Particle::MuonMinus);
    #[classattr]
    const Pi0: Self = Self(Particle::Pi0);
    #[classattr]
    const PiPlus: Self = Self(Particle::PiPlus);
    #[classattr]
    const PiMinus: Self = Self(Particle::PiMinus);
    #[classattr]
    const KLong: Self = Self(Particle::KLong);
    #[classattr]
    const KPlus: Self = Self(Particle::KPlus);
    #[classattr]
    const KMinus: Self = Self(Particle::KMinus);
    #[classattr]
    const Neutron: Self = Self(Particle::Neutron);
    #[classattr]
    const Proton: Self = Self(Particle::Proton);
    #[classattr]
    const AntiProton: Self = Self(Particle::AntiProton);
    #[classattr]
    const KShort: Self = Self(Particle::KShort);
    #[classattr]
    const Eta: Self = Self(Particle::Eta);
    #[classattr]
    const Lambda: Self = Self(Particle::Lambda);
    #[classattr]
    const SigmaPlus: Self = Self(Particle::SigmaPlus);
    #[classattr]
    const Sigma0: Self = Self(Particle::Sigma0);
    #[classattr]
    const SigmaMinus: Self = Self(Particle::SigmaMinus);
    #[classattr]
    const Xi0: Self = Self(Particle::Xi0);
    #[classattr]
    const XiMinus: Self = Self(Particle::XiMinus);
    #[classattr]
    const OmegaMinus: Self = Self(Particle::OmegaMinus);
    #[classattr]
    const AntiNeutron: Self = Self(Particle::AntiNeutron);
    #[classattr]
    const AntiLambda: Self = Self(Particle::AntiLambda);
    #[classattr]
    const AntiSigmaMinus: Self = Self(Particle::AntiSigmaMinus);
    #[classattr]
    const AntiSigma0: Self = Self(Particle::AntiSigma0);
    #[classattr]
    const AntiSigmaPlus: Self = Self(Particle::AntiSigmaPlus);
    #[classattr]
    const AntiXi0: Self = Self(Particle::AntiXi0);
    #[classattr]
    const AntiXiPlus: Self = Self(Particle::AntiXiPlus);
    #[classattr]
    const AntiOmegaPlus: Self = Self(Particle::AntiOmegaPlus);
    #[classattr]
    const Deuteron: Self = Self(Particle::Deuteron);
    #[classattr]
    const Triton: Self = Self(Particle::Triton);
    #[classattr]
    const Helium: Self = Self(Particle::Helium);
    #[classattr]
    const Geantino: Self = Self(Particle::Geantino);
    #[classattr]
    const He3: Self = Self(Particle::He3);
    #[classattr]
    const GammaOptical: Self = Self(Particle::GammaOptical);
    #[classattr]
    const Li6: Self = Self(Particle::Li6);
    #[classattr]
    const Li7: Self = Self(Particle::Li7);
    #[classattr]
    const Be7: Self = Self(Particle::Be7);
    #[classattr]
    const Be9: Self = Self(Particle::Be9);
    #[classattr]
    const B10: Self = Self(Particle::B10);
    #[classattr]
    const B11: Self = Self(Particle::B11);
    #[classattr]
    const C12: Self = Self(Particle::C12);
    #[classattr]
    const N14: Self = Self(Particle::N14);
    #[classattr]
    const O16: Self = Self(Particle::O16);
    #[classattr]
    const F19: Self = Self(Particle::F19);
    #[classattr]
    const Ne20: Self = Self(Particle::Ne20);
    #[classattr]
    const Na23: Self = Self(Particle::Na23);
    #[classattr]
    const Mg24: Self = Self(Particle::Mg24);
    #[classattr]
    const Al27: Self = Self(Particle::Al27);
    #[classattr]
    const Si28: Self = Self(Particle::Si28);
    #[classattr]
    const P31: Self = Self(Particle::P31);
    #[classattr]
    const S32: Self = Self(Particle::S32);
    #[classattr]
    const Cl35: Self = Self(Particle::Cl35);
    #[classattr]
    const Ar36: Self = Self(Particle::Ar36);
    #[classattr]
    const K39: Self = Self(Particle::K39);
    #[classattr]
    const Ca40: Self = Self(Particle::Ca40);
    #[classattr]
    const Sc45: Self = Self(Particle::Sc45);
    #[classattr]
    const Ti48: Self = Self(Particle::Ti48);
    #[classattr]
    const V51: Self = Self(Particle::V51);
    #[classattr]
    const Cr52: Self = Self(Particle::Cr52);
    #[classattr]
    const Mn55: Self = Self(Particle::Mn55);
    #[classattr]
    const Fe56: Self = Self(Particle::Fe56);
    #[classattr]
    const Co59: Self = Self(Particle::Co59);
    #[classattr]
    const Ni58: Self = Self(Particle::Ni58);
    #[classattr]
    const Cu63: Self = Self(Particle::Cu63);
    #[classattr]
    const Zn64: Self = Self(Particle::Zn64);
    #[classattr]
    const Ge74: Self = Self(Particle::Ge74);
    #[classattr]
    const Se80: Self = Self(Particle::Se80);
    #[classattr]
    const Kr84: Self = Self(Particle::Kr84);
    #[classattr]
    const Sr88: Self = Self(Particle::Sr88);
    #[classattr]
    const Zr90: Self = Self(Particle::Zr90);
    #[classattr]
    const Mo98: Self = Self(Particle::Mo98);
    #[classattr]
    const Pd106: Self = Self(Particle::Pd106);
    #[classattr]
    const Cd114: Self = Self(Particle::Cd114);
    #[classattr]
    const Sn120: Self = Self(Particle::Sn120);
    #[classattr]
    const Xe132: Self = Self(Particle::Xe132);
    #[classattr]
    const Ba138: Self = Self(Particle::Ba138);
    #[classattr]
    const Ce140: Self = Self(Particle::Ce140);
    #[classattr]
    const Sm152: Self = Self(Particle::Sm152);
    #[classattr]
    const Dy164: Self = Self(Particle::Dy164);
    #[classattr]
    const Yb174: Self = Self(Particle::Yb174);
    #[classattr]
    const W184: Self = Self(Particle::W184);
    #[classattr]
    const Pt194: Self = Self(Particle::Pt194);
    #[classattr]
    const Au197: Self = Self(Particle::Au197);
    #[classattr]
    const Hg202: Self = Self(Particle::Hg202);
    #[classattr]
    const Pb208: Self = Self(Particle::Pb208);
    #[classattr]
    const U238: Self = Self(Particle::U238);
    #[classattr]
    const Ta181: Self = Self(Particle::Ta181);
    #[classattr]
    const Rho0: Self = Self(Particle::Rho0);
    #[classattr]
    const RhoPlus: Self = Self(Particle::RhoPlus);
    #[classattr]
    const RhoMinus: Self = Self(Particle::RhoMinus);
    #[classattr]
    const Omega: Self = Self(Particle::Omega);
    #[classattr]
    const Phi: Self = Self(Particle::Phi);
    #[classattr]
    const EtaPrime: Self = Self(Particle::EtaPrime);
    #[classattr]
    const A0_980: Self = Self(Particle::A0_980);
    #[classattr]
    const F0_980: Self = Self(Particle::F0_980);
    #[classattr]
    const KStar892Zero: Self = Self(Particle::KStar892Zero);
    #[classattr]
    const KStar892Plus: Self = Self(Particle::KStar892Plus);
    #[classattr]
    const KStar892Minus: Self = Self(Particle::KStar892Minus);
    #[classattr]
    const AntiKStar892Zero: Self = Self(Particle::AntiKStar892Zero);
    #[classattr]
    const K1Plus1400: Self = Self(Particle::K1Plus1400);
    #[classattr]
    const K1Minus1400: Self = Self(Particle::K1Minus1400);
    #[classattr]
    const B1Plus1235: Self = Self(Particle::B1Plus1235);
    #[classattr]
    const Sigma1385Minus: Self = Self(Particle::Sigma1385Minus);
    #[classattr]
    const Sigma1385Zero: Self = Self(Particle::Sigma1385Zero);
    #[classattr]
    const Sigma1385Plus: Self = Self(Particle::Sigma1385Plus);
    #[classattr]
    const Jpsi: Self = Self(Particle::Jpsi);
    #[classattr]
    const EtaC: Self = Self(Particle::EtaC);
    #[classattr]
    const ChiC0: Self = Self(Particle::ChiC0);
    #[classattr]
    const ChiC1: Self = Self(Particle::ChiC1);
    #[classattr]
    const ChiC2: Self = Self(Particle::ChiC2);
    #[classattr]
    const Psi2s: Self = Self(Particle::Psi2s);
    #[classattr]
    const D0: Self = Self(Particle::D0);
    #[classattr]
    const DPlus: Self = Self(Particle::DPlus);
    #[classattr]
    const DStar0: Self = Self(Particle::DStar0);
    #[classattr]
    const DStarPlus: Self = Self(Particle::DStarPlus);
    #[classattr]
    const LambdaC: Self = Self(Particle::LambdaC);
    #[classattr]
    const AntiD0: Self = Self(Particle::AntiD0);
    #[classattr]
    const DMinus: Self = Self(Particle::DMinus);
    #[classattr]
    const DStarMinus: Self = Self(Particle::DStarMinus);
    #[classattr]
    const SigmaCPlusPlus: Self = Self(Particle::SigmaCPlusPlus);
    #[classattr]
    const DeltaPlusPlus: Self = Self(Particle::DeltaPlusPlus);

    #[staticmethod]
    fn from_particle_type(name: &str) -> Self {
        Self(Particle::from_particle_type(name))
    }

    #[staticmethod]
    fn from_geant4(name: &str) -> Option<Self> {
        Particle::from_geant4(name).map(Self)
    }

    #[staticmethod]
    fn from_pdg(pdg_id: isize) -> Self {
        Self(Particle::from_pdg(pdg_id))
    }

    #[staticmethod]
    fn from_multiplex_power(bit: usize, decaying: bool) -> Self {
        Self(Particle::from_multiplex_power(bit, decaying))
    }

    #[staticmethod]
    fn from_charge_and_mass(charge: f64, mass: f64) -> Self {
        Self(Particle::from_charge_and_mass(charge, mass))
    }

    fn is_unknown(&self) -> bool {
        self.0.is_unknown()
    }

    fn to_geant3(&self) -> usize {
        self.0.to_geant3()
    }

    fn is_lepton(&self) -> bool {
        self.0.is_lepton()
    }

    fn to_particle_type(&self) -> &str {
        self.0.to_particle_type()
    }

    fn to_evtgen(&self) -> &str {
        self.0.to_evtgen()
    }

    fn short_name(&self) -> &str {
        self.0.short_name()
    }

    fn is_fixed_mass(&self) -> bool {
        self.0.is_fixed_mass()
    }

    fn is_resonance(&self) -> bool {
        self.0.is_resonance()
    }

    fn is_detached_vertex(&self) -> bool {
        self.0.is_detached_vertex()
    }

    fn particle_name_root(&self) -> &str {
        self.0.particle_name_root()
    }

    fn particle_mass(&self) -> f64 {
        self.0.particle_mass()
    }

    fn to_geant4(&self) -> Option<String> {
        self.0.to_geant4()
    }

    fn charge_number(&self) -> isize {
        self.0.charge_number()
    }

    fn to_pdg(&self) -> isize {
        self.0.to_pdg()
    }

    fn is_decaying(&self) -> bool {
        self.0.is_decaying()
    }

    fn is_final_state(&self) -> bool {
        self.0.is_final_state()
    }

    fn to_multiplex_power(&self) -> Option<usize> {
        self.0.to_multiplex_power()
    }

    fn charge(&self) -> PyCharge {
        PyCharge(self.0.charge())
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("Particle.{:?}", self.0)
    }
}

#[pyfunction(name = "parse_timestamp")]
pub fn py_parse_timestamp(input: &str) -> PyResult<DateTime<Utc>> {
    parse_timestamp(input).map_err(|err| PyValueError::new_err(err.to_string()))
}

#[pyfunction(name = "coherent_peak")]
pub fn py_coherent_peak(run: RunNumber) -> (f64, f64) {
    coherent_peak(run)
}

pub(crate) fn parse_run_period_object(object: &Bound<'_, PyAny>) -> PyResult<RunPeriod> {
    if let Ok(period) = object.extract::<PyRef<'_, PyRunPeriod>>() {
        return Ok(period.0);
    }
    if let Ok(name) = object.extract::<String>() {
        return name
            .parse()
            .map_err(|err: GlueXCoreError| PyValueError::new_err(err.to_string()));
    }
    Err(PyValueError::new_err(
        "run_period must be a RunPeriod or string",
    ))
}
