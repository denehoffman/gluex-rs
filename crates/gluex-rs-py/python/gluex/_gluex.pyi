from collections.abc import Sequence
from datetime import datetime
from types import ModuleType

def _console_main() -> int: ...

class Histogram:
    counts: list[float]
    edges: list[float]
    errors: list[float]

    def __init__(
        self,
        counts: Sequence[float],
        edges: Sequence[float],
        errors: Sequence[float] | None = None,
    ) -> None: ...
    def as_dict(self) -> dict[str, list[float]]: ...

class RunPeriod:
    RP2016_02: RunPeriod
    RP2017_01: RunPeriod
    RP2018_01: RunPeriod
    RP2018_08: RunPeriod
    RP2019_01: RunPeriod
    RP2019_11: RunPeriod
    RP2021_08: RunPeriod
    RP2021_11: RunPeriod
    RP2022_05: RunPeriod
    RP2022_08: RunPeriod
    RP2023_01: RunPeriod
    RP2025_01: RunPeriod
    def __init__(self, name: str) -> None: ...
    @staticmethod
    def from_run(run: int) -> RunPeriod: ...
    @property
    def min_run(self) -> int: ...
    @property
    def max_run(self) -> int: ...
    @property
    def short_name(self) -> str: ...
    def contains(self, run: int) -> bool: ...
    def coherent_peak(self) -> tuple[float, float]: ...

class RESTVersionSelection:
    @staticmethod
    def current() -> RESTVersionSelection: ...
    @staticmethod
    def version(run_period: RunPeriod, version: int) -> RESTVersionSelection: ...
    @staticmethod
    def timestamp(timestamp: datetime) -> RESTVersionSelection: ...
    def resolve_timestamp(self, run_period: RunPeriod) -> datetime: ...

class DetectorSystem:
    NULL: DetectorSystem
    CDC: DetectorSystem
    FDC: DetectorSystem
    BCAL: DetectorSystem
    TOF: DetectorSystem
    CHERENKOV: DetectorSystem
    FCAL: DetectorSystem
    UPV: DetectorSystem
    TAGM: DetectorSystem
    START: DetectorSystem
    DIRC: DetectorSystem
    CCAL: DetectorSystem
    CCAL_REF: DetectorSystem
    ECAL: DetectorSystem
    ECAL_REF: DetectorSystem
    TAGH: DetectorSystem
    RF: DetectorSystem
    PS: DetectorSystem
    PSC: DetectorSystem
    FMWPC: DetectorSystem
    TPOL: DetectorSystem
    TAC: DetectorSystem
    TRD: DetectorSystem
    CTOF: DetectorSystem
    HELI: DetectorSystem
    ECAL_FCAL: DetectorSystem
    def __init__(self, name: str) -> None: ...

class Polarization:
    AMO: Polarization
    PARA0: Polarization
    PERP45: Polarization
    PARA90: Polarization
    PERP135: Polarization

class Charge:
    Charged: Charge
    Positive: Charge
    Negative: Charge
    Neutral: Charge
    AllCharges: Charge

class Particle:
    UnknownParticle: Particle
    Gamma: Particle
    Positron: Particle
    Electron: Particle
    Neutrino: Particle
    MuonPlus: Particle
    MuonMinus: Particle
    Pi0: Particle
    PiPlus: Particle
    PiMinus: Particle
    KLong: Particle
    KPlus: Particle
    KMinus: Particle
    Neutron: Particle
    Proton: Particle
    AntiProton: Particle
    KShort: Particle
    Eta: Particle
    Lambda: Particle
    SigmaPlus: Particle
    Sigma0: Particle
    SigmaMinus: Particle
    Xi0: Particle
    XiMinus: Particle
    OmegaMinus: Particle
    AntiNeutron: Particle
    AntiLambda: Particle
    AntiSigmaMinus: Particle
    AntiSigma0: Particle
    AntiSigmaPlus: Particle
    AntiXi0: Particle
    AntiXiPlus: Particle
    AntiOmegaPlus: Particle
    Deuteron: Particle
    Triton: Particle
    Helium: Particle
    Geantino: Particle
    He3: Particle
    GammaOptical: Particle
    Li6: Particle
    Li7: Particle
    Be7: Particle
    Be9: Particle
    B10: Particle
    B11: Particle
    C12: Particle
    N14: Particle
    O16: Particle
    F19: Particle
    Ne20: Particle
    Na23: Particle
    Mg24: Particle
    Al27: Particle
    Si28: Particle
    P31: Particle
    S32: Particle
    Cl35: Particle
    Ar36: Particle
    K39: Particle
    Ca40: Particle
    Sc45: Particle
    Ti48: Particle
    V51: Particle
    Cr52: Particle
    Mn55: Particle
    Fe56: Particle
    Co59: Particle
    Ni58: Particle
    Cu63: Particle
    Zn64: Particle
    Ge74: Particle
    Se80: Particle
    Kr84: Particle
    Sr88: Particle
    Zr90: Particle
    Mo98: Particle
    Pd106: Particle
    Cd114: Particle
    Sn120: Particle
    Xe132: Particle
    Ba138: Particle
    Ce140: Particle
    Sm152: Particle
    Dy164: Particle
    Yb174: Particle
    W184: Particle
    Pt194: Particle
    Au197: Particle
    Hg202: Particle
    Pb208: Particle
    U238: Particle
    Ta181: Particle
    Rho0: Particle
    RhoPlus: Particle
    RhoMinus: Particle
    Omega: Particle
    Phi: Particle
    EtaPrime: Particle
    A0_980: Particle
    F0_980: Particle
    KStar892Zero: Particle
    KStar892Plus: Particle
    KStar892Minus: Particle
    AntiKStar892Zero: Particle
    K1Plus1400: Particle
    K1Minus1400: Particle
    B1Plus1235: Particle
    Sigma1385Minus: Particle
    Sigma1385Zero: Particle
    Sigma1385Plus: Particle
    Jpsi: Particle
    EtaC: Particle
    ChiC0: Particle
    ChiC1: Particle
    ChiC2: Particle
    Psi2s: Particle
    D0: Particle
    DPlus: Particle
    DStar0: Particle
    DStarPlus: Particle
    LambdaC: Particle
    AntiD0: Particle
    DMinus: Particle
    DStarMinus: Particle
    SigmaCPlusPlus: Particle
    DeltaPlusPlus: Particle
    def __init__(self, name: str) -> None: ...
    @staticmethod
    def from_particle_type(name: str) -> Particle: ...
    @staticmethod
    def from_geant4(name: str) -> Particle | None: ...
    @staticmethod
    def from_pdg(pdg_id: int) -> Particle: ...
    @staticmethod
    def from_multiplex_power(bit: int, decaying: bool) -> Particle: ...
    @staticmethod
    def from_charge_and_mass(charge: float, mass: float) -> Particle: ...
    def is_unknown(self) -> bool: ...
    def to_geant3(self) -> int: ...
    def is_lepton(self) -> bool: ...
    def to_particle_type(self) -> str: ...
    def to_evtgen(self) -> str: ...
    def short_name(self) -> str: ...
    def is_fixed_mass(self) -> bool: ...
    def is_resonance(self) -> bool: ...
    def is_detached_vertex(self) -> bool: ...
    def particle_name_root(self) -> str: ...
    def particle_mass(self) -> float: ...
    def to_geant4(self) -> str | None: ...
    def charge_number(self) -> int: ...
    def to_pdg(self) -> int: ...
    def is_decaying(self) -> bool: ...
    def is_final_state(self) -> bool: ...
    def to_multiplex_power(self) -> int | None: ...
    def charge(self) -> Charge: ...

def parse_timestamp(input: str) -> datetime: ...
def coherent_peak(run: int) -> tuple[float, float]: ...

class _CCDBModule(ModuleType): ...
class _GenerationModule(ModuleType): ...
class _LumiModule(ModuleType): ...
class _RCDBModule(ModuleType): ...

ccdb: _CCDBModule
generation: _GenerationModule
lumi: _LumiModule
rcdb: _RCDBModule
__version__: str
