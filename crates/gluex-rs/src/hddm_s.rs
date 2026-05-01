use hddm::{HddmRead, HddmWrite};
#[allow(dead_code)]
pub const MODEL: &str = r#"<?xml version="1.0" encoding="iso-8859-1" standalone="no" ?>
<HDDM class="s" version="1.0" xmlns="http://www.gluex.org/hddm">
  <geometry maxOccurs="1" md5reconstruction="string" md5simulation="string" md5smear="string" minOccurs="0" />
  <physicsEvent eventNo="int" maxOccurs="unbounded" runNo="int">
    <dataVersionString maxOccurs="unbounded" minOccurs="0" text="string" />
    <ccdbContext maxOccurs="unbounded" minOccurs="0" text="string" />
    <reaction maxOccurs="unbounded" minOccurs="0" type="int" weight="float">
      <beam minOccurs="0" type="Particle_t">
        <momentum E="float" px="float" py="float" pz="float">
          <momentum_double minOccurs="0" E="double" px="double" py="double" pz="double" />
        </momentum>
        <polarization Px="float" Py="float" Pz="float" minOccurs="0" />
        <properties charge="int" mass="float" />
      </beam>
      <target minOccurs="0" type="Particle_t">
        <momentum E="float" px="float" py="float" pz="float">
          <momentum_double minOccurs="0" E="double" px="double" py="double" pz="double" />
        </momentum>
        <polarization Px="float" Py="float" Pz="float" minOccurs="0" />
        <properties charge="int" mass="float" />
      </target>
      <vertex maxOccurs="unbounded">
        <product decayVertex="int" id="int" maxOccurs="unbounded" mech="int" parentid="int" pdgtype="int" type="Particle_t">
          <momentum E="float" px="float" py="float" pz="float">
            <momentum_double minOccurs="0" E="double" px="double" py="double" pz="double" />
          </momentum>
          <polarization Px="float" Py="float" Pz="float" minOccurs="0" />
          <properties charge="int" mass="float" minOccurs="0" />
        </product>
        <origin t="float" vx="float" vy="float" vz="float" />
      </vertex>
      <random maxOccurs="1" minOccurs="0" seed1="int" seed2="int" seed3="int" seed4="int" />
      <userData description="string" maxOccurs="unbounded" minOccurs="0">
        <userDataFloat data="float" maxOccurs="unbounded" meaning="string" minOccurs="0" />
        <userDataInt data="int" maxOccurs="unbounded" meaning="string" minOccurs="0" />
      </userData>
    </reaction>
    <hitView minOccurs="0" version="2.0">
      <centralDC minOccurs="0">
        <cdcStraw maxOccurs="unbounded" minOccurs="0" ring="int" straw="int">
          <cdcStrawHit maxOccurs="unbounded" q="float" t="float">
            <cdcDigihit minOccurs="0" peakAmp="float" />
            <cdcHitQF QF="float" minOccurs="0" />
          </cdcStrawHit>
          <cdcStrawTruthHit d="float" itrack="int" maxOccurs="unbounded" ptype="int" q="float" t="float" />
        </cdcStraw>
        <cdcTruthPoint dEdx="float" dradius="float" maxOccurs="unbounded" minOccurs="0" phi="float" primary="boolean" ptype="int" px="float" py="float" pz="float" r="float" t="float" track="int" z="float">
          <trackID itrack="int" minOccurs="0" />
        </cdcTruthPoint>
      </centralDC>
      <forwardDC minOccurs="0">
        <fdcChamber layer="int" maxOccurs="unbounded" module="int">
          <fdcAnodeWire maxOccurs="unbounded" minOccurs="0" wire="int">
            <fdcAnodeHit dE="float" maxOccurs="unbounded" t="float" />
            <fdcAnodeTruthHit d="float" dE="float" itrack="int" maxOccurs="unbounded" ptype="int" t="float" t_unsmeared="float" />
          </fdcAnodeWire>
          <fdcCathodeStrip maxOccurs="unbounded" minOccurs="0" plane="int" strip="int">
            <fdcCathodeHit maxOccurs="unbounded" q="float" t="float">
              <fdcDigihit minOccurs="0" peakAmp="float" />
            </fdcCathodeHit>
            <fdcCathodeTruthHit itrack="int" maxOccurs="unbounded" ptype="int" q="float" t="float" />
          </fdcCathodeStrip>
          <fdcTruthPoint E="float" dEdx="float" dradius="float" maxOccurs="unbounded" minOccurs="0" primary="boolean" ptype="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float">
            <trackID itrack="int" minOccurs="0" />
          </fdcTruthPoint>
        </fdcChamber>
      </forwardDC>
      <startCntr minOccurs="0">
        <stcPaddle maxOccurs="unbounded" minOccurs="0" sector="int">
          <stcHit dE="float" maxOccurs="unbounded" t="float">
            <stcDigihit minOccurs="0" peakAmp="float" />
          </stcHit>
          <stcTruthHit dE="float" itrack="int" maxOccurs="unbounded" ptype="int" t="float" />
        </stcPaddle>
        <stcTruthPoint E="float" dEdx="float" maxOccurs="unbounded" minOccurs="0" phi="float" primary="boolean" ptype="int" px="float" py="float" pz="float" r="float" sector="int" t="float" track="int" z="float">
          <trackID itrack="int" minOccurs="0" />
        </stcTruthPoint>
      </startCntr>
      <barrelEMcal minOccurs="0">
        <bcalCell layer="int" maxOccurs="unbounded" minOccurs="0" module="int" sector="int">
          <bcalSiPMUpHit E="float" maxOccurs="unbounded" minOccurs="0" t="float" />
          <bcalSiPMDownHit E="float" maxOccurs="unbounded" minOccurs="0" t="float" />
          <bcalSiPMSpectrum bin_width="float" end="int" maxOccurs="unbounded" minOccurs="0" tstart="float" vals="string">
            <bcalSiPMTruth E="float" incident_id="int" minOccurs="0" />
          </bcalSiPMSpectrum>
          <bcalfADCHit E="float" end="int" maxOccurs="unbounded" minOccurs="0" t="float" />
          <bcalfADCDigiHit end="int" maxOccurs="unbounded" minOccurs="0" pulse_integral="int" pulse_time="int">
            <bcalfADCPeak minOccurs="0" peakAmp="float" />
          </bcalfADCDigiHit>
          <bcalTDCHit end="int" maxOccurs="unbounded" minOccurs="0" t="float" />
          <bcalTDCDigiHit end="int" maxOccurs="unbounded" minOccurs="0" time="float" />
          <bcalTruthHit E="float" incident_id="int" maxOccurs="unbounded" minOccurs="0" t="float" zLocal="float" />
        </bcalCell>
        <bcalTruthIncidentParticle id="int" maxOccurs="unbounded" minOccurs="0" ptype="int" px="float" py="float" pz="float" x="float" y="float" z="float" />
        <bcalTruthShower E="float" maxOccurs="unbounded" minOccurs="0" phi="float" primary="boolean" ptype="int" px="float" py="float" pz="float" r="float" t="float" track="int" z="float">
          <trackID itrack="int" minOccurs="0" />
        </bcalTruthShower>
      </barrelEMcal>
      <gapEMcal minOccurs="0">
        <gcalCell maxOccurs="48" minOccurs="0" module="int">
          <gcalHit E="float" maxOccurs="unbounded" minOccurs="0" t="float" zLocal="float" />
          <gcalTruthHit E="float" maxOccurs="unbounded" minOccurs="0" t="float" zLocal="float" />
        </gcalCell>
        <gcalTruthShower E="float" maxOccurs="unbounded" minOccurs="0" phi="float" primary="boolean" ptype="int" px="float" py="float" pz="float" r="float" t="float" track="int" z="float">
          <trackID itrack="int" minOccurs="0" />
        </gcalTruthShower>
      </gapEMcal>
      <Cerenkov minOccurs="0">
        <cereSection maxOccurs="unbounded" minOccurs="0" sector="int">
          <cereHit maxOccurs="unbounded" pe="float" t="float" />
          <cereTruthHit maxOccurs="unbounded" pe="float" t="float" />
        </cereSection>
        <cereTruthPoint E="float" maxOccurs="unbounded" minOccurs="0" primary="boolean" ptype="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float">
          <trackID itrack="int" minOccurs="0" />
        </cereTruthPoint>
      </Cerenkov>
      <RICH minOccurs="0">
        <richTruthHit maxOccurs="unbounded" minOccurs="0" t="float" x="float" y="float" z="float" />
        <richTruthPoint E="float" maxOccurs="unbounded" minOccurs="0" primary="boolean" ptype="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float">
          <trackID itrack="int" minOccurs="0" />
        </richTruthPoint>
      </RICH>
      <DIRC minOccurs="0">
        <dircTruthBarHit E="float" bar="int" maxOccurs="unbounded" minOccurs="0" pdg="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float" />
        <dircTruthPmtHit E="float" ch="int" key_bar="int" maxOccurs="unbounded" minOccurs="0" t="float" x="float" y="float" z="float">
          <dircTruthPmtHitExtra bbrefl="boolean" maxOccurs="unbounded" minOccurs="0" path="long" refl="int" t_fixed="float" />
        </dircTruthPmtHit>
        <dircPmtHit ch="int" maxOccurs="unbounded" minOccurs="0" t="float" />
      </DIRC>
      <forwardTOF minOccurs="0">
        <ftofCounter bar="int" maxOccurs="unbounded" minOccurs="0" plane="int">
          <ftofHit dE="float" end="int" maxOccurs="unbounded" minOccurs="0" t="float">
            <ftofDigihit minOccurs="0" peakAmp="float" />
          </ftofHit>
          <ftofTruthHit dE="float" end="int" maxOccurs="unbounded" minOccurs="0" t="float">
            <ftofTruthExtra E="float" dist="float" itrack="int" maxOccurs="unbounded" minOccurs="0" ptype="int" px="float" py="float" pz="float" x="float" y="float" z="float" />
          </ftofTruthHit>
        </ftofCounter>
        <ftofTruthPoint E="float" maxOccurs="unbounded" minOccurs="0" primary="boolean" ptype="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float">
          <trackID itrack="int" minOccurs="0" />
        </ftofTruthPoint>
      </forwardTOF>
      <forwardEMcal minOccurs="0">
        <fcalBlock column="int" maxOccurs="unbounded" minOccurs="0" row="int">
          <fcalHit E="float" maxOccurs="unbounded" t="float">
            <fcalDigihit integralOverPeak="float" minOccurs="0" />
          </fcalHit>
          <fcalTruthHit E="float" maxOccurs="unbounded" t="float">
            <fcalTruthLightGuide dE="float" maxOccurs="unbounded" t="float" />
          </fcalTruthHit>
        </fcalBlock>
        <fcalTruthShower E="float" maxOccurs="unbounded" minOccurs="0" primary="boolean" ptype="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float">
          <trackID itrack="int" minOccurs="0" />
        </fcalTruthShower>
      </forwardEMcal>
      <ComptonEMcal minOccurs="0">
        <ccalBlock column="int" maxOccurs="unbounded" minOccurs="0" row="int">
          <ccalHit E="float" maxOccurs="unbounded" t="float" />
          <ccalTruthHit E="float" maxOccurs="unbounded" t="float" />
        </ccalBlock>
        <ccalTruthShower E="float" maxOccurs="unbounded" minOccurs="0" primary="boolean" ptype="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float">
          <trackID itrack="int" minOccurs="0" />
        </ccalTruthShower>
      </ComptonEMcal>
      <upstreamEMveto minOccurs="0">
        <upvPaddle layer="int" maxOccurs="unbounded" minOccurs="0" row="int">
          <upvHit E="float" end="int" maxOccurs="unbounded" minOccurs="0" t="float" />
          <upvTruthHit E="float" end="int" maxOccurs="unbounded" minOccurs="0" t="float" xlocal="float" />
        </upvPaddle>
        <upvTruthShower E="float" maxOccurs="unbounded" minOccurs="0" primary="boolean" ptype="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float">
          <trackID itrack="int" minOccurs="0" />
        </upvTruthShower>
      </upstreamEMveto>
      <tagger minOccurs="0">
        <microChannel E="float" column="int" maxOccurs="unbounded" minOccurs="0" row="int">
          <taggerHit maxOccurs="unbounded" minOccurs="0" npe="int" t="float" tADC="float" />
          <taggerTruthHit E="float" bg="int" dE="float" maxOccurs="unbounded" minOccurs="0" t="float" />
        </microChannel>
        <hodoChannel E="float" counterId="int" maxOccurs="unbounded" minOccurs="0">
          <taggerHit maxOccurs="unbounded" minOccurs="0" npe="int" t="float" tADC="float" />
          <taggerTruthHit E="float" bg="int" dE="float" maxOccurs="unbounded" minOccurs="0" t="float" />
        </hodoChannel>
      </tagger>
      <pairSpectrometerFine minOccurs="0">
        <psTile arm="int" column="int" maxOccurs="unbounded" minOccurs="0">
          <psHit dE="float" maxOccurs="unbounded" t="float" />
          <psTruthHit dE="float" itrack="int" maxOccurs="unbounded" ptype="int" t="float" />
        </psTile>
        <psTruthPoint E="float" arm="int" column="int" dEdx="float" maxOccurs="unbounded" minOccurs="0" primary="boolean" ptype="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float">
          <trackID itrack="int" minOccurs="0" />
        </psTruthPoint>
      </pairSpectrometerFine>
      <pairSpectrometerCoarse minOccurs="0">
        <pscPaddle arm="int" maxOccurs="unbounded" minOccurs="0" module="int">
          <pscHit dE="float" maxOccurs="unbounded" t="float" />
          <pscTruthHit dE="float" itrack="int" maxOccurs="unbounded" ptype="int" t="float" />
        </pscPaddle>
        <pscTruthPoint E="float" arm="int" dEdx="float" maxOccurs="unbounded" minOccurs="0" module="int" primary="boolean" ptype="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float">
          <trackID itrack="int" minOccurs="0" />
        </pscTruthPoint>
      </pairSpectrometerCoarse>
      <tripletPolarimeter minOccurs="0">
        <tpolSector maxOccurs="unbounded" minOccurs="0" ring="int" sector="int">
          <tpolHit dE="float" maxOccurs="unbounded" t="float" />
          <tpolTruthHit dE="float" itrack="int" maxOccurs="unbounded" ptype="int" t="float" />
        </tpolSector>
        <tpolTruthPoint E="float" dEdx="float" maxOccurs="unbounded" minOccurs="0" phi="float" primary="boolean" ptype="int" px="float" py="float" pz="float" r="float" t="float" track="int">
          <trackID itrack="int" minOccurs="0" />
        </tpolTruthPoint>
      </tripletPolarimeter>
      <mcTrajectory minOccurs="0">
        <mcTrajectoryPoint E="float" dE="float" maxOccurs="unbounded" mech="int" minOccurs="0" part="int" primary_track="int" px="float" py="float" pz="float" radlen="float" step="float" t="float" track="int" x="float" y="float" z="float" />
      </mcTrajectory>
      <RFtime jtag="string" minOccurs="0" tsync="float" tunit="ns">
        <RFsubsystem jtag="string" maxOccurs="unbounded" minOccurs="0" tsync="float" tunit="ns" />
      </RFtime>
      <forwardMWPC minOccurs="0">
        <fmwpcChamber layer="int" maxOccurs="unbounded" minOccurs="0" wire="int">
          <fmwpcTruthHit dE="float" dx="float" maxOccurs="unbounded" t="float" />
          <fmwpcHit dE="float" maxOccurs="unbounded" t="float" />
        </fmwpcChamber>
        <fmwpcTruthPoint E="float" maxOccurs="unbounded" minOccurs="0" primary="boolean" ptype="int" px="float" py="float" pz="float" t="float" track="int" x="float" y="float" z="float">
          <trackID itrack="int" minOccurs="0" />
        </fmwpcTruthPoint>
      </forwardMWPC>
    </hitView>
    <reconView minOccurs="0" version="1.0">
      <tracktimebased FOM="float" Ndof="int" candidateid="int" chisq="float" id="int" maxOccurs="unbounded" minOccurs="0" trackid="int">
        <momentum E="float" px="float" py="float" pz="float">
          <momentum_double minOccurs="0" E="double" px="double" py="double" pz="double" />
        </momentum>
        <properties charge="int" mass="float" />
        <origin t="float" vx="float" vy="float" vz="float" />
        <errorMatrix Ncols="int" Nrows="int" type="string" vals="string" />
        <TrackingErrorMatrix Ncols="int" Nrows="int" type="string" vals="string" />
      </tracktimebased>
    </reconView>
  </physicsEvent>
</HDDM>
"#;
#[allow(dead_code)]
pub const HDDM_CLASS: &str = "s";
#[allow(dead_code)]
pub type Root = Hddm;
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Geometry {
    pub md5reconstruction: String,
    pub md5simulation: String,
    pub md5smear: String,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct DataVersionString {
    pub text: String,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CcdbContext {
    pub text: String,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct MomentumDouble {
    pub e: f64,
    pub px: f64,
    pub py: f64,
    pub pz: f64,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Momentum {
    pub e: f32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub momentum_double: Option<MomentumDouble>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Polarization {
    pub px: f32,
    pub py: f32,
    pub pz: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Properties {
    pub charge: i32,
    pub mass: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Beam {
    pub type_: ::hddm::Particle,
    pub momentum: Momentum,
    pub polarization: Option<Polarization>,
    pub properties: Properties,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Target {
    pub type_: ::hddm::Particle,
    pub momentum: Momentum,
    pub polarization: Option<Polarization>,
    pub properties: Properties,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Product {
    pub decay_vertex: i32,
    pub id: i32,
    pub mech: i32,
    pub parentid: i32,
    pub pdgtype: i32,
    pub type_: ::hddm::Particle,
    pub momentum: Momentum,
    pub polarization: Option<Polarization>,
    pub properties: Option<Properties>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Origin {
    pub t: f32,
    pub vx: f32,
    pub vy: f32,
    pub vz: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Vertex {
    pub product: Vec<Product>,
    pub origin: Origin,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Random {
    pub seed1: i32,
    pub seed2: i32,
    pub seed3: i32,
    pub seed4: i32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct UserDataFloat {
    pub data: f32,
    pub meaning: String,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct UserDataInt {
    pub data: i32,
    pub meaning: String,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct UserData {
    pub description: String,
    pub user_data_float: Vec<UserDataFloat>,
    pub user_data_int: Vec<UserDataInt>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Reaction {
    pub type_: i32,
    pub weight: f32,
    pub beam: Option<Beam>,
    pub target: Option<Target>,
    pub vertex: Vec<Vertex>,
    pub random: Option<Random>,
    pub user_data: Vec<UserData>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CdcDigihit {
    pub peak_amp: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CdcHitQf {
    pub qf: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CdcStrawHit {
    pub q: f32,
    pub t: f32,
    pub cdc_digihit: Option<CdcDigihit>,
    pub cdc_hit_qf: Option<CdcHitQf>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CdcStrawTruthHit {
    pub d: f32,
    pub itrack: i32,
    pub ptype: i32,
    pub q: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CdcStraw {
    pub ring: i32,
    pub straw: i32,
    pub cdc_straw_hit: Vec<CdcStrawHit>,
    pub cdc_straw_truth_hit: Vec<CdcStrawTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct TrackId {
    pub itrack: i32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CdcTruthPoint {
    pub d_edx: f32,
    pub dradius: f32,
    pub phi: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub r: f32,
    pub t: f32,
    pub track: i32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CentralDc {
    pub cdc_straw: Vec<CdcStraw>,
    pub cdc_truth_point: Vec<CdcTruthPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FdcAnodeHit {
    pub d_e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FdcAnodeTruthHit {
    pub d: f32,
    pub d_e: f32,
    pub itrack: i32,
    pub ptype: i32,
    pub t: f32,
    pub t_unsmeared: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FdcAnodeWire {
    pub wire: i32,
    pub fdc_anode_hit: Vec<FdcAnodeHit>,
    pub fdc_anode_truth_hit: Vec<FdcAnodeTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FdcDigihit {
    pub peak_amp: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FdcCathodeHit {
    pub q: f32,
    pub t: f32,
    pub fdc_digihit: Option<FdcDigihit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FdcCathodeTruthHit {
    pub itrack: i32,
    pub ptype: i32,
    pub q: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FdcCathodeStrip {
    pub plane: i32,
    pub strip: i32,
    pub fdc_cathode_hit: Vec<FdcCathodeHit>,
    pub fdc_cathode_truth_hit: Vec<FdcCathodeTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FdcTruthPoint {
    pub e: f32,
    pub d_edx: f32,
    pub dradius: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FdcChamber {
    pub layer: i32,
    pub module: i32,
    pub fdc_anode_wire: Vec<FdcAnodeWire>,
    pub fdc_cathode_strip: Vec<FdcCathodeStrip>,
    pub fdc_truth_point: Vec<FdcTruthPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct ForwardDc {
    pub fdc_chamber: Vec<FdcChamber>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct StcDigihit {
    pub peak_amp: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct StcHit {
    pub d_e: f32,
    pub t: f32,
    pub stc_digihit: Option<StcDigihit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct StcTruthHit {
    pub d_e: f32,
    pub itrack: i32,
    pub ptype: i32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct StcPaddle {
    pub sector: i32,
    pub stc_hit: Vec<StcHit>,
    pub stc_truth_hit: Vec<StcTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct StcTruthPoint {
    pub e: f32,
    pub d_edx: f32,
    pub phi: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub r: f32,
    pub sector: i32,
    pub t: f32,
    pub track: i32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct StartCntr {
    pub stc_paddle: Vec<StcPaddle>,
    pub stc_truth_point: Vec<StcTruthPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalSiPmUpHit {
    pub e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalSiPmDownHit {
    pub e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalSiPmTruth {
    pub e: f32,
    pub incident_id: i32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalSiPmSpectrum {
    pub bin_width: f32,
    pub end: i32,
    pub tstart: f32,
    pub vals: String,
    pub bcal_si_pm_truth: Option<BcalSiPmTruth>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalfAdcHit {
    pub e: f32,
    pub end: i32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalfAdcPeak {
    pub peak_amp: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalfAdcDigiHit {
    pub end: i32,
    pub pulse_integral: i32,
    pub pulse_time: i32,
    pub bcalf_adc_peak: Option<BcalfAdcPeak>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalTdcHit {
    pub end: i32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalTdcDigiHit {
    pub end: i32,
    pub time: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalTruthHit {
    pub e: f32,
    pub incident_id: i32,
    pub t: f32,
    pub z_local: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalCell {
    pub layer: i32,
    pub module: i32,
    pub sector: i32,
    pub bcal_si_pm_up_hit: Vec<BcalSiPmUpHit>,
    pub bcal_si_pm_down_hit: Vec<BcalSiPmDownHit>,
    pub bcal_si_pm_spectrum: Vec<BcalSiPmSpectrum>,
    pub bcalf_adc_hit: Vec<BcalfAdcHit>,
    pub bcalf_adc_digi_hit: Vec<BcalfAdcDigiHit>,
    pub bcal_tdc_hit: Vec<BcalTdcHit>,
    pub bcal_tdc_digi_hit: Vec<BcalTdcDigiHit>,
    pub bcal_truth_hit: Vec<BcalTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalTruthIncidentParticle {
    pub id: i32,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BcalTruthShower {
    pub e: f32,
    pub phi: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub r: f32,
    pub t: f32,
    pub track: i32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct BarrelEMcal {
    pub bcal_cell: Vec<BcalCell>,
    pub bcal_truth_incident_particle: Vec<BcalTruthIncidentParticle>,
    pub bcal_truth_shower: Vec<BcalTruthShower>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct GcalHit {
    pub e: f32,
    pub t: f32,
    pub z_local: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct GcalTruthHit {
    pub e: f32,
    pub t: f32,
    pub z_local: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct GcalCell {
    pub module: i32,
    pub gcal_hit: Vec<GcalHit>,
    pub gcal_truth_hit: Vec<GcalTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct GcalTruthShower {
    pub e: f32,
    pub phi: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub r: f32,
    pub t: f32,
    pub track: i32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct GapEMcal {
    pub gcal_cell: Option<GcalCell>,
    pub gcal_truth_shower: Vec<GcalTruthShower>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CereHit {
    pub pe: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CereTruthHit {
    pub pe: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CereSection {
    pub sector: i32,
    pub cere_hit: Vec<CereHit>,
    pub cere_truth_hit: Vec<CereTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CereTruthPoint {
    pub e: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Cerenkov {
    pub cere_section: Vec<CereSection>,
    pub cere_truth_point: Vec<CereTruthPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct RichTruthHit {
    pub t: f32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct RichTruthPoint {
    pub e: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Rich {
    pub rich_truth_hit: Vec<RichTruthHit>,
    pub rich_truth_point: Vec<RichTruthPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct DircTruthBarHit {
    pub e: f32,
    pub bar: i32,
    pub pdg: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct DircTruthPmtHitExtra {
    pub bbrefl: bool,
    pub path: i64,
    pub refl: i32,
    pub t_fixed: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct DircTruthPmtHit {
    pub e: f32,
    pub ch: i32,
    pub key_bar: i32,
    pub t: f32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub dirc_truth_pmt_hit_extra: Vec<DircTruthPmtHitExtra>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct DircPmtHit {
    pub ch: i32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Dirc {
    pub dirc_truth_bar_hit: Vec<DircTruthBarHit>,
    pub dirc_truth_pmt_hit: Vec<DircTruthPmtHit>,
    pub dirc_pmt_hit: Vec<DircPmtHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FtofDigihit {
    pub peak_amp: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FtofHit {
    pub d_e: f32,
    pub end: i32,
    pub t: f32,
    pub ftof_digihit: Option<FtofDigihit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FtofTruthExtra {
    pub e: f32,
    pub dist: f32,
    pub itrack: i32,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FtofTruthHit {
    pub d_e: f32,
    pub end: i32,
    pub t: f32,
    pub ftof_truth_extra: Vec<FtofTruthExtra>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FtofCounter {
    pub bar: i32,
    pub plane: i32,
    pub ftof_hit: Vec<FtofHit>,
    pub ftof_truth_hit: Vec<FtofTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FtofTruthPoint {
    pub e: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct ForwardTof {
    pub ftof_counter: Vec<FtofCounter>,
    pub ftof_truth_point: Vec<FtofTruthPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FcalDigihit {
    pub integral_over_peak: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FcalHit {
    pub e: f32,
    pub t: f32,
    pub fcal_digihit: Option<FcalDigihit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FcalTruthLightGuide {
    pub d_e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FcalTruthHit {
    pub e: f32,
    pub t: f32,
    pub fcal_truth_light_guide: Vec<FcalTruthLightGuide>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FcalBlock {
    pub column: i32,
    pub row: i32,
    pub fcal_hit: Vec<FcalHit>,
    pub fcal_truth_hit: Vec<FcalTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FcalTruthShower {
    pub e: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct ForwardEMcal {
    pub fcal_block: Vec<FcalBlock>,
    pub fcal_truth_shower: Vec<FcalTruthShower>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CcalHit {
    pub e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CcalTruthHit {
    pub e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CcalBlock {
    pub column: i32,
    pub row: i32,
    pub ccal_hit: Vec<CcalHit>,
    pub ccal_truth_hit: Vec<CcalTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct CcalTruthShower {
    pub e: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct ComptonEMcal {
    pub ccal_block: Vec<CcalBlock>,
    pub ccal_truth_shower: Vec<CcalTruthShower>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct UpvHit {
    pub e: f32,
    pub end: i32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct UpvTruthHit {
    pub e: f32,
    pub end: i32,
    pub t: f32,
    pub xlocal: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct UpvPaddle {
    pub layer: i32,
    pub row: i32,
    pub upv_hit: Vec<UpvHit>,
    pub upv_truth_hit: Vec<UpvTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct UpvTruthShower {
    pub e: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct UpstreamEMveto {
    pub upv_paddle: Vec<UpvPaddle>,
    pub upv_truth_shower: Vec<UpvTruthShower>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct TaggerHit {
    pub npe: i32,
    pub t: f32,
    pub t_adc: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct TaggerTruthHit {
    pub e: f32,
    pub bg: i32,
    pub d_e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct MicroChannel {
    pub e: f32,
    pub column: i32,
    pub row: i32,
    pub tagger_hit: Vec<TaggerHit>,
    pub tagger_truth_hit: Vec<TaggerTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct HodoChannel {
    pub e: f32,
    pub counter_id: i32,
    pub tagger_hit: Vec<TaggerHit>,
    pub tagger_truth_hit: Vec<TaggerTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Tagger {
    pub micro_channel: Vec<MicroChannel>,
    pub hodo_channel: Vec<HodoChannel>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PsHit {
    pub d_e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PsTruthHit {
    pub d_e: f32,
    pub itrack: i32,
    pub ptype: i32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PsTile {
    pub arm: i32,
    pub column: i32,
    pub ps_hit: Vec<PsHit>,
    pub ps_truth_hit: Vec<PsTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PsTruthPoint {
    pub e: f32,
    pub arm: i32,
    pub column: i32,
    pub d_edx: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PairSpectrometerFine {
    pub ps_tile: Vec<PsTile>,
    pub ps_truth_point: Vec<PsTruthPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PscHit {
    pub d_e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PscTruthHit {
    pub d_e: f32,
    pub itrack: i32,
    pub ptype: i32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PscPaddle {
    pub arm: i32,
    pub module: i32,
    pub psc_hit: Vec<PscHit>,
    pub psc_truth_hit: Vec<PscTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PscTruthPoint {
    pub e: f32,
    pub arm: i32,
    pub d_edx: f32,
    pub module: i32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PairSpectrometerCoarse {
    pub psc_paddle: Vec<PscPaddle>,
    pub psc_truth_point: Vec<PscTruthPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct TpolHit {
    pub d_e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct TpolTruthHit {
    pub d_e: f32,
    pub itrack: i32,
    pub ptype: i32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct TpolSector {
    pub ring: i32,
    pub sector: i32,
    pub tpol_hit: Vec<TpolHit>,
    pub tpol_truth_hit: Vec<TpolTruthHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct TpolTruthPoint {
    pub e: f32,
    pub d_edx: f32,
    pub phi: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub r: f32,
    pub t: f32,
    pub track: i32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct TripletPolarimeter {
    pub tpol_sector: Vec<TpolSector>,
    pub tpol_truth_point: Vec<TpolTruthPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct McTrajectoryPoint {
    pub e: f32,
    pub d_e: f32,
    pub mech: i32,
    pub part: i32,
    pub primary_track: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub radlen: f32,
    pub step: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct McTrajectory {
    pub mc_trajectory_point: Vec<McTrajectoryPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct RFsubsystem {
    pub jtag: String,
    pub tsync: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct RFtime {
    pub jtag: String,
    pub tsync: f32,
    pub r_fsubsystem: Vec<RFsubsystem>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FmwpcTruthHit {
    pub d_e: f32,
    pub dx: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FmwpcHit {
    pub d_e: f32,
    pub t: f32,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FmwpcChamber {
    pub layer: i32,
    pub wire: i32,
    pub fmwpc_truth_hit: Vec<FmwpcTruthHit>,
    pub fmwpc_hit: Vec<FmwpcHit>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct FmwpcTruthPoint {
    pub e: f32,
    pub primary: bool,
    pub ptype: i32,
    pub px: f32,
    pub py: f32,
    pub pz: f32,
    pub t: f32,
    pub track: i32,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub track_id: Option<TrackId>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct ForwardMwpc {
    pub fmwpc_chamber: Vec<FmwpcChamber>,
    pub fmwpc_truth_point: Vec<FmwpcTruthPoint>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct HitView {
    pub central_dc: Option<CentralDc>,
    pub forward_dc: Option<ForwardDc>,
    pub start_cntr: Option<StartCntr>,
    pub barrel_e_mcal: Option<BarrelEMcal>,
    pub gap_e_mcal: Option<GapEMcal>,
    pub cerenkov: Option<Cerenkov>,
    pub rich: Option<Rich>,
    pub dirc: Option<Dirc>,
    pub forward_tof: Option<ForwardTof>,
    pub forward_e_mcal: Option<ForwardEMcal>,
    pub compton_e_mcal: Option<ComptonEMcal>,
    pub upstream_e_mveto: Option<UpstreamEMveto>,
    pub tagger: Option<Tagger>,
    pub pair_spectrometer_fine: Option<PairSpectrometerFine>,
    pub pair_spectrometer_coarse: Option<PairSpectrometerCoarse>,
    pub triplet_polarimeter: Option<TripletPolarimeter>,
    pub mc_trajectory: Option<McTrajectory>,
    pub r_ftime: Option<RFtime>,
    pub forward_mwpc: Option<ForwardMwpc>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct ErrorMatrix {
    pub ncols: i32,
    pub nrows: i32,
    pub type_: String,
    pub vals: String,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct TrackingErrorMatrix {
    pub ncols: i32,
    pub nrows: i32,
    pub type_: String,
    pub vals: String,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Tracktimebased {
    pub fom: f32,
    pub ndof: i32,
    pub candidateid: i32,
    pub chisq: f32,
    pub id: i32,
    pub trackid: i32,
    pub momentum: Momentum,
    pub properties: Properties,
    pub origin: Origin,
    pub error_matrix: ErrorMatrix,
    pub tracking_error_matrix: TrackingErrorMatrix,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct ReconView {
    pub tracktimebased: Vec<Tracktimebased>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct PhysicsEvent {
    pub event_no: i32,
    pub run_no: i32,
    pub data_version_string: Vec<DataVersionString>,
    pub ccdb_context: Vec<CcdbContext>,
    pub reaction: Vec<Reaction>,
    pub hit_view: Option<HitView>,
    pub recon_view: Option<ReconView>,
}
#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, HddmRead, HddmWrite)]
pub struct Hddm {
    pub geometry: Option<Geometry>,
    pub physics_event: Vec<PhysicsEvent>,
}
#[allow(dead_code)]
pub fn open<P: AsRef<std::path::Path>>(path: P) -> ::hddm::HddmResult<::hddm::HddmFileReader> {
    ::hddm::HddmFileReader::open(path)
}
#[allow(dead_code)]
pub fn create<P: AsRef<std::path::Path>>(path: P) -> ::hddm::HddmResult<::hddm::HddmFileWriter> {
    ::hddm::HddmFileWriter::new(
        path,
        ::hddm::WriteMode::Create {
            model: MODEL.to_string(),
        },
        ::hddm::Compression::None,
    )
}
#[allow(dead_code)]
pub fn append<P: AsRef<std::path::Path>>(path: P) -> ::hddm::HddmResult<::hddm::HddmFileWriter> {
    ::hddm::HddmFileWriter::new(path, ::hddm::WriteMode::Append, ::hddm::Compression::None)
}
impl ::hddm::HddmSchema for Hddm {
    fn model_text() -> &'static str {
        MODEL
    }
    fn hddm_class() -> &'static str {
        HDDM_CLASS
    }
    fn model() -> &'static ::hddm::HddmModel {
        static MODEL_PARSED: std::sync::OnceLock<::hddm::HddmModel> = std::sync::OnceLock::new();
        MODEL_PARSED.get_or_init(|| {
            ::hddm::header::read_hddm_header_from_bytes(MODEL.as_bytes())
                .expect("generated HDDM model should parse")
                .0
        })
    }
}
