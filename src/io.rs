//! AmpTools, FSRoot, and Pawian event interchange for laddu datasets.
//!
//! All four-vectors use `(E, px, py, pz)` internally. Pawian beam inference
//! assumes a stationary proton and a massless photon travelling along +z.
//! Sources currently materialize a file before yielding laddu batches;
//! FSRoot output likewise buffers events until `finish`.

use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};

use laddu::{
    LadduDataError,
    data::{
        RealVec4,
        data::{EventBatch, OwnedEvent},
        io::{EventBatchIter, EventSink, EventSource, ReadPlan, SourceCapabilities, WritePlan},
        schema::Schema,
    },
    prelude::LadduDataResult,
};
use oxyroot::{ReaderTree, RootFile, WriterTree};

fn proton_mass() -> f64 {
    laddu::physics::quantum::builtin::baryons::PROTON
        .mass()
        .expect("laddu's built-in proton has a mass")
}
const KINEMATIC_TOLERANCE: f64 = 1.0e-3;

/// Ordered final-state columns, specified by count or explicit names.
pub enum FinalStateSpec {
    /// Generate `final_1`, `final_2`, and so on.
    Count(usize),
    /// Use these names in order.
    Names(Vec<String>),
}

impl From<usize> for FinalStateSpec {
    fn from(value: usize) -> Self {
        Self::Count(value)
    }
}

impl From<Vec<String>> for FinalStateSpec {
    fn from(value: Vec<String>) -> Self {
        Self::Names(value)
    }
}

impl FinalStateSpec {
    fn names(self) -> Vec<String> {
        match self {
            Self::Count(count) => (1..=count).map(|i| format!("final_{i}")).collect(),
            Self::Names(names) => names,
        }
    }
}

fn source_error(message: impl std::fmt::Display) -> LadduDataError {
    LadduDataError::Source(message.to_string())
}
fn sink_error(message: impl std::fmt::Display) -> LadduDataError {
    LadduDataError::Sink(message.to_string())
}

fn batches(
    schema: Arc<Schema>,
    events: Vec<OwnedEvent>,
    chunk_size: Option<usize>,
) -> LadduDataResult<EventBatchIter> {
    let size = chunk_size.unwrap_or(4096).max(1);
    let result = events
        .chunks(size)
        .map(|chunk| EventBatch::from_events(Arc::clone(&schema), chunk.to_vec()))
        .collect::<Vec<_>>();
    Ok(Box::new(result.into_iter()))
}

fn open_tree(path: &PathBuf, name: &str) -> LadduDataResult<ReaderTree> {
    std::panic::catch_unwind(|| {
        let mut file = RootFile::open(path).map_err(source_error)?;
        file.get_tree(name).map_err(source_error)
    })
    .map_err(|_| {
        source_error(format!(
            "{} uses a ROOT layout unsupported by oxyroot",
            path.display()
        ))
    })?
}

fn branch_f64(tree: &ReaderTree, name: &str) -> LadduDataResult<Vec<f64>> {
    let branch = tree
        .branch(name)
        .ok_or_else(|| source_error(format!("missing ROOT branch {name}")))?;
    if !matches!(
        branch.item_type_name().as_str(),
        "double" | "Double_t" | "f64"
    ) {
        return Err(source_error(format!(
            "ROOT branch {name} must be Double_t, found {}",
            branch.item_type_name()
        )));
    }
    branch
        .as_iter::<f64>()
        .map_err(source_error)
        .map(|values| values.collect())
}

fn branch_f32(tree: &ReaderTree, name: &str) -> LadduDataResult<Vec<f32>> {
    let branch = tree
        .branch(name)
        .ok_or_else(|| source_error(format!("missing ROOT branch {name}")))?;
    if !matches!(
        branch.item_type_name().as_str(),
        "float" | "Float_t" | "f32"
    ) {
        return Err(source_error(format!(
            "ROOT branch {name} must be Float_t, found {}",
            branch.item_type_name()
        )));
    }
    branch
        .as_iter::<f32>()
        .map_err(source_error)
        .map(|values| values.collect())
}

/// ROOT source for AmpTools `kin` trees.
#[derive(Clone, Debug)]
pub struct AmpToolsSource {
    path: PathBuf,
    tree: String,
    particles: usize,
    final_state_names: Vec<String>,
    pol_in_beam: bool,
}

impl AmpToolsSource {
    /// Construct a source with a particle count or ordered final-state names.
    pub fn new(path: impl Into<PathBuf>, final_state: impl Into<FinalStateSpec>) -> Self {
        let final_state_names = final_state.into().names();
        Self {
            path: path.into(),
            tree: "kin".into(),
            particles: final_state_names.len(),
            final_state_names,
            pol_in_beam: false,
        }
    }
    /// Select a ROOT tree.
    pub fn with_tree(mut self, tree: impl Into<String>) -> Self {
        self.tree = tree.into();
        self
    }
    /// Decode Zlm's polarization-vector convention in beam `px` and `py`.
    pub fn with_pol_in_beam(mut self, enabled: bool) -> Self {
        self.pol_in_beam = enabled;
        self
    }
    fn read(&self) -> LadduDataResult<Vec<OwnedEvent>> {
        let tree = open_tree(&self.path, &self.tree)?;
        let count = tree
            .branch("NumFinalState")
            .ok_or_else(|| source_error("missing NumFinalState"))?
            .as_iter::<i32>()
            .map_err(source_error)?
            .collect::<Vec<_>>();
        let beam = ["E_Beam", "Px_Beam", "Py_Beam", "Pz_Beam"]
            .map(|n| branch_f32(&tree, n))
            .into_iter()
            .collect::<LadduDataResult<Vec<_>>>()?;
        let final_state = [
            "E_FinalState",
            "Px_FinalState",
            "Py_FinalState",
            "Pz_FinalState",
        ]
        .map(|name| {
            let branch = tree
                .branch(name)
                .ok_or_else(|| source_error(format!("missing ROOT branch {name}")))?;
            match branch.item_type_name().as_str() {
                "float[]" => branch
                    .as_iter::<oxyroot::Slice<f32>>()
                    .map_err(source_error)
                    .map(|values| values.map(oxyroot::Slice::into_vec).collect::<Vec<_>>()),
                "vector<float>" => branch
                    .as_iter::<Vec<f32>>()
                    .map_err(source_error)
                    .map(|values| values.collect::<Vec<_>>()),
                other => Err(source_error(format!(
                    "ROOT branch {name} must contain Float_t, found {other}"
                ))),
            }
        })
        .into_iter()
        .collect::<LadduDataResult<Vec<_>>>()?;
        let weights = if tree.branch("Weight").is_some() {
            Some(branch_f32(&tree, "Weight")?)
        } else {
            None
        };
        let mut events = Vec::with_capacity(count.len());
        for row in 0..count.len() {
            if count[row] < 0 || count[row] as usize != self.particles {
                return Err(source_error(format!(
                    "event {row}: NumFinalState does not match configured particle count"
                )));
            }
            let beam_value = RealVec4::new(
                beam[0][row] as f64,
                beam[1][row] as f64,
                beam[2][row] as f64,
                beam[3][row] as f64,
            );
            let (beam_value, scalars) = if self.pol_in_beam {
                let fraction = beam_value.px().hypot(beam_value.py());
                if !fraction.is_finite() || fraction > 1.0 {
                    return Err(source_error(format!(
                        "event {row}: invalid Zlm polarization fraction"
                    )));
                }
                (
                    RealVec4::new(beam_value.e(), 0.0, 0.0, beam_value.e()),
                    vec![fraction, beam_value.py().atan2(beam_value.px())],
                )
            } else {
                (beam_value, vec![])
            };
            let mut p4s = vec![beam_value, RealVec4::new(proton_mass(), 0.0, 0.0, 0.0)];
            for part in 0..self.particles {
                if final_state
                    .iter()
                    .any(|column| column[row].len() != self.particles)
                {
                    return Err(source_error(format!(
                        "event {row}: final-state array length mismatch"
                    )));
                }
                p4s.push(RealVec4::new(
                    final_state[0][row][part] as f64,
                    final_state[1][row][part] as f64,
                    final_state[2][row][part] as f64,
                    final_state[3][row][part] as f64,
                ));
            }
            events.push(match &weights {
                Some(weights) => OwnedEvent::weighted(p4s, scalars, weights[row] as f64),
                None => OwnedEvent::new(p4s, scalars),
            });
        }
        Ok(events)
    }
}

impl EventSource for AmpToolsSource {
    fn schema(&self) -> LadduDataResult<Arc<Schema>> {
        let tree = open_tree(&self.path, &self.tree)?;
        let weighted = tree.branch("Weight").is_some();
        Ok(Arc::new(Schema::new(
            names_for(&self.final_state_names),
            if self.pol_in_beam {
                vec!["polarization_fraction", "polarization_angle"]
            } else {
                vec![]
            },
            weighted,
        )?))
    }
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            exact_len: true,
            ..Default::default()
        }
    }
    fn num_events(&self) -> LadduDataResult<Option<u64>> {
        Ok(Some(open_tree(&self.path, &self.tree)?.entries() as u64))
    }
    fn batches(&self, plan: ReadPlan) -> LadduDataResult<EventBatchIter> {
        batches(self.schema()?, self.read()?, plan.chunk_size)
    }
}

fn names_for(final_names: &[String]) -> Vec<String> {
    let mut result = vec!["beam".to_owned(), "target".to_owned()];
    result.extend_from_slice(final_names);
    result
}

/// ROOT source for FSRoot scalar-branch trees.
#[derive(Clone, Debug)]
pub struct FsRootSource {
    path: PathBuf,
    tree: String,
    final_state_names: Vec<String>,
    prefix: String,
    weight_branch: String,
}

impl FsRootSource {
    /// Construct a source with a particle count or ordered final-state names.
    pub fn new(path: impl Into<PathBuf>, final_state: impl Into<FinalStateSpec>) -> Self {
        let final_state_names = final_state.into().names();
        Self {
            path: path.into(),
            tree: "nt".into(),
            final_state_names,
            prefix: String::new(),
            weight_branch: "weight".into(),
        }
    }
    /// Select a ROOT tree.
    pub fn with_tree(mut self, tree: impl Into<String>) -> Self {
        self.tree = tree.into();
        self
    }
    /// Prefix all four-momentum branch names.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
    /// Select the weight branch name.
    pub fn with_weight_branch(mut self, branch: impl Into<String>) -> Self {
        self.weight_branch = branch.into();
        self
    }
    fn read(&self) -> LadduDataResult<Vec<OwnedEvent>> {
        let tree = open_tree(&self.path, &self.tree)?;
        let n = tree.entries() as usize;
        let mut columns = Vec::new();
        for suffix in std::iter::once("PB".to_owned())
            .chain((1..=self.final_state_names.len()).map(|i| format!("P{i}")))
        {
            columns.push(
                ["En", "Px", "Py", "Pz"]
                    .map(|component| {
                        branch_f64(&tree, &format!("{}{component}{suffix}", self.prefix))
                    })
                    .into_iter()
                    .collect::<LadduDataResult<Vec<_>>>()?,
            );
        }
        let weights = if tree.branch(&self.weight_branch).is_some() {
            Some(branch_f64(&tree, &self.weight_branch)?)
        } else {
            None
        };
        let mut events = Vec::with_capacity(n);
        for row in 0..n {
            let mut p4s = Vec::with_capacity(columns.len() + 1);
            let beam = &columns[0];
            p4s.push(RealVec4::new(
                beam[0][row],
                beam[1][row],
                beam[2][row],
                beam[3][row],
            ));
            p4s.push(RealVec4::new(proton_mass(), 0.0, 0.0, 0.0));
            for column in columns.iter().skip(1) {
                p4s.push(RealVec4::new(
                    column[0][row],
                    column[1][row],
                    column[2][row],
                    column[3][row],
                ));
            }
            events.push(match &weights {
                Some(weights) => OwnedEvent::weighted(p4s, vec![], weights[row]),
                None => OwnedEvent::new(p4s, vec![]),
            });
        }
        Ok(events)
    }
}

impl EventSource for FsRootSource {
    fn schema(&self) -> LadduDataResult<Arc<Schema>> {
        let tree = open_tree(&self.path, &self.tree)?;
        let weighted = tree.branch(&self.weight_branch).is_some();
        Ok(Arc::new(Schema::new(
            names_for(&self.final_state_names),
            Vec::<String>::new(),
            weighted,
        )?))
    }
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            exact_len: true,
            ..Default::default()
        }
    }
    fn num_events(&self) -> LadduDataResult<Option<u64>> {
        Ok(Some(open_tree(&self.path, &self.tree)?.entries() as u64))
    }
    fn batches(&self, plan: ReadPlan) -> LadduDataResult<EventBatchIter> {
        batches(self.schema()?, self.read()?, plan.chunk_size)
    }
}

/// Pawian four-momentum field order.
#[derive(Clone, Copy, Debug, Default)]
pub enum PawianOrder {
    /// Momentum components before energy.
    #[default]
    PxPyPzE,
    /// Energy before momentum components.
    EPxPyPz,
}
/// Pawian energy unit.
#[derive(Clone, Copy, Debug, Default)]
pub enum PawianUnit {
    /// Gigaelectronvolts.
    #[default]
    GeV,
    /// Megaelectronvolts.
    MeV,
}
/// Text source for Pawian's default event reader format.
#[derive(Clone, Debug)]
pub struct PawianSource {
    path: PathBuf,
    final_state_names: Vec<String>,
    weights: bool,
    polarization: bool,
    order: PawianOrder,
    unit: PawianUnit,
}

impl PawianSource {
    /// Construct a source with a particle count or ordered final-state names.
    pub fn new(path: impl Into<PathBuf>, final_state: impl Into<FinalStateSpec>) -> Self {
        let final_state_names = final_state.into().names();
        Self {
            path: path.into(),
            final_state_names,
            weights: false,
            polarization: false,
            order: PawianOrder::default(),
            unit: PawianUnit::default(),
        }
    }
    /// Expect one leading weight per event.
    pub fn with_weights(mut self, enabled: bool) -> Self {
        self.weights = enabled;
        self
    }
    /// Expect polarization fraction and angle per event.
    pub fn with_polarization(mut self, enabled: bool) -> Self {
        self.polarization = enabled;
        self
    }
    /// Set four-momentum field order.
    pub fn with_order(mut self, order: PawianOrder) -> Self {
        self.order = order;
        self
    }
    /// Set momentum unit.
    pub fn with_unit(mut self, unit: PawianUnit) -> Self {
        self.unit = unit;
        self
    }
    fn read(&self) -> LadduDataResult<Vec<OwnedEvent>> {
        let reader = BufReader::new(File::open(&self.path).map_err(source_error)?);
        let mut values = Vec::new();
        for (line_no, line) in reader.lines().enumerate() {
            let line = line.map_err(source_error)?;
            for token in line.split_whitespace() {
                values.push(token.parse::<f64>().map_err(|_| {
                    source_error(format!(
                        "line {}: invalid Pawian number {token}",
                        line_no + 1
                    ))
                })?);
            }
        }
        let width = usize::from(self.weights)
            + if self.polarization { 2 } else { 0 }
            + 4 * self.final_state_names.len();
        if width == 0 || values.len() % width != 0 {
            return Err(source_error("Pawian file has an incomplete event"));
        }
        let mut events = Vec::new();
        for (row, record) in values.chunks(width).enumerate() {
            let mut offset = 0;
            let weight = if self.weights {
                offset += 1;
                Some(record[0])
            } else {
                None
            };
            let scalars = if self.polarization {
                let fraction = record[offset];
                let angle = record[offset + 1];
                offset += 2;
                if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) || !angle.is_finite() {
                    return Err(source_error(format!("event {row}: invalid polarization")));
                }
                vec![fraction, angle]
            } else {
                vec![]
            };
            let scale = match self.unit {
                PawianUnit::GeV => 1.0,
                PawianUnit::MeV => 0.001,
            };
            let mut finals = Vec::new();
            for part in 0..self.final_state_names.len() {
                let q = &record[offset + part * 4..offset + part * 4 + 4];
                let (e, px, py, pz) = match self.order {
                    PawianOrder::PxPyPzE => (q[3], q[0], q[1], q[2]),
                    PawianOrder::EPxPyPz => (q[0], q[1], q[2], q[3]),
                };
                finals.push(RealVec4::new(e * scale, px * scale, py * scale, pz * scale));
            }
            let energy: f64 = finals.iter().map(|v| v.e()).sum::<f64>() - proton_mass();
            let px: f64 = finals.iter().map(|v| v.px()).sum();
            let py: f64 = finals.iter().map(|v| v.py()).sum();
            let pz: f64 = finals.iter().map(|v| v.pz()).sum();
            if !energy.is_finite()
                || energy <= 0.0
                || px.abs() > KINEMATIC_TOLERANCE
                || py.abs() > KINEMATIC_TOLERANCE
                || (energy - pz).abs() > KINEMATIC_TOLERANCE
            {
                return Err(source_error(format!(
                    "event {row}: final state is inconsistent with a z-axis photon and stationary proton (tolerance {KINEMATIC_TOLERANCE} GeV)"
                )));
            }
            let mut p4s = vec![
                RealVec4::new(energy, 0.0, 0.0, energy),
                RealVec4::new(proton_mass(), 0.0, 0.0, 0.0),
            ];
            p4s.extend(finals);
            events.push(match weight {
                Some(weight) => OwnedEvent::weighted(p4s, scalars, weight),
                None => OwnedEvent::new(p4s, scalars),
            });
        }
        Ok(events)
    }
}

impl EventSource for PawianSource {
    fn schema(&self) -> LadduDataResult<Arc<Schema>> {
        Ok(Arc::new(Schema::new(
            names_for(&self.final_state_names),
            if self.polarization {
                vec!["polarization_fraction", "polarization_angle"]
            } else {
                vec![]
            },
            self.weights,
        )?))
    }
    fn batches(&self, plan: ReadPlan) -> LadduDataResult<EventBatchIter> {
        batches(self.schema()?, self.read()?, plan.chunk_size)
    }
}

/// Pawian text sink.
pub struct PawianSink {
    path: PathBuf,
    final_state_names: Vec<String>,
    weights: bool,
    polarization: bool,
    order: PawianOrder,
    unit: PawianUnit,
    writer: Option<BufWriter<File>>,
    indices: Vec<usize>,
    polarization_indices: Option<[usize; 2]>,
}

impl PawianSink {
    /// Construct a text sink with a particle count or ordered final-state names.
    pub fn new(path: impl Into<PathBuf>, final_state: impl Into<FinalStateSpec>) -> Self {
        let final_state_names = final_state.into().names();
        Self {
            path: path.into(),
            final_state_names,
            weights: false,
            polarization: false,
            order: PawianOrder::default(),
            unit: PawianUnit::default(),
            writer: None,
            indices: vec![],
            polarization_indices: None,
        }
    }
    /// Write one leading weight per event.
    pub fn with_weights(mut self, enabled: bool) -> Self {
        self.weights = enabled;
        self
    }
    /// Write fraction and angle per event.
    pub fn with_polarization(mut self, enabled: bool) -> Self {
        self.polarization = enabled;
        self
    }
    /// Set four-momentum field order.
    pub fn with_order(mut self, order: PawianOrder) -> Self {
        self.order = order;
        self
    }
    /// Set momentum unit.
    pub fn with_unit(mut self, unit: PawianUnit) -> Self {
        self.unit = unit;
        self
    }
}

impl EventSink for PawianSink {
    fn begin(&mut self, schema: Arc<Schema>, _plan: WritePlan) -> LadduDataResult<()> {
        self.indices = self
            .final_state_names
            .iter()
            .map(|name| {
                schema
                    .p4_index(name)
                    .ok_or_else(|| sink_error(format!("missing four-vector {name}")))
            })
            .collect::<LadduDataResult<_>>()?;
        self.polarization_indices = if self.polarization {
            Some(
                ["polarization_fraction", "polarization_angle"]
                    .map(|name| {
                        schema
                            .scalar_index(name)
                            .ok_or_else(|| sink_error(format!("missing scalar {name}")))
                    })
                    .into_iter()
                    .collect::<LadduDataResult<Vec<_>>>()?
                    .try_into()
                    .unwrap(),
            )
        } else {
            None
        };
        self.writer = Some(BufWriter::new(
            File::create(&self.path).map_err(sink_error)?,
        ));
        Ok(())
    }
    fn write_batch(&mut self, batch: &EventBatch) -> LadduDataResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| sink_error("Pawian sink has not begun"))?;
        for event in batch.iter() {
            if self.weights {
                writeln!(writer, "{}", event.weight()).map_err(sink_error)?;
            }
            if let Some([fraction_index, angle_index]) = self.polarization_indices {
                let fraction = event.scalar(fraction_index);
                let angle = event.scalar(angle_index);
                if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) || !angle.is_finite() {
                    return Err(sink_error("invalid polarization value"));
                }
                writeln!(writer, "{fraction} {angle}").map_err(sink_error)?;
            }
            for &index in &self.indices {
                let v = event.p4(index);
                let s = match self.unit {
                    PawianUnit::GeV => 1.0,
                    PawianUnit::MeV => 1000.0,
                };
                let q = match self.order {
                    PawianOrder::PxPyPzE => [v.px(), v.py(), v.pz(), v.e()],
                    PawianOrder::EPxPyPz => v.components(),
                };
                writeln!(
                    writer,
                    "{} {} {} {}",
                    q[0] * s,
                    q[1] * s,
                    q[2] * s,
                    q[3] * s
                )
                .map_err(sink_error)?;
            }
        }
        Ok(())
    }
    fn finish(&mut self) -> LadduDataResult<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush().map_err(sink_error)?;
        }
        Ok(())
    }
    fn abort(&mut self) -> LadduDataResult<()> {
        self.writer.take();
        Ok(())
    }
}

/// FSRoot scalar-branch ROOT sink.
pub struct FsRootSink {
    path: PathBuf,
    tree: String,
    final_state_names: Vec<String>,
    prefix: String,
    weight_branch: String,
    schema: Option<Arc<Schema>>,
    rows: Vec<Vec<RealVec4>>,
    weights: Vec<f64>,
}

impl FsRootSink {
    /// Construct a sink with a particle count or ordered final-state names.
    pub fn new(path: impl Into<PathBuf>, final_state: impl Into<FinalStateSpec>) -> Self {
        let final_state_names = final_state.into().names();
        Self {
            path: path.into(),
            tree: "nt".into(),
            final_state_names,
            prefix: String::new(),
            weight_branch: "weight".into(),
            schema: None,
            rows: vec![],
            weights: vec![],
        }
    }
    /// Select the output tree name.
    pub fn with_tree(mut self, tree: impl Into<String>) -> Self {
        self.tree = tree.into();
        self
    }
    /// Prefix momentum branch names.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
    /// Select the weight branch name.
    pub fn with_weight_branch(mut self, name: impl Into<String>) -> Self {
        self.weight_branch = name.into();
        self
    }
}

impl EventSink for FsRootSink {
    fn begin(&mut self, schema: Arc<Schema>, _plan: WritePlan) -> LadduDataResult<()> {
        if self.final_state_names.len() < 2 {
            return Err(sink_error(
                "FSRoot requires at least two final-state vectors for s23",
            ));
        }
        for name in std::iter::once("beam").chain(self.final_state_names.iter().map(String::as_str))
        {
            if schema.p4_index(name).is_none() {
                return Err(sink_error(format!("missing four-vector {name}")));
            }
        }
        self.schema = Some(schema);
        self.rows.clear();
        self.weights.clear();
        Ok(())
    }
    fn write_batch(&mut self, batch: &EventBatch) -> LadduDataResult<()> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| sink_error("FSRoot sink has not begun"))?;
        let indices = std::iter::once("beam")
            .chain(self.final_state_names.iter().map(String::as_str))
            .map(|name| schema.p4_index(name).unwrap())
            .collect::<Vec<_>>();
        for event in batch.iter() {
            self.rows
                .push(indices.iter().map(|&index| event.p4(index)).collect());
            self.weights.push(event.weight());
        }
        Ok(())
    }
    fn finish(&mut self) -> LadduDataResult<()> {
        let mut file = RootFile::create(&self.path).map_err(sink_error)?;
        let mut tree = WriterTree::new(&self.tree);
        for part in 0..=self.final_state_names.len() {
            let suffix = if part == 0 {
                "PB".to_owned()
            } else {
                format!("P{part}")
            };
            for (component, field) in ["En", "Px", "Py", "Pz"].into_iter().enumerate() {
                let data = self
                    .rows
                    .iter()
                    .map(|row| row[part].components()[component])
                    .collect::<Vec<_>>();
                tree.new_branch(&format!("{}{field}{suffix}", self.prefix), data.into_iter());
            }
        }
        let weights = std::mem::take(&mut self.weights);
        tree.new_branch(&self.weight_branch, weights.into_iter());
        let s12 = self
            .rows
            .iter()
            .map(|row| invariant_mass_squared(row[0], row[1]))
            .collect::<Vec<_>>();
        let s23 = self
            .rows
            .iter()
            .map(|row| invariant_mass_squared(row[1], row[2]))
            .collect::<Vec<_>>();
        tree.new_branch("s12", s12.into_iter());
        tree.new_branch("s23", s23.into_iter());
        tree.write(&mut file).map_err(sink_error)?;
        file.close().map_err(sink_error)?;
        self.rows.clear();
        self.schema = None;
        Ok(())
    }
    fn abort(&mut self) -> LadduDataResult<()> {
        self.rows.clear();
        self.weights.clear();
        self.schema = None;
        Ok(())
    }
}

fn invariant_mass_squared(a: RealVec4, b: RealVec4) -> f64 {
    let e = a.e() + b.e();
    let px = a.px() + b.px();
    let py = a.py() + b.py();
    let pz = a.pz() + b.pz();
    e * e - px * px - py * py - pz * pz
}

#[cfg(test)]
mod tests {
    use super::*;
    use laddu::prelude::Dataset;

    fn example() -> LadduDataResult<Dataset> {
        let schema = Arc::new(Schema::new(
            ["beam", "target", "final_1", "final_2"],
            ["polarization_fraction", "polarization_angle"],
            true,
        )?);
        let event = OwnedEvent::weighted(
            vec![
                RealVec4::new(5.0, 0.0, 0.0, 5.0),
                RealVec4::new(proton_mass(), 0.0, 0.0, 0.0),
                RealVec4::new(2.5, 0.0, 0.0, 2.0),
                RealVec4::new(3.438_272_088_16, 0.0, 0.0, 3.0),
            ],
            vec![0.4, 0.25],
            1.5,
        );
        Ok(Dataset::from_batch(EventBatch::from_events(
            schema,
            [event],
        )?))
    }

    #[test]
    fn pawian_round_trip_infers_beam_and_preserves_radian_angles() -> LadduDataResult<()> {
        let dir = tempfile::tempdir().map_err(sink_error)?;
        let path = dir.path().join("events.dat");
        example()?.write_to(
            &mut PawianSink::new(&path, vec!["final_1".into(), "final_2".into()])
                .with_weights(true)
                .with_polarization(true)
                .with_order(PawianOrder::EPxPyPz)
                .with_unit(PawianUnit::MeV),
        )?;
        let source = PawianSource::new(&path, vec!["final_1".into(), "final_2".into()])
            .with_weights(true)
            .with_polarization(true)
            .with_order(PawianOrder::EPxPyPz)
            .with_unit(PawianUnit::MeV);
        let batch = source.batches(ReadPlan::default())?.next().unwrap()?;
        let event = batch.iter().next().unwrap();
        assert!((event.p4(0).e() - 5.0).abs() < 1e-10);
        assert!((event.scalar(1) - 0.25).abs() < 1e-10);
        assert!((event.weight() - 1.5).abs() < 1e-10);
        Ok(())
    }

    #[test]
    fn fsroot_round_trip_uses_double_scalar_branches() -> LadduDataResult<()> {
        let dir = tempfile::tempdir().map_err(sink_error)?;
        let path = dir.path().join("events.root");
        example()?.write_to(&mut FsRootSink::new(
            &path,
            vec!["final_1".into(), "final_2".into()],
        ))?;
        let source = FsRootSource::new(&path, vec!["final_1".into(), "final_2".into()]);
        let batch = source.batches(ReadPlan::default())?.next().unwrap()?;
        let event = batch.iter().next().unwrap();
        assert!((event.p4(0).e() - 5.0).abs() < 1e-10);
        assert!((event.weight() - 1.5).abs() < 1e-10);
        Ok(())
    }

    #[test]
    fn amptools_float_columns_decode_pol_in_beam() -> LadduDataResult<()> {
        let dir = tempfile::tempdir().map_err(sink_error)?;
        let path = dir.path().join("amptools.root");
        let mut file = RootFile::create(&path).map_err(sink_error)?;
        let mut tree = WriterTree::new("kin");
        tree.new_branch("NumFinalState", [2_i32].into_iter());
        for (name, value) in [
            ("E_Beam", 5.0_f32),
            ("Px_Beam", 0.4),
            ("Py_Beam", 0.0),
            ("Pz_Beam", 5.0),
            ("Weight", 1.5),
        ] {
            tree.new_branch(name, [value].into_iter());
        }
        for (name, values) in [
            ("E_FinalState", vec![2.5_f32, 3.438_272]),
            ("Px_FinalState", vec![0.0, 0.0]),
            ("Py_FinalState", vec![0.0, 0.0]),
            ("Pz_FinalState", vec![2.0, 3.0]),
        ] {
            tree.new_branch(name, [values].into_iter());
        }
        tree.write(&mut file).map_err(sink_error)?;
        file.close().map_err(sink_error)?;
        drop(file);
        let source = AmpToolsSource::new(&path, 2).with_pol_in_beam(true);
        let batch = source.batches(ReadPlan::default())?.next().unwrap()?;
        let event = batch.iter().next().unwrap();
        assert!((event.scalar(0) - 0.4).abs() < 1e-6);
        assert_eq!(event.p4(0).px(), 0.0);
        assert!((event.weight() - 1.5).abs() < 1e-6);
        Ok(())
    }
}
