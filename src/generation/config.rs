//! Strict, editor-friendly configuration for standalone event generation.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    str::FromStr,
};

use laddu::{
    physics::{
        channel::Channel, histogram::Histogram, quantum::ParticleProperties, vectors::RealVec3,
    },
    prelude::{
        ChannelGenerator, CompiledModel, Execution, ExprNode, MassProposal, ModelEvaluator,
        ScalarSource, TComponent, TDistribution, Vec4, VertexProposal,
    },
};
use schemars::{JsonSchema, Schema, schema_for};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    Particle,
    generation::{GlueXIdExt, species::gluex_particle_from_external_ids},
};

/// Current generation configuration format version.
pub const FORMAT_VERSION: u32 = 1;

/// Errors reported while parsing, validating, or compiling a generation file.
#[derive(Debug, Error)]
pub enum GenerationConfigError {
    /// JSON syntax or structural validation failed.
    #[error("invalid generation JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// A semantic constraint was violated.
    #[error("{path}: {message}")]
    Validation {
        /// JSON-style path to the invalid value.
        path: String,
        /// Human-readable explanation.
        message: String,
    },
    /// Laddu rejected the compiled channel.
    #[error("compiled channel is invalid: {0}")]
    Laddu(String),
}

/// Result type for generation configuration operations.
pub type GenerationConfigResult<T> = Result<T, GenerationConfigError>;

fn invalid(path: impl Into<String>, message: impl Into<String>) -> GenerationConfigError {
    GenerationConfigError::Validation {
        path: path.into(),
        message: message.into(),
    }
}

/// A standalone generation configuration.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GenerationConfig {
    /// Optional schema URI used by JSON-aware editors.
    #[serde(rename = "$schema", default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Configuration format version. Currently this must be `1`.
    pub version: u32,
    /// Human-readable channel name.
    pub name: String,
    /// Photon beam specification.
    pub beam: BeamConfig,
    /// Fixed proton target specification.
    #[serde(default)]
    pub target: TargetConfig,
    /// Two-to-two production vertex and its subsequent decay trees.
    pub production: ProductionConfig,
    /// Optional serialized Laddu model used as the event intensity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<serde_json::Value>")]
    pub model: Option<CompiledModel>,
    /// Free model parameters in Laddu's parameter order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Vec<f64>>,
    /// Additional generated scalar columns available to the model.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub scalars: BTreeMap<String, ScalarDistribution>,
    /// Optional manual rejection-envelope override.
    ///
    /// When omitted, Laddu computes a certified model-less phase-space bound.
    #[serde(default, skip_serializing_if = "GenerationSettings::is_default")]
    pub generation: GenerationSettings,
}

/// Photon beam configuration.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BeamConfig {
    /// Unique channel edge name.
    #[serde(default = "default_beam_name")]
    pub name: String,
    /// Beam-energy proposal in `GeV`.
    pub energy: ScalarDistribution,
}

fn default_beam_name() -> String {
    "beam".to_owned()
}

/// Fixed proton target configuration.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    /// Unique channel edge name.
    #[serde(default = "default_target_name")]
    pub name: String,
}

fn default_target_name() -> String {
    "target".to_owned()
}

impl Default for TargetConfig {
    fn default() -> Self {
        Self {
            name: default_target_name(),
        }
    }
}

/// A scalar proposal distribution.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScalarDistribution {
    /// Use one value.
    Fixed {
        /// Fixed value.
        value: f64,
    },
    /// Draw uniformly from `[min, max)`.
    Uniform {
        /// Lower bound.
        min: f64,
        /// Upper bound.
        max: f64,
    },
    /// Draw from an inline piecewise-constant histogram.
    Histogram {
        /// Bin edges, with one more edge than weight.
        edges: Vec<f64>,
        /// Nonnegative bin weights.
        weights: Vec<f64>,
    },
}

/// The primary two-to-two production vertex.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProductionConfig {
    /// Name of the production vertex.
    #[serde(default = "default_production_name")]
    pub name: String,
    /// Exactly two outgoing particles or resonances.
    pub products: [ParticleNode; 2],
    /// Momentum-transfer proposal and beam-to-product pairing.
    pub transfer: TransferConfig,
}

fn default_production_name() -> String {
    "production".to_owned()
}

/// Momentum-transfer configuration for production.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransferConfig {
    /// Outgoing product paired with the photon beam in Mandelstam `t`.
    pub outgoing: String,
    /// Transfer-density proposal.
    pub distribution: TransferDistribution,
    /// Optional lower `t` limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t_min: Option<f64>,
    /// Optional upper `t` limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t_max: Option<f64>,
}

/// A normalized momentum-transfer density.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TransferDistribution {
    /// Uniform in `t`.
    Uniform,
    /// Proportional to `exp(slope * t)`.
    Exponential {
        /// Exponential slope.
        slope: f64,
    },
    /// Pole-like density.
    Pole {
        /// Exchanged-particle mass in `GeV`.
        exchange_mass: f64,
        /// Positive denominator power.
        power: f64,
    },
    /// Piecewise-constant histogram density.
    Histogram {
        /// Histogram bin edges.
        edges: Vec<f64>,
        /// Nonnegative histogram weights.
        weights: Vec<f64>,
    },
    /// Positive weighted mixture of component densities.
    Mixture {
        /// Mixture components.
        components: Vec<TransferMixtureComponent>,
    },
}

/// One weighted component in a transfer-density mixture.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransferMixtureComponent {
    /// Positive mixture weight.
    pub weight: f64,
    /// Component density.
    pub component: TransferComponent,
}

/// A non-mixture transfer-density component.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TransferComponent {
    /// Uniform in `t`.
    Uniform,
    /// Proportional to `exp(slope * t)`.
    Exponential {
        /// Exponential slope.
        slope: f64,
    },
    /// Pole-like density.
    Pole {
        /// Exchanged-particle mass in `GeV`.
        exchange_mass: f64,
        /// Positive denominator power.
        power: f64,
    },
    /// Piecewise-constant histogram density.
    Histogram {
        /// Histogram bin edges.
        edges: Vec<f64>,
        /// Nonnegative histogram weights.
        weights: Vec<f64>,
    },
}

/// A named particle, optionally followed by a binary decay.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParticleNode {
    /// Unique edge name used in diagnostics and Laddu expressions.
    pub name: String,
    /// `GlueX` enum name, common particle label, or PDG code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub particle: Option<ParticleReference>,
    /// Optional fixed or sampled invariant mass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mass: Option<MassDistribution>,
    /// Optional binary decay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decay: Option<Box<DecayConfig>>,
}

/// A particle identifier accepted by generation JSON.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ParticleReference {
    /// `GlueX` enum name or a recognized `GlueX` particle label.
    Name(String),
    /// PDG Monte Carlo identifier.
    Pdg(isize),
}

/// Invariant-mass proposal for an intermediate state.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MassDistribution {
    /// Use one invariant mass in `GeV`.
    Fixed {
        /// Fixed mass.
        value: f64,
    },
    /// Draw uniformly in a mass range, clipped to kinematic support.
    Uniform {
        /// Lower mass bound.
        min: f64,
        /// Upper mass bound.
        max: f64,
    },
}

/// A one-to-two isotropic decay.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecayConfig {
    /// Name of the decay vertex.
    pub name: String,
    /// Exactly two decay products.
    pub products: [ParticleNode; 2],
}

/// Rejection-envelope settings for unweighted generation.
///
/// By default, Laddu computes a certified phase-space envelope. The manual
/// maximum is retained as an expert override for unit-model generation.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GenerationSettings {
    /// Manually supplied maximum target weight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_weight: Option<f64>,
    /// Number of proposals used to estimate an envelope for model-weighted generation.
    #[serde(default = "default_pilot_proposals")]
    pub pilot_proposals: usize,
    /// Factor used when growing an explicitly supplied envelope.
    #[serde(default = "default_safety_scale")]
    pub safety_scale: f64,
}

const fn default_safety_scale() -> f64 {
    2.0
}

const fn default_pilot_proposals() -> usize {
    10_000
}

impl Default for GenerationSettings {
    fn default() -> Self {
        Self {
            max_weight: None,
            pilot_proposals: default_pilot_proposals(),
            safety_scale: default_safety_scale(),
        }
    }
}

impl GenerationSettings {
    const fn is_default(&self) -> bool {
        self.max_weight.is_none()
            && self.pilot_proposals == default_pilot_proposals()
            && self.safety_scale.to_bits() == default_safety_scale().to_bits()
    }
}

impl GenerationConfig {
    /// Parse strict JSON and perform semantic validation.
    ///
    /// # Errors
    ///
    /// Returns a path-oriented diagnostic for malformed or invalid input.
    pub fn from_json(input: &str) -> GenerationConfigResult<Self> {
        let config: Self = serde_json::from_str(input)?;
        config.validate()?;
        Ok(config)
    }

    /// Return the generated JSON Schema shared by the CLI and editors.
    #[must_use]
    pub fn json_schema() -> Schema {
        schema_for!(Self)
    }

    /// Validate cross-field and physics-facing invariants.
    ///
    /// # Errors
    ///
    /// Returns the first semantic error with its configuration path.
    pub fn validate(&self) -> GenerationConfigResult<()> {
        if self.version != FORMAT_VERSION {
            return Err(invalid(
                "$.version",
                format!("expected {FORMAT_VERSION}, got {}", self.version),
            ));
        }
        if self.name.trim().is_empty() {
            return Err(invalid("$.name", "must not be empty"));
        }
        let mut names = HashSet::new();
        check_name("$.beam.name", &self.beam.name, &mut names)?;
        check_name("$.target.name", &self.target.name, &mut names)?;
        validate_scalar("$.beam.energy", &self.beam.energy)?;
        check_name("$.production.name", &self.production.name, &mut names)?;
        for (index, node) in self.production.products.iter().enumerate() {
            validate_node(&format!("$.production.products[{index}]"), node, &mut names)?;
        }
        if !self
            .production
            .products
            .iter()
            .any(|node| node.name == self.production.transfer.outgoing)
        {
            return Err(invalid(
                "$.production.transfer.outgoing",
                "must name one of the two production products",
            ));
        }
        validate_transfer("$.production.transfer", &self.production.transfer)?;
        for (name, source) in &self.scalars {
            check_name(&format!("$.scalars.{name}"), name, &mut names)?;
            validate_extra_scalar(&format!("$.scalars.{name}"), source)?;
        }
        match (&self.model, &self.parameters) {
            (None, Some(_)) => {
                return Err(invalid("$.parameters", "parameters require a model"));
            }
            (Some(model), parameters) => {
                let values = parameters
                    .as_deref()
                    .map_or_else(|| model.params().initial_free_values(), <[f64]>::to_vec);
                model
                    .params()
                    .values(&values)
                    .map_err(|error| invalid("$.parameters", error.to_string()))?;
            }
            (None, None) => {}
        }
        if self.generation.pilot_proposals == 0 {
            return Err(invalid(
                "$.generation.pilot_proposals",
                "must be greater than zero",
            ));
        }
        if !self.generation.safety_scale.is_finite() || self.generation.safety_scale <= 1.0 {
            return Err(invalid(
                "$.generation.safety_scale",
                "must be finite and greater than one",
            ));
        }
        if self
            .generation
            .max_weight
            .is_some_and(|max_weight| !max_weight.is_finite() || max_weight <= 0.0)
        {
            return Err(invalid(
                "$.generation.max_weight",
                "must be finite and positive",
            ));
        }
        Ok(())
    }

    /// Compute a stable digest excluding editor and preparation metadata.
    ///
    /// # Errors
    ///
    /// Returns an error only if the typed configuration cannot be serialized.
    pub fn semantic_sha256(&self) -> GenerationConfigResult<String> {
        let mut value = serde_json::to_value(self)?;
        let Some(object) = value.as_object_mut() else {
            return Err(invalid(
                "$",
                "internal error: generation configuration did not serialize as an object",
            ));
        };
        object.remove("$schema");
        let encoded = serde_json::to_vec(&value)?;
        Ok(format!("{:x}", Sha256::digest(encoded)))
    }

    /// Compile this description to a validated Laddu channel.
    ///
    /// # Errors
    ///
    /// Returns an error if validation or Laddu channel construction fails.
    pub fn to_channel(&self) -> GenerationConfigResult<Channel> {
        self.validate()?;
        let mut channel = Channel::new(self.name.clone());
        let beam_properties = particle_properties(Particle::Gamma);
        channel
            .edge(&self.beam.name)
            .p4(Vec4::event(&self.beam.name))
            .properties(&beam_properties)
            .initial_energy_source_direction(scalar_source(&self.beam.energy)?, RealVec3::z())
            .output()
            .set_beam_id()
            .map_err(|error| GenerationConfigError::Laddu(error.to_string()))?;
        let target_properties = particle_properties(Particle::Proton);
        channel
            .edge(&self.target.name)
            .p4(Vec4::event(&self.target.name))
            .properties(&target_properties)
            .initial_momentum(RealVec3::zero())
            .output()
            .set_target_id()
            .map_err(|error| GenerationConfigError::Laddu(error.to_string()))?;
        for node in &self.production.products {
            add_node(&mut channel, node)?;
        }
        let transfer = transfer_distribution(&self.production.transfer)?;
        channel
            .vertex(&self.production.name)
            .incoming([&self.beam.name, &self.target.name])
            .outgoing([
                &self.production.products[0].name,
                &self.production.products[1].name,
            ])
            .generation(VertexProposal::t_exchange(
                (&self.beam.name, &self.production.transfer.outgoing),
                transfer,
            ));
        for node in &self.production.products {
            add_decays(&mut channel, node);
        }
        laddu::prelude::ChannelGenerator::new(channel.clone())
            .map_err(|error| GenerationConfigError::Laddu(error.to_string()))?;
        Ok(channel)
    }

    /// Compile the channel and attach all configured scalar sources.
    ///
    /// # Errors
    ///
    /// Returns an error when the channel or a scalar source is invalid.
    pub fn to_generator(&self) -> GenerationConfigResult<ChannelGenerator> {
        let mut generator = ChannelGenerator::new(self.to_channel()?)
            .map_err(|error| GenerationConfigError::Laddu(error.to_string()))?;
        for (name, source) in &self.scalars {
            generator
                .add_scalar(name, scalar_source(source)?)
                .map_err(|error| GenerationConfigError::Laddu(error.to_string()))?;
        }
        Ok(generator)
    }

    /// Prepare the optional model and its configured parameters for generation.
    ///
    /// # Errors
    ///
    /// Returns an error when model parameters or runtime preparation are invalid.
    pub fn model_evaluator(&self) -> GenerationConfigResult<Option<ModelEvaluator>> {
        self.validate()?;
        let Some(model) = &self.model else {
            return Ok(None);
        };
        let free_values = self
            .parameters
            .clone()
            .unwrap_or_else(|| model.params().initial_free_values());
        let values = model
            .params()
            .values(&free_values)
            .map_err(|error| invalid("$.parameters", error.to_string()))?;
        ModelEvaluator::prepare(model, values, &Execution::default())
            .map(Some)
            .map_err(|error| GenerationConfigError::Laddu(error.to_string()))
    }

    /// Validate all components needed by standalone execution.
    ///
    /// # Errors
    ///
    /// Returns an error when channel compilation, scalar attachment, model
    /// preparation, or model scalar dependencies are invalid.
    pub fn validate_execution(&self) -> GenerationConfigResult<()> {
        self.to_generator()?;
        self.model_evaluator()?;
        if let Some(model) = &self.model {
            for node in model.graph().nodes() {
                if let ExprNode::EventScalar(name) = node
                    && !self.scalars.contains_key(name.as_ref())
                {
                    return Err(invalid(
                        "$.model",
                        format!("requires missing scalar branch `{name}`"),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Convert a supported Laddu channel into the canonical `GlueX` format.
    ///
    /// Only the intentionally narrow standalone-generator subset is accepted:
    /// a photon beam and fixed proton target (identified by `GlueX` labels or,
    /// for Python-authored channels, the canonical `beam`/`target` names), one
    /// two-to-two production vertex, and a tree of isotropic binary decays.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic when the channel uses topology or proposals that
    /// the standalone format cannot represent.
    pub fn try_from_channel(channel: &Channel) -> GenerationConfigResult<Self> {
        let beam = find_labeled_initial(channel, "beam")?;
        let target = find_labeled_initial(channel, "target")?;
        require_particle(beam, Particle::Gamma, "beam")?;
        require_particle(target, Particle::Proton, "target")?;
        let energy = match beam.initial_momentum() {
            Some(laddu::prelude::InitialMomentum::EnergyDirection { energy, direction })
                if direction.x == 0.0 && direction.y == 0.0 && direction.z > 0.0 =>
            {
                Ok(scalar_distribution(energy))
            }
            _ => Err(invalid(
                format!("channel edge `{}`", beam.name()),
                "beam must use a scalar energy source along +z",
            )),
        }?;
        match target.initial_momentum() {
            Some(laddu::prelude::InitialMomentum::Momentum(momentum))
                if momentum.x == 0.0 && momentum.y == 0.0 && momentum.z == 0.0 => {}
            _ => {
                return Err(invalid(
                    format!("channel edge `{}`", target.name()),
                    "target must be a proton at rest",
                ));
            }
        }
        let production = channel
            .vertices()
            .filter(|vertex| {
                vertex.incoming().len() == 2
                    && vertex.incoming().iter().any(|name| name == beam.name())
                    && vertex.incoming().iter().any(|name| name == target.name())
            })
            .collect::<Vec<_>>();
        if production.len() != 1 {
            return Err(invalid(
                "channel",
                format!(
                    "expected exactly one production vertex consuming beam and target, found {}",
                    production.len()
                ),
            ));
        }
        let production = production[0];
        if production.outgoing().len() != 2 {
            return Err(invalid(
                format!("channel vertex `{}`", production.name()),
                "production must have exactly two outgoing edges",
            ));
        }
        let products = [
            node_from_channel(channel, &production.outgoing()[0])?,
            node_from_channel(channel, &production.outgoing()[1])?,
        ];
        let (outgoing, distribution, t_min, t_max) = transfer_from_vertex(production, beam.name())?;
        let config = Self {
            schema: None,
            version: FORMAT_VERSION,
            name: channel.name().to_owned(),
            beam: BeamConfig {
                name: beam.name().to_owned(),
                energy,
            },
            target: TargetConfig {
                name: target.name().to_owned(),
            },
            production: ProductionConfig {
                name: production.name().to_owned(),
                products,
                transfer: TransferConfig {
                    outgoing,
                    distribution,
                    t_min,
                    t_max,
                },
            },
            model: None,
            parameters: None,
            scalars: BTreeMap::new(),
            generation: GenerationSettings::default(),
        };
        config.validate()?;
        Ok(config)
    }
}

fn find_labeled_initial<'a>(
    channel: &'a Channel,
    label: &str,
) -> GenerationConfigResult<&'a laddu::prelude::Edge> {
    let matches = channel
        .initial_edges()
        .filter(|edge| {
            edge.properties()
                .and_then(|properties| properties.id("gluex"))
                .and_then(|id| id.label_value())
                == Some(label)
        })
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        return Ok(matches[0]);
    }
    // Python-side channels commonly carry PDG metadata but not the optional
    // GlueX string labels. The canonical edge names are unambiguous for the
    // standalone format, so use them as a compatibility fallback when no
    // labeled edge was found.
    if matches.is_empty() {
        let named = channel
            .initial_edges()
            .filter(|edge| edge.name() == label)
            .collect::<Vec<_>>();
        if named.len() == 1 {
            return Ok(named[0]);
        }
    }
    Err(invalid(
        "channel",
        format!(
            "expected exactly one initial edge labeled gluex={label}, found {}",
            matches.len()
        ),
    ))
}

fn edge_particle(edge: &laddu::prelude::Edge) -> GenerationConfigResult<Particle> {
    let properties = edge
        .properties()
        .ok_or_else(|| invalid(edge.name(), "edge has no particle properties"))?;
    gluex_particle_from_external_ids(properties.ids())
        .map_err(|error| invalid(edge.name(), error.to_string()))
}

fn require_particle(
    edge: &laddu::prelude::Edge,
    expected: Particle,
    role: &str,
) -> GenerationConfigResult<()> {
    let actual = edge_particle(edge)?;
    if actual != expected {
        return Err(invalid(
            edge.name(),
            format!("{role} must be {expected}, got {actual}"),
        ));
    }
    Ok(())
}

fn scalar_distribution(source: &ScalarSource) -> ScalarDistribution {
    match source {
        ScalarSource::Constant(value) => ScalarDistribution::Fixed { value: *value },
        ScalarSource::Uniform { low, high } => ScalarDistribution::Uniform {
            min: *low,
            max: *high,
        },
        ScalarSource::Histogram(histogram) => ScalarDistribution::Histogram {
            edges: histogram.bin_edges().to_vec(),
            weights: histogram.counts().to_vec(),
        },
    }
}

fn node_from_channel(channel: &Channel, edge_name: &str) -> GenerationConfigResult<ParticleNode> {
    let edge = channel
        .edges()
        .find(|edge| edge.name() == edge_name)
        .ok_or_else(|| invalid(edge_name, "vertex refers to an unknown edge"))?;
    let particle = edge
        .properties()
        .and_then(|properties| gluex_particle_from_external_ids(properties.ids()).ok())
        .filter(|particle| !particle.is_unknown())
        .map(|particle| ParticleReference::Name(particle.to_string()));
    let mass = match edge.mass_proposal() {
        Some(MassProposal::Fixed { mass }) => Some(MassDistribution::Fixed { value: *mass }),
        Some(MassProposal::Uniform { low, high }) => Some(MassDistribution::Uniform {
            min: *low,
            max: *high,
        }),
        None if particle.is_none() => {
            let value = edge
                .properties()
                .ok_or_else(|| invalid(edge_name, "edge has no particle or mass metadata"))?
                .mass()
                .map_err(|error| invalid(edge_name, error.to_string()))?;
            Some(MassDistribution::Fixed { value })
        }
        None => None,
    };
    let decays = channel
        .vertices()
        .filter(|vertex| vertex.incoming() == [edge_name])
        .collect::<Vec<_>>();
    if decays.len() > 1 {
        return Err(invalid(
            edge_name,
            "edge is consumed by more than one decay vertex",
        ));
    }
    let decay = decays
        .first()
        .map(|vertex| {
            if vertex.outgoing().len() != 2
                || !matches!(
                    vertex.generation(),
                    None | Some(VertexProposal::TwoBodyDecay)
                )
            {
                return Err(invalid(
                    vertex.name(),
                    "decays must be isotropic one-to-two vertices",
                ));
            }
            Ok(Box::new(DecayConfig {
                name: vertex.name().to_owned(),
                products: [
                    node_from_channel(channel, &vertex.outgoing()[0])?,
                    node_from_channel(channel, &vertex.outgoing()[1])?,
                ],
            }))
        })
        .transpose()?;
    if decay.is_none() && !edge.is_output() {
        return Err(invalid(
            edge_name,
            "a final-state edge must be marked as output",
        ));
    }
    Ok(ParticleNode {
        name: edge_name.to_owned(),
        particle,
        mass,
        decay,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScatteringWire {
    incoming_edge: String,
    outgoing_edge: String,
    distribution: DistributionWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DistributionWire {
    components: Vec<(f64, TComponent)>,
    #[serde(default)]
    t_min: Option<f64>,
    #[serde(default)]
    t_max: Option<f64>,
}

fn transfer_from_vertex(
    vertex: &laddu::prelude::Vertex,
    beam_name: &str,
) -> GenerationConfigResult<(String, TransferDistribution, Option<f64>, Option<f64>)> {
    let Some(VertexProposal::TwoBodyScattering { proposal }) = vertex.generation() else {
        return Err(invalid(
            vertex.name(),
            "production requires a two-body scattering proposal",
        ));
    };
    let wire: ScatteringWire = serde_json::from_value(serde_json::to_value(proposal)?)?;
    if wire.incoming_edge != beam_name {
        return Err(invalid(
            vertex.name(),
            "production transfer pairing must use the photon beam",
        ));
    }
    let distribution = if wire.distribution.components.len() == 1 {
        transfer_distribution_from_component(&wire.distribution.components[0].1)
    } else {
        TransferDistribution::Mixture {
            components: wire
                .distribution
                .components
                .iter()
                .map(|(weight, component)| TransferMixtureComponent {
                    weight: *weight,
                    component: transfer_component_from_laddu(component),
                })
                .collect(),
        }
    };
    Ok((
        wire.outgoing_edge,
        distribution,
        wire.distribution.t_min,
        wire.distribution.t_max,
    ))
}

fn transfer_distribution_from_component(component: &TComponent) -> TransferDistribution {
    match component {
        TComponent::Uniform => TransferDistribution::Uniform,
        TComponent::Exponential { slope } => TransferDistribution::Exponential { slope: *slope },
        TComponent::Pole {
            exchange_mass,
            power,
        } => TransferDistribution::Pole {
            exchange_mass: *exchange_mass,
            power: *power,
        },
        TComponent::Histogram { histogram } => TransferDistribution::Histogram {
            edges: histogram.bin_edges().to_vec(),
            weights: histogram.counts().to_vec(),
        },
    }
}

fn transfer_component_from_laddu(component: &TComponent) -> TransferComponent {
    match transfer_distribution_from_component(component) {
        TransferDistribution::Uniform => TransferComponent::Uniform,
        TransferDistribution::Exponential { slope } => TransferComponent::Exponential { slope },
        TransferDistribution::Pole {
            exchange_mass,
            power,
        } => TransferComponent::Pole {
            exchange_mass,
            power,
        },
        TransferDistribution::Histogram { edges, weights } => {
            TransferComponent::Histogram { edges, weights }
        }
        TransferDistribution::Mixture { .. } => {
            unreachable!("one Laddu component cannot itself be a mixture")
        }
    }
}

fn check_name(path: &str, name: &str, names: &mut HashSet<String>) -> GenerationConfigResult<()> {
    if name.trim().is_empty() {
        return Err(invalid(path, "must not be empty"));
    }
    if !names.insert(name.to_owned()) {
        return Err(invalid(path, format!("duplicate channel name `{name}`")));
    }
    Ok(())
}

fn validate_node(
    path: &str,
    node: &ParticleNode,
    names: &mut HashSet<String>,
) -> GenerationConfigResult<()> {
    check_name(&format!("{path}.name"), &node.name, names)?;
    let particle = node
        .particle
        .as_ref()
        .map(resolve_particle)
        .transpose()
        .map_err(|message| invalid(format!("{path}.particle"), message))?;
    if particle.is_none() && node.mass.is_none() {
        return Err(invalid(path, "requires `particle` or an explicit `mass`"));
    }
    if node.decay.is_none() {
        let particle = particle.ok_or_else(|| {
            invalid(
                format!("{path}.particle"),
                "final-state particles must use a supported GlueX name or PDG code",
            )
        })?;
        if particle.is_unknown() {
            return Err(invalid(
                format!("{path}.particle"),
                format!("{particle} is not a supported GlueX final state"),
            ));
        }
        if matches!(node.mass, Some(MassDistribution::Uniform { .. })) {
            return Err(invalid(
                format!("{path}.mass"),
                "final-state masses cannot be sampled",
            ));
        }
    }
    if let Some(mass) = &node.mass {
        validate_mass(&format!("{path}.mass"), mass)?;
    }
    if let Some(decay) = &node.decay {
        check_name(&format!("{path}.decay.name"), &decay.name, names)?;
        for (index, product) in decay.products.iter().enumerate() {
            validate_node(&format!("{path}.decay.products[{index}]"), product, names)?;
        }
    }
    Ok(())
}

fn validate_mass(path: &str, mass: &MassDistribution) -> GenerationConfigResult<()> {
    match *mass {
        MassDistribution::Fixed { value } if value.is_finite() && value >= 0.0 => Ok(()),
        MassDistribution::Uniform { min, max }
            if min.is_finite() && max.is_finite() && min >= 0.0 && max > min =>
        {
            Ok(())
        }
        MassDistribution::Fixed { .. } => Err(invalid(path, "mass must be finite and nonnegative")),
        MassDistribution::Uniform { .. } => {
            Err(invalid(path, "uniform mass requires finite 0 <= min < max"))
        }
    }
}

fn validate_scalar(path: &str, source: &ScalarDistribution) -> GenerationConfigResult<()> {
    match source {
        ScalarDistribution::Fixed { value } if value.is_finite() && *value > 0.0 => Ok(()),
        ScalarDistribution::Uniform { min, max }
            if min.is_finite() && max.is_finite() && *min > 0.0 && max > min =>
        {
            Ok(())
        }
        ScalarDistribution::Histogram { edges, weights } => {
            validate_histogram(path, edges, weights)
        }
        ScalarDistribution::Fixed { .. } => {
            Err(invalid(path, "beam energy must be positive and finite"))
        }
        ScalarDistribution::Uniform { .. } => Err(invalid(
            path,
            "uniform beam energy requires finite 0 < min < max",
        )),
    }
}

fn validate_extra_scalar(path: &str, source: &ScalarDistribution) -> GenerationConfigResult<()> {
    scalar_source(source)?
        .support()
        .map(|_| ())
        .map_err(|error| invalid(path, error.to_string()))
}

fn validate_histogram(path: &str, edges: &[f64], weights: &[f64]) -> GenerationConfigResult<()> {
    if edges.len() != weights.len() + 1 || weights.is_empty() {
        return Err(invalid(
            path,
            "histogram requires N nonempty weights and N+1 edges",
        ));
    }
    if edges
        .windows(2)
        .any(|pair| !pair[0].is_finite() || !pair[1].is_finite() || pair[1] <= pair[0])
    {
        return Err(invalid(
            path,
            "histogram edges must be finite and strictly increasing",
        ));
    }
    if weights
        .iter()
        .any(|weight| !weight.is_finite() || *weight < 0.0)
        || !weights.iter().any(|weight| *weight > 0.0)
    {
        return Err(invalid(
            path,
            "histogram weights must be finite, nonnegative, and not all zero",
        ));
    }
    Ok(())
}

fn validate_transfer(path: &str, transfer: &TransferConfig) -> GenerationConfigResult<()> {
    if transfer.t_min.is_some_and(|value| !value.is_finite())
        || transfer.t_max.is_some_and(|value| !value.is_finite())
        || matches!((transfer.t_min, transfer.t_max), (Some(min), Some(max)) if max <= min)
    {
        return Err(invalid(
            path,
            "t limits must be finite and satisfy t_min < t_max",
        ));
    }
    validate_transfer_distribution(path, &transfer.distribution)
}

fn validate_transfer_distribution(
    path: &str,
    distribution: &TransferDistribution,
) -> GenerationConfigResult<()> {
    match distribution {
        TransferDistribution::Uniform => Ok(()),
        TransferDistribution::Exponential { slope } if slope.is_finite() => Ok(()),
        TransferDistribution::Pole {
            exchange_mass,
            power,
        } if exchange_mass.is_finite()
            && *exchange_mass >= 0.0
            && power.is_finite()
            && *power > 0.0 =>
        {
            Ok(())
        }
        TransferDistribution::Histogram { edges, weights } => {
            validate_histogram(path, edges, weights)
        }
        TransferDistribution::Mixture { components } => {
            if components.is_empty() {
                return Err(invalid(path, "mixture must contain at least one component"));
            }
            for (index, component) in components.iter().enumerate() {
                if !component.weight.is_finite() || component.weight <= 0.0 {
                    return Err(invalid(
                        format!("{path}.components[{index}].weight"),
                        "mixture weight must be finite and positive",
                    ));
                }
                validate_transfer_component(
                    &format!("{path}.components[{index}]"),
                    &component.component,
                )?;
            }
            Ok(())
        }
        TransferDistribution::Exponential { .. } => {
            Err(invalid(path, "exponential slope must be finite"))
        }
        TransferDistribution::Pole { .. } => Err(invalid(
            path,
            "pole requires a finite nonnegative exchange_mass and finite positive power",
        )),
    }
}

fn validate_transfer_component(
    path: &str,
    component: &TransferComponent,
) -> GenerationConfigResult<()> {
    let distribution = match component {
        TransferComponent::Uniform => TransferDistribution::Uniform,
        TransferComponent::Exponential { slope } => {
            TransferDistribution::Exponential { slope: *slope }
        }
        TransferComponent::Pole {
            exchange_mass,
            power,
        } => TransferDistribution::Pole {
            exchange_mass: *exchange_mass,
            power: *power,
        },
        TransferComponent::Histogram { edges, weights } => TransferDistribution::Histogram {
            edges: edges.clone(),
            weights: weights.clone(),
        },
    };
    validate_transfer_distribution(path, &distribution)
}

fn resolve_particle(reference: &ParticleReference) -> Result<Particle, String> {
    let particle = match reference {
        ParticleReference::Pdg(code) => Particle::from_pdg(*code),
        ParticleReference::Name(name) => {
            Particle::from_str(name).unwrap_or_else(|_| Particle::from_particle_type(name))
        }
    };
    if particle.is_unknown()
        && !matches!(reference, ParticleReference::Name(name) if name == "Unknown")
        && !matches!(reference, ParticleReference::Pdg(0))
    {
        return Err(format!("unknown particle identifier {reference}"));
    }
    Ok(particle)
}

impl fmt::Display for ParticleReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(name) => formatter.write_str(name),
            Self::Pdg(code) => code.fmt(formatter),
        }
    }
}

fn particle_properties(particle: Particle) -> ParticleProperties {
    ParticleProperties::unknown()
        .with_name(particle.to_string())
        .with_mass(particle.particle_mass())
        .with_id(
            "pdg",
            i64::try_from(particle.to_pdg()).expect("PDG code fits i64"),
        )
        .with_id("gluex", particle.to_particle_type())
}

fn scalar_source(source: &ScalarDistribution) -> GenerationConfigResult<ScalarSource> {
    match source {
        ScalarDistribution::Fixed { value } => Ok(ScalarSource::constant(*value)),
        ScalarDistribution::Uniform { min, max } => Ok(ScalarSource::uniform(*min, *max)),
        ScalarDistribution::Histogram { edges, weights } => Ok(ScalarSource::histogram(
            Histogram::new(weights.clone(), edges.clone())
                .map_err(|error| GenerationConfigError::Laddu(error.to_string()))?,
        )),
    }
}

fn add_node(channel: &mut Channel, node: &ParticleNode) -> GenerationConfigResult<()> {
    let mut edge = channel.edge(&node.name);
    if let Some(reference) = &node.particle {
        edge.properties(&particle_properties(resolve_particle(reference).map_err(
            |message| invalid(format!("particle `{}`", node.name), message),
        )?));
    } else if let Some(MassDistribution::Fixed { value }) = node.mass {
        edge.properties(
            &ParticleProperties::unknown()
                .with_name(&node.name)
                .with_mass(value),
        );
    }
    if let Some(mass) = &node.mass {
        match *mass {
            MassDistribution::Fixed { value } => {
                edge.mass_proposal(MassProposal::fixed(value));
            }
            MassDistribution::Uniform { min, max } => {
                edge.mass_proposal(MassProposal::uniform(min, max));
            }
        }
    }
    if node.decay.is_none() {
        edge.p4(Vec4::event(&node.name)).output();
    } else {
        edge.generated_only();
    }
    if let Some(decay) = &node.decay {
        for product in &decay.products {
            add_node(channel, product)?;
        }
    }
    Ok(())
}

fn add_decays(channel: &mut Channel, node: &ParticleNode) {
    if let Some(decay) = &node.decay {
        channel
            .vertex(&decay.name)
            .incoming([&node.name])
            .outgoing([&decay.products[0].name, &decay.products[1].name])
            .generation(VertexProposal::isotropic_decay());
        for product in &decay.products {
            add_decays(channel, product);
        }
    }
}

fn histogram_component(edges: &[f64], weights: &[f64]) -> GenerationConfigResult<TComponent> {
    Ok(TComponent::Histogram {
        histogram: Histogram::new(weights.to_vec(), edges.to_vec())
            .map_err(|error| GenerationConfigError::Laddu(error.to_string()))?,
    })
}

fn transfer_component(component: &TransferComponent) -> GenerationConfigResult<TComponent> {
    match component {
        TransferComponent::Uniform => Ok(TComponent::Uniform),
        TransferComponent::Exponential { slope } => Ok(TComponent::Exponential { slope: *slope }),
        TransferComponent::Pole {
            exchange_mass,
            power,
        } => Ok(TComponent::Pole {
            exchange_mass: *exchange_mass,
            power: *power,
        }),
        TransferComponent::Histogram { edges, weights } => histogram_component(edges, weights),
    }
}

fn transfer_distribution(config: &TransferConfig) -> GenerationConfigResult<TDistribution> {
    let distribution = match &config.distribution {
        TransferDistribution::Uniform => TDistribution::uniform(),
        TransferDistribution::Exponential { slope } => TDistribution::exponential(*slope),
        TransferDistribution::Pole {
            exchange_mass,
            power,
        } => TDistribution::pole(*exchange_mass, *power),
        TransferDistribution::Histogram { edges, weights } => TDistribution::histogram(
            Histogram::new(weights.clone(), edges.clone())
                .map_err(|error| GenerationConfigError::Laddu(error.to_string()))?,
        ),
        TransferDistribution::Mixture { components } => TDistribution::mixture(
            components
                .iter()
                .map(|component| Ok((component.weight, transfer_component(&component.component)?)))
                .collect::<GenerationConfigResult<Vec<_>>>()?,
        ),
    };
    distribution
        .with_limits(config.t_min, config.t_max)
        .map_err(|error| GenerationConfigError::Laddu(error.to_string()))
}

/// Confirm that all output particle metadata can be mapped back to `GlueX`.
///
/// # Errors
///
/// Returns an error if a compiled final-state edge is not writable to HDDM.
pub fn validate_hddm_species(channel: &Channel) -> GenerationConfigResult<()> {
    for edge in channel.edges().filter(|edge| edge.is_output()) {
        let properties = edge
            .properties()
            .ok_or_else(|| invalid(edge.name(), "output edge has no particle properties"))?;
        gluex_particle_from_external_ids(properties.ids())
            .map_err(|error| invalid(edge.name(), error.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{GenerationConfig, validate_hddm_species};

    const EXAMPLE: &str = include_str!("../../examples/generation/piplus-neutron.json");

    #[test]
    fn standalone_channel_round_trips_through_laddu() {
        let original = GenerationConfig::from_json(EXAMPLE).unwrap();
        let channel = original.to_channel().unwrap();
        let converted = GenerationConfig::try_from_channel(&channel).unwrap();
        let rebuilt = converted.to_channel().unwrap();

        assert_eq!(rebuilt.name(), channel.name());
        assert_eq!(
            rebuilt
                .edges()
                .map(laddu::prelude::Edge::name)
                .collect::<Vec<_>>(),
            channel
                .edges()
                .map(laddu::prelude::Edge::name)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn decaying_intermediates_are_not_hddm_outputs() {
        let config = GenerationConfig::from_json(
            r#"{
                "version": 1,
                "name": "gamma p -> X p, X -> KShort KShort",
                "beam": {
                    "name": "beam",
                    "energy": { "kind": "uniform", "min": 8.0, "max": 9.0 }
                },
                "target": { "name": "target" },
                "production": {
                    "name": "production",
                    "products": [
                        {
                            "name": "X",
                            "mass": { "kind": "uniform", "min": 1.0, "max": 2.0 },
                            "decay": {
                                "name": "decay",
                                "products": [
                                    { "name": "ks1", "particle": "KShort" },
                                    { "name": "ks2", "particle": "KShort" }
                                ]
                            }
                        },
                        { "name": "recoil", "particle": "Proton" }
                    ],
                    "transfer": {
                        "outgoing": "X",
                        "distribution": { "kind": "exponential", "slope": 1.2 },
                        "t_min": -1.0,
                        "t_max": 0.0
                    }
                }
            }"#,
        )
        .unwrap();
        let channel = config.to_channel().unwrap();

        let output = |name: &str| {
            channel
                .edges()
                .find(|edge| edge.name() == name)
                .unwrap()
                .is_output()
        };
        assert!(!output("X"));
        assert!(output("ks1"));
        assert!(output("ks2"));
        assert!(output("recoil"));
        validate_hddm_species(&channel).unwrap();
    }

    #[test]
    fn semantic_digest_ignores_schema_location() {
        let mut config = GenerationConfig::from_json(EXAMPLE).unwrap();
        let first = config.semantic_sha256().unwrap();
        config.schema = Some("https://example.invalid/schema.json".to_owned());
        assert_eq!(config.semantic_sha256().unwrap(), first);
    }

    #[test]
    fn python_style_initial_edges_fall_back_to_canonical_names() {
        let original = GenerationConfig::from_json(EXAMPLE).unwrap();
        let mut channel = original.to_channel().unwrap();
        for name in ["beam", "target"] {
            let mut properties = channel
                .edges()
                .find(|edge| edge.name() == name)
                .and_then(laddu::prelude::Edge::properties)
                .unwrap()
                .clone();
            properties.ids.shift_remove("gluex");
            channel.edge(name).properties(&properties);
        }

        let converted = GenerationConfig::try_from_channel(&channel).unwrap();
        assert_eq!(converted.beam.name, "beam");
        assert_eq!(converted.target.name, "target");
    }
}
