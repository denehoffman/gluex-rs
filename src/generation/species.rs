//! Mapping between experiment-neutral laddu species metadata and `GlueX` particle IDs.

use std::{error::Error, fmt};

use crate::core::particles::Particle as GluexParticle;
use laddu::physics::quantum::ExternalId;

/// Error returned when generic particle species metadata cannot be mapped to `GlueX`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpeciesMappingError {
    /// The species used a code namespace that this mapper does not understand.
    UnsupportedNamespace {
        /// Unsupported code namespace.
        namespace: String,
        /// External ID.
        id: ExternalId,
    },
    /// The species code cannot fit in the `GlueX` particle-code conversion path.
    CodeOutOfRange {
        /// Numeric species code.
        id: i64,
    },
    /// The species code is not known to the `GlueX` particle table.
    UnknownCode {
        /// Species namespace.
        namespace: String,
        /// Numeric species code.
        id: i64,
    },
    /// The species label is not known to the `GlueX` particle table.
    UnknownLabel {
        /// Species namespace.
        namespace: String,
        /// Species label.
        label: String,
    },
    /// No valid namespace was found among all ids
    NoValidNamespace,
}

impl fmt::Display for SpeciesMappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedNamespace { namespace, id } => match id {
                ExternalId::Code { value } => write!(
                    f,
                    "unsupported particle species namespace '{namespace}' for code {value}"
                ),
                ExternalId::Label { value } => write!(
                    f,
                    "unsupported particle species namespace '{namespace}' for label {value}"
                ),
            },
            Self::CodeOutOfRange { id } => {
                write!(
                    f,
                    "particle species code {id} is out of range for GlueX mapping"
                )
            }
            Self::UnknownCode { namespace, id } => {
                write!(
                    f,
                    "unknown particle species code {id} in namespace '{namespace}'"
                )
            }
            Self::UnknownLabel { namespace, label } => {
                write!(
                    f,
                    "unknown particle species label '{label}' in namespace '{namespace}'"
                )
            }
            Self::NoValidNamespace => write!(
                f,
                "No valid namespace was found among particle ids, all particles must have a 'gluex' or 'pdg' identifier"
            ),
        }
    }
}

impl Error for SpeciesMappingError {}

fn gluex_particle_from_pdg_external_id(
    id: &ExternalId,
) -> Result<GluexParticle, SpeciesMappingError> {
    match id {
        ExternalId::Code { value } => {
            let pdg = isize::try_from(*value)
                .map_err(|_| SpeciesMappingError::CodeOutOfRange { id: *value })?;
            let particle = GluexParticle::from_pdg(pdg);
            if particle.is_unknown() && *value != 0 {
                Err(SpeciesMappingError::UnknownCode {
                    namespace: "pdg".to_string(),
                    id: *value,
                })
            } else {
                Ok(particle)
            }
        }
        ExternalId::Label { value: _ } => Err(SpeciesMappingError::UnsupportedNamespace {
            namespace: "pdg".to_string(),
            id: id.clone(),
        }),
    }
}

fn gluex_particle_from_gluex_external_id(
    id: &ExternalId,
) -> Result<GluexParticle, SpeciesMappingError> {
    match id {
        ExternalId::Code { value: _ } => Err(SpeciesMappingError::UnsupportedNamespace {
            namespace: "gluex".to_string(),
            id: id.clone(),
        }),
        ExternalId::Label { value } => {
            let particle = GluexParticle::from_particle_type(value);
            if particle.is_unknown() && value != "Unknown" {
                Err(SpeciesMappingError::UnknownLabel {
                    namespace: "gluex".to_string(),
                    label: value.clone(),
                })
            } else {
                Ok(particle)
            }
        }
    }
}

/// Map an experiment-neutral laddu external ID to a `GlueX` particle ID.
///
/// # Errors
///
/// Returns [`SpeciesMappingError`] when the external ID cannot be mapped to a known `GlueX`
/// particle.
pub fn gluex_particle_from_external_id(
    namespace: &str,
    id: &ExternalId,
) -> Result<GluexParticle, SpeciesMappingError> {
    match namespace.to_lowercase().as_str() {
        "pdg" => Ok(gluex_particle_from_pdg_external_id(id)?),
        "gluex" => Ok(gluex_particle_from_gluex_external_id(id)?),
        _ => Err(SpeciesMappingError::UnsupportedNamespace {
            namespace: namespace.to_string(),
            id: id.clone(),
        }),
    }
}

/// Return the first laddu ID that maps to a valid `GlueX` particle ID.
///
/// # Errors
///
/// Returns [`SpeciesMappingError`] if none of the ids have a valid namespace.
pub fn gluex_particle_from_external_ids<'a, I>(ids: I) -> Result<GluexParticle, SpeciesMappingError>
where
    I: IntoIterator<Item = (&'a String, &'a ExternalId)>,
{
    for (namespace, id) in ids {
        if let Ok(particle) = gluex_particle_from_external_id(namespace, id) {
            return Ok(particle); // TODO: this needs work, right now it doesn't forward errors so
            // they are lost
        }
    }
    Err(SpeciesMappingError::NoValidNamespace)
}

#[cfg(test)]
mod tests {
    use super::{
        SpeciesMappingError, gluex_particle_from_external_id,
        gluex_particle_from_gluex_external_id, gluex_particle_from_pdg_external_id,
    };
    use crate::core::particles::Particle as GluexParticle;
    use laddu::physics::quantum::ExternalId;

    #[test]
    fn maps_pdg_codes_to_gluex_particles() {
        let cases = [
            (22, GluexParticle::Gamma),
            (2212, GluexParticle::Proton),
            (211, GluexParticle::PiPlus),
            (-211, GluexParticle::PiMinus),
            (321, GluexParticle::KPlus),
            (-321, GluexParticle::KMinus),
            (310, GluexParticle::KShort),
        ];
        for (pdg, expected) in cases {
            assert_eq!(
                gluex_particle_from_pdg_external_id(&ExternalId::code(pdg)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn maps_labels_to_gluex_particles() {
        assert_eq!(
            gluex_particle_from_gluex_external_id(&ExternalId::label("KShort")).unwrap(),
            GluexParticle::KShort
        );
        assert_eq!(
            gluex_particle_from_gluex_external_id(&ExternalId::label("Proton")).unwrap(),
            GluexParticle::Proton
        );
    }

    #[test]
    fn maps_species_to_hddm_particles() {
        assert_eq!(
            gluex_particle_from_external_id("pdg", &ExternalId::code(22)).unwrap(),
            GluexParticle::Gamma
        );
        assert_eq!(
            gluex_particle_from_external_id("gluex", &ExternalId::label("KShort")).unwrap(),
            GluexParticle::KShort
        );
    }

    #[test]
    fn unknown_mapping_behavior_is_explicit() {
        assert_eq!(
            gluex_particle_from_external_id("geant4", &ExternalId::code(2212)),
            Err(SpeciesMappingError::UnsupportedNamespace {
                namespace: "geant4".to_string(),
                id: ExternalId::code(2212),
            })
        );
        assert_eq!(
            gluex_particle_from_external_id("pdg", &ExternalId::code(1_234_567_890)),
            Err(SpeciesMappingError::UnknownCode {
                namespace: "pdg".to_string(),
                id: 1_234_567_890
            })
        );
        assert_eq!(
            gluex_particle_from_external_id("gluex", &ExternalId::label("not-a-gluex-particle")),
            Err(SpeciesMappingError::UnknownLabel {
                namespace: "gluex".to_string(),
                label: "not-a-gluex-particle".to_string(),
            })
        );
    }
}
