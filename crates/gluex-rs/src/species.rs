//! Mapping between experiment-neutral laddu species metadata and `GlueX` particle IDs.

use std::{error::Error, fmt};

use gluex_core::particles::Particle as GluexParticle;
use laddu::ParticleSpecies;

/// Error returned when generic particle species metadata cannot be mapped to `GlueX`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpeciesMappingError {
    /// The species used a code namespace that this mapper does not understand.
    UnsupportedNamespace {
        /// Unsupported code namespace.
        namespace: String,
        /// Numeric species code.
        id: i64,
    },
    /// The species code cannot fit in the `GlueX` particle-code conversion path.
    CodeOutOfRange {
        /// Numeric species code.
        id: i64,
    },
    /// The species code is not known to the `GlueX` particle table.
    UnknownCode {
        /// Numeric species code.
        id: i64,
        /// Optional species code namespace.
        namespace: Option<String>,
    },
    /// The species label is not known to the `GlueX` particle table.
    UnknownLabel {
        /// Species label.
        label: String,
    },
}

impl fmt::Display for SpeciesMappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedNamespace { namespace, id } => {
                write!(
                    f,
                    "unsupported particle species namespace '{namespace}' for code {id}"
                )
            }
            Self::CodeOutOfRange { id } => {
                write!(
                    f,
                    "particle species code {id} is out of range for GlueX mapping"
                )
            }
            Self::UnknownCode { id, namespace } => {
                if let Some(namespace) = namespace {
                    write!(
                        f,
                        "unknown particle species code {id} in namespace '{namespace}'"
                    )
                } else {
                    write!(f, "unknown particle species code {id}")
                }
            }
            Self::UnknownLabel { label } => {
                write!(f, "unknown particle species label '{label}'")
            }
        }
    }
}

impl Error for SpeciesMappingError {}

/// Map experiment-neutral laddu species metadata to a `GlueX` particle ID.
///
/// Numeric codes are interpreted as PDG codes when the namespace is absent or equal to `"pdg"`
/// ignoring ASCII case. Labels are interpreted with `GlueX`'s canonical particle-name table.
///
/// # Errors
///
/// Returns [`SpeciesMappingError`] when the namespace is unsupported or the code/label does not
/// resolve to a known `GlueX` particle.
pub fn gluex_particle_from_species(
    species: &ParticleSpecies,
) -> Result<GluexParticle, SpeciesMappingError> {
    match species {
        ParticleSpecies::Code { id, namespace } => {
            if let Some(namespace) = namespace {
                if !namespace.eq_ignore_ascii_case("pdg") {
                    return Err(SpeciesMappingError::UnsupportedNamespace {
                        namespace: namespace.clone(),
                        id: *id,
                    });
                }
            }
            let pdg = isize::try_from(*id)
                .map_err(|_| SpeciesMappingError::CodeOutOfRange { id: *id })?;
            let particle = GluexParticle::from_pdg(pdg);
            if particle.is_unknown() && *id != 0 {
                Err(SpeciesMappingError::UnknownCode {
                    id: *id,
                    namespace: namespace.clone(),
                })
            } else {
                Ok(particle)
            }
        }
        ParticleSpecies::Label(label) => {
            let particle = GluexParticle::from_string(label);
            if particle.is_unknown() && label != "Unknown" {
                Err(SpeciesMappingError::UnknownLabel {
                    label: label.clone(),
                })
            } else {
                Ok(particle)
            }
        }
    }
}

/// Map experiment-neutral laddu species metadata to an HDDM particle ID through `GlueX` metadata.
///
/// # Errors
///
/// Returns [`SpeciesMappingError`] when the species metadata cannot be mapped to a known `GlueX`
/// particle.
pub fn hddm_particle_from_species(
    species: &ParticleSpecies,
) -> Result<hddm::Particle, SpeciesMappingError> {
    Ok(gluex_particle_from_species(species)?.into())
}

#[cfg(test)]
mod tests {
    use super::{gluex_particle_from_species, hddm_particle_from_species, SpeciesMappingError};
    use gluex_core::particles::Particle as GluexParticle;
    use laddu::ParticleSpecies;

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
                gluex_particle_from_species(&ParticleSpecies::code(pdg)).unwrap(),
                expected
            );
            assert_eq!(
                gluex_particle_from_species(&ParticleSpecies::with_namespace("pdg", pdg)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn maps_labels_to_gluex_particles() {
        assert_eq!(
            gluex_particle_from_species(&ParticleSpecies::label("KShort")).unwrap(),
            GluexParticle::KShort
        );
        assert_eq!(
            gluex_particle_from_species(&ParticleSpecies::label("Proton")).unwrap(),
            GluexParticle::Proton
        );
    }

    #[test]
    fn maps_species_to_hddm_particles() {
        assert_eq!(
            hddm_particle_from_species(&ParticleSpecies::code(22)).unwrap(),
            hddm::Particle::Gamma
        );
        assert_eq!(
            hddm_particle_from_species(&ParticleSpecies::label("KShort")).unwrap(),
            hddm::Particle::KShort
        );
    }

    #[test]
    fn unknown_mapping_behavior_is_explicit() {
        assert_eq!(
            gluex_particle_from_species(&ParticleSpecies::with_namespace("geant4", 2212)),
            Err(SpeciesMappingError::UnsupportedNamespace {
                namespace: "geant4".to_string(),
                id: 2212,
            })
        );
        assert_eq!(
            gluex_particle_from_species(&ParticleSpecies::code(999_999_999)),
            Err(SpeciesMappingError::UnknownCode {
                id: 999_999_999,
                namespace: None,
            })
        );
        assert_eq!(
            gluex_particle_from_species(&ParticleSpecies::label("not-a-gluex-particle")),
            Err(SpeciesMappingError::UnknownLabel {
                label: "not-a-gluex-particle".to_string(),
            })
        );
    }
}
