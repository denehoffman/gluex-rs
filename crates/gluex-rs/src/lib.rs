//! A Rust library for unified `GlueX` data analysis.

// pub(crate) mod hddm_s {
//     include!(concat!(env!("OUT_DIR"), "/hddm_s.rs"));
// }

pub(crate) mod hddm_s;

/// Monte Carlo generation utilities
pub mod mcgen;

/// Particle species mapping utilities.
pub mod species;
