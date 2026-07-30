//! Build script to generate proper HDDM model code at compile time
use std::{
    env,
    fs::{self, File},
    io::BufReader,
    path::PathBuf,
};

use hddm::header::read_header_streaming;
use hddm_rs::generate_rust;

fn main() -> anyhow::Result<()> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let model_path = manifest_dir.join("models/mc.hddm");
    println!("cargo:rerun-if-changed={}", model_path.display());
    let file = File::open(&model_path)?;
    let mut reader = BufReader::new(file);
    let (model, model_text) = read_header_streaming(&mut reader)?;
    let generated = generate_rust(&model, &model_text)?;
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let out_file = out_dir.join("hddm_s.rs");
    fs::write(&out_file, generated)?;
    Ok(())
}
