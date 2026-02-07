//! Binary entry point for the `gluex-lumi` CLI.

use gluex_lumi::cli;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    cli::cli()
}
