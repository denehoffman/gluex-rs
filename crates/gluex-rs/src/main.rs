//! Binary entry point for the unified `gluex` CLI.

use gluex_rs::cli;

fn main() -> std::process::ExitCode {
    cli::cli()
}
