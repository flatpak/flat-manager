use clap::Parser;
use std::process;

use flatmanager::gentoken::{run_gentoken, GentokenArgs};

#[derive(Debug, Parser)]
#[command(
    about = "Generate token for flat-manager.",
    long_about = None,
    disable_version_flag = true
)]
struct Cli {
    #[command(flatten)]
    args: GentokenArgs,
}

fn main() {
    process::exit(run_gentoken(Cli::parse().args));
}
