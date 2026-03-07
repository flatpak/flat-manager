mod client;

use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process;

#[allow(dead_code)]
#[derive(Debug, Parser)]
#[command(
    name = "flat-manager-client",
    disable_version_flag = true,
    disable_help_subcommand = true
)]
struct Cli {
    #[arg(short, long, help = "Enable verbose output")]
    verbose: bool,
    #[arg(long, help = "Enable debugging output")]
    debug: bool,
    #[arg(long, value_name = "OUTPUT", help = "Write output json to file")]
    output: Option<PathBuf>,
    #[arg(long, help = "Print output json")]
    print_output: bool,
    #[arg(long, value_name = "TOKEN", help = "Use this token")]
    token: Option<String>,
    #[arg(
        long = "token-file",
        value_name = "TOKEN_FILE",
        help = "Use token from file"
    )]
    token_file: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[allow(dead_code)]
#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Create new build")]
    Create(CreateArgs),
    #[command(about = "Push to repo manager")]
    Push(PushArgs),
    #[command(about = "Commit build")]
    Commit(CommitArgs),
    #[command(about = "Publish build")]
    Publish(PublishArgs),
    #[command(about = "Purge build")]
    Purge(PurgeArgs),
    #[command(about = "Prune repository")]
    Prune(PruneArgs),
    #[command(name = "create-token", about = "Create subset token")]
    CreateToken(CreateTokenArgs),
    #[command(name = "follow-job", about = "Follow existing job log")]
    FollowJob(FollowJobArgs),
}

#[allow(dead_code)]
#[derive(Debug, Args)]
struct CreateArgs {
    #[arg(value_name = "manager_url", help = "Remote repo manager URL")]
    manager_url: String,
    #[arg(value_name = "repo", help = "Repo name")]
    repo: String,
    #[arg(value_name = "app_id", help = "App ID")]
    app_id: Option<String>,
    #[arg(
        long = "public_download",
        help = "Allow public read access to the build repo"
    )]
    public_download: bool,
    #[arg(
        long = "no_public_download",
        help = "Allow public read access to the build repo"
    )]
    no_public_download: bool,
    #[arg(
        long = "build-log-url",
        value_name = "BUILD_LOG_URL",
        help = "Set URL of the build log for the whole build"
    )]
    build_log_url: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Args)]
struct PushArgs {
    #[arg(value_name = "build_url", help = "Remote build URL")]
    build_url: String,
    #[arg(value_name = "repo_path", help = "Local repository")]
    repo_path: String,
    #[arg(value_name = "branches", help = "Branches to push")]
    branches: Vec<String>,
    #[arg(long, help = "Commit build after pushing")]
    commit: bool,
    #[arg(long, help = "Publish build after committing")]
    publish: bool,
    #[arg(
        long = "extra-id",
        value_name = "EXTRA_ID",
        help = "Add extra collection-id"
    )]
    extra_id: Vec<String>,
    #[arg(
        long = "ignore-delta",
        value_name = "IGNORE_DELTA",
        help = "Don't upload deltas matching this glob"
    )]
    ignore_delta: Vec<String>,
    #[arg(long, help = "Wait for commit/publish to finish")]
    wait: bool,
    #[arg(long = "wait-update", help = "Wait for update-repo to finish")]
    wait_update: bool,
    #[arg(long = "minimal-token", help = "Create minimal token for the upload")]
    minimal_token: bool,
    #[arg(
        long = "end-of-life",
        value_name = "END_OF_LIFE",
        help = "Set end of life"
    )]
    end_of_life: Option<String>,
    #[arg(
        long = "end-of-life-rebase",
        value_name = "END_OF_LIFE_REBASE",
        help = "Set new ID which will supercede the current one"
    )]
    end_of_life_rebase: Option<String>,
    #[arg(
        long = "token-type",
        value_name = "TOKEN_TYPE",
        help = "Set token type"
    )]
    token_type: Option<i32>,
    #[arg(
        long = "build-log-url",
        value_name = "BUILD_LOG_URL",
        help = "Set URL of the build log for each uploaded ref"
    )]
    build_log_url: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Args)]
struct CommitArgs {
    #[arg(long, help = "Wait for commit to finish")]
    wait: bool,
    #[arg(
        long = "end-of-life",
        value_name = "END_OF_LIFE",
        help = "Set end of life"
    )]
    end_of_life: Option<String>,
    #[arg(
        long = "end-of-life-rebase",
        value_name = "END_OF_LIFE_REBASE",
        help = "Set new ID which will supercede the current one"
    )]
    end_of_life_rebase: Option<String>,
    #[arg(
        long = "token-type",
        value_name = "TOKEN_TYPE",
        help = "Set token type"
    )]
    token_type: Option<i32>,
    #[arg(value_name = "build_url", help = "Remote build URL")]
    build_url: String,
}

#[allow(dead_code)]
#[derive(Debug, Args)]
struct PublishArgs {
    #[arg(long, help = "Wait for publish to finish")]
    wait: bool,
    #[arg(long = "wait-update", help = "Wait for update-repo to finish")]
    wait_update: bool,
    #[arg(value_name = "build_url", help = "Remote build URL")]
    build_url: String,
}

#[allow(dead_code)]
#[derive(Debug, Args)]
struct PurgeArgs {
    #[arg(value_name = "build_url", help = "Remote build URL")]
    build_url: String,
}

#[allow(dead_code)]
#[derive(Debug, Args)]
struct PruneArgs {
    #[arg(value_name = "manager_url", help = "Remote repo manager URL")]
    manager_url: String,
    #[arg(value_name = "repo", help = "Repo name")]
    repo: String,
}

#[allow(dead_code)]
#[derive(Debug, Args)]
struct CreateTokenArgs {
    #[arg(value_name = "manager_url", help = "Remote repo manager URL")]
    manager_url: String,
    #[arg(value_name = "name", help = "Name")]
    name: String,
    #[arg(value_name = "subject", help = "Subject")]
    subject: String,
    #[arg(value_name = "scope", help = "Scope")]
    scope: Vec<String>,
    #[arg(
        long,
        value_name = "DURATION",
        default_value_t = 60 * 60 * 24,
        help = "Duration until expires, in seconds"
    )]
    duration: i32,
}

#[allow(dead_code)]
#[derive(Debug, Args)]
struct FollowJobArgs {
    #[arg(value_name = "job_url", help = "URL of job")]
    job_url: String,
}

fn main() {
    let argv: Vec<String> = env::args_os()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();

    let matches = match Cli::command().try_get_matches_from(&argv) {
        Ok(matches) => matches,
        Err(err) => err.exit(),
    };

    let Cli {
        verbose,
        debug,
        output,
        print_output,
        token,
        token_file,
        command,
    } = Cli::from_arg_matches(&matches).unwrap_or_else(|err| err.exit());

    let _ = (verbose, debug, print_output, token, token_file);

    if command.is_none() {
        println!("No subcommand specified, see --help for usage");
        process::exit(1);
    }

    let payload = serde_json::to_string_pretty(&argv).expect("argv is serializable");
    println!("{payload}");

    if let Some(path) = output {
        if let Err(err) = fs::write(&path, format!("{payload}\n")) {
            eprintln!("Failed to write {}: {err}", path.display());
            process::exit(1);
        }
    }
}
