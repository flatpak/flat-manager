mod client;

use clap::{Args, Parser, Subcommand};
use client::{ApiClient, ClientError, JobPoller};
use flatmanager::gentoken::{run_gentoken, GentokenArgs};
use log::LevelFilter;
use serde_json::{json, Value};
use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process;

#[derive(Debug, Parser)]
#[command(
    name = "flat-manager-client",
    subcommand_required = true,
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
    command: Command,
}

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
    #[command(name = "gentoken", about = "Generate token for flat-manager")]
    Gentoken(GentokenArgs),
}

impl Command {
    fn name(&self) -> &'static str {
        match self {
            Command::Create(_) => "create",
            Command::Push(_) => "push",
            Command::Commit(_) => "commit",
            Command::Publish(_) => "publish",
            Command::Purge(_) => "purge",
            Command::Prune(_) => "prune",
            Command::CreateToken(_) => "create-token",
            Command::FollowJob(_) => "follow-job",
            Command::Gentoken(_) => "gentoken",
        }
    }
}

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
        conflicts_with = "no_public_download",
        help = "Allow public read access to the build repo"
    )]
    public_download: bool,
    #[arg(
        long = "no_public_download",
        help = "Disallow public read access to the build repo"
    )]
    no_public_download: bool,
    #[arg(
        long = "build-log-url",
        value_name = "BUILD_LOG_URL",
        help = "Set URL of the build log for the whole build"
    )]
    build_log_url: Option<String>,
}

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

#[derive(Debug, Args)]
struct PublishArgs {
    #[arg(long, help = "Wait for publish to finish")]
    wait: bool,
    #[arg(long = "wait-update", help = "Wait for update-repo to finish")]
    wait_update: bool,
    #[arg(value_name = "build_url", help = "Remote build URL")]
    build_url: String,
}

#[derive(Debug, Args)]
struct PurgeArgs {
    #[arg(value_name = "build_url", help = "Remote build URL")]
    build_url: String,
}

#[derive(Debug, Args)]
struct PruneArgs {
    #[arg(value_name = "manager_url", help = "Remote repo manager URL")]
    manager_url: String,
    #[arg(value_name = "repo", help = "Repo name")]
    repo: String,
}

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
    duration: i64,
}

#[derive(Debug, Args)]
struct FollowJobArgs {
    #[arg(value_name = "job_url", help = "URL of job")]
    job_url: String,
}

fn resolve_token(
    token: &Option<String>,
    token_file: Option<&PathBuf>,
) -> Result<String, ClientError> {
    if let Some(token) = token {
        return Ok(token.clone());
    }
    if let Some(path) = token_file {
        let contents = fs::read(path)?;
        let first_line = contents.split(|&b| b == b'\n').next().unwrap_or(&contents);
        return Ok(String::from_utf8_lossy(first_line).trim().to_string());
    }
    if let Ok(token) = env::var("REPO_TOKEN") {
        return Ok(token);
    }
    Err(ClientError::Usage(
        "No token available, pass with --token, --token-file or $REPO_TOKEN".into(),
    ))
}

async fn run(
    command: Command,
    client: &ApiClient,
    print_output: bool,
) -> (&'static str, Result<Value, ClientError>) {
    match command {
        Command::Create(args) => {
            let public_download = match (args.public_download, args.no_public_download) {
                (true, _) => Some(true),
                (_, true) => Some(false),
                _ => None,
            };
            let resp = client
                .create_build(
                    &args.manager_url,
                    &args.repo,
                    args.app_id.as_deref(),
                    public_download,
                    args.build_log_url.as_deref(),
                )
                .await;
            if let Ok(ref resp) = resp {
                if !print_output {
                    if let Some(ref loc) = resp.location {
                        println!("{loc}");
                    }
                }
            }
            (
                "create",
                resp.map(|r| {
                    let mut body = r.body;
                    if let Some(loc) = r.location {
                        body["location"] = serde_json::Value::String(loc);
                    }
                    body
                }),
            )
        }
        Command::Purge(args) => (
            "purge",
            client
                .purge_build(&args.build_url)
                .await
                .map(|response| response.body),
        ),
        Command::Prune(args) => (
            "prune",
            client
                .prune_repo(&args.manager_url, &args.repo)
                .await
                .map(|response| response.body),
        ),
        Command::CreateToken(args) => {
            let resp = client
                .create_token(
                    &args.manager_url,
                    &args.name,
                    &args.subject,
                    &args.scope,
                    args.duration,
                )
                .await;
            if let Ok(ref resp) = resp {
                if !print_output {
                    if let Some(token) = resp.body.get("token").and_then(Value::as_str) {
                        println!("{token}");
                    }
                }
            }
            ("create-token", resp.map(|response| response.body))
        }
        Command::Commit(args) => (
            "commit",
            client
                .commit_build(
                    &args.build_url,
                    args.end_of_life.as_deref(),
                    args.end_of_life_rebase.as_deref(),
                    args.token_type,
                )
                .await,
        ),
        Command::Publish(args) => (
            "publish",
            client
                .publish_build(&args.build_url, args.wait, args.wait_update)
                .await,
        ),
        Command::FollowJob(args) => (
            "follow-job",
            JobPoller::new(client, &args.job_url)
                .poll_to_completion()
                .await,
        ),
        Command::Gentoken(_) => (
            "gentoken",
            Err(ClientError::Usage(
                "gentoken is handled before API client initialization".into(),
            )),
        ),
        Command::Push(args) => ("push", client.push_build(&args).await),
    }
}

#[tokio::main]
async fn main() {
    let Cli {
        verbose,
        debug,
        output,
        print_output,
        token,
        token_file,
        command,
    } = Cli::parse();

    let log_level = if debug {
        LevelFilter::Debug
    } else if verbose {
        LevelFilter::Info
    } else {
        LevelFilter::Warn
    };

    env_logger::Builder::new()
        .target(env_logger::Target::Stderr)
        .filter_level(log_level)
        .format(|buf, record| {
            writeln!(
                buf,
                "{}: {}: {}",
                record.module_path().unwrap_or(record.target()),
                record.level(),
                record.args()
            )
        })
        .init();

    let command = match command {
        Command::Gentoken(mut args) => {
            if verbose {
                args.enable_verbose();
            }
            process::exit(run_gentoken(args))
        }
        command => command,
    };

    let default_cmd_name = command.name();
    let token = resolve_token(&token, token_file.as_ref());
    let output_path = output;

    let (cmd_name, result) = match token {
        Ok(token) => {
            let client = ApiClient::new(token);
            run(command, &client, print_output).await
        }
        Err(err) => (default_cmd_name, Err(err)),
    };

    let (output, exit_code) = match result {
        Ok(data) => (json!({ "command": cmd_name, "result": data }), 0),
        Err(ref e) => {
            eprintln!("{e}");
            (json!({ "command": cmd_name, "error": e.to_json() }), 1)
        }
    };

    if print_output {
        println!("{}", serde_json::to_string_pretty(&output).unwrap());
    }
    if let Some(ref path) = output_path {
        let text = serde_json::to_string_pretty(&output).unwrap();
        fs::write(path, format!("{text}\n")).unwrap_or_else(|e| {
            eprintln!("Failed to write {}: {e}", path.display());
        });
    }
    process::exit(exit_code);
}
