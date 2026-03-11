use chrono::{Duration, Utc};
use clap::Args;
use jwt::{encode, EncodingKey, Header};
use std::fs;
use std::io::{self, Read};

use crate::tokens::{Claims, ClaimsScope};

fn default_duration() -> i64 {
    Duration::days(365).num_seconds()
}

#[derive(Debug, Args)]
pub struct GentokenArgs {
    #[arg(short, long, help = "Be verbose")]
    verbose: bool,
    #[arg(
        long,
        default_value = "default",
        value_name = "NAME",
        help = "Name for the token"
    )]
    name: String,
    #[arg(
        long,
        default_value = "build",
        value_name = "SUB",
        help = "Subject (default: build)"
    )]
    sub: String,
    #[arg(
        long = "scope",
        value_name = "SCOPE",
        help = "Add scope (default if none: [build, upload, download, publish, jobs]"
    )]
    scope_values: Vec<String>,
    #[arg(
        long = "prefix",
        value_name = "PREFIX",
        help = "Add ref prefix (default if none: ['']"
    )]
    prefixes: Vec<String>,
    #[arg(
        long = "repo",
        value_name = "REPO",
        help = "Add repo (default if none: ['']"
    )]
    repos: Vec<String>,
    #[arg(long, help = "The secret is base64 encoded")]
    base64: bool,
    #[arg(long, value_name = "SECRET", help = "Secret used to encode the token")]
    secret: Option<String>,
    #[arg(
        long = "secret-file",
        value_name = "SECRET_FILE",
        help = "Load secret from file (or - for stdin)"
    )]
    secret_file: Option<String>,
    #[arg(
        long,
        default_value_t = default_duration(),
        value_name = "DURATION",
        help = "Duration for key in seconds (default 1 year)"
    )]
    duration: i64,
    #[arg(
        long = "token-type",
        default_value = "app",
        value_name = "TOKEN_TYPE",
        help = "Token type"
    )]
    token_type: String,
    #[arg(
        long = "branch",
        value_name = "BRANCH",
        help = "Add branch (default if none: ['stable']"
    )]
    branches: Vec<String>,
}

impl GentokenArgs {
    pub fn enable_verbose(&mut self) {
        self.verbose = true;
    }
}

fn read_secret(filename: &str) -> io::Result<String> {
    let mut contents = String::new();
    if filename == "-" {
        io::stdin().read_to_string(&mut contents)?;
    } else {
        let mut file = fs::File::open(filename)?;
        file.read_to_string(&mut contents)?;
    }
    Ok(contents)
}

pub fn run_gentoken(args: GentokenArgs) -> i32 {
    let GentokenArgs {
        verbose,
        base64,
        name,
        sub,
        secret,
        secret_file,
        duration,
        scope_values,
        mut prefixes,
        mut repos,
        token_type,
        mut branches,
    } = args;

    let secret_contents;

    let scope: Vec<ClaimsScope> = if scope_values.is_empty() {
        vec![
            ClaimsScope::Build,
            ClaimsScope::Upload,
            ClaimsScope::Download,
            ClaimsScope::Publish,
            ClaimsScope::Jobs,
        ]
    } else {
        match scope_values
            .into_iter()
            .map(|scope_value| scope_value.parse::<ClaimsScope>())
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(scope) => scope,
            Err(err) => {
                eprintln!("{err}");
                return 1;
            }
        }
    };

    if prefixes.is_empty() {
        prefixes = vec!["".to_string()];
    }

    if repos.is_empty() {
        repos = vec!["".to_string()];
    }

    if branches.is_empty() {
        branches = vec!["stable".to_string()];
    }

    if let Some(secret) = secret {
        secret_contents = secret;
    } else if let Some(filename) = secret_file {
        match read_secret(&filename) {
            Ok(contents) => secret_contents = contents,
            Err(err) => {
                eprintln!("Error reading secrets: {err}");
                return 1;
            }
        }
    } else {
        eprintln!("No secret specified, use --secret or --secret-file");
        return 1;
    }

    let key = if base64 {
        match EncodingKey::from_base64_secret(secret_contents.trim()) {
            Ok(key) => key,
            Err(err) => {
                eprintln!("Error decoding base64 secret: {err}");
                return 1;
            }
        }
    } else {
        EncodingKey::from_secret(secret_contents.trim().as_bytes())
    };

    let claims = Claims {
        name: Some(name),
        sub,
        exp: Utc::now().timestamp() + duration,
        jti: None,
        scope,
        prefixes,
        apps: vec![],
        repos,
        branches,
        token_type: Some(token_type),
    };

    if verbose {
        println!("Token: {}", serde_json::to_string(&claims).unwrap());
    }

    match encode(&Header::default(), &claims, &key) {
        Ok(token) => {
            println!("{token}");
            0
        }
        Err(err) => {
            eprintln!("Error encoding token: {err}");
            1
        }
    }
}
