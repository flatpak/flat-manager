use chrono::{Duration, Utc};
use jwt::{encode, EncodingKey, Header};
use std::fs;
use std::io;
use std::io::prelude::*;
use std::process;

use argparse::{ArgumentParser, List, Store, StoreOption, StoreTrue};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    scope: Vec<String>,
    name: String,
    prefixes: Vec<String>,
    repos: Vec<String>,
    exp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    builder_id: Option<String>,
}

fn read_secret(filename: String) -> io::Result<String> {
    let mut contents = String::new();
    if filename == "-" {
        io::stdin().read_to_string(&mut contents)?;
    } else {
        let mut file = fs::File::open(filename)?;
        file.read_to_string(&mut contents)?;
    }
    Ok(contents)
}

fn main() {
    let mut verbose = false;
    let mut base64 = false;
    let mut name = "default".to_string();
    let mut sub = "build".to_string();
    let mut secret: Option<String> = None;
    let mut secret_file: Option<String> = None;
    let mut duration: i64 = Duration::days(365).num_seconds();
    let mut scope: Vec<String> = vec![];
    let mut prefixes: Vec<String> = vec![];
    let mut repos: Vec<String> = vec![];
    let mut builder_id: Option<String> = None;

    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Generate token for flat-manager.");
        ap.refer(&mut verbose)
            .add_option(&["-v", "--verbose"], StoreTrue, "Be verbose");
        ap.refer(&mut name)
            .add_option(&["--name"], Store, "Name for the token");
        ap.refer(&mut sub)
            .add_option(&["--sub"], Store, "Subject (default: build)");
        ap.refer(&mut scope).add_option(
            &["--scope"],
            List,
            "Add scope (default if none: [build, upload, download, publish, jobs]",
        );
        ap.refer(&mut prefixes).add_option(
            &["--prefix"],
            List,
            "Add ref prefix (default if none: ['']",
        );
        ap.refer(&mut repos)
            .add_option(&["--repo"], List, "Add repo (default if none: ['']");
        ap.refer(&mut base64)
            .add_option(&["--base64"], StoreTrue, "The secret is base64 encoded");
        ap.refer(&mut secret).add_option(
            &["--secret"],
            StoreOption,
            "Secret used to encode the token",
        );
        ap.refer(&mut secret_file).add_option(
            &["--secret-file"],
            StoreOption,
            "Load secret from file (or - for stdin)",
        );
        ap.refer(&mut duration).add_option(
            &["--duration"],
            Store,
            "Duration for key in seconds (default 1 year)",
        );
        ap.refer(&mut builder_id).add_option(
            &["--builder-id"],
            StoreOption,
            "Builder ID (default: none)",
        );
        ap.parse_args_or_exit();
    }

    let secret_contents;

    if scope.is_empty() {
        scope = vec![
            "build".to_string(),
            "upload".to_string(),
            "download".to_string(),
            "publish".to_string(),
            "jobs".to_string(),
        ];
    }

    if prefixes.is_empty() {
        prefixes = vec!["".to_string()];
    }

    if repos.is_empty() {
        repos = vec!["".to_string()];
    }

    if let Some(s) = secret {
        secret_contents = s;
    } else if let Some(filename) = secret_file {
        match read_secret(filename) {
            Ok(contents) => secret_contents = contents,
            Err(e) => {
                eprintln!("Error reading secrets: {e}");
                process::exit(1)
            }
        }
    } else {
        eprintln!("No secret specified, use --secret or --secret-file");
        process::exit(1)
    }

    let key = if base64 {
        EncodingKey::from_base64_secret(secret_contents.trim()).unwrap()
    } else {
        EncodingKey::from_secret(secret_contents.trim().as_bytes())
    };

    let claims = Claims {
        sub,
        scope,
        prefixes,
        repos,
        name: name.clone(),
        exp: Utc::now().timestamp() + duration,
        builder_id,
    };

    if verbose {
        println!("Token: {}", serde_json::to_string(&claims).unwrap());
    }

    let token = encode(&Header::default(), &claims, &key).unwrap();
    println!("{token}");
}
