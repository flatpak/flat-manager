use actix::prelude::*;
use actix_web::{self, middleware};
use actix_web::{App, http::Method, HttpRequest, fs::NamedFile};
use models::DbExecutor;
use std::path::PathBuf;
use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;
use std::io;
use std;
use std::process::{Command};
use serde;
use serde_json;
use serde::Deserialize;
use base64;

use errors::ApiError;
use api;
use tokens::{TokenParser};
use jobs::{JobQueue};
use actix_web::dev::FromParam;

fn as_base64<S>(key: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer
{
    serializer.serialize_str(&base64::encode(key))
}

fn from_base64<'de,D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where D: serde::Deserializer<'de>
{
    use serde::de::Error;
    String::deserialize(deserializer)
        .and_then(|string| base64::decode(&string).map_err(|err| Error::custom(err.to_string())))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // "build", "build/N"
    pub scope: Vec<String>, // "build", "upload" "publish"
    pub prefixes: Vec<String>, // [''] => all, ['org.foo'] => org.foo + org.foo.bar (but not org.foobar)
    pub repos: Vec<String>, // list of repo names or a '' for match all
    pub name: String, // for debug/logs only
    pub exp: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct SubsetConfig {
    pub collection_id: String,
    pub base_url: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RepoConfig {
    #[serde(skip)]
    pub name: String,
    pub suggested_repo_name: Option<String>,
    pub path: PathBuf,
    pub collection_id: Option<String>,
    pub gpg_key: Option<String>,
    #[serde(skip)]
    pub gpg_key_content: Option<String>,
    pub base_url: Option<String>,
    pub runtime_repo_url: Option<String>,
    pub subsets: HashMap<String, SubsetConfig>,
    pub post_publish_script: Option<String>,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> i32 {
    8080
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct Config {
    pub database_url: String,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: i32,
    #[serde(default)]
    pub base_url: String,
    pub gpg_homedir: Option<String>,
    #[serde(serialize_with = "as_base64", deserialize_with = "from_base64")]
    pub secret: Vec<u8>,
    pub repos: HashMap<String, RepoConfig>,
    pub build_repo_base: PathBuf,
    pub build_gpg_key: Option<String>,
    #[serde(skip)]
    pub build_gpg_key_content: Option<String>,
}

impl RepoConfig {
    pub fn get_base_url(&self, config: &Config) -> String {
        match &self.base_url {
            Some(base_url) => base_url.clone(),
            None => format!("{}/repo/{}", config.base_url, self.name)
        }
    }
}

impl Config {
    pub fn get_repoconfig(&self, name: &str) -> Result<&RepoConfig, ApiError> {
        self.repos.get(name).ok_or_else (|| ApiError::BadRequest("No such repo".to_string()))
    }
}


fn load_gpg_key (maybe_gpg_homedir: &Option<String>, maybe_gpg_key: &Option<String>) -> io::Result<Option<String>> {
    match maybe_gpg_key {
        Some(gpg_key) => {
            let mut cmd = Command::new("gpg2");
            if let Some(gpg_homedir) = maybe_gpg_homedir {
                cmd.arg(&format!("--homedir={}", gpg_homedir));
            }
            cmd
                .arg("--export")
                .arg(gpg_key);

            let output = cmd.output()?;
            if output.status.success() {
                Ok(Some(base64::encode(&output.stdout)))
            } else {
                Err(io::Error::new(io::ErrorKind::Other, "gpg2 --export failed"))
            }
        },
        None => Ok(None),
    }
}


pub fn load_config<P: AsRef<Path>>(path: P) -> io::Result<Config> {
    let config_contents = std::fs::read_to_string(path)?;
    let mut config_data: Config = serde_json::from_str(&config_contents).map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

    config_data.build_gpg_key_content = load_gpg_key (&config_data.gpg_homedir, &config_data.build_gpg_key)?;
    for (reponame, repoconfig) in &mut config_data.repos {
        repoconfig.name = reponame.clone();
        repoconfig.gpg_key_content = load_gpg_key (&config_data.gpg_homedir, &config_data.build_gpg_key)?;
    }

    if config_data.base_url == "" {
        config_data.base_url = format!("http://{}:{}", config_data.host, config_data.port)
    }

    Ok(config_data)
}

#[derive(Clone)]
pub struct AppState {
    pub db: Addr<DbExecutor>,
    pub config: Arc<Config>,
    pub job_queue: Addr<JobQueue>,
}

fn handle_build_repo(req: &HttpRequest<AppState>) -> actix_web::Result<NamedFile> {
    let tail: String = req.match_info().query("tail")?;
    let id: String = req.match_info().query("id")?;
    let state = req.state();

    // Strip out any "../.." or other unsafe things
    let relpath = PathBuf::from_param(tail.trim_left_matches('/'))?;
    // The id won't have slashes, but it could have ".." or some other unsafe thing
    let safe_id = PathBuf::from_param(&id)?;
    let path = Path::new(&state.config.build_repo_base).join(&safe_id).join(&relpath);
    NamedFile::open(path).or_else(|_e| {
        let fallback_path = Path::new(&state.config.build_repo_base).join(&safe_id).join("parent").join(&relpath);
        Ok(NamedFile::open(fallback_path)?)
    })
}

fn handle_repo(req: &HttpRequest<AppState>) -> actix_web::Result<NamedFile> {
    let tail: String = req.match_info().query("tail")?;
    let repo: String = req.match_info().query("repo")?;
    let state = req.state();
    let repoconfig = state.config.get_repoconfig(&repo)?;
    // Strip out any "../.." or other unsafe things
    let relpath = PathBuf::from_param(tail.trim_left_matches('/'))?;
    let path = Path::new(&repoconfig.path).join(relpath);
    Ok(NamedFile::open(path)?)
}

pub fn create_app(
    db: Addr<DbExecutor>,
    config: &Arc<Config>,
    job_queue: Addr<JobQueue>,
) -> App<AppState> {
    let state = AppState {
        db: db.clone(),
        job_queue: job_queue.clone(),
        config: config.clone(),
    };

    App::with_state(state)
        .middleware(middleware::Logger::default())
        .scope("/api/v1", |scope| {
            scope
                .middleware(TokenParser::new(&config.secret))
                .resource("/token_subset", |r| r.method(Method::POST).with(api::token_subset))
                .resource("/job/{id}", |r| { r.name("show_job"); r.method(Method::POST).with(api::get_job)})
                .resource("/build", |r| { r.method(Method::POST).with(api::create_build);
                                          r.method(Method::GET).with(api::builds) })
                .resource("/build/{id}", |r| { r.name("show_build"); r.method(Method::GET).with(api::get_build) })
                .resource("/build/{id}/build_ref", |r| r.method(Method::POST).with(api::create_build_ref))
                .resource("/build/{id}/build_ref/{ref_id}", |r| { r.name("show_build_ref"); r.method(Method::GET).with(api::get_build_ref) })
                .resource("/build/{id}/missing_objects", |r| r.method(Method::GET).with(api::missing_objects))
                .resource("/build/{id}/add_extra_ids", |r| r.method(Method::POST).with(api::add_extra_ids))
                .resource("/build/{id}/upload", |r| r.method(Method::POST).with(api::upload))
                .resource("/build/{id}/commit", |r| { r.name("show_commit_job");
                                                      r.method(Method::POST).with(api::commit);
                                                      r.method(Method::GET).with(api::get_commit_job) })
                .resource("/build/{id}/publish", |r| { r.name("show_publish_job");
                                                      r.method(Method::POST).with(api::publish);
                                                       r.method(Method::GET).with(api::get_publish_job) })
                .resource("/build/{id}/purge", |r| { r.method(Method::POST).with(api::purge) })
        })
        .scope("/build-repo/{id}", |scope| {
            scope.handler("/", |req: &HttpRequest<AppState>| handle_build_repo(req))
        })
        .handler("/repo/{repo}/", |req: &HttpRequest<AppState>| handle_repo(req))
        .resource("/status", |r| r.method(Method::GET).with(api::status))
        .resource("/status/{id}", |r| r.method(Method::GET).with(api::job_status))
}
