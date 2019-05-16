use actix::prelude::*;
use actix_web::{self, App, http::Method, HttpRequest, HttpResponse,
                fs::NamedFile, error::ErrorNotFound};
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
use num_cpus;

use errors::ApiError;
use api;
use deltas::DeltaGenerator;
use tokens::{TokenParser};
use jobs::{JobQueue};
use actix_web::dev::FromParam;
use logger::Logger;

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


fn match_glob(glob: &str, s: &str) -> bool
{
    if let Some(index) = glob.find("*") {
        let (glob_start, glob_rest) = glob.split_at(index);
        if !s.starts_with(glob_start) {
            return false;
        }
        let (_, s_rest) = s.split_at(index);

        let mut glob_chars = glob_rest.chars();
        let mut s_chars = s_rest.chars();

        /* Consume '*' */
        glob_chars.next();
        let glob_after_star = glob_chars.as_str();

        /* Consume at least one, fail if none */
        if s_chars.next() == None {
            return false
        }

        loop {
            if match_glob(glob_after_star, s_chars.as_str()) {
                return true
            }
            if s_chars.next() == None {
                break
            }
        }
        return false
    } else {
        return glob == s
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_glob() {
        assert!(match_glob("foo", "foo"));
        assert!(!match_glob("foo", "fooo"));
        assert!(!match_glob("fooo", "foo"));
        assert!(!match_glob("foo", "br"));
        assert!(!match_glob("foo", "bar"));
        assert!(!match_glob("foo", "baar"));
        assert!(match_glob("*", "foo"));
        assert!(!match_glob("*foo", "foo"));
        assert!(match_glob("fo*", "foo"));
        assert!(match_glob("foo*", "foobar"));
        assert!(!match_glob("foo*gazonk", "foogazonk"));
        assert!(match_glob("foo*gazonk", "foobgazonk"));
        assert!(match_glob("foo*gazonk", "foobagazonk"));
        assert!(match_glob("foo*gazonk", "foobargazonk"));
        assert!(!match_glob("foo*gazonk", "foobargazonkk"));
        assert!(!match_glob("foo*gazonk*test", "foobargazonk"));
        assert!(!match_glob("foo*gazonk*test", "foobargazonktest"));
        assert!(match_glob("foo*gazonk*test", "foobargazonkWOOtest"));
        assert!(!match_glob("foo*gazonk*test", "foobargazonkWOOtestXX"));
    }
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
pub struct DeltaConfig {
    pub id: Vec<String>,
    #[serde(default)]
    pub arch: Vec<String>,
    pub depth: u32,
}

impl DeltaConfig {
    pub fn matches_ref(&self, id: &str, arch: &str) -> bool {
        self.id.iter().any(|id_glob| match_glob(id_glob, id)) &&
            (self.arch.is_empty() ||
             self.arch.iter().any(|arch_glob| match_glob(arch_glob, arch)))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct SubsetConfig {
    pub collection_id: String,
    pub base_url: Option<String>,
}

fn default_depth() -> u32 {
    5
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RepoConfig {
    #[serde(skip)]
    pub name: String,
    pub suggested_repo_name: Option<String>,
    pub path: PathBuf,
    #[serde(default)]
    pub private: bool,
    pub collection_id: Option<String>,
    #[serde(default)]
    pub deploy_collection_id: bool,
    pub gpg_key: Option<String>,
    #[serde(skip)]
    pub gpg_key_content: Option<String>,
    pub base_url: Option<String>,
    pub runtime_repo_url: Option<String>,
    pub subsets: HashMap<String, SubsetConfig>,
    pub post_publish_script: Option<String>,
    #[serde(default)]
    pub deltas: Vec<DeltaConfig>,
    #[serde(default = "default_depth")]
    pub appstream_delta_depth: u32,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> i32 {
    8080
}

fn default_numcpu() -> u32 {
    num_cpus::get() as u32
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
    #[serde(default)]
    pub delay_update_secs: u64,
    #[serde(default = "default_numcpu")]
    pub local_delta_threads: u32,
}

impl RepoConfig {
    pub fn get_abs_repo_path(&self) -> PathBuf {
        let mut repo_path = std::env::current_dir().unwrap_or_else(|_e| PathBuf::from("/"));

        repo_path.push(&self.path);
        repo_path
    }

    pub fn get_base_url(&self, config: &Config) -> String {
        match &self.base_url {
            Some(base_url) => base_url.clone(),
            None => format!("{}/repo/{}", config.base_url, self.name)
        }
    }

    pub fn get_delta_depth_for_ref(&self, ref_name: &str) -> u32 {
        if ref_name == "ostree-metadata" {
            0
        } else if ref_name.starts_with("appstream/") {
            1 /* The old appstream format doesn't delta well, so not need for depth */
        } else if ref_name.starts_with("appstream2/") {
            self.appstream_delta_depth /* This updates often, so lets have some more */
        } else if ref_name.starts_with("app/") || ref_name.starts_with("runtime/") {
            let parts : Vec<&str> = ref_name.split("/").collect();
            if parts.len() == 4 {
                let id = parts[1];
                let arch = parts[2];
                for dc in &self.deltas {
                    if dc.matches_ref(id, arch) {
                        return dc.depth
                    }
                }
            };
            0
        } else {
            0 /* weird ref? */
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
    pub delta_generator: Addr<DeltaGenerator>,
}

fn handle_build_repo(req: &HttpRequest<AppState>) -> actix_web::Result<NamedFile> {
    let tail: String = req.match_info().query("tail")?;
    let id: String = req.match_info().query("id")?;
    let state = req.state();

    // Strip out any "../.." or other unsafe things
    let relpath = PathBuf::from_param(tail.trim_start_matches('/'))?;
    // The id won't have slashes, but it could have ".." or some other unsafe thing
    let safe_id = PathBuf::from_param(&id)?;
    let path = Path::new(&state.config.build_repo_base).join(&safe_id).join(&relpath);
    if path.is_dir() {
        return Err(ErrorNotFound("Ignoring directory"));
    }
    NamedFile::open(path).or_else(|_e| {
        let fallback_path = Path::new(&state.config.build_repo_base).join(&safe_id).join("parent").join(&relpath);
        if fallback_path.is_dir() {
            Err(ErrorNotFound("Ignoring directory"))
        } else {
            Ok(NamedFile::open(fallback_path)?)
        }
    })
}

fn handle_repo(req: &HttpRequest<AppState>) -> actix_web::Result<NamedFile> {
    let tail: String = req.match_info().query("tail")?;
    let repo: String = req.match_info().query("repo")?;
    let state = req.state();
    let repoconfig = state.config.get_repoconfig(&repo)?;
    // Strip out any "../.." or other unsafe things
    let relpath = PathBuf::from_param(tail.trim_start_matches('/'))?;
    let path = Path::new(&repoconfig.path).join(&relpath);
    if path.is_dir() {
        return Err(ErrorNotFound("Ignoring directory"));
    }
    match NamedFile::open(path) {
        Ok(file) => Ok(file),
        Err(e) => {
            // Was this a delta, if so check the deltas queued for deletion
            if relpath.starts_with("deltas") {
                let tmp_path = Path::new(&repoconfig.path).join("tmp").join(&relpath);
                if tmp_path.is_dir() {
                    Err(ErrorNotFound("Ignoring directory"))
                } else {
                    Ok(NamedFile::open(tmp_path)?)
                }
            } else {
                Err(e)?
            }
        },
    }
}

pub fn create_app(
    db: Addr<DbExecutor>,
    config: &Arc<Config>,
    job_queue: Addr<JobQueue>,
    delta_generator: &Addr<DeltaGenerator>,
) -> App<AppState> {
    let state = AppState {
        db: db.clone(),
        job_queue: job_queue.clone(),
        config: config.clone(),
        delta_generator: delta_generator.clone(),
    };

    App::with_state(state)
        .middleware(Logger::default())
        .scope("/api/v1", |scope| {
            scope
                .middleware(TokenParser::new(&config.secret))
                .resource("/token_subset", |r| r.method(Method::POST).with(api::token_subset))
                .resource("/job/{id}", |r| { r.name("show_job"); r.method(Method::GET).with(api::get_job)})
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
                .resource("/delta/worker", |r| r.route().f(api::ws_delta))
                .resource("/delta/upload/{repo}", |r| r.method(Method::POST).with(api::delta_upload))
        })
        .resource("/build-repo/{id}/{tail:.*}", |r| {
            r.get().f(handle_build_repo);
            r.head().f(handle_build_repo);
            r.f(|_| HttpResponse::MethodNotAllowed())
        })
        .scope("/repo", |scope| {
            scope
                .middleware(TokenParser::new(&config.secret))
                .resource("/{repo}/{tail:.*}", |r| {
                    r.name("repo");
                    r.get().f(handle_repo);
                    r.head().f(handle_repo);
                    r.f(|_| HttpResponse::MethodNotAllowed())
                })
        })
        .resource("/status", |r| r.method(Method::GET).with(api::status))
        .resource("/status/{id}", |r| r.method(Method::GET).with(api::job_status))
}
