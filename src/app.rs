use actix::prelude::*;
use actix_files::NamedFile;
use actix_service::Service;
use actix_web::dev::Server;
use actix_web::error::{ErrorBadRequest, ErrorNotFound};
use actix_web::http::header::{HeaderValue, CACHE_CONTROL};
use actix_web::web::Data;
use actix_web::Responder;
use actix_web::{self, http, middleware, web, App, HttpRequest, HttpResponse, HttpServer};
use futures3::TryFutureExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::Display;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use crate::api;
use crate::db::Db;
use crate::deltas::DeltaGenerator;
use crate::errors::ApiError;
use crate::jobs::JobQueue;
use crate::logger::Logger;
use crate::ostree;
use crate::tokens::{ClaimsValidator, TokenParser};
use crate::Pool;

// Ensure we strip out .. and other risky things to avoid escaping out of the base dir
fn canonicalize_path(path: &str) -> Result<PathBuf, actix_web::Error> {
    let mut buf = PathBuf::new();

    for segment in path.split('/') {
        if segment == ".." {
            if !buf.pop() {
                return Err(ErrorBadRequest("Path segments goes outside parent"));
            }
        } else if segment.starts_with('.') {
            return Err(ErrorBadRequest("Path segments starts with ."));
        } else if segment.starts_with('*') {
            return Err(ErrorBadRequest("Path segments starts with *"));
        } else if segment.ends_with(':') {
            return Err(ErrorBadRequest("Path segments ends with :"));
        } else if segment.ends_with('>') {
            return Err(ErrorBadRequest("Path segments ends with >"));
        } else if segment.ends_with('<') {
            return Err(ErrorBadRequest("Path segments end with <"));
        } else if segment.is_empty() {
            continue;
        } else if cfg!(windows) && segment.contains('\\') {
            return Err(ErrorBadRequest("Path segments contains with \\"));
        } else {
            buf.push(segment)
        }
    }

    Ok(buf)
}

fn from_base64<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    String::deserialize(deserializer)
        .and_then(|string| base64::decode(string).map_err(|err| Error::custom(err.to_string())))
}

fn from_opt_base64<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    String::deserialize(deserializer)
        .and_then(|string| base64::decode(string).map_err(|err| Error::custom(err.to_string())))
        .map(Some)
}

fn match_glob(glob: &str, s: &str) -> bool {
    if let Some(index) = glob.find('*') {
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
        if s_chars.next().is_none() {
            return false;
        }

        loop {
            if match_glob(glob_after_star, s_chars.as_str()) {
                return true;
            }
            if s_chars.next().is_none() {
                break;
            }
        }
        false
    } else {
        glob == s
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClaimsScope {
    // Permission to list all jobs in the system. Should not be given to untrusted parties.
    Jobs,
    // Permission to create, list, and purge builds, to get a build's jobs, and to commit uploaded files to the build.
    Build,
    // Permission to upload files and refs to builds.
    Upload,
    // Permission to publish builds.
    Publish,
    // Permission to upload deltas for a repo. Should not be given to untrusted parties.
    Generate,
    // Permission to list builds and to download a build repo.
    Download,
    #[serde(other)]
    Unknown,
}

impl Display for ClaimsScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_ascii_lowercase())
    }
}

/* Claims are used in two forms, one for API calls, and one for
 * general repo access, the second one is simpler and just uses scope
 * for the allowed ids, and sub means the user doing the access (which
 * is not verified). */
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // "build", "build/N", or user id for repo tokens
    pub exp: i64,

    // Below are optional, and not used for repo tokens
    #[serde(default)]
    pub scope: Vec<ClaimsScope>,
    #[serde(default)]
    pub prefixes: Vec<String>, // [''] => all, ['org.foo'] => org.foo + org.foo.bar (but not org.foobar)
    #[serde(default)]
    pub repos: Vec<String>, // list of repo names or a '' for match all
    pub name: Option<String>, // for debug/logs only
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct DeltaConfig {
    pub id: Vec<String>,
    #[serde(default)]
    pub arch: Vec<String>,
    pub depth: u32,
}

impl DeltaConfig {
    pub fn matches_ref(&self, id: &str, arch: &str) -> bool {
        self.id.iter().any(|id_glob| match_glob(id_glob, id))
            && (self.arch.is_empty()
                || self
                    .arch
                    .iter()
                    .any(|arch_glob| match_glob(arch_glob, arch)))
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct SubsetConfig {
    pub collection_id: String,
    pub base_url: Option<String>,
}

fn default_depth() -> u32 {
    5
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RepoConfig {
    #[serde(skip)]
    pub name: String,
    pub suggested_repo_name: Option<String>,
    pub path: PathBuf,
    #[serde(default)]
    pub default_token_type: i32,
    #[serde(default)]
    pub require_auth_for_token_types: Vec<i32>,
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

#[derive(Deserialize, Debug, Clone)]
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
    #[serde(deserialize_with = "from_base64")]
    pub secret: Vec<u8>,
    #[serde(default, deserialize_with = "from_opt_base64")]
    pub repo_secret: Option<Vec<u8>>,
    pub repos: HashMap<String, RepoConfig>,
    pub build_repo_base: PathBuf,
    pub build_gpg_key: Option<String>,
    #[serde(skip)]
    pub build_gpg_key_content: Option<String>,
    #[serde(default)]
    pub delay_update_secs: u64,
    #[serde(default = "default_numcpu")]
    pub local_delta_threads: u32,
    pub storefront_info_endpoint: Option<String>,
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
            None => format!("{}/repo/{}", config.base_url, self.name),
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
            let parts: Vec<&str> = ref_name.split('/').collect();
            if parts.len() == 4 {
                let id = parts[1];
                let arch = parts[2];
                for dc in &self.deltas {
                    if dc.matches_ref(id, arch) {
                        return dc.depth;
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
        self.repos
            .get(name)
            .ok_or_else(|| ApiError::BadRequest("No such repo".to_string()))
    }

    pub fn get_repoconfig_from_path(&self, path: &Path) -> Result<&RepoConfig, ApiError> {
        for (repo, config) in self.repos.iter() {
            if path.starts_with(repo) {
                return Ok(config);
            }
        }
        Err(ApiError::BadRequest("No such repo".to_string()))
    }
}

fn load_gpg_key(
    maybe_gpg_homedir: &Option<String>,
    maybe_gpg_key: &Option<String>,
) -> io::Result<Option<String>> {
    match maybe_gpg_key {
        Some(gpg_key) => {
            let mut cmd = Command::new("gpg2");
            if let Some(gpg_homedir) = maybe_gpg_homedir {
                cmd.arg(&format!("--homedir={}", gpg_homedir));
            }
            cmd.arg("--export").arg(gpg_key);

            let output = cmd.output()?;
            if output.status.success() {
                Ok(Some(base64::encode(&output.stdout)))
            } else {
                Err(io::Error::new(io::ErrorKind::Other, "gpg2 --export failed"))
            }
        }
        None => Ok(None),
    }
}

pub fn load_config<P: AsRef<Path>>(path: P) -> io::Result<Config> {
    let config_contents = std::fs::read_to_string(path)?;
    let mut config_data: Config = serde_json::from_str(&config_contents)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

    config_data.build_gpg_key_content =
        load_gpg_key(&config_data.gpg_homedir, &config_data.build_gpg_key)?;
    for (reponame, repoconfig) in &mut config_data.repos {
        repoconfig.name = reponame.clone();
        repoconfig.gpg_key_content =
            load_gpg_key(&config_data.gpg_homedir, &config_data.build_gpg_key)?;
    }

    if config_data.base_url.is_empty() {
        config_data.base_url = format!("http://{}:{}", config_data.host, config_data.port)
    }

    Ok(config_data)
}

#[derive(Deserialize)]
pub struct BuildRepoParams {
    id: i32,
    tail: String,
}

fn handle_build_repo(
    config: Data<Config>,
    params: actix_web::web::Path<BuildRepoParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = actix_web::Error> {
    Box::pin(handle_build_repo_async(config, params, db, req)).compat()
}

async fn handle_build_repo_async(
    config: Data<Config>,
    params: actix_web::web::Path<BuildRepoParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, actix_web::Error> {
    let build = db.lookup_build(params.id).await?;
    if !build.public_download {
        req.has_token_repo(&build.repo)?;
        req.has_token_claims(&format!("build/{}", build.id), ClaimsScope::Download)?;
        if let Some(app_id) = build.app_id {
            req.has_token_prefix(&app_id)
                /* Hide the app ID of the build, since we can't access it */
                .map_err(|_| {
                    ApiError::NotEnoughPermissions(
                        "Build's app ID not matching prefix in token".to_string(),
                    )
                })?;
        }
    }

    let relpath = canonicalize_path(params.tail.trim_start_matches('/'))?;
    let realid = canonicalize_path(&params.id.to_string())?;
    let path = Path::new(&config.build_repo_base)
        .join(realid)
        .join(&relpath);
    if path.is_dir() {
        return Err(ErrorNotFound("Ignoring directory"));
    }

    NamedFile::open(path)
        .or_else(|_e| {
            let fallback_path = Path::new(&config.build_repo_base)
                .join(params.id.to_string())
                .join("parent")
                .join(&relpath);
            if fallback_path.is_dir() {
                Err(ErrorNotFound("Ignoring directory"))
            } else {
                NamedFile::open(fallback_path).map_err(|e| e.into())
            }
        })?
        .respond_to(&req)
}

fn get_commit_for_file(path: &Path) -> Option<ostree::OstreeCommit> {
    if path.file_name() == Some(OsStr::new("superblock")) {
        if let Ok(superblock) = ostree::load_delta_superblock_file(path) {
            return Some(superblock.commit);
        }
    }

    if path.extension() == Some(OsStr::new("commit")) {
        if let Ok(commit) = ostree::load_commit_file(path) {
            return Some(commit);
        }
    }
    None
}

struct RepoHeadersData {
    nocache: bool,
}

fn apply_extra_headers(resp: &mut actix_web::dev::ServiceResponse) {
    let mut nocache = false;
    if let Some(data) = resp.request().extensions().get::<RepoHeadersData>() {
        nocache = data.nocache;
    }
    if nocache {
        resp.headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
}

fn verify_repo_token(
    req: &HttpRequest,
    commit: ostree::OstreeCommit,
    repoconfig: &RepoConfig,
    path: &Path,
) -> Result<(), ApiError> {
    let token_type = commit
        .metadata
        .get("xa.token-type")
        .map(|v| v.as_i32_le().unwrap_or(0))
        .unwrap_or(repoconfig.default_token_type);
    if !repoconfig
        .require_auth_for_token_types
        .contains(&token_type)
    {
        return Ok(());
    }

    req.extensions_mut()
        .insert(RepoHeadersData { nocache: true });

    let commit_refs = commit
        .metadata
        .get("ostree.ref-binding")
        .ok_or_else(|| {
            ApiError::InternalServerError(format!("No ref binding for commit {:?}", path))
        })?
        .as_string_vec()?;
    let mut result = Ok(());
    // If there are any normal flatpak refs, the token must match at least one:
    for commit_ref in commit_refs {
        let ref_parts: Vec<&str> = commit_ref.split('/').collect();
        if (ref_parts[0] == "app" || ref_parts[0] == "runtime") && ref_parts.len() > 2 {
            result = req.has_token_prefix(ref_parts[1]);
            if result.is_ok() {
                break; // Early exit, we have a match
            }
        }
    }
    result
}

fn handle_repo(config: Data<Config>, req: HttpRequest) -> Result<HttpResponse, actix_web::Error> {
    let tail = req.match_info().query("tail");
    let tailpath = canonicalize_path(tail.trim_start_matches('/'))?;
    let repoconfig = config.get_repoconfig_from_path(&tailpath)?;

    let namepath = Path::new(&repoconfig.name);
    let relpath = tailpath
        .strip_prefix(namepath)
        .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
    let path = Path::new(&repoconfig.path).join(relpath);
    if path.is_dir() {
        return Err(ErrorNotFound("Ignoring directory"));
    }

    if let Some(commit) = get_commit_for_file(&path) {
        verify_repo_token(&req, commit, repoconfig, &path)?;
    }

    NamedFile::open(path)
        .or_else(|e| {
            // Was this a delta, if so check the deltas queued for deletion
            if relpath.starts_with("deltas") {
                let tmp_path = Path::new(&repoconfig.path).join("tmp").join(relpath);
                if tmp_path.is_dir() {
                    Err(ErrorNotFound("Ignoring directory"))
                } else {
                    NamedFile::open(tmp_path).map_err(|e| e.into())
                }
            } else {
                Err(e).map_err(|e| e.into())
            }
        })?
        .respond_to(&req)
}

pub fn create_app(
    pool: Pool,
    config: &Arc<Config>,
    job_queue: Addr<JobQueue>,
    delta_generator: Addr<DeltaGenerator>,
) -> Server {
    let c = config.clone();
    let secret = config.secret.clone();
    let repo_secret = config
        .repo_secret
        .as_ref()
        .unwrap_or_else(|| config.secret.as_ref())
        .clone();
    let http_server = HttpServer::new(move || {
        App::new()
            .data(job_queue.clone())
            .data(delta_generator.clone())
            .register_data(Data::new((*c).clone()))
            .data(Db(pool.clone()))
            .wrap(Logger::default())
            .wrap(middleware::Compress::new(
                http::header::ContentEncoding::Identity,
            ))
            .service(
                web::scope("/api/v1")
                    .wrap(TokenParser::new(&secret))
                    .service(
                        web::resource("/token_subset").route(web::post().to(api::token_subset)),
                    )
                    .service(
                        web::resource("/job/{id}")
                            .name("show_job")
                            .route(web::get().to_async(api::get_job)),
                    )
                    .service(
                        web::resource("/build")
                            .route(web::post().to_async(api::create_build))
                            .route(web::get().to_async(api::builds)),
                    )
                    .service(
                        web::resource("/build/{id}")
                            .name("show_build")
                            .route(web::get().to_async(api::get_build)),
                    )
                    .service(
                        web::resource("/build/{id}/build_ref")
                            .route(web::post().to_async(api::create_build_ref)),
                    )
                    .service(
                        web::resource("/build/{id}/build_ref/{ref_id}")
                            .name("show_build_ref")
                            .route(web::get().to_async(api::get_build_ref)),
                    )
                    .service(
                        web::resource("/build/{id}/missing_objects")
                            .data(web::JsonConfig::default().limit(1024 * 1024 * 10))
                            .route(web::get().to_async(api::missing_objects)),
                    )
                    .service(
                        web::resource("/build/{id}/add_extra_ids")
                            .route(web::post().to_async(api::add_extra_ids)),
                    )
                    .service(
                        web::resource("/build/{id}/upload")
                            .route(web::post().to_async(api::upload)),
                    )
                    .service(
                        web::resource("/build/{id}/commit")
                            .name("show_commit_job")
                            .route(web::post().to_async(api::commit))
                            .route(web::get().to_async(api::get_commit_job)),
                    )
                    .service(
                        web::resource("/build/{id}/publish")
                            .name("show_publish_job")
                            .route(web::post().to_async(api::publish))
                            .route(web::get().to_async(api::get_publish_job)),
                    )
                    .service(
                        web::resource("/build/{id}/purge").route(web::post().to_async(api::purge)),
                    )
                    .service(web::resource("/delta/worker").route(web::get().to(api::ws_delta)))
                    .service(
                        web::resource("/delta/upload/{repo}")
                            .route(web::post().to_async(api::delta_upload)),
                    ),
            )
            .service(
                web::scope("/repo")
                    .wrap(TokenParser::optional(&repo_secret))
                    .wrap_fn(|req, srv| {
                        srv.call(req).map(|mut resp| {
                            apply_extra_headers(&mut resp);
                            resp
                        })
                    })
                    .service(
                        web::resource("/{tail:.*}")
                            .name("repo")
                            .route(web::get().to(handle_repo))
                            .route(web::head().to(handle_repo))
                            .to(HttpResponse::MethodNotAllowed),
                    ),
            )
            .service(
                web::resource("/build-repo/{id}/{tail:.*}")
                    .wrap(TokenParser::optional(&secret))
                    .route(web::get().to_async(handle_build_repo))
                    .route(web::head().to_async(handle_build_repo))
                    .to(HttpResponse::MethodNotAllowed),
            )
            .service(web::resource("/status").route(web::get().to_async(api::status)))
            .service(web::resource("/status/{id}").route(web::get().to_async(api::job_status)))
    });

    let bind_to = format!("{}:{}", config.host, config.port);
    let server = http_server
        .bind(&bind_to)
        .unwrap()
        .disable_signals()
        .start();

    log::info!("Started http server: {}", bind_to);

    server
}
