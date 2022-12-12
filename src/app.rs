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
use std::ffi::OsStr;
use std::fmt::Display;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use crate::api;
use crate::config::{Config, RepoConfig};
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

    #[serde(default)]
    pub scope: Vec<ClaimsScope>,
    #[serde(default)]
    pub prefixes: Vec<String>, // [''] => all, ['org.foo'] => org.foo + org.foo.bar (but not org.foobar)
    #[serde(default)]
    pub apps: Vec<String>, // like prefixes, but only exact matches
    #[serde(default)]
    pub repos: Vec<String>, // list of repo names or a '' for match all
    pub name: Option<String>, // for debug/logs only
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
