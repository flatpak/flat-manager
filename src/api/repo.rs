use actix::prelude::*;
use actix_files::NamedFile;
use actix_web::error::{ErrorBadRequest, ErrorNotFound};
use actix_web::http::header::{HeaderValue, CACHE_CONTROL};
use actix_web::web::Data;
use actix_web::Responder;
use actix_web::{self, HttpRequest, HttpResponse};
use futures3::TryFutureExt;
use serde::Deserialize;
use std::ffi::OsStr;
use std::path::Path;
use std::path::PathBuf;

use crate::config::{Config, RepoConfig};
use crate::db::Db;
use crate::errors::ApiError;
use crate::ostree;
use crate::tokens::{ClaimsScope, ClaimsValidator};

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

#[derive(Deserialize)]
pub struct BuildRepoParams {
    id: i32,
    tail: String,
}

pub fn handle_build_repo(
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
            ApiError::InternalServerError(format!("No ref binding for commit {path:?}"))
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

pub fn handle_repo(
    config: Data<Config>,
    req: HttpRequest,
) -> Result<HttpResponse, actix_web::Error> {
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

struct RepoHeadersData {
    nocache: bool,
}

pub fn apply_extra_headers(resp: &mut actix_web::dev::ServiceResponse) {
    let mut nocache = false;
    if let Some(data) = resp.request().extensions().get::<RepoHeadersData>() {
        nocache = data.nocache;
    }
    if nocache {
        resp.headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
}
