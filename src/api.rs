use actix::prelude::*;
use actix_multipart::Multipart;
use actix_web::middleware::BodyEncoding;
use actix_web::web::{Data, Json, Path};
use actix_web::{error, http};
use actix_web::{web, HttpRequest, HttpResponse, ResponseError, Result};
use actix_web_actors::ws;

use chrono::Utc;
use futures::future;
use futures::future::Future;
use futures3::compat::Future01CompatExt;
use futures3::TryFutureExt;
use log::warn;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::clone::Clone;
use std::env;
use std::fs;
use std::io;
use std::io::Write;
use std::os::unix;
use std::os::unix::fs::PermissionsExt;
use std::path;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::NamedTempFile;

use crate::app::{Claims, Config};
use crate::db::*;
use crate::deltas::{DeltaGenerator, RemoteWorker};
use crate::errors::ApiError;
use crate::jobs::{JobQueue, ProcessJobs};
use crate::models::{Job, JobKind, JobStatus, NewBuild, NewBuildRef};
use crate::tokens::{self, ClaimsValidator};
use askama::Template;

fn init_ostree_repo(
    repo_path: &path::Path,
    parent_repo_path: &path::Path,
    build_id: i32,
    opt_collection_id: &Option<String>,
) -> io::Result<()> {
    let parent_repo_absolute_path = env::current_dir()?.join(parent_repo_path);

    for &d in [
        "extensions",
        "objects",
        "refs/heads",
        "refs/mirrors",
        "refs/remotes",
        "state",
        "tmp/cache",
    ]
    .iter()
    {
        fs::create_dir_all(repo_path.join(d))?;
    }

    unix::fs::symlink(&parent_repo_absolute_path, repo_path.join("parent"))?;

    let mut file = fs::File::create(repo_path.join("config"))?;
    file.write_all(
        format!(
            r#"[core]
repo_version=1
mode=archive-z2
min-free-space-size=500MB
{}parent={}"#,
            match opt_collection_id {
                Some(collection_id) =>
                    format!("collection-id={}.Build{}\n", collection_id, build_id),
                _ => "".to_string(),
            },
            parent_repo_absolute_path.display()
        )
        .as_bytes(),
    )?;
    Ok(())
}

fn respond_with_url<T>(
    data: &T,
    req: &HttpRequest,
    name: &str,
    elements: &[String],
) -> Result<HttpResponse, ApiError>
where
    T: Serialize,
{
    match req.url_for(name, elements) {
        Ok(url) => Ok(HttpResponse::Ok()
            .header(http::header::LOCATION, url.to_string())
            .json(data)),
        Err(e) => Err(ApiError::InternalServerError(format!(
            "Can't get url for {} {:?}: {}",
            name, elements, e
        ))),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenSubsetArgs {
    sub: String,
    scope: Vec<String>,
    duration: i64,
    prefixes: Option<Vec<String>>,
    repos: Option<Vec<String>>,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenSubsetResponse {
    token: String,
}

pub fn repos_is_subset(maybe_subset_repos: &Option<Vec<String>>, claimed_repos: &[String]) -> bool {
    match maybe_subset_repos {
        Some(subset_repos) => subset_repos
            .iter()
            .all(|subset_repo| tokens::repo_matches_one_claimed(subset_repo, claimed_repos)),
        None => true,
    }
}

pub fn prefix_is_subset(
    maybe_subset_prefix: &Option<Vec<String>>,
    claimed_prefixes: &[String],
) -> bool {
    match maybe_subset_prefix {
        Some(subset_prefix) => subset_prefix
            .iter()
            .all(|s| tokens::id_matches_one_prefix(s, claimed_prefixes)),
        None => true,
    }
}

pub fn token_subset(
    args: Json<TokenSubsetArgs>,
    config: Data<Config>,
    req: HttpRequest,
) -> HttpResponse {
    if let Some(claims) = req.get_claims() {
        let new_exp = Utc::now()
            .timestamp()
            .saturating_add(i64::max(args.duration, 0));
        if new_exp <= claims.exp
            && tokens::sub_has_prefix(&args.sub, &claims.sub)
            && args.scope.iter().all(|s| claims.scope.contains(s))
            && prefix_is_subset(&args.prefixes, &claims.prefixes)
            && repos_is_subset(&args.repos, &claims.repos)
        {
            let new_claims = Claims {
                sub: args.sub.clone(),
                scope: args.scope.clone(),
                name: Some(claims.name.unwrap_or_else(|| "".to_string()) + "/" + &args.name),
                prefixes: {
                    if let Some(ref prefixes) = args.prefixes {
                        prefixes.clone()
                    } else {
                        claims.prefixes.clone()
                    }
                },
                repos: {
                    if let Some(ref repos) = args.repos {
                        repos.clone()
                    } else {
                        claims.repos
                    }
                },
                exp: new_exp,
            };
            return match jwt::encode(
                &jwt::Header::default(),
                &new_claims,
                &jwt::EncodingKey::from_secret(config.secret.as_ref()),
            ) {
                Ok(token) => HttpResponse::Ok().json(TokenSubsetResponse { token }),
                Err(e) => ApiError::InternalServerError(e.to_string()).error_response(),
            };
        }
    };
    ApiError::NotEnoughPermissions("No token presented".to_string()).error_response()
}

#[derive(Deserialize, Debug)]
pub struct JobPathParams {
    id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct JobArgs {
    log_offset: Option<usize>,
}

pub fn get_job(
    args: Json<JobArgs>,
    params: Path<JobPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(get_job_async(args, params, db, req)).compat()
}

async fn get_job_async(
    args: Json<JobArgs>,
    params: Path<JobPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims("build", "jobs")?;
    let job = db.lookup_job(params.id, args.log_offset).await?;
    Ok(HttpResponse::Ok().json(job))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateBuildArgs {
    repo: String,
}

pub fn create_build(
    args: Json<CreateBuildArgs>,
    db: Data<Db>,
    config: Data<Config>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(create_build_async(args, db, config, req)).compat()
}

async fn create_build_async(
    args: Json<CreateBuildArgs>,
    db: Data<Db>,
    config: Data<Config>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let repo1 = args.repo.clone();
    let repo2 = args.repo.clone();

    req.has_token_claims("build", "build")?;
    req.has_token_repo(&repo1)?;

    let repoconfig = config.get_repoconfig(&repo2).map(|rc| rc.clone())?; // Ensure the repo exists

    let build = db
        .new_build(NewBuild {
            repo: args.repo.clone(),
        })
        .await?;
    let build_repo_path = config.build_repo_base.join(build.id.to_string());
    let upload_path = build_repo_path.join("upload");

    init_ostree_repo(
        &build_repo_path,
        &repoconfig.path,
        build.id,
        &repoconfig.collection_id,
    )?;
    init_ostree_repo(&upload_path, &repoconfig.path, build.id, &None)?;

    respond_with_url(&build, &req, "show_build", &[build.id.to_string()])
}

pub fn builds(
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(builds_async(db, req)).compat()
}

async fn builds_async(db: Data<Db>, req: HttpRequest) -> Result<HttpResponse, ApiError> {
    req.has_token_claims("build", "build")?;
    let builds = db.list_builds().await?;
    Ok(HttpResponse::Ok().json(builds))
}

#[derive(Deserialize)]
pub struct BuildPathParams {
    id: i32,
}

pub fn get_build(
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(get_build_async(params, db, req)).compat()
}

async fn get_build_async(
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims(&format!("build/{}", params.id), "build")
        /* We allow getting a build for uploaders too, as it is similar info, and useful */
        .or_else(|_| req.has_token_claims(&format!("build/{}", params.id), "upload"))?;
    let build = db.lookup_build(params.id).await?;
    Ok(HttpResponse::Ok().json(build))
}

#[derive(Deserialize)]
pub struct RefPathParams {
    id: i32,
    ref_id: i32,
}

pub fn get_build_ref(
    params: Path<RefPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(get_build_ref_async(params, db, req)).compat()
}

async fn get_build_ref_async(
    params: Path<RefPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims(&format!("build/{}", params.id), "build")?;
    let build_ref = db.lookup_build_ref(params.id, params.ref_id).await?;
    Ok(HttpResponse::Ok().json(build_ref))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MissingObjectsArgs {
    wanted: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MissingObjectsResponse {
    missing: Vec<String>,
}

fn has_object(build_id: i32, object: &str, config: &Data<Config>) -> bool {
    let subpath: path::PathBuf = ["objects", &object[..2], &object[2..]].iter().collect();
    let build_path = config
        .build_repo_base
        .join(build_id.to_string())
        .join("upload")
        .join(&subpath);
    if build_path.exists() {
        true
    } else {
        let parent_path = config
            .build_repo_base
            .join(build_id.to_string())
            .join("parent")
            .join(&subpath);
        parent_path.exists()
    }
}

pub fn missing_objects(
    args: Json<MissingObjectsArgs>,
    params: Path<BuildPathParams>,
    config: Data<Config>,
    req: HttpRequest,
) -> HttpResponse {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "upload") {
        return e.error_response();
    }
    let mut missing = vec![];
    for object in &args.wanted {
        if !has_object(params.id, object, &config) {
            missing.push(object.to_string());
        }
    }
    HttpResponse::Ok()
        .encoding(http::header::ContentEncoding::Gzip)
        .json(MissingObjectsResponse { missing })
}

fn validate_ref(ref_name: &str, req: &HttpRequest) -> Result<(), ApiError> {
    let ref_parts: Vec<&str> = ref_name.split('/').collect();

    match ref_parts[0] {
        "screenshots" => {
            if ref_parts.len() != 2 {
                return Err(ApiError::BadRequest(format!(
                    "Invalid ref_name {}",
                    ref_name
                )));
            }
            Ok(())
        }
        "app" | "runtime" => {
            if ref_parts.len() != 4 {
                return Err(ApiError::BadRequest(format!(
                    "Invalid ref_name {}",
                    ref_name
                )));
            }
            req.has_token_prefix(ref_parts[1])
        }
        _ => Err(ApiError::BadRequest(format!(
            "Invalid ref_name {}",
            ref_name
        ))),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateBuildRefArgs {
    #[serde(rename = "ref")]
    ref_name: String,
    commit: String,
}

pub fn create_build_ref(
    args: Json<CreateBuildRefArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(create_build_ref_async(args, params, db, req)).compat()
}

async fn create_build_ref_async(
    args: Json<CreateBuildRefArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims(&format!("build/{}", params.id), "upload")
        .and_then(|_| validate_ref(&args.ref_name, &req))?;

    let build_id = params.id;
    let build = db.lookup_build(params.id).await?;

    req.has_token_repo(&build.repo)?;
    let buildref = db
        .new_build_ref(NewBuildRef {
            build_id,
            ref_name: args.ref_name.clone(),
            commit: args.commit.clone(),
        })
        .await?;

    respond_with_url(
        &buildref,
        &req,
        "show_build_ref",
        &[params.id.to_string(), buildref.id.to_string()],
    )
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddExtraIdsArgs {
    ids: Vec<String>,
}

fn validate_id(id: &str) -> Result<(), ApiError> {
    if !id
        .split('.')
        .all(|element| !element.is_empty() && element.chars().all(|ch| ch.is_alphanumeric()))
    {
        Err(ApiError::BadRequest(format!("Invalid extra id {}", id)))
    } else {
        Ok(())
    }
}

pub fn add_extra_ids(
    args: Json<AddExtraIdsArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(add_extra_ids_async(args, params, db, req)).compat()
}

async fn add_extra_ids_async(
    args: Json<AddExtraIdsArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let ids = args.ids.clone();
    req.has_token_claims(&format!("build/{}", params.id), "upload")?;

    ids.iter().try_for_each(|id| validate_id(id))?;

    let req2 = req.clone();
    let build_id = params.id;
    let build = db.lookup_build(params.id).await?;
    /* Validate token */
    req2.has_token_repo(&build.repo)?;
    let build = db.add_extra_ids(build_id, args.ids.clone()).await?;
    respond_with_url(&build, &req, "show_build", &[build_id.to_string()])
}

fn is_all_lower_hexdigits(s: &str) -> bool {
    !s.contains(|c: char| !c.is_digit(16) || c.is_uppercase())
}

fn filename_parse_object(filename: &str) -> Option<path::PathBuf> {
    let v: Vec<&str> = filename.split('.').collect();

    if v.len() != 2 {
        return None;
    }

    if v[0].len() != 64 || !is_all_lower_hexdigits(v[0]) {
        return None;
    }

    if v[1] != "dirmeta" && v[1] != "dirtree" && v[1] != "filez" && v[1] != "commit" {
        return None;
    }

    Some(
        path::Path::new("objects")
            .join(&filename[..2])
            .join(&filename[2..]),
    )
}

fn is_all_digits(s: &str) -> bool {
    !s.contains(|c: char| !c.is_digit(10))
}

/* Delta part filenames with no slashes, ending with .{part}.delta.
 * Examples:
 * oS6QiSBxQF5nJZBVS6MJ6tCk_KN63I72Y7QipgUTh5w-sdm_iU8hHZYwDpmzYBAP6cJQ5MX5VLxoGF+j+Q1OGPQ.superblock.delta
 * oS6QiSBxQF5nJZBVS6MJ6tCk_KN63I72Y7QipgUTh5w-sdm_iU8hHZYwDpmzYBAP6cJQ5MX5VLxoGF+j+Q1OGPQ.0.delta
 * sdm_iU8hHZYwDpmzYBAP6cJQ5MX5VLxoGF+j+Q1OGPQ.superblock.delta
 * sdm_iU8hHZYwDpmzYBAP6cJQ5MX5VLxoGF+j+Q1OGPQ.0.delta
 */
fn filename_parse_delta(name: &str) -> Option<path::PathBuf> {
    let v: Vec<&str> = name.split('.').collect();

    if v.len() != 3 {
        return None;
    }

    if v[2] != "delta" {
        return None;
    }

    if v[1] != "superblock" && !is_all_digits(v[1]) {
        return None;
    }

    if !(v[0].len() == 43 || (v[0].len() == 87 && v[0].chars().nth(43) == Some('-'))) {
        return None;
    }

    Some(
        path::Path::new("deltas")
            .join(&v[0][..2])
            .join(&v[0][2..])
            .join(&v[1]),
    )
}

fn get_upload_subpath(
    field: &actix_multipart::Field,
    state: &Arc<UploadState>,
) -> error::Result<path::PathBuf, ApiError> {
    let cd = field.content_disposition().ok_or_else(|| {
        ApiError::BadRequest("No content disposition for multipart item".to_string())
    })?;
    let filename = cd
        .get_filename()
        .ok_or_else(|| ApiError::BadRequest("No filename for multipart item".to_string()))?;
    // We verify the format below, but just to make sure we never allow anything like a path
    if filename.contains('/') {
        return Err(ApiError::BadRequest("Invalid upload filename".to_string()));
    }

    if !state.only_deltas {
        if let Some(path) = filename_parse_object(filename) {
            return Ok(path);
        }
    }

    if let Some(path) = filename_parse_delta(filename) {
        return Ok(path);
    }

    Err(ApiError::BadRequest("Invalid upload filename".to_string()))
}

struct UploadState {
    repo_path: path::PathBuf,
    only_deltas: bool,
}

fn start_save(
    subpath: &path::Path,
    state: &Arc<UploadState>,
) -> Result<(NamedTempFile, path::PathBuf)> {
    let absolute_path = state.repo_path.join(subpath);

    if let Some(parent) = absolute_path.parent() {
        fs::create_dir_all(&parent)?;
    }

    let tmp_dir = state.repo_path.join("tmp");
    fs::create_dir_all(&tmp_dir)?;

    let named_file = NamedTempFile::new_in(&tmp_dir)?;
    Ok((named_file, absolute_path))
}

fn save_file(
    field: actix_multipart::Field,
    state: &Arc<UploadState>,
) -> Box<dyn Future<Item = i64, Error = ApiError>> {
    let repo_subpath = match get_upload_subpath(&field, state) {
        Ok(subpath) => subpath,
        Err(e) => return Box::new(future::err(e)),
    };

    let (named_file, object_file) = match start_save(&repo_subpath, state) {
        Ok((named_file, object_file)) => (named_file, object_file),
        Err(e) => return Box::new(future::err(ApiError::InternalServerError(e.to_string()))),
    };

    // We need file in two continuations below, so put it in a Rc+RefCell
    let shared_file = Rc::new(RefCell::new(named_file));
    let shared_file2 = shared_file.clone();
    Box::new(
        field
            .fold(0i64, move |acc, bytes| {
                let rt = shared_file
                    .borrow_mut()
                    .write_all(bytes.as_ref())
                    .map(|_| acc + bytes.len() as i64)
                    .map_err(|e| {
                        actix_multipart::MultipartError::Payload(error::PayloadError::Io(e))
                    });
                future::result(rt)
            })
            .map_err(|e| ApiError::InternalServerError(e.to_string()))
            .and_then(move |res| {
                // persist consumes the named file, so we need to
                // completely move it out of the shared Rc+RefCell
                let named_file = Rc::try_unwrap(shared_file2).unwrap().into_inner();
                match named_file.persist(&object_file) {
                    Ok(persisted_file) => {
                        if let Ok(metadata) = persisted_file.metadata() {
                            let mut perms = metadata.permissions();
                            perms.set_mode(0o644);
                            if let Err(_e) = fs::set_permissions(&object_file, perms) {
                                warn!("Can't change permissions on uploaded file");
                            }
                        } else {
                            warn!("Can't get permissions on uploaded file");
                        };
                        future::result(Ok(res))
                    }
                    Err(e) => future::err(ApiError::InternalServerError(e.to_string())),
                }
            }),
    )
}

pub fn upload(
    multipart: Multipart,
    req: HttpRequest,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    config: Data<Config>,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(upload_async(multipart, req, params, db, config)).compat()
}

async fn upload_async(
    multipart: Multipart,
    req: HttpRequest,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    config: Data<Config>,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims(&format!("build/{}", params.id), "upload")?;

    let uploadstate = Arc::new(UploadState {
        only_deltas: false,
        repo_path: config
            .build_repo_base
            .join(params.id.to_string())
            .join("upload"),
    });

    let req2 = req.clone();
    let build = db.lookup_build(params.id).await?;
    req2.has_token_repo(&build.repo)?;
    multipart
        .map_err(|e| ApiError::InternalServerError(e.to_string()))
        .map(move |field| save_file(field, &uploadstate).into_stream())
        .flatten()
        .collect()
        .map(|sizes| HttpResponse::Ok().json(sizes))
        .from_err()
        .compat()
        .await
}

pub fn get_commit_job(
    args: Json<JobArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(get_commit_job_async(args, params, db, req)).compat()
}

async fn get_commit_job_async(
    args: Json<JobArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims(&format!("build/{}", params.id), "build")?;
    let job = db.lookup_commit_job(params.id, args.log_offset).await?;
    Ok(HttpResponse::Ok().json(job))
}

#[derive(Deserialize)]
pub struct CommitArgs {
    endoflife: Option<String>,
    endoflife_rebase: Option<String>,
    token_type: Option<i32>,
}

pub fn commit(
    args: Json<CommitArgs>,
    params: Path<BuildPathParams>,
    job_queue: Data<Addr<JobQueue>>,
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(commit_async(args, params, job_queue, db, req)).compat()
}

async fn commit_async(
    args: Json<CommitArgs>,
    params: Path<BuildPathParams>,
    job_queue: Data<Addr<JobQueue>>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims(&format!("build/{}", params.id), "build")?;

    let req2 = req.clone();
    let build_id = params.id;
    let build = db.lookup_build(build_id).await?;
    req2.has_token_repo(&build.repo)?;
    let job = db
        .start_commit_job(
            build_id,
            args.endoflife.clone(),
            args.endoflife_rebase.clone(),
            args.token_type,
        )
        .await?;
    job_queue.do_send(ProcessJobs(None));
    respond_with_url(&job, &req, "show_commit_job", &[params.id.to_string()])
}

pub fn get_publish_job(
    args: Json<JobArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(get_publish_job_async(args, params, db, req)).compat()
}

async fn get_publish_job_async(
    args: Json<JobArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims(&format!("build/{}", params.id), "build")?;
    let job = db.lookup_publish_job(params.id, args.log_offset).await?;
    Ok(HttpResponse::Ok().json(job))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishArgs {}

pub fn publish(
    _args: Json<PublishArgs>,
    params: Path<BuildPathParams>,
    job_queue: Data<Addr<JobQueue>>,
    db: Data<Db>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(publish_async(_args, params, job_queue, db, req)).compat()
}

async fn publish_async(
    _args: Json<PublishArgs>,
    params: Path<BuildPathParams>,
    job_queue: Data<Addr<JobQueue>>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims(&format!("build/{}", params.id), "publish")?;
    let build_id = params.id;
    let req2 = req.clone();

    let build = db.lookup_build(build_id).await?;
    req2.has_token_repo(&build.repo)?;

    let job = db.start_publish_job(build_id, build.repo.clone()).await?;
    job_queue.do_send(ProcessJobs(Some(build.repo)));

    respond_with_url(&job, &req, "show_publish_job", &[params.id.to_string()])
}

pub fn purge(
    params: Path<BuildPathParams>,
    db: Data<Db>,
    config: Data<Config>,
    req: HttpRequest,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(purge_async(params, db, config, req)).compat()
}

async fn purge_async(
    params: Path<BuildPathParams>,
    db: Data<Db>,
    config: Data<Config>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims(&format!("build/{}", params.id), "build")?;

    let build_repo_path = config.build_repo_base.join(params.id.to_string());
    let build_id = params.id;
    let req2 = req.clone();
    let db2 = db.clone();

    let build = db.lookup_build(build_id).await?;

    req2.has_token_repo(&build.repo)?;
    db.init_purge(build_id).await?;

    let res = fs::remove_dir_all(&build_repo_path);
    let build = db2
        .finish_purge(
            build_id,
            match res {
                Ok(()) => None,
                Err(e) => Some(e.to_string()),
            },
        )
        .await?;

    respond_with_url(&build, &req, "show_build", &[build_id.to_string()])
}

#[derive(Template)]
#[template(path = "job.html")]
struct JobStatusData {
    id: i32,
    kind: String,
    status: String,
    contents: String,
    results: String,
    log: String,
    finished: bool,
}

fn job_status_data(job: Job) -> JobStatusData {
    JobStatusData {
        id: job.id,
        kind: JobKind::from_db(job.kind).map_or("Unknown".to_string(), |k| format!("{:?}", k)),
        status: JobStatus::from_db(job.status)
            .map_or("Unknown".to_string(), |s| format!("{:?}", s)),
        contents: job.contents,
        results: job.results.unwrap_or_default(),
        log: job.log,
        finished: job.status >= JobStatus::Ended as i16,
    }
}

pub fn job_status(
    params: Path<JobPathParams>,
    db: Data<Db>,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(job_status_async(params, db)).compat()
}

async fn job_status_async(
    params: Path<JobPathParams>,
    db: Data<Db>,
) -> Result<HttpResponse, ApiError> {
    let job = db.lookup_job(params.id, None).await?;
    let s = job_status_data(job).render().unwrap();
    Ok(HttpResponse::Ok().content_type("text/html").body(s))
}

#[derive(Template)]
#[template(path = "status.html")]
struct Status {
    jobs: Vec<JobStatusData>,
    version: String,
}

pub fn status(db: Data<Db>) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(status_async(db)).compat()
}

async fn status_async(db: Data<Db>) -> Result<HttpResponse, ApiError> {
    let jobs = db.list_active_jobs().await?;

    let s = Status {
        jobs: jobs.into_iter().map(job_status_data).collect(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    }
    .render()
    .unwrap();
    Ok(HttpResponse::Ok().content_type("text/html").body(s))
}

#[derive(Deserialize)]
pub struct DeltaUploadParams {
    repo: String,
}

pub fn delta_upload(
    multipart: Multipart,
    params: Path<DeltaUploadParams>,
    req: HttpRequest,
    config: Data<Config>,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    futures::done(req.has_token_claims("delta", "generate"))
        .and_then(move |_| futures::done(config.get_repoconfig(&params.repo).map(|rc| rc.clone())))
        .and_then(move |repoconfig| {
            let uploadstate = Arc::new(UploadState {
                only_deltas: true,
                repo_path: repoconfig.get_abs_repo_path(),
            });
            multipart
                .map_err(|e| ApiError::InternalServerError(e.to_string()))
                .map(move |field| save_file(field, &uploadstate).into_stream())
                .flatten()
                .collect()
                .map(|sizes| HttpResponse::Ok().json(sizes))
        })
}

pub fn ws_delta(
    req: HttpRequest,
    config: Data<Config>,
    delta_generator: Data<Addr<DeltaGenerator>>,
    stream: web::Payload,
) -> Result<HttpResponse, actix_web::Error> {
    if let Err(e) = req.has_token_claims("delta", "generate") {
        return Ok(e.error_response());
    }
    let remote = req
        .connection_info()
        .remote()
        .unwrap_or("Unknown")
        .to_string();
    ws::start(
        RemoteWorker::new(&config, &delta_generator, remote),
        &req,
        stream,
    )
}
