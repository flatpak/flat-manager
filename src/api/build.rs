use actix::prelude::*;
use actix_multipart::Multipart;
use actix_web::web::{self, Data, Json, Path, Query};
use actix_web::{HttpRequest, HttpResponse, ResponseError, Result};

use chrono::Utc;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::clone::Clone;
use std::fs;
use std::path;
use std::sync::Arc;

use crate::config::Config;
use crate::db::*;
use crate::errors::{ApiError, JobError};
use crate::jobs::{update_build_status_after_check, JobQueue, ProcessJobs};
use crate::models::{Build, BuildRef, Check, CheckStatus, NewBuild, NewBuildRef};
use crate::ostree::init_ostree_repo;
use crate::tokens::{self, Claims, ClaimsScope, ClaimsValidator};

use super::utils::{respond_with_url, save_file, UploadState};

#[derive(Deserialize, Debug)]
pub struct JobPathParams {
    pub id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct JobArgs {
    log_offset: Option<usize>,
}

pub async fn get_job(
    args: Json<JobArgs>,
    params: Path<JobPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims("build", ClaimsScope::Jobs)?;
    let job = db.lookup_job(params.id, args.log_offset).await?;
    Ok(HttpResponse::Ok().json(job))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ReviewArgs {
    new_status: CheckStatus,
    new_results: Option<String>,
}

pub async fn review_check(
    args: Json<ReviewArgs>,
    params: Path<JobPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims("build", ClaimsScope::ReviewCheck)?;

    db.set_check_status(params.id, args.new_status.clone(), args.new_results.clone())
        .await?;

    let check = db.get_check_by_job_id(params.id).await?;
    web::block(move || {
        let mut conn = db
            .pool
            .get()
            .map_err(|err| JobError::InternalError(err.to_string()))?;
        update_build_status_after_check(check.build_id, &mut conn)
    })
    .await
    .map_err(|err| ApiError::InternalServerError(err.to_string()))?
    .map_err(|err| ApiError::InternalServerError(err.to_string()))?;

    Ok(HttpResponse::NoContent().finish())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateBuildArgs {
    repo: String,
    app_id: Option<String>,
    public_download: Option<bool>,
    build_log_url: Option<String>,
}

pub async fn create_build(
    args: Json<CreateBuildArgs>,
    db: Data<Db>,
    config: Data<Config>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims("build", ClaimsScope::Build)?;
    req.has_token_repo(&args.repo)?;

    if let Some(app_id) = &args.app_id {
        req.has_token_prefix(app_id)?;
    }

    let repoconfig = config.get_repoconfig(&args.repo).cloned()?; // Ensure the repo exists

    // If public_download is not specified, it defaults to true if there is no app ID (old style builds) and false
    // if there is one.
    let public_download = args
        .public_download
        .unwrap_or_else(|| args.app_id.is_none());

    let token_name = if let Some(ref claims) = req.get_claims() {
        if let Some(ref name) = claims.name {
            name.clone()
        } else {
            "-".to_string()
        }
    } else {
        "-".to_string()
    };

    let token_type = if let Some(ref claims) = req.get_claims() {
        claims.token_type.clone()
    } else {
        None
    };

    let build = db
        .new_build(NewBuild {
            repo: args.repo.clone(),
            app_id: args.app_id.clone(),
            public_download,
            build_log_url: args.build_log_url.clone(),
            token_name: Some(token_name),
            token_type,
        })
        .await?;
    let build_repo_path = config.build_repo_base.join(build.id.to_string());
    let upload_path = build_repo_path.join("upload");

    init_ostree_repo(
        &build_repo_path,
        &repoconfig.path,
        &repoconfig.collection_id.map(|id| (id, build.id)),
    )?;
    init_ostree_repo(&upload_path, &repoconfig.path, &None)?;

    respond_with_url(&build, &req, "show_build", &[build.id.to_string()])
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListBuildsArgs {
    app_id: Option<String>,
}

pub async fn builds(
    query: Query<ListBuildsArgs>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims("build", ClaimsScope::Build)
        // also allow downloaders to list builds
        .or_else(|_| req.has_token_claims("build", ClaimsScope::Download))?;

    let builds = if let Some(app_id) = query.app_id.clone() {
        req.has_token_prefix(&app_id)?;
        db.list_builds_for_app(app_id).await?
    } else {
        db.list_builds().await?
    };

    Ok(HttpResponse::Ok().json(builds))
}

fn has_token_for_build(req: &HttpRequest, build: &Build) -> Result<(), ApiError> {
    req.has_token_repo(&build.repo)?;

    if let Some(app_id) = &build.app_id {
        req.has_token_prefix(app_id)
            /* Hide the app ID of the build, since we can't access it */
            .map_err(|_| {
                ApiError::NotEnoughPermissions(
                    "Build's app ID not matching prefix in token".to_string(),
                )
            })
    } else {
        Ok(())
    }
}

async fn lookup_authorized_build(
    req: &HttpRequest,
    db: &Db,
    build_id: i32,
    scope: ClaimsScope,
) -> Result<Build, ApiError> {
    req.has_token_claims(&format!("build/{build_id}"), scope)?;
    let build = db.lookup_build(build_id).await?;
    has_token_for_build(req, &build)?;
    Ok(build)
}

async fn lookup_authorized_build_any(
    req: &HttpRequest,
    db: &Db,
    build_id: i32,
    scopes: &[ClaimsScope],
) -> Result<Build, ApiError> {
    let required_sub = format!("build/{build_id}");
    let mut allowed = false;
    let mut last_err = None;

    for scope in scopes {
        match req.has_token_claims(&required_sub, scope.clone()) {
            Ok(()) => {
                allowed = true;
                break;
            }
            Err(err) => last_err = Some(err),
        }
    }

    if !allowed {
        return Err(last_err.unwrap_or_else(|| {
            ApiError::NotEnoughPermissions("No accepted scope configured".to_string())
        }));
    }

    let build = db.lookup_build(build_id).await?;
    has_token_for_build(req, &build)?;
    Ok(build)
}

#[derive(Deserialize)]
pub struct BuildPathParams {
    id: i32,
}

pub async fn get_build(
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    /* We allow getting a build for uploaders too, as it is similar info, and useful */
    let build = lookup_authorized_build_any(
        &req,
        db.get_ref(),
        params.id,
        &[ClaimsScope::Build, ClaimsScope::Upload],
    )
    .await?;

    Ok(HttpResponse::Ok().json(build))
}

#[derive(Debug, Serialize)]
pub struct BuildExtended {
    build: Build,
    build_refs: Vec<BuildRef>,
    checks: Vec<Check>,
}

pub async fn get_build_extended(
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let build = lookup_authorized_build_any(
        &req,
        db.get_ref(),
        params.id,
        &[ClaimsScope::Build, ClaimsScope::Upload],
    )
    .await?;

    let build_refs = db.lookup_build_refs(params.id).await?;
    let checks = db.lookup_checks(params.id).await?;

    Ok(HttpResponse::Ok().json(BuildExtended {
        build,
        build_refs,
        checks,
    }))
}

#[derive(Deserialize)]
pub struct RefPathParams {
    id: i32,
    ref_id: i32,
}

pub async fn get_build_ref(
    params: Path<RefPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Build).await?;

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

pub async fn missing_objects(
    args: Json<MissingObjectsArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    config: Data<Config>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Upload).await?;

    let missing = args
        .wanted
        .iter()
        .filter(|object| !has_object(params.id, object, &config))
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    Ok(HttpResponse::Ok().json(MissingObjectsResponse { missing }))
}

fn validate_ref(ref_name: &str, req: &HttpRequest) -> Result<(), ApiError> {
    let ref_parts: Vec<&str> = ref_name.split('/').collect();

    match ref_parts[0] {
        "screenshots" => {
            if ref_parts.len() != 2 {
                return Err(ApiError::BadRequest(format!("Invalid ref_name {ref_name}")));
            }
            Ok(())
        }
        "app" | "runtime" => {
            if ref_parts.len() != 4 {
                return Err(ApiError::BadRequest(format!("Invalid ref_name {ref_name}")));
            }
            req.has_token_prefix(ref_parts[1])
        }
        _ => Err(ApiError::BadRequest(format!("Invalid ref_name {ref_name}"))),
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateBuildRefArgs {
    #[serde(rename = "ref")]
    ref_name: String,
    commit: String,
    build_log_url: Option<String>,
}

pub async fn create_build_ref(
    args: Json<CreateBuildRefArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let build_id = params.id;
    lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Upload).await?;
    validate_ref(&args.ref_name, &req)?;

    let existing_ref = db
        .lookup_build_ref_by_name(build_id, args.ref_name.clone())
        .await?;

    let buildref = match existing_ref {
        Some(existing) if existing.commit == args.commit => existing,
        _ => {
            db.upsert_build_ref(NewBuildRef {
                build_id,
                ref_name: args.ref_name.clone(),
                commit: args.commit.clone(),
                build_log_url: args.build_log_url.clone(),
            })
            .await?
        }
    };

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
        Err(ApiError::BadRequest(format!("Invalid extra id {id}")))
    } else {
        Ok(())
    }
}

pub async fn add_extra_ids(
    args: Json<AddExtraIdsArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Upload).await?;
    args.ids.iter().try_for_each(|id| validate_id(id))?;

    let build = db.add_extra_ids(params.id, args.ids.clone()).await?;
    respond_with_url(&build, &req, "show_build", &[params.id.to_string()])
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenSubsetArgs {
    sub: String,
    scope: Vec<ClaimsScope>,
    duration: i64,
    prefixes: Option<Vec<String>>,
    apps: Option<Vec<String>>,
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

pub fn apps_is_subset(maybe_subset_apps: Option<&[String]>, claimed_apps: &[String]) -> bool {
    match maybe_subset_apps {
        Some(subset_apps) => subset_apps.iter().all(|s| claimed_apps.contains(s)),
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
            && apps_is_subset(args.apps.as_deref(), &claims.apps)
            && repos_is_subset(&args.repos, &claims.repos)
        {
            let new_claims = Claims {
                sub: args.sub.clone(),
                scope: args.scope.clone(),
                name: Some(claims.name.unwrap_or_default() + "/" + &args.name),
                jti: claims.jti.clone(),
                prefixes: {
                    if let Some(ref prefixes) = args.prefixes {
                        prefixes.clone()
                    } else {
                        claims.prefixes.clone()
                    }
                },
                apps: {
                    if let Some(ref apps) = args.apps {
                        apps.clone()
                    } else {
                        claims.apps.clone()
                    }
                },
                repos: {
                    if let Some(ref repos) = args.repos {
                        repos.clone()
                    } else {
                        claims.repos
                    }
                },
                branches: claims.branches.clone(),
                token_type: claims.token_type.clone(),
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

pub async fn upload(
    multipart: Multipart,
    req: HttpRequest,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    config: Data<Config>,
) -> Result<HttpResponse, ApiError> {
    let uploadstate = Arc::new(UploadState {
        only_deltas: false,
        repo_path: config
            .build_repo_base
            .join(params.id.to_string())
            .join("upload"),
    });

    lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Upload).await?;

    let mut multipart = multipart;
    let mut sizes = Vec::new();
    while let Some(field) = multipart.next().await {
        let field = field.map_err(|e| ApiError::InternalServerError(e.to_string()))?;
        sizes.push(save_file(field, &uploadstate).await?);
    }

    Ok(HttpResponse::Ok().json(sizes))
}

pub async fn get_commit_job(
    args: Json<JobArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let build = lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Build).await?;

    let job_id = build.commit_job_id.ok_or(ApiError::NotFound)?;
    let job = db.lookup_job(job_id, args.log_offset).await?;

    Ok(HttpResponse::Ok().json(job))
}

#[derive(Deserialize)]
pub struct CommitArgs {
    endoflife: Option<String>,
    endoflife_rebase: Option<String>,
    token_type: Option<i32>,
}

pub async fn commit(
    args: Json<CommitArgs>,
    params: Path<BuildPathParams>,
    job_queue: Data<Addr<JobQueue>>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Build).await?;

    let job = db
        .start_commit_job(
            params.id,
            args.endoflife.clone(),
            args.endoflife_rebase.clone(),
            args.token_type,
        )
        .await?;

    job_queue.do_send(ProcessJobs(None));
    respond_with_url(&job, &req, "show_commit_job", &[params.id.to_string()])
}

pub async fn get_publish_job(
    args: Json<JobArgs>,
    params: Path<BuildPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let build = lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Build).await?;

    let job_id = build.publish_job_id.ok_or(ApiError::NotFound)?;
    let job = db.lookup_job(job_id, args.log_offset).await?;

    Ok(HttpResponse::Ok().json(job))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishArgs {}

pub async fn publish(
    _args: Json<PublishArgs>,
    params: Path<BuildPathParams>,
    job_queue: Data<Addr<JobQueue>>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let build =
        lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Publish).await?;

    let job = db.start_publish_job(params.id, build.repo.clone()).await?;
    job_queue.do_send(ProcessJobs(Some(build.repo)));

    respond_with_url(&job, &req, "show_publish_job", &[params.id.to_string()])
}

#[derive(Deserialize)]
pub struct BuildCheckPathParams {
    id: i32,
    check_name: String,
}

pub async fn get_check_job(
    args: Json<JobArgs>,
    params: Path<BuildCheckPathParams>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let build = lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Build).await?;

    let checks = db.lookup_checks(build.id).await?;
    let check = checks
        .iter()
        .find(|check| check.check_name == params.check_name);

    if let Some(check) = check {
        let job = db.lookup_job(check.job_id, args.log_offset).await?;
        Ok(HttpResponse::Ok().json(job))
    } else {
        Err(ApiError::NotFound)
    }
}

pub async fn purge(
    params: Path<BuildPathParams>,
    db: Data<Db>,
    config: Data<Config>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let build_repo_path = config.build_repo_base.join(params.id.to_string());
    lookup_authorized_build(&req, db.get_ref(), params.id, ClaimsScope::Build).await?;

    db.init_purge(params.id).await?;

    let res = fs::remove_dir_all(&build_repo_path);
    let build = db
        .finish_purge(
            params.id,
            match res {
                Ok(()) => None,
                Err(e) => Some(e.to_string()),
            },
        )
        .await?;

    respond_with_url(&build, &req, "show_build", &[params.id.to_string()])
}

#[derive(Deserialize)]
pub struct RepublishPathParams {
    repo: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepublishArgs {
    app: String,
    endoflife: Option<String>,
    endoflife_rebase: Option<String>,
}

pub async fn republish(
    args: Json<RepublishArgs>,
    params: Path<RepublishPathParams>,
    job_queue: Data<Addr<JobQueue>>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims("build", ClaimsScope::Republish)?;
    req.has_token_prefix(&args.app)?;
    req.has_token_repo(&params.repo)?;

    let job = db
        .start_republish_job(
            params.repo.clone(),
            args.app.clone(),
            args.endoflife.clone(),
            args.endoflife_rebase.clone(),
        )
        .await?;
    job_queue.do_send(ProcessJobs(Some(params.repo.clone())));

    respond_with_url(&job, &req, "show_job", &[job.id.to_string()])
}
