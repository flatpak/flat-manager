use actix::prelude::*;
use actix_web::{dev, error, multipart, http};
use actix_web::{AsyncResponder, FutureResponse, HttpMessage, HttpRequest, HttpResponse, Json, Path, Result, State,};

use futures::prelude::*;
use futures::future;
use std::cell::RefCell;
use std::clone::Clone;
use std::env;
use std::fs;
use std::io;
use std::io::Write;
use std::os::unix;
use std::path;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::NamedTempFile;
use chrono::{Utc};
use jwt;

use app::{AppState,Claims};
use errors::ApiError;
use db::*;
use models::{NewBuild,NewBuildRef};
use actix_web::ResponseError;
use tokens::{self, ClaimsValidator};
use models::DbExecutor;
use jobs::ProcessJobs;

fn init_ostree_repo(repo_path: &path::PathBuf, parent_repo_path: &path::PathBuf, build_id: i32, opt_collection_id: &Option<String>) -> io::Result<()> {
    let parent_repo_absolute_path = env::current_dir()?.join(parent_repo_path);

    for &d in ["extensions",
               "objects",
               "refs/heads",
               "refs/mirrors",
               "refs/remotes",
               "state",
               "tmp/cache"].iter() {
        fs::create_dir_all(repo_path.join(d))?;
    }

    unix::fs::symlink(&parent_repo_absolute_path, repo_path.join("parent"))?;

    let mut file = fs::File::create(repo_path.join("config"))?;
    file.write_all(format!(
r#"[core]
repo_version=1
mode=archive-z2
{}parent={}"#,
                           match opt_collection_id {
                               Some(collection_id) => format!("collection-id={}.Build{}\n", collection_id, build_id),
                               _ => "".to_string(),
                           },
                           parent_repo_absolute_path.display()).as_bytes())?;
    Ok(())
}

fn db_request<M: DbRequest> (state: &AppState,
                             msg: M) ->
    Box<Future<Item = <M as DbRequest>::DbType, Error = ApiError>>
    where
    DbExecutor: actix::Handler<DbRequestWrapper<M>>
{
    Box::new(
        state
            .db
            .send(DbRequestWrapper::<M> (msg))
            .from_err()
            .and_then(move |res| match res {
                Ok(ok_res) =>  return future::ok(ok_res),
                Err(db_err) => future::err(db_err)
            })
            .from_err())
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

pub fn repos_is_subset(maybe_subset_repos: &Option<Vec<String>>, claimed_repos: &Vec<String>) -> bool {
    match maybe_subset_repos  {
        Some(subset_repos) => subset_repos.iter().all(|subset_repo| tokens::repo_matches_one_claimed(subset_repo, claimed_repos)),
        None => true,
    }
}

pub fn prefix_is_subset(maybe_subset_prefix: &Option<Vec<String>>, claimed_prefixes: &Vec<String>) -> bool {
    match maybe_subset_prefix  {
        Some(subset_prefix) => subset_prefix.iter().all(|s| tokens::id_matches_one_prefix(s, &claimed_prefixes)),
        None => true,
    }
}

pub fn token_subset(
    args: Json<TokenSubsetArgs>,
    state: State<AppState>,
    req: HttpRequest<AppState>
) -> HttpResponse {
    if let Some(claims) = req.get_claims() {
        let new_exp = Utc::now().timestamp().saturating_add(i64::max(args.duration, 0));
        if new_exp <= claims.exp &&
            tokens::sub_has_prefix (&args.sub, &claims.sub) &&
            args.scope.iter().all(|s| claims.scope.contains(s)) &&
            prefix_is_subset(&args.prefixes, &claims.prefixes) &&
            repos_is_subset(&args.repos, &claims.repos) {
                let new_claims = Claims {
                    sub: args.sub.clone(),
                    scope: args.scope.clone(),
                    name: claims.name + "/" + &args.name,
                    prefixes: { if let Some(ref prefixes) = args.prefixes { prefixes.clone() } else { claims.prefixes.clone() } },
                    repos: { if let Some(ref repos) = args.repos { repos.clone() } else { claims.repos.clone() } },
                    exp: new_exp,
                };
                return match jwt::encode(&jwt::Header::default(), &new_claims, &state.config.secret) {
                    Ok(token) => HttpResponse::Ok().json(TokenSubsetResponse{ token: token }),
                    Err(e) => ApiError::InternalServerError(e.to_string()).error_response()
                }
            }
    };
    ApiError::NotEnoughPermissions.error_response()
}

#[derive(Deserialize)]
pub struct JobPathParams {
    id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobArgs {
    #[serde(rename = "log-offset")]
    log_offset: Option<usize>,
}

pub fn get_job(
    args: Json<JobArgs>,
    params: Path<JobPathParams>,
    state: State<AppState>,
    req: HttpRequest<AppState>,
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims("build", "jobs") {
        return From::from(e);
    }

    db_request (&state,
                LookupJob {
                    id: params.id,
                    log_offset: args.log_offset,
                })
        .and_then(|job| Ok(HttpResponse::Ok().json(job)))
        .from_err()
        .responder()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateBuildArgs {
    repo: String
}

pub fn create_build(
    args: Json<CreateBuildArgs>,
    state: State<AppState>,
    req: HttpRequest<AppState>
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims("build", "build") {
        return From::from(e);
    }

    if let Err(e) = req.has_token_repo(&args.repo) {
        return From::from(e);
    }

    // Ensure the repo exists
    let repoconfig = match state.config.get_repoconfig(&args.repo) {
        Ok(repoconfig) => repoconfig,
        Err(e) => return From::from(e)
    }.clone();

    db_request (&state,
                NewBuild {
                    repo: args.repo.clone(),
                })
        .and_then(move |build| {
            let build_repo_path = state.config.build_repo_base_path.join(build.id.to_string());
            let upload_path = build_repo_path.join("upload");

            init_ostree_repo (&build_repo_path, &repoconfig.path, build.id, &repoconfig.collection_id)?;
            init_ostree_repo (&upload_path, &repoconfig.path, build.id, &None)?;

            match req.url_for("show_build", &[build.id.to_string()]) {
                Ok(url) => Ok(HttpResponse::Ok()
                              .header(http::header::LOCATION, url.to_string())
                              .json(build)),
                Err(e) => Ok(e.error_response())
            }
        })
        .from_err()
        .responder()
}

pub fn builds(
    state: State<AppState>,
    req: HttpRequest<AppState>
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims("build", "build") {
        return From::from(e);
    }
    db_request (&state, ListBuilds { })
        .and_then(move |builds| Ok(HttpResponse::Ok().json(builds)))
        .from_err()
        .responder()
}


#[derive(Deserialize)]
pub struct BuildPathParams {
    id: i32,
}

pub fn get_build(
    params: Path<BuildPathParams>,
    state: State<AppState>,
    req: HttpRequest<AppState>,
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "build") {
        return From::from(e);
    }
    db_request (&state, LookupBuild { id: params.id })
        .and_then(|build| Ok(HttpResponse::Ok().json(build)))
        .from_err()
        .responder()
}

#[derive(Deserialize)]
pub struct RefPathParams {
    id: i32,
    ref_id: i32,
}

pub fn get_build_ref(
    params: Path<RefPathParams>,
    state: State<AppState>,
    req: HttpRequest<AppState>,
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "build") {
        return From::from(e);
    }
    db_request (&state,
                LookupBuildRef {
                    id: params.id,
                    ref_id: params.ref_id,
                })
        .and_then(|build_ref| Ok(HttpResponse::Ok().json(build_ref)))
        .from_err()
        .responder()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MissingObjectsArgs {
    wanted: Vec<String>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MissingObjectsResponse {
    missing: Vec<String>
}

fn has_object (build_id: i32, object: &str, state: &State<AppState>) -> bool
{
    let subpath: path::PathBuf = ["objects", &object[..2], &object[2..]].iter().collect();
    let build_path = state.config.build_repo_base_path.join(build_id.to_string()).join("upload").join(&subpath);
    if build_path.exists() {
        true
    } else {
        let parent_path = state.config.build_repo_base_path.join(build_id.to_string()).join("parent").join(&subpath);
        parent_path.exists()
    }
}

pub fn missing_objects(
    args: Json<MissingObjectsArgs>,
    params: Path<BuildPathParams>,
    state: State<AppState>,
    req: HttpRequest<AppState>,
) -> HttpResponse {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "upload") {
        return e.error_response();
    }
    let mut missing = vec![];
    for object in &args.wanted {
        if ! has_object (params.id, object, &state) {
            missing.push(object.to_string());
        }
    }
    HttpResponse::Ok()
        .content_encoding(http::ContentEncoding::Gzip)
        .json(MissingObjectsResponse { missing: missing })
}

fn validate_ref (ref_name: &String, req: &HttpRequest<AppState>) -> Result<(),ApiError>
{
    let ref_parts: Vec<&str> = ref_name.split('/').collect();

    match ref_parts[0] {
        "screenshots" => {
            if ref_parts.len() != 2 {
                return Err(ApiError::BadRequest(format!("Invalid ref_name {}", ref_name)))
            }
            Ok(())
        },
        "app" | "runtime" => {
            if ref_parts.len() != 4 {
                return Err(ApiError::BadRequest(format!("Invalid ref_name {}", ref_name)))
            }
            req.has_token_prefix(ref_parts[1])
        },
        _  => Err(ApiError::BadRequest(format!("Invalid ref_name {}", ref_name))),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateBuildRefArgs {
    #[serde(rename = "ref")] ref_name: String,
    commit: String,
}

pub fn create_build_ref (
    args: Json<CreateBuildRefArgs>,
    params: Path<BuildPathParams>,
    state: State<AppState>,
    req: HttpRequest<AppState>,
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "upload") {
        return From::from(e);
    }

    if let Err(e) = validate_ref(&args.ref_name, &req) {
        return From::from(e);
    }

    let req2 = req.clone();
    let build_id = params.id;
    db_request (&state, LookupBuild { id: params.id })
        .and_then (move |build| req2.has_token_repo(&build.repo))
        .and_then (move |_ok| {
            db_request (&state,
                        NewBuildRef {
                            build_id: build_id,
                            ref_name: args.ref_name.clone(),
                            commit: args.commit.clone(),
                        })
        })
        .and_then(move |buildref| {
            match req.url_for("show_build_ref", &[params.id.to_string(), buildref.id.to_string()]) {
                Ok(url) => Ok(HttpResponse::Ok()
                              .header(http::header::LOCATION, url.to_string())
                              .json(buildref)),
                Err(e) => Ok(e.error_response())
            }
        })
        .from_err()
        .responder()
}

fn objectname_is_valid(name: &str) -> bool {
    let v: Vec<&str> = name.splitn(2, ".").collect();

    v.len() == 2 &&
        v[0].len() == 64  &&
        !v[0].contains(|c: char| !(c.is_digit(16) && !c.is_uppercase())) &&
        (v[1] == "dirmeta" ||
         v[1] == "dirtree" ||
         v[1] == "filez" ||
         v[1] == "commit")
}

fn get_object_name(field: &multipart::Field<dev::Payload>) -> error::Result<String, ApiError> {
    let cd = field.content_disposition().ok_or(
        ApiError::BadRequest("No content disposition for multipart item".to_string()))?;
    let filename = cd.get_filename().ok_or(
        ApiError::BadRequest("No filename for multipart item".to_string()))?;
    if !objectname_is_valid(filename) {
        Err(ApiError::BadRequest("Invalid object name".to_string()))
    } else {
        Ok(filename.to_string())
    }
}

struct UploadState {
    repo_path: path::PathBuf,
}

fn start_save(
    object: String,
    state: &Arc<UploadState>,
) -> Result<(NamedTempFile,path::PathBuf)> {
    let objects_dir = state.repo_path.join("objects");
    let object_dir = objects_dir.join(&object[..2]);
    let object_file = object_dir.join(&object[2..]);

    fs::create_dir_all(object_dir)?;

    let tmp_dir = state.repo_path.join("tmp");
    fs::create_dir_all(&tmp_dir)?;

    let named_file = NamedTempFile::new_in(&tmp_dir)?;
    Ok((named_file, object_file))
}

fn save_file(
    field: multipart::Field<dev::Payload>,
    state: &Arc<UploadState>
) -> Box<Future<Item = i64, Error = ApiError>> {
    let object = match get_object_name (&field) {
        Ok(name) => name,
        Err(e) => return Box::new(future::err(e)),
    };

    let (named_file, object_file) = match start_save (object, state) {
        Ok((named_file, object_file)) => (named_file, object_file),
        Err(e) => return Box::new(future::err(ApiError::InternalServerError(e.to_string()))),
    };

    // We need file in two continuations below, so put it in a Rc+RefCell
    let shared_file = Rc::new(RefCell::new(named_file));
    let shared_file2 = shared_file.clone();
    Box::new(
        field
            .fold(0i64, move |acc, bytes| {
                let rt = shared_file.borrow_mut()
                    .write_all(bytes.as_ref())
                    .map(|_| acc + bytes.len() as i64)
                    .map_err(|e| {
                        error::MultipartError::Payload(error::PayloadError::Io(e))
                    });
                future::result(rt)
            })
            .map_err(|e| {
                ApiError::InternalServerError(e.to_string())
            })
            .and_then (move |res| {
                // persist consumes the named file, so we need to
                // completely move it out of the shared Rc+RefCell
                let named_file = Rc::try_unwrap(shared_file2).unwrap().into_inner();
                match named_file.persist(object_file) {
                    Ok(_persisted_file) => future::result(Ok(res)),
                    Err(e) => future::err(ApiError::InternalServerError(e.to_string()))
                }
            }),
    )
}

fn handle_multipart_item(
    item: multipart::MultipartItem<dev::Payload>,
    state: &Arc<UploadState>
) -> Box<Stream<Item = i64, Error = ApiError>> {
    match item {
        multipart::MultipartItem::Field(field) => {
            Box::new(save_file(field, state).into_stream())
        }
        multipart::MultipartItem::Nested(mp) => {
            let s = state.clone();
            Box::new(mp
                     .map_err(|e| {
                         ApiError::InternalServerError(e.to_string())
                     })
                     .map(move |item| { handle_multipart_item (item, &s) })
                     .flatten())
        }
    }
}

pub fn upload(
    params: Path<BuildPathParams>,
    req: HttpRequest<AppState>,
    state: State<AppState>,
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "upload") {
        return From::from(e);
    }
    let uploadstate = Arc::new(UploadState { repo_path: state.config.build_repo_base_path.join(params.id.to_string()).join("upload") });
    let req2 = req.clone();
    db_request (&state, LookupBuild { id: params.id })
        .and_then (move |build| req2.has_token_repo(&build.repo))
        .and_then (move |_ok| {
            Box::new(
                req.multipart()
                    .map_err(|e| ApiError::InternalServerError(e.to_string()))
                    .map(move |item| { handle_multipart_item (item, &uploadstate) })
                    .flatten()
                    .collect()
                    .map(|sizes| HttpResponse::Ok().json(sizes))
                    .from_err()
            )
        })
        .from_err()
        .responder()
}

pub fn get_commit_job(
    args: Json<JobArgs>,
    params: Path<BuildPathParams>,
    state: State<AppState>,
    req: HttpRequest<AppState>,
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "build") {
        return From::from(e);
    }
    db_request (&state,
                LookupCommitJob {
                    build_id: params.id,
                    log_offset: args.log_offset,
                })
        .and_then(|job| Ok(HttpResponse::Ok().json(job)))
        .from_err()
        .responder()
}

#[derive(Deserialize)]
pub struct CommitArgs {
    endoflife: Option<String>,
}

pub fn commit(
    args: Json<CommitArgs>,
    params: Path<BuildPathParams>,
    state: State<AppState>,
    req: HttpRequest<AppState>,
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "build") {
        return From::from(e);
    }
    let job_queue = state.job_queue.clone();
    let req2 = req.clone();
    let build_id = params.id;
    db_request (&state, LookupBuild { id: build_id })
        .and_then (move |build| req2.has_token_repo(&build.repo))
        .and_then (move |_ok| {
            db_request (&state,
                        StartCommitJob {
                            id: build_id,
                            endoflife: args.endoflife.clone(),
                        })
        })
        .and_then(move |job| {
            job_queue.do_send(ProcessJobs());
            match req.url_for("show_commit_job", &[params.id.to_string()]) {
                Ok(url) => Ok(HttpResponse::Ok()
                              .header(http::header::LOCATION, url.to_string())
                              .json(job)),
                Err(e) => Ok(e.error_response())
            }
        })
        .from_err()
        .responder()
}


pub fn get_publish_job(
    args: Json<JobArgs>,
    params: Path<BuildPathParams>,
    state: State<AppState>,
    req: HttpRequest<AppState>,
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "build") {
        return From::from(e);
    }
    db_request (&state,
                LookupPublishJob {
                    build_id: params.id,
                    log_offset: args.log_offset,
                })
        .and_then(|job| Ok(HttpResponse::Ok().json(job)))
        .from_err()
        .responder()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishArgs {
    subsets: Option<Vec<String>>
}

pub fn publish(
    args: Json<PublishArgs>,
    params: Path<BuildPathParams>,
    state: State<AppState>,
    req: HttpRequest<AppState>,
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "publish") {
        return From::from(e);
    }

    let subsets = args.subsets.as_ref().cloned().unwrap_or(Vec::new());
    let subsets2 = subsets.clone();

    let job_queue = state.job_queue.clone();
    let build_id = params.id;
    let req2 = req.clone();
    let config = state.config.clone();

    db_request (&state, LookupBuild { id: build_id })
        .and_then (move |build| {
            req2.has_token_repo(&build.repo)?;
            let repoconfig = config.get_repoconfig(&build.repo)?;
            for subset in subsets.iter() {
                if !repoconfig.subsets.contains_key (subset) {
                    return Err(ApiError::InternalServerError(format!("Subset {} unknown", subset)))
                }
            }
            Ok(())
        })
        .and_then (move |_ok| {
            db_request (&state,
                        StartPublishJob {
                            id: build_id,
                            subsets: subsets2,
                        })
                .and_then(move |job| {
                    job_queue.do_send(ProcessJobs());
                    match req.url_for("show_publish_job", &[params.id.to_string()]) {
                        Ok(url) => Ok(HttpResponse::Ok()
                                      .header(http::header::LOCATION, url.to_string())
                                      .json(job)),
                        Err(e) => Ok(e.error_response())
                    }
                })
        })
        .from_err()
        .responder()
}

pub fn purge(
    params: Path<BuildPathParams>,
    state: State<AppState>,
    req: HttpRequest<AppState>,
) -> FutureResponse<HttpResponse> {
    if let Err(e) = req.has_token_claims(&format!("build/{}", params.id), "build") {
        return From::from(e);
    }

    let build_repo_path = state.config.build_repo_base_path.join(params.id.to_string());
    let build_id = params.id;
    let req2 = req.clone();
    let state2 = state.clone();
    db_request (&state, LookupBuild { id: build_id })
        .and_then (move |build| req2.has_token_repo(&build.repo))
        .and_then (move |_ok| {
            db_request (&state,
                        InitPurge {
                            id: build_id,
                        })
        })
        .and_then(move |_ok| {
            let res = fs::remove_dir_all(&build_repo_path);
            db_request (&state2,
                        FinishPurge {
                            id: build_id,
                            error: match res {
                                Ok(()) => None,
                                Err(e) => Some(e.to_string()),
                            },
                        })
        })
        .and_then(move |build| {
            match req.url_for("show_build", &[build_id.to_string()]) {
                Ok(url) => Ok(HttpResponse::Ok()
                              .header(http::header::LOCATION, url.to_string())
                              .json(build)),
                Err(e) => Ok(e.error_response())
            }
        })
        .from_err()
        .responder()
}
