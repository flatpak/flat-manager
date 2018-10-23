use actix_web::{dev, error, multipart, http};
use actix_web::{AsyncResponder, FutureResponse, HttpMessage, HttpRequest, HttpResponse, Json, Path, Result, State,};
use actix_web::error::{ErrorBadRequest,};

use futures::future;
use futures::{Future, Stream};
use std::fs;
use std::path;
use std::io::Write;
use std::io;
use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;
use tempfile::NamedTempFile;

use app::{AppState};
use db::{CreateBuild, CreateBuildRef, LookupBuild, LookupBuildRef};
use models::{NewBuildRef};
use actix_web::ResponseError;

pub fn create_build(
    (state, req): (State<AppState>, HttpRequest<AppState>)
) -> FutureResponse<HttpResponse> {
    state
        .db
        .send(CreateBuild { })
        .from_err()
        .and_then(move |res| match res {
            Ok(build) => {
                match req.url_for("show_build", &[build.id.to_string()]) {
                    Ok(url) => Ok(HttpResponse::Ok()
                                  .header(http::header::LOCATION, url.to_string())
                                  .json(build)),
                    Err(e) => Ok(e.error_response())
                }
            },
            Err(e) => Ok(e.error_response())
        })
        .responder()
}

#[derive(Deserialize)]
pub struct BuildPathParams {
    id: i32,
}

pub fn get_build(
    (params, state): (Path<BuildPathParams>, State<AppState>),
) -> FutureResponse<HttpResponse> {
    state
        .db
        .send(LookupBuild { id: params.id })
        .from_err()
        .and_then(|res| match res {
            Ok(build) => Ok(HttpResponse::Ok().json(build)),
            Err(e) => Ok(e.error_response())
        })
        .responder()
}

#[derive(Deserialize)]
pub struct RefPathParams {
    id: i32,
    ref_id: i32,
}

pub fn get_build_ref(
    (params, state): (Path<RefPathParams>, State<AppState>),
) -> FutureResponse<HttpResponse> {
    state
        .db
        .send(LookupBuildRef {
            id: params.id,
            ref_id: params.ref_id,
        })
        .from_err()
        .and_then(|res| match res {
            Ok(build_ref) => Ok(HttpResponse::Ok().json(build_ref)),
            Err(e) => Ok(e.error_response())
        })
        .responder()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MissingObjectsArgs {
    wanted: Vec<String>
}

fn has_object (build_id: i32, object: &str, state: &State<AppState>) -> bool
{
    let subpath: path::PathBuf = ["objects", &object[..2], &object[2..]].iter().collect();
    let build_path = state.build_repo_base_path.join(build_id.to_string()).join(&subpath);
    if build_path.exists() {
        true
    } else {
        let main_path = state.repo_path.join(&subpath);
        main_path.exists()
    }
}

pub fn missing_objects(
    (missing_objects, params, state): (Json<MissingObjectsArgs>, Path<BuildPathParams>, State<AppState>),
) -> HttpResponse {
    let mut missing = vec![];
    for object in &missing_objects.wanted {
        if ! has_object (params.id, object, &state) {
            missing.push(object);
        }
    }
    HttpResponse::Ok()
        .content_encoding(http::ContentEncoding::Gzip)
        .json(&missing)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateBuildRefArgs {
    #[serde(rename = "ref")] ref_name: String,
    commit: String,
}

pub fn create_build_ref (
    (args, params, state, req): (Json<CreateBuildRefArgs>, Path<BuildPathParams>, State<AppState>, HttpRequest<AppState>),
) -> FutureResponse<HttpResponse> {
    state
        .db
        .send(CreateBuildRef {
            data: NewBuildRef {
                build_id: params.id,
                ref_name: args.ref_name.clone(),
                commit: args.commit.clone(),
            }
        })
        .from_err()
        .and_then(move |res| match res {
            Ok(buildref) =>  {
                match req.url_for("show_build_ref", &[params.id.to_string(), buildref.id.to_string()]) {
                    Ok(url) => Ok(HttpResponse::Ok()
                                  .header(http::header::LOCATION, url.to_string())
                                  .json(buildref)),
                    Err(e) => Ok(e.error_response())
                }
            },
            Err(e) => Ok(e.error_response())
        })
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

fn get_object_name(field: &multipart::Field<dev::Payload>) -> error::Result<String, io::Error> {
    let cd = field.content_disposition().ok_or(
        io::Error::new(io::ErrorKind::InvalidInput,
                       "No content disposition for multipart item"))?;
    let filename = cd.get_filename().ok_or(
        io::Error::new(io::ErrorKind::InvalidInput,
                       "No filename for multipart item"))?;
    if !objectname_is_valid(filename) {
        Err(io::Error::new(io::ErrorKind::InvalidInput,
                           "Invalid object name"))
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
) -> Box<Future<Item = i64, Error = error::Error>> {
    let object = match  get_object_name (&field) {
        Ok(name) => name,
        Err(e) => return Box::new(future::err(ErrorBadRequest(e))),
    };

    let (named_file, object_file) = match start_save (object, state) {
        Ok((named_file, object_file)) => (named_file, object_file),
        Err(e) => return Box::new(future::err(error::ErrorInternalServerError(e))),
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
                        println!("file.write_all failed: {:?}", e);
                        error::MultipartError::Payload(error::PayloadError::Io(e))
                    });
                future::result(rt)
            })
            .map_err(|e| {
                println!("save_file failed, {:?}", e);
                error::ErrorInternalServerError(e)
            }).and_then (move |res| {
                // persist consumes the named file, so we need to
                // completely move it out of the shared Rc+RefCell
                let named_file = Rc::try_unwrap(shared_file2).unwrap().into_inner();
                match named_file.persist(object_file) {
                    Ok(_persisted_file) => future::result(Ok(res)),
                    Err(e) => future::err(error::ErrorInternalServerError(e))
                }
            }),
    )
}

fn handle_multipart_item(
    item: multipart::MultipartItem<dev::Payload>,
    state: &Arc<UploadState>
) -> Box<Stream<Item = i64, Error = error::Error>> {
    match item {
        multipart::MultipartItem::Field(field) => {
            Box::new(save_file(field, state).into_stream())
        }
        multipart::MultipartItem::Nested(mp) => {
            let s = state.clone();
            Box::new(mp.map_err(error::ErrorInternalServerError)
                     .map(move |item| { handle_multipart_item (item, &s) })
                     .flatten())
        }
    }
}

pub fn upload(
    (params, req): (Path<BuildPathParams>, HttpRequest<AppState>)
) -> FutureResponse<HttpResponse> {
    let state = req.state();
    let uploadstate = Arc::new(UploadState { repo_path: state.build_repo_base_path.join(params.id.to_string()) });
    Box::new(
        req.multipart()
            .map_err(error::ErrorInternalServerError)
            .map(move |item| { handle_multipart_item (item, &uploadstate) })
            .flatten()
            .collect()
            .map(|sizes| HttpResponse::Ok().json(sizes))
            .map_err(|e| {
                println!("failed: {}", e);
                e
            }),
    )
}
