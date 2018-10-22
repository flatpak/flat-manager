extern crate actix;
extern crate actix_web;
extern crate chrono;
#[macro_use]
extern crate diesel;
extern crate dotenv;
extern crate env_logger;
extern crate futures;
extern crate r2d2;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate tempfile;

use actix::prelude::*;
use actix_web::{dev, error, fs, http, middleware, multipart, server,};
use actix_web::{App, AsyncResponder, FutureResponse, HttpMessage, HttpRequest, HttpResponse, Json, Path, Result, State,};
use actix_web::error::{ErrorBadRequest,};

use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use futures::future;
use futures::{Future, Stream};
use dotenv::dotenv;
use std::env;
use std::path;
use std::io::Write;
use std::io;
use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;
use tempfile::NamedTempFile;


mod db;
mod models;
mod schema;

use db::{CreateBuild, LookupBuild, DbExecutor};

struct AppState {
    db: Addr<DbExecutor>,
    repo_path: path::PathBuf,
    build_repo_base_path: path::PathBuf,
}

fn create_build(
    state: State<AppState>,
) -> FutureResponse<HttpResponse> {
    state
        .db
        .send(CreateBuild { })
        .from_err()
        .and_then(|res| match res {
            Ok(build) => Ok(HttpResponse::Ok().json(build)),
            Err(_) => Ok(HttpResponse::InternalServerError().into()),
        })
        .responder()
}

#[derive(Deserialize)]
pub struct BuildPathParams {
    id: i32,
}

fn get_build(
    (params, state): (Path<BuildPathParams>, State<AppState>),
) -> FutureResponse<HttpResponse> {
    state
        .db
        .send(LookupBuild { id: params.id })
        .from_err()
        .and_then(|res| match res {
            Ok(build) => Ok(HttpResponse::Ok().json(build)),
            Err(_) => Ok(HttpResponse::InternalServerError().into()),
        })
        .responder()
}

#[derive(Debug, Serialize, Deserialize)]
struct MissingObjects {
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

fn missing_objects(
    (missing_objects, params, state): (Json<MissingObjects>, Path<BuildPathParams>, State<AppState>),
) -> HttpResponse {
    let mut missing = vec![];
    for object in &missing_objects.wanted {
        if ! has_object (params.id, object, &state) {
            missing.push(object);
        }
    }
    println!("{}: {:?}", params.id, &missing);
    HttpResponse::Ok().json(&missing)
}

fn handle_build_repo(req: &HttpRequest<AppState>) -> Result<fs::NamedFile> {
    let tail: String = req.match_info().query("tail")?;
    let id: String = req.match_info().query("id")?;
    let state = req.state();
    let path = path::Path::new(&state.build_repo_base_path).join(&id).join(tail.trim_left_matches('/'));
    println!("Handle build repo {:?}", path);
    fs::NamedFile::open(path).or_else(|_e| {
        let fallback_path = path::Path::new(&state.repo_path).join(tail.trim_left_matches('/'));
        Ok(fs::NamedFile::open(fallback_path)?)
    })
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

    std::fs::create_dir_all(object_dir)?;

    let tmp_dir = state.repo_path.join("tmp");
    std::fs::create_dir_all(&tmp_dir)?;

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

fn upload(
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

fn main() {
    ::std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();
    let sys = actix::System::new("repo-manage");

    dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    let repo_path = env::var_os("REPO_PATH")
        .expect("REPO_PATH must be set");
    let build_repo_base_path = env::var_os("BUILD_REPO_BASE_PATH")
        .expect("BUILD_REPO_BASE_PATH must be set");

    let manager = ConnectionManager::<PgConnection>::new(database_url);
    let pool = r2d2::Pool::builder()
        .build(manager)
        .expect("Failed to create pool.");

    let addr = SyncArbiter::start(3, move || DbExecutor(pool.clone()));


    server::new(move || {
        let state = AppState {
            db: addr.clone(),
            repo_path: path::PathBuf::from(&repo_path),
            build_repo_base_path: path::PathBuf::from(&build_repo_base_path),
        };

        let repo_static_files = fs::StaticFiles::new(&state.repo_path)
            .expect("failed constructing repo handler");

        App::with_state(state)
            .middleware(middleware::Logger::default())
            .resource("/api/v1/build", |r| r.method(http::Method::POST).with(create_build))
            .resource("/api/v1/build/{id}", |r| r.method(http::Method::GET).with(get_build))
            .resource("/api/v1/build/{id}/upload", |r| r.method(http::Method::POST).with(upload))
            .resource("/api/v1/build/{id}/missing_objects", |r| r.method(http::Method::GET).with(missing_objects))
            .scope("/build-repo/{id}", |scope| {
                scope.handler("/", |req: &HttpRequest<AppState>| handle_build_repo(req))
            })
            .handler("/repo", repo_static_files)
    }).bind("127.0.0.1:8080")
        .unwrap()
        .start();

    println!("Started http server: 127.0.0.1:8080");
    let _ = sys.run();
}
