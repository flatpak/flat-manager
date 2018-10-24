use actix::prelude::*;
use actix_web::{self, fs, middleware};
use actix_web::{App, http::Method, HttpRequest, fs::NamedFile};
use models::DbExecutor;
use std::path::PathBuf;
use std::path::Path;
use std::ffi::OsString;

use api;

#[derive(Clone)]
pub struct AppState {
    pub db: Addr<DbExecutor>,
    pub repo_path: PathBuf,
    pub build_repo_base_path: PathBuf,
    pub collection_id: Option<String>,
}

fn handle_build_repo(req: &HttpRequest<AppState>) -> actix_web::Result<NamedFile> {
    let tail: String = req.match_info().query("tail")?;
    let id: String = req.match_info().query("id")?;
    let state = req.state();
    let path = Path::new(&state.build_repo_base_path).join(&id).join(tail.trim_left_matches('/'));
    println!("Handle build repo {:?}", path);
    NamedFile::open(path).or_else(|_e| {
        let fallback_path = Path::new(&state.repo_path).join(tail.trim_left_matches('/'));
        Ok(NamedFile::open(fallback_path)?)
    })
}

pub fn create_app(
    db: Addr<DbExecutor>,
    repo_path: &OsString,
    build_repo_base_path: &OsString,
    collection_id: &Option<OsString>,
) -> App<AppState> {
    let state = AppState {
        db: db.clone(),
        repo_path: PathBuf::from(repo_path),
        build_repo_base_path: PathBuf::from(build_repo_base_path),
        collection_id: collection_id.as_ref().map(|os| os.clone().into_string().expect("non-utf8 collection id")),
    };

    let repo_static_files = fs::StaticFiles::new(&state.repo_path)
        .expect("failed constructing repo handler");

    App::with_state(state)
        .middleware(middleware::Logger::default())
        .resource("/api/v1/build", |r| r.method(Method::POST).with(api::create_build))
        .resource("/api/v1/build/{id}", |r| { r.name("show_build"); r.method(Method::GET).with(api::get_build) })
        .resource("/api/v1/build/{id}/build_ref", |r| r.method(Method::POST).with(api::create_build_ref))
        .resource("/api/v1/build/{id}/build_ref/{ref_id}", |r| { r.name("show_build_ref"); r.method(Method::GET).with(api::get_build_ref) })
        .resource("/api/v1/build/{id}/upload", |r| r.method(Method::POST).with(api::upload))
        .resource("/api/v1/build/{id}/commit", |r| r.method(Method::POST).with(api::commit))
        .resource("/api/v1/build/{id}/missing_objects", |r| r.method(Method::GET).with(api::missing_objects))
        .scope("/build-repo/{id}", |scope| {
            scope.handler("/", |req: &HttpRequest<AppState>| handle_build_repo(req))
        })
        .handler("/repo", repo_static_files)
}
