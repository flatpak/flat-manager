use actix::prelude::*;
use actix_web::{self, fs, middleware};
use actix_web::{App, http::Method, HttpRequest, fs::NamedFile};
use base64;
use models::DbExecutor;
use std::path::PathBuf;
use std::path::Path;
use std::env;

use api;
use tokens::{TokenParser};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // "build", "build/N"
    pub scope: Vec<String>, // "build", "upload" "publish"
    pub name: String, // for debug/logs only
}

#[derive(Clone)]
pub struct AppState {
    pub db: Addr<DbExecutor>,
    pub repo_path: PathBuf,
    pub build_repo_base_path: PathBuf,
    pub collection_id: Option<String>,
    pub gpg_homedir: Option<String>,
    pub build_gpg_key: Option<String>,
    pub main_gpg_key: Option<String>,
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
) -> App<AppState> {
    let repo_path = env::var_os("REPO_PATH")
        .expect("REPO_PATH must be set");
    let build_repo_base_path = env::var_os("BUILD_REPO_BASE_PATH")
        .expect("BUILD_REPO_BASE_PATH must be set");

    let secret_base64 = env::var("SECRET")
        .expect("SECRET must be set");
    let secret = base64::decode(&secret_base64).unwrap();
    let state = AppState {
        db: db.clone(),
        repo_path: PathBuf::from(repo_path),
        build_repo_base_path: PathBuf::from(build_repo_base_path),
        collection_id: env::var("COLLECTION_ID").ok(),
        gpg_homedir: env::var("GPG_HOMEDIR").ok(),
        build_gpg_key: env::var("BUILD_GPG_KEY").ok(),
        main_gpg_key: env::var("MAIN_GPG_KEY").ok(),
    };

    let repo_static_files = fs::StaticFiles::new(&state.repo_path)
        .expect("failed constructing repo handler");

    App::with_state(state)
        .middleware(middleware::Logger::default())
        .scope("/api/v1", |scope| {
            scope
                .middleware(TokenParser::new(&secret))
                .resource("/build", |r| r.method(Method::POST).with(api::create_build))
                .resource("/build/{id}", |r| { r.name("show_build"); r.method(Method::GET).with(api::get_build) })
                .resource("/build/{id}/build_ref", |r| r.method(Method::POST).with(api::create_build_ref))
                .resource("/build/{id}/build_ref/{ref_id}", |r| { r.name("show_build_ref"); r.method(Method::GET).with(api::get_build_ref) })
                .resource("/build/{id}/upload", |r| r.method(Method::POST).with(api::upload))
                .resource("/build/{id}/commit", |r| r.method(Method::POST).with(api::commit))
                .resource("/build/{id}/missing_objects", |r| r.method(Method::GET).with(api::missing_objects))
        })
        .scope("/build-repo/{id}", |scope| {
            scope.handler("/", |req: &HttpRequest<AppState>| handle_build_repo(req))
        })
        .handler("/repo", repo_static_files)
}
