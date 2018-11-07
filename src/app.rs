use actix::prelude::*;
use actix_web::{self, fs, middleware};
use actix_web::{App, http::Method, HttpRequest, fs::NamedFile};
use models::DbExecutor;
use std::path::PathBuf;
use std::path::Path;
use std::sync::mpsc;
use std::sync::Arc;

use api;
use tokens::{TokenParser};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // "build", "build/N"
    pub scope: Vec<String>, // "build", "upload" "publish"
    pub name: String, // for debug/logs only
    pub exp: i64,
}

pub struct Config {
    pub repo_path: PathBuf,
    pub build_repo_base_path: PathBuf,
    pub base_url: String,
    pub collection_id: Option<String>,
    pub gpg_homedir: Option<String>,
    pub build_gpg_key: Option<String>,
    pub main_gpg_key: Option<String>,
    pub secret: Vec<u8>,
}

#[derive(Clone)]
pub struct AppState {
    pub db: Addr<DbExecutor>,
    pub config: Arc<Config>,
    pub job_tx_channel: mpsc::Sender<()>,
}

fn handle_build_repo(req: &HttpRequest<AppState>) -> actix_web::Result<NamedFile> {
    let tail: String = req.match_info().query("tail")?;
    let id: String = req.match_info().query("id")?;
    let state = req.state();
    let path = Path::new(&state.config.build_repo_base_path).join(&id).join(tail.trim_left_matches('/'));
    NamedFile::open(path).or_else(|_e| {
        let fallback_path = Path::new(&state.config.repo_path).join(tail.trim_left_matches('/'));
        Ok(NamedFile::open(fallback_path)?)
    })
}

pub fn create_app(
    db: Addr<DbExecutor>,
    config: &Arc<Config>,
    job_tx_channel: mpsc::Sender<()>
) -> App<AppState> {
    let state = AppState {
        db: db.clone(),
        job_tx_channel: job_tx_channel.clone(),
        config: config.clone(),
    };

    let repo_static_files = fs::StaticFiles::new(&state.config.repo_path)
        .expect("failed constructing repo handler");

    App::with_state(state)
        .middleware(middleware::Logger::default())
        .scope("/api/v1", |scope| {
            scope
                .middleware(TokenParser::new(&config.secret))
                .resource("/token_subset", |r| r.method(Method::POST).with(api::token_subset))
                .resource("/job/{id}", |r| { r.name("show_job"); r.method(Method::POST).with(api::get_job)})
                .resource("/build", |r| r.method(Method::POST).with(api::create_build))
                .resource("/build/{id}", |r| { r.name("show_build"); r.method(Method::GET).with(api::get_build) })
                .resource("/build/{id}/build_ref", |r| r.method(Method::POST).with(api::create_build_ref))
                .resource("/build/{id}/build_ref/{ref_id}", |r| { r.name("show_build_ref"); r.method(Method::GET).with(api::get_build_ref) })
                .resource("/build/{id}/missing_objects", |r| r.method(Method::GET).with(api::missing_objects))
                .resource("/build/{id}/upload", |r| r.method(Method::POST).with(api::upload))
                .resource("/build/{id}/commit", |r| r.method(Method::POST).with(api::commit))
                .resource("/build/{id}/publish", |r| r.method(Method::POST).with(api::publish))
        })
        .scope("/build-repo/{id}", |scope| {
            scope.handler("/", |req: &HttpRequest<AppState>| handle_build_repo(req))
        })
        .handler("/repo", repo_static_files)
}
