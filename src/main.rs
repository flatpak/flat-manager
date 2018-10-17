//! Actix web diesel example
//!
//! Diesel does not support tokio, so we have to run it in separate threads.
//! Actix supports sync actors by default, so we going to create sync actor
//! that use diesel. Technically sync actors are worker style actors, multiple
//! of them can run in parallel and process messages from same queue.
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate diesel;
extern crate actix;
extern crate actix_web;
extern crate env_logger;
extern crate futures;
extern crate r2d2;
extern crate chrono;
extern crate dotenv;

use actix::prelude::*;
use actix_web::{
    http, middleware, server, App, AsyncResponder, FutureResponse, HttpResponse, HttpRequest,
    Result, Path, State, fs,
};

use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use futures::Future;
use dotenv::dotenv;
use std::env;
use std::path;
use std::ffi::OsString;

mod db;
mod models;
mod schema;

use db::{CreateBuild, LookupBuild, DbExecutor};

struct AppState {
    db: Addr<DbExecutor>,
    repo_path: OsString,
    build_repo_base_path: OsString,
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
pub struct GetBuildParams {
    id: i32,
}


fn get_build(
    (params, state): (Path<GetBuildParams>, State<AppState>),
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
            repo_path: repo_path.clone(),
            build_repo_base_path: build_repo_base_path.clone(),
        };

        let repo_static_files = fs::StaticFiles::new(&state.repo_path)
            .expect("failed constructing repo handler");

        App::with_state(state)
            .middleware(middleware::Logger::default())
            .resource("/build", |r| r.method(http::Method::POST).with(create_build))
            .resource("/build/{id}", |r| r.method(http::Method::GET).with(get_build))
            .scope("/build/{id}", |scope| {
                scope.handler("/repo", |req: &HttpRequest<AppState>| handle_build_repo(req))
            })
            .handler("/repo", repo_static_files)
    }).bind("127.0.0.1:8080")
        .unwrap()
        .start();

    println!("Started http server: 127.0.0.1:8080");
    let _ = sys.run();
}
