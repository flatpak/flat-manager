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
    http, middleware, server, App, AsyncResponder, FutureResponse, HttpResponse, Path,
    State, fs,
};

use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use futures::Future;
use dotenv::dotenv;
use std::env;
use std::ffi::OsString;

mod db;
mod models;
mod schema;

use db::{CreateBuild, LookupBuild, DbExecutor};

struct AppState {
    db: Addr<DbExecutor>,
    repo_path: OsString,
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

fn main() {
    ::std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();
    let sys = actix::System::new("repo-manage");

    dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    let repo_path = env::var_os("REPO_PATH")
        .expect("REPO_PATH must be set");

    let manager = ConnectionManager::<PgConnection>::new(database_url);
    let pool = r2d2::Pool::builder()
        .build(manager)
        .expect("Failed to create pool.");

    let addr = SyncArbiter::start(3, move || DbExecutor(pool.clone()));


    server::new(move || {
        let state = AppState {
            db: addr.clone(),
            repo_path: repo_path.clone(),
        };

        let repo_static_files = fs::StaticFiles::new(&state.repo_path)
            .expect("failed constructing repo handler");

        App::with_state(state)
            .middleware(middleware::Logger::default())
            .resource("/build", |r| r.method(http::Method::POST).with(create_build))
            .resource("/build/{id}", |r| r.method(http::Method::GET).with(get_build))
            .handler("/repo", repo_static_files)
    }).bind("127.0.0.1:8080")
        .unwrap()
        .start();

    println!("Started http server: 127.0.0.1:8080");
    let _ = sys.run();
}
