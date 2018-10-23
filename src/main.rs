#![allow(proc_macro_derive_resolution_fallback)]

extern crate actix;
extern crate actix_web;
extern crate chrono;
#[macro_use] extern crate diesel;
extern crate dotenv;
extern crate env_logger;
#[macro_use] extern crate failure;
extern crate futures;
extern crate r2d2;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate tempfile;

use actix::prelude::*;
use actix_web::{dev, error, multipart, server,};
use actix_web::{AsyncResponder, FutureResponse, HttpMessage, HttpRequest, HttpResponse, Json, Path, Result, State,};
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

mod api;
mod db;
mod models;
mod schema;
mod errors;
mod app;

use app::{AppState};
use db::{CreateBuild, CreateBuildRef, LookupBuild};
use models::{DbExecutor, NewBuildRef};

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
        app::create_app(addr.clone(), &repo_path, &build_repo_base_path)
    }).bind("127.0.0.1:8080")
        .unwrap()
        .start();

    println!("Started http server: 127.0.0.1:8080");
    let _ = sys.run();
}
