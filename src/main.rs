#![allow(proc_macro_derive_resolution_fallback)]

extern crate actix;
extern crate actix_net;
extern crate actix_web;
extern crate base64;
extern crate chrono;
#[macro_use] extern crate diesel;
extern crate dotenv;
extern crate env_logger;
#[macro_use] extern crate failure;
extern crate futures;
extern crate r2d2;
extern crate serde;
#[macro_use] extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate tempfile;
extern crate jsonwebtoken as jwt;
#[macro_use]
extern crate log;
extern crate libc;

use actix::prelude::*;
use actix::{Actor, actors::signal};
use actix_web::{server, server::StopServer};
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use dotenv::dotenv;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

mod api;
mod app;
mod db;
mod errors;
mod models;
mod schema;
mod tokens;
mod jobs;

use models::{DbExecutor};
use jobs::{JobQueue, StopJobQueue, start_job_executor};

struct HandleSignals {
    server: Addr<actix_net::server::Server>,
    job_queue: Addr<JobQueue>,
}

impl Actor for HandleSignals {
    type Context = Context<Self>;
}

impl Handler<signal::Signal> for HandleSignals {
    type Result = ();

    fn handle(&mut self, msg: signal::Signal, ctx: &mut Context<Self>) {
        let (stop, graceful) = match msg.0 {
            signal::SignalType::Int => {
                info!("SIGINT received, exiting");
                (true, false)
            }
            signal::SignalType::Term => {
                info!("SIGTERM received, exiting");
                (true, true)
            }
            signal::SignalType::Quit => {
                info!("SIGQUIT received, exiting");
                (true, false)
            }
            _ => (false, false),
        };
        if stop {
            info!("Stopping http server");
            ctx.spawn(
                self.server
                    .send(StopServer { graceful: graceful })
                    .into_actor(self)
                    .then(|_result, actor, _ctx| {
                        info!("Stopping job processing");
                        actor.job_queue
                            .send(StopJobQueue())
                            .into_actor(actor)
                    })
                    .then(|_result, _actor, ctx| {
                        info!("Stopped job processing");
                        ctx.run_later(Duration::from_millis(300), |_, _| {
                            System::current().stop();
                        });
                        actix::fut::ok(())
                    })
            );
        };
    }
}

fn main() {
    ::std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let sys = actix::System::new("repo-manage");

    dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    let repo_path = env::var_os("REPO_PATH")
        .expect("REPO_PATH must be set");
    let build_repo_base_path = env::var_os("BUILD_REPO_BASE_PATH")
        .expect("BUILD_REPO_BASE_PATH must be set");
    let secret_base64 = env::var("SECRET")
        .expect("SECRET must be set");

    let secret = base64::decode(&secret_base64).unwrap();
    let config = Arc::new(app::Config {
        repo_path: PathBuf::from(repo_path),
        build_repo_base_path: PathBuf::from(build_repo_base_path),
        base_url: env::var("BASE_URL").unwrap_or("http://127.0.0.1:8080".to_string()),
        collection_id: env::var("COLLECTION_ID").ok(),
        gpg_homedir: env::var("GPG_HOMEDIR").ok(),
        build_gpg_key: env::var("BUILD_GPG_KEY").ok(),
        main_gpg_key: env::var("MAIN_GPG_KEY").ok(),
        secret: secret.clone(),
    });

    let manager = ConnectionManager::<PgConnection>::new(database_url.clone());
    let pool = r2d2::Pool::builder()
        .build(manager)
        .expect("Failed to create pool.");

    let pool_copy = pool.clone();
    let db_addr = SyncArbiter::start(3, move || DbExecutor(pool_copy.clone()));

    let pool_copy2 = pool.clone();
    let jobs_addr = start_job_executor(config.clone(), pool_copy2.clone());

    let jobs_addr_copy = jobs_addr.clone();
    let http_server = server::new(move || {
        app::create_app(db_addr.clone(), &config, jobs_addr_copy.clone())
    });
    let server_addr = http_server
        .bind("127.0.0.1:8080")
        .unwrap()
        .disable_signals()
        .start();

    let signal_handler = HandleSignals{
        server: server_addr,
        job_queue: jobs_addr.clone(),
    }.start();

    let signals = System::current().registry().get::<signal::ProcessSignals>();
    signals.do_send(signal::Subscribe(signal_handler.clone().recipient()));

    info!("Started http server: 127.0.0.1:8080");
    let _ = sys.run();
}
