#![allow(proc_macro_derive_resolution_fallback)]

#[macro_use]
extern crate diesel;
#[macro_use]
extern crate diesel_migrations;

mod api;
mod app;
mod db;
mod delayed;
mod deltas;
pub mod errors;
mod jobs;
mod logger;
mod models;
pub mod ostree;
mod schema;
mod storefront;
mod tokens;

use actix::prelude::*;
use actix_web::dev::Server;
use app::Config;
use deltas::{DeltaGenerator, StopDeltaGenerator};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, ManageConnection};
use futures3::compat::Compat;
use futures3::FutureExt;
use jobs::{JobQueue, StopJobQueue};
use log::info;
use std::path;
use std::sync::Arc;
use std::time::Duration;
use tokio_signal::unix::Signal;

pub use deltas::{RemoteClientMessage, RemoteServerMessage};
pub use errors::DeltaGenerationError;

type Pool = diesel::r2d2::Pool<ConnectionManager<PgConnection>>;

pub fn load_config(path: &path::Path) -> Arc<Config> {
    let config_data = app::load_config(&path)
        .unwrap_or_else(|_| panic!("Failed to read config file {:?}", &path));
    Arc::new(config_data)
}

embed_migrations!();

fn connect_to_db(config: &Arc<Config>) -> r2d2::Pool<ConnectionManager<PgConnection>> {
    let manager = ConnectionManager::<PgConnection>::new(config.database_url.clone());

    {
        let conn = manager.connect().unwrap();
        embedded_migrations::run_with_output(&conn, &mut std::io::stdout()).unwrap();
    }

    r2d2::Pool::builder()
        .build(manager)
        .expect("Failed to create pool.")
}

fn start_delta_generator(config: &Arc<Config>) -> Addr<DeltaGenerator> {
    deltas::start_delta_generator(config.clone())
}

fn start_job_queue(
    config: &Arc<Config>,
    pool: &Pool,
    delta_generator: &Addr<DeltaGenerator>,
) -> Addr<JobQueue> {
    jobs::cleanup_started_jobs(pool).expect("Failed to cleanup started jobs");
    jobs::start_job_executor(config.clone(), delta_generator.clone(), pool.clone())
}

fn handle_signal(
    sig: i32,
    server: &Server,
    job_queue: Addr<JobQueue>,
    delta_generator: Addr<DeltaGenerator>,
) -> impl Future<Item = (), Error = std::io::Error> {
    let graceful = match sig {
        tokio_signal::unix::SIGINT => {
            info!("SIGINT received, exiting");
            false
        }
        tokio_signal::unix::SIGTERM => {
            info!("SIGTERM received, exiting");
            true
        }
        tokio_signal::unix::SIGQUIT => {
            info!("SIGQUIT received, exiting");
            false
        }
        _ => false,
    };

    info!("Stopping http server");
    server
        .stop(graceful)
        .then(move |_result| {
            info!("Stopping delta generator");
            delta_generator.send(StopDeltaGenerator())
        })
        .then(move |_result| {
            info!("Stopping job processing");
            job_queue.send(StopJobQueue())
        })
        .then(|_| {
            info!("Exiting...");
            let future = tokio::time::sleep(Duration::from_millis(300)).map(|_| {
                let result: Result<(), ()> = Ok(());
                result
            });
            Compat::new(Box::pin(future))
        })
        .then(|_| {
            System::current().stop();
            Ok(())
        })
}

fn handle_signals(
    server: Server,
    job_queue: Addr<JobQueue>,
    delta_generator: Addr<DeltaGenerator>,
) {
    let sigint = Signal::new(tokio_signal::unix::SIGINT).flatten_stream();
    let sigterm = Signal::new(tokio_signal::unix::SIGTERM).flatten_stream();
    let sigquit = Signal::new(tokio_signal::unix::SIGQUIT).flatten_stream();
    let handle_signals = sigint
        .select(sigterm)
        .select(sigquit)
        .for_each(move |sig| {
            handle_signal(sig, &server, job_queue.clone(), delta_generator.clone())
        })
        .map_err(|_| ());

    actix::spawn(handle_signals);
}

pub fn start(config: &Arc<Config>) -> Server {
    let pool = connect_to_db(config);

    let delta_generator = start_delta_generator(config);

    let job_queue = start_job_queue(config, &pool, &delta_generator);

    let app = app::create_app(pool, config, job_queue.clone(), delta_generator.clone());

    handle_signals(app.clone(), job_queue, delta_generator);

    app
}
