#![allow(proc_macro_derive_resolution_fallback)]

extern crate actix;
extern crate actix_net;
extern crate actix_web;
extern crate askama;
extern crate base64;
extern crate byteorder;
extern crate chrono;
#[macro_use] extern crate diesel;
#[macro_use] extern crate diesel_migrations;
extern crate env_logger;
#[macro_use] extern crate failure;
extern crate futures;
extern crate r2d2;
extern crate serde;
#[macro_use] extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate tempfile;
extern crate jsonwebtoken as jwt;
#[macro_use] extern crate log;
extern crate libc;
extern crate walkdir;
extern crate hex;
extern crate filetime;
extern crate num_cpus;
extern crate time;
extern crate tokio;
extern crate tokio_process;
extern crate rand;
extern crate glib;
extern crate glib_sys;

mod api;
mod app;
mod db;
pub mod errors;
mod models;
mod schema;
mod tokens;
mod jobs;
pub mod ostree;
mod deltas;
mod delayed;
mod logger;

use actix::prelude::*;
use actix::{Actor, actors::signal};
use actix_web::{server, server::StopServer};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, ManageConnection, Pool};
use std::path;
use std::sync::Arc;
use std::time::Duration;

use app::Config;
use deltas::{DeltaGenerator,StopDeltaGenerator};
use jobs::{JobQueue, StopJobQueue};
use models::DbExecutor;

pub use deltas::{RemoteClientMessage,RemoteServerMessage};
pub use errors::{DeltaGenerationError};

pub fn load_config(path: &path::Path) -> Arc<Config> {
    let config_data = app::load_config(&path).expect(&format!("Failed to read config file {:?}", &path));
    Arc::new(config_data)
}

embed_migrations!();

fn connect_to_db(config: &Arc<Config>) -> Pool<ConnectionManager<PgConnection>> {
    let manager = ConnectionManager::<PgConnection>::new(config.database_url.clone());

    {
        let conn = manager.connect().unwrap();
        embedded_migrations::run_with_output(&conn, &mut std::io::stdout()).unwrap();
    }

    let pool = r2d2::Pool::builder()
        .build(manager)
        .expect("Failed to create pool.");

    pool
}

fn start_delta_generator(config: &Arc<Config>) -> Addr<DeltaGenerator> {
    deltas::start_delta_generator(config.clone())
}

fn start_db_executor(pool: &Pool<ConnectionManager<PgConnection>>) -> Addr<DbExecutor> {
    let pool_copy = pool.clone();
    SyncArbiter::start(3, move || DbExecutor(pool_copy.clone()))
}

fn start_job_queue(config: &Arc<Config>,
                   pool: &Pool<ConnectionManager<PgConnection>>,
                   delta_generator: &Addr<DeltaGenerator>) -> Addr<JobQueue> {
    jobs::cleanup_started_jobs(&pool).expect("Failed to cleanup started jobs");
    jobs::start_job_executor(config.clone(), delta_generator.clone(), pool.clone())
}

fn start_service(config: &Arc<Config>,
                 db_executor: &Addr<DbExecutor>,
                 job_queue: &Addr<JobQueue>,
                 delta_generator: &Addr<DeltaGenerator>) -> Addr<actix_net::server::Server> {
    let db_executor_copy = db_executor.clone();
    let job_queue_copy = job_queue.clone();
    let config_copy = config.clone();
    let delta_generator_copy = delta_generator.clone();
    let http_server = server::new(move || {
        app::create_app(db_executor_copy.clone(), &config_copy, job_queue_copy.clone(), &delta_generator_copy)
    });

    let bind_to = format!("{}:{}", config.host, config.port);
    let server =
        http_server
        .bind(&bind_to)
        .unwrap()
        .disable_signals()
        .start();

    info!("Started http server: {}", bind_to);

    server
}

struct HandleSignals {
    server: Addr<actix_net::server::Server>,
    job_queue: Addr<JobQueue>,
    delta_generator: Addr<DeltaGenerator>,
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
                        actor.delta_generator
                            .send(StopDeltaGenerator())
                            .into_actor(actor)
                    })
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

fn handle_signals(server: &Addr<actix_net::server::Server>,
                  job_queue: &Addr<JobQueue>,
                  delta_generator: &Addr<DeltaGenerator>) {
    let signal_handler = HandleSignals{
        server: server.clone(),
        job_queue: job_queue.clone(),
        delta_generator: delta_generator.clone(),
    }.start();

    let signals = System::current().registry().get::<signal::ProcessSignals>();
    signals.do_send(signal::Subscribe(signal_handler.clone().recipient()));
}


pub fn start(config: &Arc<Config>) -> actix::Addr<actix_net::server::Server>{
    let pool = connect_to_db(config);

    let delta_generator = start_delta_generator(config);

    let db_executor = start_db_executor(&pool);
    let job_queue = start_job_queue(config, &pool, &delta_generator);

    let server = start_service(config, &db_executor, &job_queue, &delta_generator);

    handle_signals(&server, &job_queue, &delta_generator);

    server
}
