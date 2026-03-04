use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

mod api;
mod app;
mod config;
mod db;
mod delayed;
mod deltas;
pub mod errors;
mod jobs;
mod logger;
mod models;
pub mod ostree;
mod schema;
pub mod tokens;

use actix::prelude::*;
use actix_web::dev::{Server, ServerHandle};
use config::Config;
use deltas::{DeltaGenerator, StopDeltaGenerator};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, ManageConnection};
use jobs::{JobQueue, StopJobQueue};
use log::info;
use std::path;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};

pub use errors::DeltaGenerationError;

type Pool = diesel::r2d2::Pool<ConnectionManager<PgConnection>>;

pub fn load_config(path: &path::Path) -> Arc<Config> {
    let config_data =
        app::load_config(path).unwrap_or_else(|_| panic!("Failed to read config file {:?}", &path));
    Arc::new(config_data)
}
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations/");

fn connect_to_db(config: &Arc<Config>) -> r2d2::Pool<ConnectionManager<PgConnection>> {
    let manager = ConnectionManager::<PgConnection>::new(config.database_url.clone());
    {
        let mut conn = manager.connect().unwrap();
        log::info!("Running DB Migrations...");
        conn.run_pending_migrations(MIGRATIONS)
            .expect("Failed to run migrations");
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

fn handle_signals(
    server: ServerHandle,
    job_queue: Addr<JobQueue>,
    delta_generator: Addr<DeltaGenerator>,
) {
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sigquit = signal(SignalKind::quit()).unwrap();

        let graceful = tokio::select! {
            _ = sigint.recv() => {
                info!("SIGINT received, exiting");
                false
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received, exiting");
                true
            }
            _ = sigquit.recv() => {
                info!("SIGQUIT received, exiting");
                false
            }
        };

        info!("Stopping http server");
        server.stop(graceful).await;

        info!("Stopping delta generator");
        let _ = delta_generator.send(StopDeltaGenerator()).await;

        info!("Stopping job processing");
        let _ = job_queue.send(StopJobQueue()).await;

        info!("Exiting...");
        tokio::time::sleep(Duration::from_millis(300)).await;
        System::current().stop();
    });
}

pub fn start(config: &Arc<Config>) -> Server {
    let pool = connect_to_db(config);

    let delta_generator = start_delta_generator(config);

    let job_queue = start_job_queue(config, &pool, &delta_generator);

    let app = app::create_app(pool, config, job_queue.clone(), delta_generator.clone());

    handle_signals(app.handle(), job_queue, delta_generator);

    app
}
