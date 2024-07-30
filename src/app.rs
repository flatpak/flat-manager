use actix::prelude::*;
use actix_service::Service;
use actix_web::dev::Server;
use actix_web::web::Data;
use actix_web::{self, http, middleware, web, App, HttpResponse, HttpServer};
use base64::{engine::general_purpose, Engine as _};
use std::io;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use crate::api;
use crate::api::repo::apply_extra_headers;
use crate::config::Config;
use crate::db::Db;
use crate::deltas::DeltaGenerator;
use crate::jobs::JobQueue;
use crate::logger::Logger;
use crate::tokens::TokenParser;
use crate::Pool;

fn load_gpg_key(
    maybe_gpg_homedir: &Option<String>,
    maybe_gpg_key: &Option<String>,
) -> io::Result<Option<String>> {
    match maybe_gpg_key {
        Some(gpg_key) => {
            let mut cmd = Command::new("gpg2");
            if let Some(gpg_homedir) = maybe_gpg_homedir {
                cmd.arg(&format!("--homedir={gpg_homedir}"));
            }
            cmd.arg("--export").arg(gpg_key);

            let output = cmd.output()?;
            if output.status.success() {
                Ok(Some(general_purpose::STANDARD.encode(&output.stdout)))
            } else {
                Err(io::Error::new(io::ErrorKind::Other, "gpg2 --export failed"))
            }
        }
        None => Ok(None),
    }
}

pub fn load_config<P: AsRef<Path>>(path: P) -> io::Result<Config> {
    let config_contents = std::fs::read_to_string(path)?;
    let mut config_data: Config = serde_json::from_str(&config_contents)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

    config_data.build_gpg_key_content =
        load_gpg_key(&config_data.gpg_homedir, &config_data.build_gpg_key)?;
    for (reponame, repoconfig) in &mut config_data.repos {
        reponame.clone_into(&mut repoconfig.name);
        repoconfig.gpg_key_content =
            load_gpg_key(&config_data.gpg_homedir, &config_data.build_gpg_key)?;
    }

    if config_data.base_url.is_empty() {
        config_data.base_url = format!("http://{}:{}", config_data.host, config_data.port)
    }

    Ok(config_data)
}

pub fn create_app(
    pool: Pool,
    config: &Arc<Config>,
    job_queue: Addr<JobQueue>,
    delta_generator: Addr<DeltaGenerator>,
) -> Server {
    let c = config.clone();
    let secret = config.secret.clone();
    let repo_secret = config
        .repo_secret
        .as_ref()
        .unwrap_or_else(|| config.secret.as_ref())
        .clone();

    let db = Db(pool);

    let http_server = HttpServer::new(move || {
        App::new()
            .data(job_queue.clone())
            .data(delta_generator.clone())
            .register_data(Data::new((*c).clone()))
            .data(db.clone())
            .wrap(Logger::default())
            .wrap(middleware::Compress::new(
                http::header::ContentEncoding::Identity,
            ))
            .service(
                web::scope("/api/v1")
                    .wrap(TokenParser::new(db.clone(), &c, &secret))
                    .service(
                        web::resource("/tokens/get_list")
                            .route(web::post().to_async(api::tokens::get_tokens)),
                    )
                    .service(
                        web::resource("/tokens/revoke")
                            .route(web::post().to_async(api::tokens::revoke_tokens)),
                    )
                    .service(
                        web::resource("/token_subset")
                            .route(web::post().to(api::build::token_subset)),
                    )
                    .service(
                        web::resource("/job/{id}")
                            .name("show_job")
                            .route(web::get().to_async(api::build::get_job)),
                    )
                    .service(
                        web::resource("/job/{id}/check/review")
                            .name("review_check")
                            .route(web::post().to_async(api::build::review_check)),
                    )
                    .service(
                        web::resource("/build")
                            .route(web::post().to_async(api::build::create_build))
                            .route(web::get().to_async(api::build::builds)),
                    )
                    .service(
                        web::resource("/build/{id}")
                            .name("show_build")
                            .route(web::get().to_async(api::build::get_build)),
                    )
                    .service(
                        web::resource("/build/{id}/extended")
                            .name("show_build_extended")
                            .route(web::get().to_async(api::build::get_build_extended)),
                    )
                    .service(
                        web::resource("/build/{id}/build_ref")
                            .route(web::post().to_async(api::build::create_build_ref)),
                    )
                    .service(
                        web::resource("/build/{id}/build_ref/{ref_id}")
                            .name("show_build_ref")
                            .route(web::get().to_async(api::build::get_build_ref)),
                    )
                    .service(
                        web::resource("/build/{id}/missing_objects")
                            .data(web::JsonConfig::default().limit(1024 * 1024 * 10))
                            .route(web::get().to_async(api::build::missing_objects)),
                    )
                    .service(
                        web::resource("/build/{id}/add_extra_ids")
                            .route(web::post().to_async(api::build::add_extra_ids)),
                    )
                    .service(
                        web::resource("/build/{id}/upload")
                            .route(web::post().to_async(api::build::upload)),
                    )
                    .service(
                        web::resource("/build/{id}/commit")
                            .name("show_commit_job")
                            .route(web::post().to_async(api::build::commit))
                            .route(web::get().to_async(api::build::get_commit_job)),
                    )
                    .service(
                        web::resource("/build/{id}/publish")
                            .name("show_publish_job")
                            .route(web::post().to_async(api::build::publish))
                            .route(web::get().to_async(api::build::get_publish_job)),
                    )
                    .service(
                        web::resource("/build/{id}/check/{check_name}/job")
                            .name("show_check_job")
                            .route(web::get().to_async(api::build::get_check_job)),
                    )
                    .service(
                        web::resource("/build/{id}/purge")
                            .route(web::post().to_async(api::build::purge)),
                    )
                    .service(
                        web::resource("/repo/{repo}/republish")
                            .route(web::post().to_async(api::build::republish)),
                    )
                    .service(
                        web::resource("/delta/worker").route(web::get().to(api::delta::ws_delta)),
                    )
                    .service(
                        web::resource("/delta/upload/{repo}")
                            .route(web::post().to_async(api::delta::delta_upload)),
                    ),
            )
            .service(
                web::scope("/repo")
                    .wrap(TokenParser::optional(db.clone(), &c, &repo_secret))
                    .wrap_fn(|req, srv| {
                        srv.call(req).map(|mut resp| {
                            apply_extra_headers(&mut resp);
                            resp
                        })
                    })
                    .service(
                        web::resource("/{tail:.*}")
                            .name("repo")
                            .route(web::get().to(api::repo::handle_repo))
                            .route(web::head().to(api::repo::handle_repo))
                            .to(HttpResponse::MethodNotAllowed),
                    ),
            )
            .service(
                web::resource("/build-repo/{id}/{tail:.*}")
                    .wrap(TokenParser::optional(db.clone(), &c, &secret))
                    .route(web::get().to_async(api::repo::handle_build_repo))
                    .route(web::head().to_async(api::repo::handle_build_repo))
                    .to(HttpResponse::MethodNotAllowed),
            )
            .service(web::resource("/status").route(web::get().to_async(api::status::status)))
            .service(
                web::resource("/status/{id}").route(web::get().to_async(api::status::job_status)),
            )
    });

    let bind_to = format!("{}:{}", config.host, config.port);
    let server = http_server
        .bind(&bind_to)
        .unwrap()
        .disable_signals()
        .start();

    log::info!("Started http server: {}", bind_to);

    server
}
