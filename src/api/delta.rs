use actix::prelude::*;
use actix_multipart::Multipart;
use actix_web::web::{Data, Path};
use actix_web::{web, HttpRequest, HttpResponse, ResponseError, Result};
use actix_web_actors::ws;

use futures::future::Future;
use serde::Deserialize;
use std::clone::Clone;
use std::sync::Arc;

use crate::config::Config;
use crate::deltas::{DeltaGenerator, RemoteWorker};
use crate::errors::ApiError;
use crate::tokens::{ClaimsScope, ClaimsValidator};

use super::utils::{save_file, UploadState};

#[derive(Deserialize)]
pub struct DeltaUploadParams {
    repo: String,
}

pub fn delta_upload(
    multipart: Multipart,
    params: Path<DeltaUploadParams>,
    req: HttpRequest,
    config: Data<Config>,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    futures::done(req.has_token_claims("delta", ClaimsScope::Generate))
        .and_then(move |_| futures::done(config.get_repoconfig(&params.repo).map(|rc| rc.clone())))
        .and_then(move |repoconfig| {
            let uploadstate = Arc::new(UploadState {
                only_deltas: true,
                repo_path: repoconfig.get_abs_repo_path(),
            });
            multipart
                .map_err(|e| ApiError::InternalServerError(e.to_string()))
                .map(move |field| save_file(field, &uploadstate).into_stream())
                .flatten()
                .collect()
                .map(|sizes| HttpResponse::Ok().json(sizes))
        })
}

pub fn ws_delta(
    req: HttpRequest,
    config: Data<Config>,
    delta_generator: Data<Addr<DeltaGenerator>>,
    stream: web::Payload,
) -> Result<HttpResponse, actix_web::Error> {
    if let Err(e) = req.has_token_claims("delta", ClaimsScope::Generate) {
        return Ok(e.error_response());
    }
    let remote = req
        .connection_info()
        .remote()
        .unwrap_or("Unknown")
        .to_string();
    ws::start(
        RemoteWorker::new(&config, &delta_generator, remote),
        &req,
        stream,
    )
}
