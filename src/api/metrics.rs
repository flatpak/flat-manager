use actix_web::web::Data;
use actix_web::{HttpResponse, Responder};
use std::sync::Arc;

use crate::metrics::Metrics;

pub async fn metrics(metrics: Data<Arc<Metrics>>) -> impl Responder {
    HttpResponse::Ok()
        .content_type("application/openmetrics-text; version=1.0.0; charset=utf-8")
        .body(metrics.encode())
}
