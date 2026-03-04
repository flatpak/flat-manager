use actix_web::web::{Data, Json};
use actix_web::{HttpRequest, HttpResponse, Result};
use serde::Deserialize;

use crate::db::Db;
use crate::errors::ApiError;
use crate::tokens::{ClaimsScope, ClaimsValidator};

#[derive(Deserialize)]
pub struct TokenArgs {
    token_ids: Vec<String>,
}

pub async fn get_tokens(
    args: Json<TokenArgs>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims("", ClaimsScope::TokenManagement)?;

    let tokens = db.get_tokens(args.token_ids.clone()).await?;

    Ok(HttpResponse::Ok().json(tokens))
}

pub async fn revoke_tokens(
    args: Json<TokenArgs>,
    db: Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.has_token_claims("", ClaimsScope::TokenManagement)?;

    db.revoke_tokens(args.token_ids.clone()).await?;

    Ok(HttpResponse::NoContent().finish())
}
