use actix_web::web::{Data, Json};
use actix_web::{HttpRequest, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;

use crate::db::Db;
use crate::errors::ApiError;
use crate::tokens::{ClaimsScope, ClaimsValidator};
use crate::webhooks::{self, WebhookQueue};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedeliverArgs {
    before: DateTime<Utc>,
}

pub async fn redeliver(
    args: Json<RedeliverArgs>,
    db: Data<Db>,
    queue: Data<Option<WebhookQueue>>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    req.validate_claims(|claims| {
        if !claims.scope.contains(&ClaimsScope::TokenManagement) {
            return Err(ApiError::NotEnoughPermissions(
                "Missing TokenManagement scope".to_string(),
            ));
        }
        Ok(())
    })?;

    let queue = queue
        .as_ref()
        .as_ref()
        .ok_or_else(|| ApiError::BadRequest("Webhooks are not enabled".to_string()))?;
    let queued = webhooks::redeliver(&db.pool, queue, args.before).await?;

    Ok(HttpResponse::Ok().json(json!({ "queued": queued })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::Claims;
    use actix_web::HttpMessage;
    use diesel::pg::PgConnection;
    use diesel::r2d2::ConnectionManager;

    fn request(scope: Vec<ClaimsScope>) -> HttpRequest {
        let req = actix_web::test::TestRequest::default().to_http_request();
        req.extensions_mut().insert(Claims {
            name: None,
            sub: "".to_string(),
            exp: i64::MAX,
            jti: None,
            scope,
            prefixes: vec![],
            apps: vec![],
            repos: vec![],
            branches: vec![],
            token_type: None,
        });
        req
    }

    fn args() -> Json<RedeliverArgs> {
        Json(serde_json::from_str(r#"{"before":"2026-11-23T09:30:00Z"}"#).unwrap())
    }

    fn db() -> Data<Db> {
        let manager = ConnectionManager::<PgConnection>::new("postgres://unused");
        Data::new(Db {
            pool: crate::Pool::builder().build_unchecked(manager),
            metrics: None,
        })
    }

    #[test]
    fn args_take_an_rfc3339_before_timestamp() {
        let args: RedeliverArgs =
            serde_json::from_str(r#"{"before":"2026-11-23T10:30:00+01:00"}"#).unwrap();

        assert_eq!(
            args.before,
            DateTime::parse_from_rfc3339("2026-11-23T09:30:00Z").unwrap()
        );
        for invalid in [
            r#"{}"#,
            r#"{"before":"yesterday"}"#,
            r#"{"before":1795000000}"#,
            r#"{"before":"2026-11-23T09:30:00Z","after":"2026-11-23T09:30:00Z"}"#,
        ] {
            assert!(
                serde_json::from_str::<RedeliverArgs>(invalid).is_err(),
                "{invalid}"
            );
        }
    }

    #[tokio::test]
    async fn redeliver_requires_the_token_management_scope() {
        let without_token = actix_web::test::TestRequest::default().to_http_request();
        let wrong_scope = request(vec![ClaimsScope::Build, ClaimsScope::Publish]);

        for req in [without_token, wrong_scope] {
            let result = redeliver(args(), db(), Data::new(None), req).await;

            assert!(matches!(result, Err(ApiError::NotEnoughPermissions(_))));
        }
    }

    #[tokio::test]
    async fn redeliver_fails_when_webhooks_are_not_enabled() {
        let req = request(vec![ClaimsScope::TokenManagement]);

        let result = redeliver(args(), db(), Data::new(None), req).await;

        assert!(matches!(result, Err(ApiError::BadRequest(_))));
    }
}
