pub use flat_manager_common::tokens::*;

use actix_service::{Service, Transform};
use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::error::Error;
use actix_web::http::header::{HeaderValue, AUTHORIZATION};
use actix_web::{HttpMessage, HttpRequest, Result};
use jwt::{decode, DecodingKey, Validation};
use std::future::{ready, Future, Ready};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::db::Db;
use crate::errors::ApiError;

pub trait ClaimsValidator {
    fn get_claims(&self) -> Option<Claims>;
    fn validate_claims<Func>(&self, func: Func) -> Result<(), ApiError>
    where
        Func: Fn(&Claims) -> Result<(), ApiError>;
    fn has_token_claims(
        &self,
        required_sub: &str,
        required_scope: ClaimsScope,
    ) -> Result<(), ApiError>;
    fn has_token_prefix(&self, id: &str) -> Result<(), ApiError>;
    fn has_token_repo(&self, repo: &str) -> Result<(), ApiError>;
}
impl ClaimsValidator for HttpRequest {
    fn get_claims(&self) -> Option<Claims> {
        self.extensions().get::<Claims>().cloned()
    }

    fn validate_claims<Func>(&self, func: Func) -> Result<(), ApiError>
    where
        Func: Fn(&Claims) -> Result<(), ApiError>,
    {
        if let Some(claims) = self.extensions().get::<Claims>() {
            func(claims)
        } else {
            Err(ApiError::TokenRequired)
        }
    }

    fn has_token_claims(
        &self,
        required_sub: &str,
        required_scope: ClaimsScope,
    ) -> Result<(), ApiError> {
        self.validate_claims(|claims| {
            // Matches using a path-prefix style comparison:
            //  claim.sub == "build" should match required_sub == "build" or "build/N[/...]"
            //  claim.sub == "build/N" should only matchs required_sub == "build/N[/...]"
            if !sub_has_prefix(required_sub, &claims.sub) {
                return Err(ApiError::NotEnoughPermissions(format!(
                    "Not matching sub '{required_sub}' in token"
                )));
            }
            if !claims.scope.contains(&required_scope) {
                return Err(ApiError::NotEnoughPermissions(format!(
                    "Not matching scope '{required_scope}' in token"
                )));
            }
            Ok(())
        })
    }

    /* A token prefix is something like org.my.App, and should allow
     * you to create refs like org.my.App, org.my.App.Debug, and
     * org.my.App.Some.Long.Thing. However, it should not allow
     * org.my.AppSuffix. Also checks the "apps" field for exact matches
     * only.
     */
    fn has_token_prefix(&self, id: &str) -> Result<(), ApiError> {
        self.validate_claims(|claims| {
            if claims.prefixes.is_empty() {
                return Ok(());
            }
            if !id_matches_one_prefix(id, &claims.prefixes)
                && !claims.apps.contains(&id.to_string())
            {
                return Err(ApiError::NotEnoughPermissions(format!(
                    "Id {id} not matching prefix in token"
                )));
            }
            Ok(())
        })
    }

    fn has_token_repo(&self, repo: &str) -> Result<(), ApiError> {
        self.validate_claims(|claims| {
            if !repo_matches_one_claimed(repo, &claims.repos) {
                return Err(ApiError::NotEnoughPermissions(
                    "Not matching repo in token".to_string(),
                ));
            }
            Ok(())
        })
    }
}

pub struct Inner {
    db: Db,
    prefix: Option<String>,
    secret: Vec<u8>,
    optional: bool,
}

fn parse_authorization(prefix: Option<String>, header: &HeaderValue) -> Result<String, ApiError> {
    // "Bearer *" length
    if header.len() < 8 {
        return Err(ApiError::InvalidToken(
            "Header length too short".to_string(),
        ));
    }

    let mut parts = header
        .to_str()
        .map_err(|_| ApiError::InvalidToken("Cannot convert header to string".to_string()))?
        .splitn(2, ' ');
    match parts.next() {
        Some("Bearer") => (),
        _ => {
            return Err(ApiError::InvalidToken(
                "Token scheme is not Bearer".to_string(),
            ))
        }
    }

    let mut token = parts
        .next()
        .ok_or_else(|| ApiError::InvalidToken("No token value in header".to_string()))?;

    if let Some(prefix) = prefix {
        token = token.strip_prefix(&prefix).unwrap_or(token);
    }

    Ok(token.to_string())
}

fn validate_claims(secret: Vec<u8>, token: String) -> Result<Claims, ApiError> {
    let mut validation = Validation::default();

    validation.validate_exp = false;

    let token_data = match decode::<Claims>(
        &token,
        &DecodingKey::from_secret(secret.as_ref()),
        &validation,
    ) {
        Ok(c) => c,
        Err(_err) => return Err(ApiError::InvalidToken("Invalid token claims".to_string())),
    };

    let claims = token_data.claims;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    if claims.exp < now {
        return Err(ApiError::InvalidToken("Token is expired".to_string()));
    }

    Ok(claims)
}

pub struct TokenParser(Rc<Inner>);

impl TokenParser {
    pub fn new(db: Db, config: &Config, secret: &[u8]) -> TokenParser {
        TokenParser(Rc::new(Inner {
            db,
            prefix: config.token_prefix.clone(),
            secret: secret.to_vec(),
            optional: false,
        }))
    }
    pub fn optional(db: Db, config: &Config, secret: &[u8]) -> TokenParser {
        TokenParser(Rc::new(Inner {
            db,
            prefix: config.token_prefix.clone(),
            secret: secret.to_vec(),
            optional: true,
        }))
    }
}

impl<S: 'static, B> Transform<S, ServiceRequest> for TokenParser
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type InitError = ();
    type Transform = TokenParserMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(TokenParserMiddleware {
            service: Rc::new(service),
            inner: self.0.clone(),
        }))
    }
}

/// TokenParser middleware
pub struct TokenParserMiddleware<S> {
    service: Rc<S>,
    inner: Rc<Inner>,
}

fn get_token(
    optional: bool,
    prefix: Option<String>,
    req: &ServiceRequest,
) -> Result<Option<String>, ApiError> {
    let header = match req.headers().get(AUTHORIZATION) {
        Some(h) => h,
        None => {
            if optional {
                return Ok(None);
            }
            return Err(ApiError::InvalidToken(
                "No Authorization header".to_string(),
            ));
        }
    };
    let token = parse_authorization(prefix, header)?;
    Ok(Some(token))
}

async fn check_token_async(db: Db, secret: Vec<u8>, token: String) -> Result<Claims, ApiError> {
    let claims = validate_claims(secret, token)?;

    /* If the token has an ID, make sure it has not been revoked. */
    if let Some(jti) = &claims.jti {
        if let Err(e) = db.check_token(jti.clone(), claims.exp).await {
            log::warn!("Attempt to use a revoked token: '{jti}'");
            return Err(e);
        }
    }

    Ok(claims)
}

impl<S, B> Service<ServiceRequest> for TokenParserMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let srv = self.service.clone();
        let secret = self.inner.secret.clone();
        let prefix = self.inner.prefix.clone();
        let db = self.inner.db.clone();
        let optional = self.inner.optional;

        Box::pin(async move {
            let token = match get_token(optional, prefix, &req) {
                Ok(token) => token,
                Err(e) => return Ok(req.error_response(e).map_into_right_body()),
            };

            let maybe_claims = match token {
                Some(token) => match check_token_async(db, secret, token).await {
                    Ok(claims) => Some(claims),
                    Err(e) => return Ok(req.error_response(e).map_into_right_body()),
                },
                None => None,
            };

            let c = maybe_claims.clone();

            if let Some(claims) = maybe_claims {
                req.extensions_mut().insert(claims);
            }

            let resp = srv.call(req).await?.map_into_left_body();
            if resp.status() == 401 || resp.status() == 403 {
                if let Some(ref claims) = c {
                    log::info!("Presented claims: {:?}", claims);
                }
            }
            Ok(resp)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jwt::{encode, EncodingKey, Header};
    use serde_json::json;

    fn unix_now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[test]
    fn validate_claims_accepts_tokens_with_branches_claim() {
        let secret = b"compat-secret".to_vec();
        let token = encode(
            &Header::default(),
            &json!({
                "sub": "build",
                "exp": unix_now() + 60,
                "scope": ["build"],
                "branches": ["stable"],
            }),
            &EncodingKey::from_secret(secret.as_ref()),
        )
        .unwrap();

        let claims = validate_claims(secret, token).unwrap();

        assert_eq!(claims.sub, "build");
        assert!(claims.scope.contains(&ClaimsScope::Build));
    }
}
