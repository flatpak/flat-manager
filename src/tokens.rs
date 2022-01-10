use actix_service::{Service, Transform};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::error::Error;
use actix_web::http::header::{HeaderValue, AUTHORIZATION};
use actix_web::{HttpMessage, HttpRequest, Result};
use futures::future::{ok, Either, FutureResult};
use futures::{Future, Poll};
use jwt::{decode, DecodingKey, Validation};
use std::rc::Rc;

use crate::app::Claims;
use crate::errors::ApiError;

pub trait ClaimsValidator {
    fn get_claims(&self) -> Option<Claims>;
    fn validate_claims<Func>(&self, func: Func) -> Result<(), ApiError>
    where
        Func: Fn(&Claims) -> Result<(), ApiError>;
    fn has_token_claims(&self, required_sub: &str, required_scope: &str) -> Result<(), ApiError>;
    fn has_token_prefix(&self, id: &str) -> Result<(), ApiError>;
    fn has_token_repo(&self, repo: &str) -> Result<(), ApiError>;
}

pub fn sub_has_prefix(required_sub: &str, claimed_sub: &str) -> bool {
    // Matches using a path-prefix style comparison:
    //  claimed_sub == "build" should match required_sub == "build" or "build/N[/...]"
    //  claimed_sub == "build/N" should only matchs required_sub == "build/N[/...]"
    if let Some(rest) = required_sub.strip_prefix(claimed_sub) {
        if rest.is_empty() || rest.starts_with('/') {
            return true;
        }
    };
    false
}

pub fn id_matches_prefix(id: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    if let Some(rest) = id.strip_prefix(prefix) {
        if rest.is_empty() || rest.starts_with('.') {
            return true;
        }
    };
    false
}

pub fn id_matches_one_prefix(id: &str, prefixes: &[String]) -> bool {
    prefixes.iter().any(|prefix| id_matches_prefix(id, prefix))
}

pub fn repo_matches_claimed(repo: &str, claimed_repo: &str) -> bool {
    if claimed_repo.is_empty() {
        return true;
    }
    repo == claimed_repo
}

pub fn repo_matches_one_claimed(repo: &str, claimed_repos: &[String]) -> bool {
    claimed_repos
        .iter()
        .any(|claimed_repo| repo_matches_claimed(repo, claimed_repo))
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
            Err(ApiError::NotEnoughPermissions(
                "No token specified".to_string(),
            ))
        }
    }

    fn has_token_claims(&self, required_sub: &str, required_scope: &str) -> Result<(), ApiError> {
        self.validate_claims(|claims| {
            // Matches using a path-prefix style comparison:
            //  claim.sub == "build" should match required_sub == "build" or "build/N[/...]"
            //  claim.sub == "build/N" should only matchs required_sub == "build/N[/...]"
            if !sub_has_prefix(required_sub, &claims.sub) {
                return Err(ApiError::NotEnoughPermissions(format!(
                    "Not matching sub '{}' in token",
                    required_sub
                )));
            }
            if !claims.scope.contains(&required_scope.to_string()) {
                return Err(ApiError::NotEnoughPermissions(format!(
                    "Not matching scope '{}' in token",
                    required_scope
                )));
            }
            Ok(())
        })
    }

    /* A token prefix is something like org.my.App, and should allow
     * you to create refs like org.my.App, org.my.App.Debug, and
     * org.my.App.Some.Long.Thing. However, it should not allow
     * org.my.AppSuffix.
     */
    fn has_token_prefix(&self, id: &str) -> Result<(), ApiError> {
        self.validate_claims(|claims| {
            if !id_matches_one_prefix(id, &claims.prefixes) {
                return Err(ApiError::NotEnoughPermissions(format!(
                    "Id {} not matching prefix in token",
                    id
                )));
            }
            Ok(())
        })
    }

    fn has_token_repo(&self, repo: &str) -> Result<(), ApiError> {
        self.validate_claims(|claims| {
            if !repo_matches_one_claimed(&repo.to_string(), &claims.repos) {
                return Err(ApiError::NotEnoughPermissions(
                    "Not matching repo in token".to_string(),
                ));
            }
            Ok(())
        })
    }
}

pub struct Inner {
    secret: Vec<u8>,
    optional: bool,
}

impl Inner {
    fn parse_authorization(&self, header: &HeaderValue) -> Result<String, ApiError> {
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
            Some(scheme) if scheme == "Bearer" => (),
            _ => {
                return Err(ApiError::InvalidToken(
                    "Token scheme is not Bearer".to_string(),
                ))
            }
        }

        let token = parts
            .next()
            .ok_or_else(|| ApiError::InvalidToken("No token value in header".to_string()))?;

        Ok(token.to_string())
    }

    fn validate_claims(&self, token: String) -> Result<Claims, ApiError> {
        let validation = Validation {
            ..Validation::default()
        };

        let token_data = match decode::<Claims>(
            &token,
            &DecodingKey::from_secret(self.secret.as_ref()),
            &validation,
        ) {
            Ok(c) => c,
            Err(_err) => return Err(ApiError::InvalidToken("Invalid token claims".to_string())),
        };

        Ok(token_data.claims)
    }
}

pub struct TokenParser(Rc<Inner>);

impl TokenParser {
    pub fn new(secret: &[u8]) -> TokenParser {
        TokenParser(Rc::new(Inner {
            secret: secret.to_vec(),
            optional: false,
        }))
    }
    pub fn optional(secret: &[u8]) -> TokenParser {
        TokenParser(Rc::new(Inner {
            secret: secret.to_vec(),
            optional: true,
        }))
    }
}

impl<S: 'static, B> Transform<S> for TokenParser
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = TokenParserMiddleware<S>;
    type Future = FutureResult<Self::Transform, Self::InitError>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(TokenParserMiddleware {
            service,
            inner: self.0.clone(),
        })
    }
}

/// TokenParser middleware
pub struct TokenParserMiddleware<S> {
    service: S,
    inner: Rc<Inner>,
}

impl<S> TokenParserMiddleware<S> {
    fn check_token(&self, req: &ServiceRequest) -> Result<Option<Claims>, ApiError> {
        let header = match req.headers().get(AUTHORIZATION) {
            Some(h) => h,
            None => {
                if self.inner.optional {
                    return Ok(None);
                }
                return Err(ApiError::InvalidToken(
                    "No Authorization header".to_string(),
                ));
            }
        };
        let token = self.inner.parse_authorization(header)?;
        let claims = self.inner.validate_claims(token)?;
        Ok(Some(claims))
    }
}

impl<S, B> Service for TokenParserMiddleware<S>
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = Error;
    #[allow(clippy::type_complexity)]
    type Future = Either<
        //S::Future,
        Box<dyn Future<Item = Self::Response, Error = Self::Error>>,
        FutureResult<Self::Response, Self::Error>,
    >;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.service.poll_ready()
    }

    fn call(&mut self, req: ServiceRequest) -> Self::Future {
        let maybe_claims = match self.check_token(&req) {
            Err(e) => return Either::B(ok(req.error_response(e))),
            Ok(c) => c,
        };

        let c = maybe_claims.clone();

        if let Some(claims) = maybe_claims {
            req.extensions_mut().insert(claims);
        }

        Either::A(Box::new(self.service.call(req).and_then(move |resp| {
            if resp.status() == 401 || resp.status() == 403 {
                if let Some(ref claims) = c {
                    log::info!("Presented claims: {:?}", claims);
                }
            }
            Ok(resp)
        })))
    }
}
