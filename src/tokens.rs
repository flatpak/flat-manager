use actix_web::{HttpRequest, Result};
use actix_web::middleware::{Middleware, Started};
use actix_web::http::header::{HeaderValue, AUTHORIZATION};
use jwt::{decode, Validation};

use app::Claims;
use errors::ApiError;

pub trait ClaimsValidator {
    fn get_claims(&self) -> Option<Claims>;
    fn validate_claims<Func>(&self, func: Func) -> bool
        where Func: Fn(&Claims) -> bool;
    fn has_token_claims(&self, required_sub: &str, required_scope: &str) -> Result<(), ApiError>;
}

pub fn sub_has_prefix(required_sub: &str, claimed_sub: &str) -> bool {
    // Matches using a path-prefix style comparison:
    //  claimed_sub == "build" should match required_sub == "build" or "build/N[/...]"
    //  claimed_sub == "build/N" should only matchs required_sub == "build/N[/...]"
    if required_sub.starts_with(claimed_sub) {
        let rest = &required_sub[claimed_sub.len()..];
        if rest.len() == 0 || rest.starts_with("/") {
            return true
        }
    };
    false
}

impl<S> ClaimsValidator for HttpRequest<S> {
    fn get_claims(&self) -> Option<Claims> {
        self.extensions().get::<Claims>().cloned()
    }

    fn validate_claims<Func>(&self, func: Func) -> bool
        where Func: Fn(&Claims) -> bool {
        if let Some(claims) = self.extensions().get::<Claims>() {
            func(claims)
        } else {
            false
        }
    }

    fn has_token_claims(&self, required_sub: &str, required_scope: &str) -> Result<(), ApiError> {
        if self.validate_claims(
            |claims| {
                // Matches using a path-prefix style comparison:
                //  claim.sub == "build" should match required_sub == "build" or "build/N[/...]"
                //  claim.sub == "build/N" should only matchs required_sub == "build/N[/...]"
                if sub_has_prefix(required_sub, &claims.sub) {
                    claims.scope.contains(&required_scope.to_string())
                } else {
                    false
                }
            }) {
            Ok(())
        } else {
            Err(ApiError::NotEnoughPermissions)
        }
    }
}

pub struct TokenParser {
    secret: Vec<u8>,
}

impl TokenParser {
    pub fn new(secret: &[u8]) -> Self {
        TokenParser { secret: secret.to_vec() }
    }

    fn parse_authorization(&self, header: &HeaderValue) -> Result<String, ApiError> {
        // "Bearer *" length
        if header.len() < 8 {
            return Err(ApiError::InvalidToken);
        }

        let mut parts = header.to_str().or(Err(ApiError::InvalidToken))?.splitn(2, ' ');
        match parts.next() {
            Some(scheme) if scheme == "Bearer" => (),
            _ => return Err(ApiError::InvalidToken),
        }

        let token = parts.next().ok_or(ApiError::InvalidToken)?;

        Ok(token.to_string())
    }

    fn validate_claims(&self, token: String) -> Result<Claims, ApiError> {
        let validation = Validation {
            ..Validation::default()
        };

        let token_data = match decode::<Claims>(&token, &self.secret, &validation) {
            Ok(c) => c,
            Err(_err) => return Err(ApiError::InvalidToken),
        };

        Ok(token_data.claims)
    }
}

impl<S: 'static> Middleware<S> for TokenParser {
    fn start(&self, req: &HttpRequest<S>) -> Result<Started> {
        let header = req.headers().get(AUTHORIZATION).ok_or(ApiError::InvalidToken)?;
        let token = self.parse_authorization(header)?;
        let claims = self.validate_claims(token)?;

        info!("{} Presented token: {:?}", req.connection_info().remote().unwrap_or("-"), &claims);

        req.extensions_mut().insert(claims);

        Ok(Started::Done)
    }
}
