use actix_web::{HttpRequest, Result};
use actix_web::error::{ParseError, ErrorUnauthorized};
use actix_web::http::header::{HeaderValue, AUTHORIZATION};
use actix_web::middleware::{Middleware, Started};
use jwt::{decode, Validation};

use app::Claims;
use errors::ApiError;

pub trait ClaimsValidator {
    fn validate_claims<Func>(&self, func: Func) -> bool
        where Func: Fn(&Claims) -> bool;
    fn has_token_claims(&self, required_sub: &str, required_scope: &str) -> Result<(), ApiError>;
}

impl<S> ClaimsValidator for HttpRequest<S> {
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
                if required_sub.starts_with(&claims.sub) {
                    let rest = &required_sub[claims.sub.len()..];
                    if rest.len() == 0 || rest.starts_with("/") {
                        return claims.scope.contains(&required_scope.to_string())
                    }
                }
                false
            }) {
            Ok(())
        } else {
            Err(ApiError::InvalidToken("Token invalid".to_string()))
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

    fn parse_authorization(&self, header: &HeaderValue) -> Result<String, ParseError> {
        // "Bearer *" length
        if header.len() < 8 {
            return Err(ParseError::Header);
        }

        let mut parts = header.to_str().or(Err(ParseError::Header))?.splitn(2, ' ');
        match parts.next() {
            Some(scheme) if scheme == "Bearer" => (),
            _ => return Err(ParseError::Header),
        }

        let token = parts.next().ok_or(ParseError::Header)?;

        Ok(token.to_string())
    }

    fn validate_claims(&self, token: String) -> Result<Claims> {
        let validation = Validation {
            ..Validation::default()
        };

        let token_data = match decode::<Claims>(&token, &self.secret, &validation) {
            Ok(c) => c,
            Err(err) => return Err(ErrorUnauthorized(err)),
        };

        Ok(token_data.claims)
    }
}

impl<S: 'static> Middleware<S> for TokenParser {
    fn start(&self, req: &HttpRequest<S>) -> Result<Started> {
        let header = req.headers().get(AUTHORIZATION).ok_or(ErrorUnauthorized("No bearer token"))?;
        let token = self.parse_authorization(header).or(Err(ErrorUnauthorized("Invalid bearer token")))?;
        let claims = self.validate_claims(token)?;

        req.extensions_mut().insert(claims);

        Ok(Started::Done)
    }
}
