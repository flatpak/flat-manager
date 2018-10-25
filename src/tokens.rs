use actix_web::{HttpRequest, Result};
use actix_web::error::{ParseError, ErrorUnauthorized};
use actix_web::http::header::{HeaderValue, AUTHORIZATION};
use actix_web::middleware::{Middleware, Started};
use jwt::{decode, Validation};

use app::Claims;

pub trait ClaimsValidator {
    fn validate_claims(&self, claims: &Claims) -> bool;
}

pub struct SubValidator {
    sub: String,
}
impl SubValidator {
    pub fn new(sub: &str) -> Self {
        SubValidator { sub: sub.to_string() }
    }
}

impl ClaimsValidator for SubValidator {
    fn validate_claims(&self, claims: &Claims) -> bool {
        claims.sub == self.sub
    }
}

pub struct TokenCheck<T>(pub T);

impl<S: 'static, T: ClaimsValidator + 'static> Middleware<S> for TokenCheck<T> {
    fn start(&self, req: &HttpRequest<S>) -> Result<Started> {
        if let Some(claims) = req.extensions().get::<Claims>() {
            if self.0.validate_claims(claims) {
                return Ok(Started::Done);
            }
        };

        Err(ErrorUnauthorized("Failed to match claims"))
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
            sub: Some("repo".to_string()),
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
