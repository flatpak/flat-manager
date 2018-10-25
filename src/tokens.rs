use actix_web::{HttpRequest, Result};
use actix_web::error::{ParseError, ErrorUnauthorized};
use actix_web::http::header::{HeaderValue, AUTHORIZATION};
use actix_web::middleware::{Middleware, Started};
use jwt::{decode, Validation};

use app::Claims;

pub trait ValidateClaims {
    fn validate_claim<F>(&self, func: F) -> bool
        where F: Fn(&Claims) -> bool;
}

impl<S> ValidateClaims for HttpRequest<S> {
    fn validate_claim<F>(&self, func: F) -> bool
        where F: Fn(&Claims) -> bool {
        if let Some(claims) = self.extensions().get::<Claims>() {
            return func(claims)
        }
        false
    }
}

pub struct TokenService {
    secret: Vec<u8>,
}

impl TokenService {
    pub fn new(secret: &[u8]) -> Self {
        TokenService { secret: secret.to_vec() }
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

impl<S: 'static> Middleware<S> for TokenService {
    fn start(&self, req: &HttpRequest<S>) -> Result<Started> {
        let header = req.headers().get(AUTHORIZATION).ok_or(ErrorUnauthorized("No bearer token"))?;
        let token = self.parse_authorization(header).or(Err(ErrorUnauthorized("Invalid bearer token")))?;
        let claims = self.validate_claims(token)?;

        req.extensions_mut().insert(claims);

        Ok(Started::Done)
    }
}
