use actix_web::{HttpRequest, HttpResponse, Result};
use actix_web::middleware::{Middleware, Started, Finished};
use actix_web::http::header::{HeaderValue, AUTHORIZATION};
use jwt::{decode, Validation};

use app::{AppState,Claims};
use errors::ApiError;

pub trait ClaimsValidator {
    fn get_claims(&self) -> Option<Claims>;
    fn validate_claims<Func>(&self, func: Func) -> Result<(), ApiError>
        where Func: Fn(&Claims) -> Result<(), ApiError>;
    fn has_token_claims(&self, required_sub: &str, required_scope: &str) -> Result<(), ApiError>;
    fn has_token_prefix(&self, id: &str) -> Result<(), ApiError>;
    fn has_token_repo(&self, repo: &str) -> Result<(), ApiError>;
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

pub fn id_matches_prefix(id: &str, prefix: &str) -> bool {
    if prefix == "" {
        return true
    }
    if id.starts_with(prefix) {
        let rest = &id[prefix.len()..];
        if rest.len() == 0 || rest.starts_with(".") {
            return true
        }
    };
    false
}

pub fn id_matches_one_prefix(id: &str, prefixes: &Vec<String>) -> bool {
    prefixes.iter().any(|prefix| id_matches_prefix(id, prefix))
}

pub fn repo_matches_claimed(repo: &str, claimed_repo: &str) -> bool {
    if claimed_repo == "" {
        return true
    }
    repo == claimed_repo
}

pub fn repo_matches_one_claimed(repo: &str, claimed_repos: &Vec<String>) -> bool {
    claimed_repos.iter().any(|claimed_repo| repo_matches_claimed(repo, claimed_repo))
}

impl<S> ClaimsValidator for HttpRequest<S> {
    fn get_claims(&self) -> Option<Claims> {
        self.extensions().get::<Claims>().cloned()
    }

    fn validate_claims<Func>(&self, func: Func) -> Result<(), ApiError>
        where Func: Fn(&Claims) -> Result<(), ApiError> {
        if let Some(claims) = self.extensions().get::<Claims>() {
            func(claims)
        } else {
            Err(ApiError::NotEnoughPermissions("No token specified".to_string()))
        }
    }

    fn has_token_claims(&self, required_sub: &str, required_scope: &str) -> Result<(), ApiError> {
        self.validate_claims(
            |claims| {
                // Matches using a path-prefix style comparison:
                //  claim.sub == "build" should match required_sub == "build" or "build/N[/...]"
                //  claim.sub == "build/N" should only matchs required_sub == "build/N[/...]"
                if !sub_has_prefix(required_sub, &claims.sub) {
                    return Err(ApiError::NotEnoughPermissions("Not matching sub in token".to_string()))
                }
                if !claims.scope.contains(&required_scope.to_string()) {
                    return Err(ApiError::NotEnoughPermissions("Not matching scope in token".to_string()))
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
                return Err(ApiError::NotEnoughPermissions("Not matching prefix in token".to_string()));
            }
            Ok(())
        })
    }

    fn has_token_repo(&self, repo: &str) -> Result<(), ApiError> {
        self.validate_claims(
            |claims| {
                if !repo_matches_one_claimed(&repo.to_string(), &claims.repos) {
                    return Err(ApiError::NotEnoughPermissions("Not matching repo in token".to_string()))
                }
                Ok(())
            })
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

impl Middleware<AppState> for TokenParser {
    fn start(&self, req: &HttpRequest<AppState>) -> Result<Started> {
        let header = req.headers().get(AUTHORIZATION).ok_or(ApiError::InvalidToken)?;
        let token = self.parse_authorization(header)?;
        let claims = self.validate_claims(token)?;

        req.extensions_mut().insert(claims);

        Ok(Started::Done)
    }

    fn finish(&self, req: &HttpRequest<AppState>, resp: &HttpResponse) -> Finished {
        if resp.status() == 401 {
            if let Some(ref claims) = req.get_claims() {
                info!("Presented: {:?}", claims);
            }
        }

        Finished::Done
    }

}
