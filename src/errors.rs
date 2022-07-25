use crate::ostree::OstreeError;
use actix_web::error::BlockingError;
use actix_web::http::StatusCode;
use actix_web::{error::ResponseError, HttpResponse};
use diesel::result::Error as DieselError;
use failure::Fail;
use serde_json::json;
use std::io;

#[derive(Fail, Debug, Clone)]
pub enum DeltaGenerationError {
    #[fail(display = "{}", _0)]
    Failed(String),
}

impl DeltaGenerationError {
    pub fn new(s: &str) -> Self {
        DeltaGenerationError::Failed(s.to_string())
    }
}

impl From<io::Error> for DeltaGenerationError {
    fn from(e: io::Error) -> Self {
        DeltaGenerationError::new(&e.to_string())
    }
}

impl From<OstreeError> for DeltaGenerationError {
    fn from(e: OstreeError) -> Self {
        DeltaGenerationError::new(&e.to_string())
    }
}

#[derive(Fail, Debug, Clone)]
pub enum JobError {
    #[fail(display = "InternalError: {}", _0)]
    InternalError(String),

    #[fail(display = "DbError: {}", _0)]
    DBError(String),
}

impl JobError {
    pub fn new(s: &str) -> Self {
        JobError::InternalError(s.to_string())
    }
}

pub type JobResult<T> = Result<T, JobError>;

impl From<DieselError> for JobError {
    fn from(e: DieselError) -> Self {
        JobError::DBError(e.to_string())
    }
}

impl From<OstreeError> for JobError {
    fn from(e: OstreeError) -> Self {
        JobError::InternalError(e.to_string())
    }
}

impl From<OstreeError> for ApiError {
    fn from(e: OstreeError) -> Self {
        ApiError::InternalServerError(e.to_string())
    }
}

impl From<BlockingError<ApiError>> for ApiError {
    fn from(e: BlockingError<ApiError>) -> Self {
        match e {
            BlockingError::Error(e) => e,
            BlockingError::Canceled => {
                ApiError::InternalServerError("Blocking operation cancelled".to_string())
            }
        }
    }
}

impl From<r2d2::Error> for ApiError {
    fn from(e: r2d2::Error) -> Self {
        ApiError::InternalServerError(format!("Database error: {}", e))
    }
}

impl From<DeltaGenerationError> for JobError {
    fn from(e: DeltaGenerationError) -> Self {
        JobError::InternalError(format!("Failed to generate delta: {}", e))
    }
}

impl From<io::Error> for JobError {
    fn from(e: io::Error) -> Self {
        JobError::InternalError(e.to_string())
    }
}

#[derive(Fail, Debug)]
pub enum ApiError {
    #[fail(display = "Internal Server Error ({})", _0)]
    InternalServerError(String),

    #[fail(display = "NotFound")]
    NotFound,

    #[fail(display = "BadRequest: {}", _0)]
    BadRequest(String),

    #[fail(display = "WrongRepoState({}): {}", _2, _0)]
    WrongRepoState(String, String, String),

    #[fail(display = "WrongPublishedState({}): {}", _2, _0)]
    WrongPublishedState(String, String, String),

    #[fail(display = "InvalidToken: {}", _0)]
    InvalidToken(String),

    #[fail(display = "NotEnoughPermissions")]
    NotEnoughPermissions(String),
}

impl From<DieselError> for ApiError {
    fn from(e: DieselError) -> Self {
        match e {
            DieselError::NotFound => ApiError::NotFound,
            _ => ApiError::InternalServerError(e.to_string()),
        }
    }
}

impl From<io::Error> for ApiError {
    fn from(io_error: io::Error) -> Self {
        ApiError::InternalServerError(io_error.to_string())
    }
}

impl From<actix::MailboxError> for ApiError {
    fn from(e: actix::MailboxError) -> Self {
        ApiError::InternalServerError(e.to_string())
    }
}

impl ApiError {
    pub fn to_json(&self) -> serde_json::Value {
        match *self {
            ApiError::InternalServerError(ref _internal_message) => json!({
                "status": 500,
                "error-type": "internal-error",
                "message": "Internal Server Error"
            }),
            ApiError::NotFound => json!({
                "status": 404,
                "error-type": "not-found",
                "message": "Not found",
            }),
            ApiError::BadRequest(ref message) => json!({
                "status": 400,
                "error-type": "generic-error",
                "message": message,
            }),
            ApiError::WrongRepoState(ref message, ref expected, ref state) => json!({
                "status": 400,
                "message": message,
                "error-type": "wrong-repo-state",
                "current-state": state,
                "expected-state": expected,
            }),
            ApiError::WrongPublishedState(ref message, ref expected, ref state) => json!({
                "status": 400,
                "message": message,
                "error-type": "wrong-published-state",
                "current-state": state,
                "expected-state": expected,
            }),
            ApiError::InvalidToken(ref message) => json!({
                "status": 401,
                "error-type": "invalid-token",
                "message": message,
            }),
            ApiError::NotEnoughPermissions(ref message) => json!({
                "status": 403,
                "error-type": "token-insufficient",
                "message": format!("Not enough permissions: {}", message),
            }),
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match *self {
            ApiError::InternalServerError(ref _internal_message) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::BadRequest(ref _message) => StatusCode::BAD_REQUEST,
            ApiError::WrongRepoState(_, _, _) => StatusCode::BAD_REQUEST,
            ApiError::WrongPublishedState(_, _, _) => StatusCode::BAD_REQUEST,
            ApiError::InvalidToken(_) => StatusCode::UNAUTHORIZED,
            ApiError::NotEnoughPermissions(ref _message) => StatusCode::FORBIDDEN,
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        if let ApiError::InternalServerError(internal_message) = self {
            log::error!("Responding with internal error: {}", internal_message);
        }
        if let ApiError::NotEnoughPermissions(internal_message) = self {
            log::error!(
                "Responding with NotEnoughPermissions error: {}",
                internal_message
            );
        }
        HttpResponse::build(self.status_code()).json(self.to_json())
    }

    fn render_response(&self) -> HttpResponse {
        self.error_response()
    }
}
