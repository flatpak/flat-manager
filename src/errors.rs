use actix;
use actix_web::{error::ResponseError, HttpResponse, FutureResponse};
use diesel::result::{Error as DieselError};
use futures::future;
use std::io;
use actix_web::http::StatusCode;
use ostree::OstreeError;

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
        match e {
            _ => {
                JobError::DBError(e.to_string())
            }
        }
    }
}

impl From<OstreeError> for JobError {
    fn from(e: OstreeError) -> Self {
        match e {
            _ => {
                JobError::InternalError(e.to_string())
            }
        }
    }
}

impl From<DeltaGenerationError> for JobError {
    fn from(e: DeltaGenerationError) -> Self {
        match e {
            _ => {
                JobError::InternalError(format!("Failed to generate delta: {}", e.to_string()))
            }
        }
    }
}

impl From<io::Error> for JobError {
    fn from(e: io::Error) -> Self {
        match e {
            _ => {
                JobError::InternalError(e.to_string())
            }
        }
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
    WrongRepoState(String,String,String),

    #[fail(display = "WrongPublishedState({}): {}", _2, _0 )]
    WrongPublishedState(String,String,String),

    #[fail(display = "InvalidToken: {}", _0)]
    InvalidToken(String),

    #[fail(display = "NotEnoughPermissions")]
    NotEnoughPermissions(String),
}

impl From<DieselError> for ApiError {
    fn from(e: DieselError) -> Self {
        match e {
            DieselError::NotFound => ApiError::NotFound,
            _ => {
                ApiError::InternalServerError(e.to_string())
            }
        }
    }
}

impl From<io::Error> for ApiError {
    fn from(io_error: io::Error) -> Self {
        match io_error {
            _ => {
                ApiError::InternalServerError(io_error.to_string())
            }
        }
    }
}

impl From<actix::MailboxError> for ApiError {
    fn from(e: actix::MailboxError) -> Self {
        match e {
            _ => {
                ApiError::InternalServerError(e.to_string())
            }
        }
    }
}

impl ApiError {
    pub fn to_json(&self) -> String {
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
                "status": 401,
                "error-type": "token-insufficient",
                "message": format!("Not enough permissions: {})", message),
            }),
        }
        .to_string()
    }

    pub fn status_code(&self) -> StatusCode {
        match *self {
            ApiError::InternalServerError(ref _internal_message) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::BadRequest(ref _message) => StatusCode::BAD_REQUEST,
            ApiError::WrongRepoState(_,_,_) => StatusCode::BAD_REQUEST,
            ApiError::WrongPublishedState(_,_,_) => StatusCode::BAD_REQUEST,
            ApiError::InvalidToken(_) => StatusCode::UNAUTHORIZED,
            ApiError::NotEnoughPermissions(ref _message) => StatusCode::UNAUTHORIZED,
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        if let ApiError::InternalServerError(internal_message) = self {
            error!("Responding with internal error: {}", internal_message);
        }
        if let ApiError::NotEnoughPermissions(internal_message) = self {
            error!("Responding with NotEnoughPermissions error: {}", internal_message);
        }
        HttpResponse::build(self.status_code()).json(self.to_json())
    }
}

impl From<ApiError> for FutureResponse<HttpResponse> {
    fn from(e: ApiError) -> Self {
        Box::new(future::ok(e.error_response()))
    }
}
