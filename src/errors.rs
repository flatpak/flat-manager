use actix;
use actix_web::{error::ResponseError, HttpResponse, FutureResponse};
use diesel::result::{Error as DieselError};
use futures::future;
use std::io;
use actix_web::http::StatusCode;

#[derive(Fail, Debug)]
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

    #[fail(display = "InvalidToken")]
    InvalidToken,

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
                "message": "Internal Server Error"
            }),
            ApiError::NotFound => json!({
                "status": 404,
                "message": "Not found",
            }),
            ApiError::BadRequest(ref message) => json!({
                "status": 400,
                "message": message,
            }),
            ApiError::InvalidToken => json!({
                "status": 401,
                "message": "Invalid token",
            }),
            ApiError::NotEnoughPermissions(ref message) => json!({
                "status": 401,
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
            ApiError::InvalidToken => StatusCode::UNAUTHORIZED,
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
