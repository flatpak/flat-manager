use actix;
use actix_web::{error::ResponseError, HttpResponse, FutureResponse};
use diesel::result::{Error as DieselError};
use futures::future;
use std::io;
use actix_web::http::StatusCode;

#[derive(Fail, Debug)]
pub enum WorkerError {
    #[fail(display = "InternalError: {}", _0)]
    InternalError(String),

    #[fail(display = "DbError: {}", _0)]
    DBError(String),
}

impl WorkerError {
    pub fn new(s: &str) -> Self {
        WorkerError::InternalError(s.to_string())
    }
}

pub type WorkerResult<T> = Result<T, WorkerError>;

impl From<DieselError> for WorkerError {
    fn from(e: DieselError) -> Self {
        match e {
            _ => {
                println!("Diesel error: {:?}", e);
                WorkerError::DBError(e.to_string())
            }
        }
    }
}

impl From<io::Error> for WorkerError {
    fn from(e: io::Error) -> Self {
        match e {
            _ => {
                println!("worker io error: {:?}", e);
                WorkerError::InternalError(e.to_string())
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
    NotEnoughPermissions,
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
            ApiError::NotEnoughPermissions => json!({
                "status": 401,
                "message": "Not enough permissions",
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
            ApiError::NotEnoughPermissions => StatusCode::UNAUTHORIZED,
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(self.to_json())
    }
}

impl From<ApiError> for FutureResponse<HttpResponse> {
    fn from(e: ApiError) -> Self {
        Box::new(future::ok(e.error_response()))
    }
}
