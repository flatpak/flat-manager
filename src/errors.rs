use actix;
use actix_web::{error::ResponseError, HttpResponse, FutureResponse};
use diesel::result::{Error as DieselError};
use futures::future;
use std::io;

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
    #[fail(display = "Internal Server Error")]
    InternalServerError,

    #[fail(display = "NotFound")]
    NotFound,

    #[fail(display = "BadRequest: {}", _0)]
    BadRequest(String),

    #[fail(display = "InvalidToken: {}", _0)]
    InvalidToken(String),
}

impl From<DieselError> for ApiError {
    fn from(e: DieselError) -> Self {
        match e {
            DieselError::NotFound => ApiError::NotFound,
            _ => {
                println!("Diesel error: {:?}", e);
                ApiError::InternalServerError
            }
        }
    }
}

impl From<actix::MailboxError> for ApiError {
    fn from(e: actix::MailboxError) -> Self {
        match e {
            _ => {
                println!("Actix mailbox error: {:?}", e);
                ApiError::InternalServerError
            }
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match *self {
            ApiError::InternalServerError => HttpResponse::InternalServerError().json("Internal Server Error"),
            ApiError::NotFound => HttpResponse::NotFound().json("Not found"),
            ApiError::BadRequest(ref message) => HttpResponse::BadRequest().json(message),
            ApiError::InvalidToken(ref message) => HttpResponse::Unauthorized().json(message),
        }
    }
}

impl From<ApiError> for FutureResponse<HttpResponse> {
    fn from(e: ApiError) -> Self {
        Box::new(future::ok(e.error_response()))
    }
}
