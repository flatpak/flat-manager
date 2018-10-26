use actix_web::{error::ResponseError, HttpResponse, FutureResponse};
use diesel::result::{Error as DieselError};
use futures::future;

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
