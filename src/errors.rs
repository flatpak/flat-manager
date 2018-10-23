use actix_web::{error::ResponseError, HttpResponse};
use diesel::result::{Error as DieselError};

#[derive(Fail, Debug)]
pub enum ApiError {
    #[fail(display = "Internal Server Error")]
    InternalServerError,

    #[fail(display = "NotFound")]
    NotFound,
}

impl From<DieselError> for ApiError {
    fn from(e: DieselError) -> Self {
        match e {
            DieselError::NotFound => ApiError::NotFound,
            _ => ApiError::InternalServerError
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match *self {
            ApiError::InternalServerError => HttpResponse::InternalServerError().json("Internal Server Error"),
            ApiError::NotFound => HttpResponse::NotFound().json("Not found"),
        }
    }
}
