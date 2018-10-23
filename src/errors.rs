use actix_web::{error::ResponseError, HttpResponse};

#[derive(Fail, Debug)]
pub enum ApiError {
    #[fail(display = "Internal Server Error")]
    InternalServerError,

    #[fail(display = "BadRequest: {}", _0)]
    BadRequest(String),
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match *self {
            ApiError::InternalServerError => {
                HttpResponse::InternalServerError().json("Internal Server Error")
            },
            ApiError::BadRequest(ref message) => HttpResponse::BadRequest().json(message),
        }
    }
}
