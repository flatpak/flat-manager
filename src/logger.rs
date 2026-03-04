//! Request logging middleware
use actix_service::{Service, Transform};
use actix_web::body::{BodySize, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::error::Error;
use actix_web::http::StatusCode;
use bytes::Bytes;
use std::future::{ready, Future, Ready};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Instant;

use crate::tokens::ClaimsValidator;

pub struct Logger(Rc<Inner>);

struct RequestData {
    time: Instant,
    remote_ip: String,
    request_line: String,
    user_agent: String,
}

struct ResponseData {
    status: StatusCode,
    token_name: String,
    size: usize,
}

pub struct Inner {}

impl Inner {
    fn log(&self, req: &RequestData, resp: &ResponseData) {
        let rt = req.time.elapsed().as_secs_f64();

        log::info!(
            "{} \"{}\" {} {} {} {} {:.6}",
            req.remote_ip,
            req.request_line,
            resp.token_name,
            resp.status.as_u16(),
            resp.size,
            req.user_agent,
            rt
        );
    }
}

impl Logger {
    pub fn default() -> Logger {
        Logger(Rc::new(Inner {}))
    }
}

impl<S, B> Transform<S, ServiceRequest> for Logger
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<StreamLog<B>>;
    type Error = Error;
    type InitError = ();
    type Transform = LoggerMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(LoggerMiddleware {
            service,
            inner: self.0.clone(),
        }))
    }
}

/// Logger middleware
pub struct LoggerMiddleware<S> {
    inner: Rc<Inner>,
    service: S,
}

type LoggerResponse<B> =
    Pin<Box<dyn Future<Output = Result<ServiceResponse<StreamLog<B>>, Error>>>>;

impl<S, B> Service<ServiceRequest> for LoggerMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<StreamLog<B>>;
    type Error = Error;
    type Future = LoggerResponse<B>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let now = Instant::now();
        let remote_ip = req
            .peer_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "-".to_string());

        let request_line = if req.query_string().is_empty() {
            format!("{} {} {:?}", req.method(), req.path(), req.version())
        } else {
            format!(
                "{} {}?{} {:?}",
                req.method(),
                req.path(),
                req.query_string(),
                req.version()
            )
        };

        let user_agent = if let Some(val) = req.headers().get("User-Agent") {
            val.to_str().unwrap_or("-")
        } else {
            "-"
        }
        .to_string();

        let fut = self.service.call(req);
        let inner = self.inner.clone();
        let request_data = RequestData {
            time: now,
            remote_ip,
            request_line,
            user_agent,
        };

        Box::pin(async move {
            let res = fut.await?;

            if let Some(error) = res.response().error() {
                if res.response().head().status != StatusCode::INTERNAL_SERVER_ERROR {
                    log::debug!("Error in response: {:?}", error);
                }
            }

            let token_name = if let Some(ref claims) = res.request().get_claims() {
                if let Some(ref name) = claims.name {
                    name.clone()
                } else {
                    "-".to_string()
                }
            } else {
                "-".to_string()
            };

            let response_data = ResponseData {
                token_name,
                status: res.response().head().status,
                size: 0,
            };

            Ok(res.map_body(move |_, body| StreamLog {
                body: Box::pin(body),
                inner,
                request_data,
                response_data,
            }))
        })
    }
}

pub struct StreamLog<B> {
    body: Pin<Box<B>>,
    inner: Rc<Inner>,
    request_data: RequestData,
    response_data: ResponseData,
}

impl<B> Drop for StreamLog<B> {
    fn drop(&mut self) {
        self.inner.log(&self.request_data, &self.response_data);
    }
}

impl<B: MessageBody> MessageBody for StreamLog<B> {
    type Error = B::Error;

    fn size(&self) -> BodySize {
        self.body.as_ref().get_ref().size()
    }

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Self::Error>>> {
        // SAFETY: The pinned `body` field is never moved out; we only poll it and update counters.
        let this = unsafe { self.get_unchecked_mut() };

        match this.body.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.response_data.size += chunk.len();
                Poll::Ready(Some(Ok(chunk)))
            }
            poll => poll,
        }
    }
}
