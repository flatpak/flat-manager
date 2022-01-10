//! Request logging middleware
use actix_service::{Service, Transform};
use actix_web::dev::{BodySize, MessageBody, ResponseBody, ServiceRequest, ServiceResponse};
use actix_web::error::Error;
use actix_web::http::StatusCode;
use bytes::Bytes;
use futures::future::{ok, FutureResult};
use futures::{Async, Future, Poll};
use std::marker::PhantomData;
use std::rc::Rc;
use time;

use tokens::ClaimsValidator;

pub struct Logger(Rc<Inner>);

struct RequestData {
    time: time::Tm,
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
        let rt = ((time::now() - req.time).num_nanoseconds().unwrap_or(0) as f64) / 1_000_000_000.0;

        info!(
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

impl<S, B> Transform<S> for Logger
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    B: MessageBody,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<StreamLog<B>>;
    type Error = Error;
    type InitError = ();
    type Transform = LoggerMiddleware<S>;
    type Future = FutureResult<Self::Transform, Self::InitError>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(LoggerMiddleware {
            service,
            inner: self.0.clone(),
        })
    }
}

/// Logger middleware
pub struct LoggerMiddleware<S> {
    inner: Rc<Inner>,
    service: S,
}

impl<S, B> Service for LoggerMiddleware<S>
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    B: MessageBody,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<StreamLog<B>>;
    type Error = Error;
    type Future = LoggerResponse<S, B>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.service.poll_ready()
    }

    fn call(&mut self, req: ServiceRequest) -> Self::Future {
        let now = time::now();

        let remote_ip = req.connection_info().remote().unwrap_or("-").to_string();

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
            if let Ok(s) = val.to_str() {
                s
            } else {
                "-"
            }
        } else {
            "-"
        }
        .to_string();

        LoggerResponse {
            fut: self.service.call(req),
            inner: self.inner.clone(),
            request_data: Some(RequestData {
                time: now,
                remote_ip: remote_ip,
                request_line: request_line,
                user_agent: user_agent,
            }),
            _t: PhantomData,
        }
    }
}

pub struct LoggerResponse<S, B>
where
    B: MessageBody,
    S: Service,
{
    fut: S::Future,
    inner: Rc<Inner>,
    request_data: Option<RequestData>,
    _t: PhantomData<(B,)>,
}

impl<S, B> Future for LoggerResponse<S, B>
where
    B: MessageBody,
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
{
    type Item = ServiceResponse<StreamLog<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let res = futures::try_ready!(self.fut.poll());

        if let Some(error) = res.response().error() {
            if res.response().head().status != StatusCode::INTERNAL_SERVER_ERROR {
                debug!("Error in response: {:?}", error);
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
            token_name: token_name,
            status: res.response().head().status,
            size: 0,
        };

        Ok(Async::Ready(res.map_body(move |_, body| {
            ResponseBody::Body(StreamLog {
                body,
                inner: self.inner.clone(),
                request_data: self.request_data.take().unwrap(),
                response_data: response_data,
            })
        })))
    }
}

pub struct StreamLog<B> {
    body: ResponseBody<B>,
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
    fn size(&self) -> BodySize {
        self.body.size()
    }

    fn poll_next(&mut self) -> Poll<Option<Bytes>, Error> {
        match self.body.poll_next()? {
            Async::Ready(Some(chunk)) => {
                self.response_data.size += chunk.len();
                Ok(Async::Ready(Some(chunk)))
            }
            val => Ok(val),
        }
    }
}
