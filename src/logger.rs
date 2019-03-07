//! Request logging middleware
use time;
use actix_web::error::Result;
use actix_web::{HttpMessage,HttpRequest,HttpResponse};
use actix_web::middleware::{Finished, Middleware, Started};
use tokens::ClaimsValidator;

pub struct Logger {
}

impl Logger {
    pub fn default() -> Logger {
        Logger {
        }
    }
}

struct StartTime(time::Tm);

impl Logger {
    fn log<S>(&self, req: &HttpRequest<S>, resp: &HttpResponse) {
        if let Some(entry_time) = req.extensions().get::<StartTime>() {

            let remote_ip = req.connection_info().remote().unwrap_or("-").to_string();

            let request_line = if req.query_string().is_empty() {
                format!("{} {} {:?}",
                        req.method(),
                        req.path(),
                        req.version())
            } else {
                format!("{} {}?{} {:?}",
                        req.method(),
                        req.path(),
                        req.query_string(),
                        req.version())
            };

            let user_agent = if let Some(val) = req.headers().get("User-Agent") {
                if let Ok(s) = val.to_str() {
                    s
                } else {
                    "-"
                }
            } else {
                "-"
            }.to_string();

            let rt = ((time::now() - entry_time.0).num_nanoseconds().unwrap_or(0) as f64) / 1_000_000_000.0;

            let token_name = if let Some(ref claims) = req.get_claims() {
                claims.name.clone()
            } else {
                "-".to_string()
            };

            info!("{} \"{}\" {} {} {} {} {:.6}",
                  remote_ip,
                  request_line,
                  token_name,
                  resp.status().as_u16(),
                  resp.response_size(),
                  user_agent,
                  rt);
        }
    }
}

impl<S> Middleware<S> for Logger {
    fn start(&self, req: &HttpRequest<S>) -> Result<Started> {
        req.extensions_mut().insert(StartTime(time::now()));
        Ok(Started::Done)
    }

    fn finish(&self, req: &HttpRequest<S>, resp: &HttpResponse) -> Finished {
        self.log(req, resp);
        Finished::Done
    }
}
