use reqwest::header::LOCATION;
use serde::Serialize;
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Instant};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("API error from {url} (status {status}): {body}")]
    Http {
        url: String,
        status: u16,
        body: Value,
    },

    #[error("Job failed: {0}")]
    FailedJob(Value),

    #[error("{0}")]
    Usage(String),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl ClientError {
    pub fn is_retryable(&self) -> bool {
        match self {
            ClientError::Http { status, .. } => *status >= 500,
            ClientError::Reqwest(err) => err.is_connect() || err.is_timeout(),
            _ => false,
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            ClientError::Http { url, status, body } => json!({
                "type": "api",
                "url": url,
                "status_code": status,
                "details": body,
            }),
            ClientError::FailedJob(job) => json!({
                "type": "job",
                "job": job,
            }),
            ClientError::Usage(message) => json!({
                "type": "usage",
                "details": {
                    "message": message,
                },
            }),
            ClientError::Reqwest(err) => json!({
                "type": "exception",
                "details": {
                    "error-type": "reqwest",
                    "message": err.to_string(),
                },
            }),
            ClientError::Io(err) => json!({
                "type": "exception",
                "details": {
                    "error-type": "io",
                    "message": err.to_string(),
                },
            }),
        }
    }
}

pub struct ApiResponse {
    pub body: Value,
    pub location: Option<String>,
}

pub struct ApiClient {
    client: reqwest::Client,
    token: String,
}

const PURGE_IN_USE_MESSAGE: &str = "Can't prune build while in use";

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct CreateBuildRequest<'a> {
    repo: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    app_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    public_download: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    build_log_url: Option<&'a str>,
}

fn is_build_in_use_purge_error(body: &Value) -> bool {
    body.get("message").and_then(Value::as_str) == Some(PURGE_IN_USE_MESSAGE)
}

impl ApiClient {
    pub fn new(token: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(90 * 60))
            .build()
            .expect("failed to build reqwest client");

        Self {
            client,
            token: token.into(),
        }
    }

    pub async fn post_json<T>(&self, url: &str, body: &T) -> Result<ApiResponse, ClientError>
    where
        T: Serialize + ?Sized,
    {
        let deadline = Instant::now() + Duration::from_secs(300);
        let mut backoff_cap_secs = 1_u64;

        loop {
            let result = async {
                let response = self
                    .client
                    .post(url)
                    .bearer_auth(&self.token)
                    .json(body)
                    .send()
                    .await?;
                let status = response.status();
                let location = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);

                if status.as_u16() != 200 {
                    let text = response.text().await?;
                    let body = serde_json::from_str(&text).unwrap_or_else(|_| {
                        json!({
                            "message": format!("Non-json error from server: {text}"),
                        })
                    });

                    return Err(ClientError::Http {
                        url: url.to_owned(),
                        status: status.as_u16(),
                        body,
                    });
                }

                let body = response.json().await?;

                Ok(ApiResponse { body, location })
            }
            .await;

            match result {
                Ok(response) => return Ok(response),
                Err(err) if err.is_retryable() => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(err);
                    }

                    let remaining = deadline.saturating_duration_since(now);
                    let cap = Duration::from_secs(backoff_cap_secs.min(60));
                    let sleep_for = if cap <= Duration::from_secs(1) {
                        cap
                    } else {
                        let cap_ms = cap.as_millis() as u64;
                        let min_ms = 1_000_u64;
                        let span_ms = cap_ms - min_ms;
                        let seed = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or(Duration::ZERO)
                            .subsec_nanos() as u64;
                        Duration::from_millis(min_ms + (seed % (span_ms + 1)))
                    }
                    .min(remaining);

                    if sleep_for.is_zero() {
                        return Err(err);
                    }

                    sleep(sleep_for).await;
                    backoff_cap_secs = backoff_cap_secs.saturating_mul(2).min(60);
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn create_build(
        &self,
        manager_url: &str,
        repo: &str,
        app_id: Option<&str>,
        public_download: Option<bool>,
        build_log_url: Option<&str>,
    ) -> Result<ApiResponse, ClientError> {
        let url = format!("{}/api/v1/build", manager_url.trim_end_matches('/'));
        let body = CreateBuildRequest {
            repo,
            app_id,
            public_download,
            build_log_url,
        };
        self.post_json(&url, &body).await
    }

    pub async fn purge_build(&self, build_url: &str) -> Result<ApiResponse, ClientError> {
        let url = format!("{}/purge", build_url.trim_end_matches('/'));
        let body = json!({});

        match self.post_json(&url, &body).await {
            Ok(response) => Ok(response),
            Err(ClientError::Http {
                status: 400, body, ..
            }) if is_build_in_use_purge_error(&body) => Ok(ApiResponse {
                body: json!({}),
                location: None,
            }),
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_build_in_use_purge_error() {
        assert!(is_build_in_use_purge_error(&json!({
            "message": PURGE_IN_USE_MESSAGE,
        })));
        assert!(!is_build_in_use_purge_error(&json!({
            "message": "some other error",
        })));
        assert!(!is_build_in_use_purge_error(&json!({})));
    }
}
