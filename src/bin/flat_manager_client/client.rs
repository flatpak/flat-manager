use serde_json::{json, Value};

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
