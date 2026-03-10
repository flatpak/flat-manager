use reqwest::{header::LOCATION, Method};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::error::Error;
use std::io::ErrorKind;
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
    Json(#[from] serde_json::Error),

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
            ClientError::Reqwest(_) | ClientError::Json(_) | ClientError::Io(_) => {
                let error_type = match self {
                    ClientError::Reqwest(_) => "reqwest",
                    ClientError::Json(_) => "json",
                    ClientError::Io(_) => "io",
                    _ => unreachable!(),
                };

                json!({
                    "type": "exception",
                    "details": {
                        "error-type": error_type,
                        "message": self.to_string(),
                    },
                })
            }
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

#[derive(Serialize)]
struct PruneRequest<'a> {
    repo: &'a str,
}

#[derive(Serialize)]
struct CreateTokenRequest<'a> {
    name: &'a str,
    sub: &'a str,
    scope: &'a [String],
    duration: i64,
}

#[derive(Serialize)]
struct CommitRequest<'a> {
    endoflife: Option<&'a str>,
    endoflife_rebase: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_type: Option<i32>,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct JobRequest {
    log_offset: usize,
}

#[derive(Debug, Deserialize)]
struct BuildExtendedResponse {
    build: BuildSummary,
    #[serde(default)]
    checks: Vec<BuildCheck>,
}

#[derive(Debug, Deserialize)]
struct BuildSummary {
    repo_state: i16,
}

#[derive(Debug, Deserialize, Serialize)]
struct BuildCheck {
    check_name: String,
    build_id: i32,
    job_id: i32,
    status: i16,
    status_reason: Option<String>,
    results: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum JobStatus {
    Queued = 0,
    Running = 1,
    Completed = 2,
    Failed = 3,
}

impl<'de> Deserialize<'de> for JobStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match u8::deserialize(deserializer)? {
            0 => Ok(JobStatus::Queued),
            1 => Ok(JobStatus::Running),
            2 => Ok(JobStatus::Completed),
            3 => Ok(JobStatus::Failed),
            value => Err(serde::de::Error::custom(format!(
                "invalid job status {value}"
            ))),
        }
    }
}

fn is_build_in_use_purge_error(body: &Value) -> bool {
    body.get("message").and_then(Value::as_str) == Some(PURGE_IN_USE_MESSAGE)
}

fn is_connection_reset(err: &reqwest::Error) -> bool {
    let mut source = err.source();

    while let Some(source_err) = source {
        if let Some(io_err) = source_err.downcast_ref::<std::io::Error>() {
            if io_err.kind() == ErrorKind::ConnectionReset {
                return true;
            }
        }
        source = source_err.source();
    }

    false
}

fn poll_sleep_duration(iterations_since_change: u32) -> Duration {
    if iterations_since_change <= 1 {
        Duration::from_secs(1)
    } else if iterations_since_change < 5 {
        Duration::from_secs(3)
    } else if iterations_since_change < 15 {
        Duration::from_secs(5)
    } else if iterations_since_change < 30 {
        Duration::from_secs(10)
    } else {
        Duration::from_secs(60)
    }
}

pub fn reparse_job_results(job: &mut Value) -> Result<(), ClientError> {
    if let Some(results) = job.get_mut("results") {
        if let Some(results_str) = results.as_str() {
            *results = serde_json::from_str(results_str)?;
        }
    }

    Ok(())
}

fn with_location(mut body: Value, location: String) -> Value {
    body["location"] = Value::String(location);
    body
}

fn build_url_to_api(build_url: &str) -> Result<String, ClientError> {
    let build_url = build_url.trim_end_matches('/');
    let mut parts = build_url.rsplitn(3, '/');
    let _build_id = parts.next();
    let segment = parts.next();
    let api_base = parts.next();

    match (segment, api_base) {
        (Some("build"), Some(api_base)) => Ok(api_base.to_string()),
        _ => Err(ClientError::Usage(format!(
            "Invalid build URL: {build_url}"
        ))),
    }
}

fn publish_current_state(body: &Value) -> Option<String> {
    if let Some(state) = body.get("current-state").and_then(Value::as_str) {
        return Some(state.to_string());
    }

    body.as_str().and_then(|text| {
        serde_json::from_str::<Value>(text).ok().and_then(|parsed| {
            parsed
                .get("current-state")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
    })
}

fn update_repo_job_id(job: &Value) -> Option<i64> {
    job.get("results")
        .and_then(|results| results.get("update-repo-job"))
        .and_then(Value::as_i64)
}

fn failed_check(checks: &[BuildCheck]) -> Option<&BuildCheck> {
    checks.iter().find(|check| check.status == 3)
}

pub struct JobPoller<'a> {
    client: &'a ApiClient,
    job_url: String,
    printed_len: usize,
    iterations_since_change: u32,
    error_iterations: u32,
    old_status: JobStatus,
    reported_delay: bool,
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

    async fn request_with_retry<T>(
        &self,
        method: Method,
        url: &str,
        body: Option<&T>,
    ) -> Result<ApiResponse, ClientError>
    where
        T: Serialize + ?Sized,
    {
        let deadline = Instant::now() + Duration::from_secs(300);
        let mut backoff_cap_secs = 1_u64;

        loop {
            let result = async {
                let mut request = self
                    .client
                    .request(method.clone(), url)
                    .bearer_auth(&self.token);
                if let Some(body) = body {
                    request = request.json(body);
                }
                let response = request.send().await?;
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

    pub async fn post_json<T>(&self, url: &str, body: &T) -> Result<ApiResponse, ClientError>
    where
        T: Serialize + ?Sized,
    {
        self.request_with_retry(Method::POST, url, Some(body)).await
    }

    pub async fn get(&self, url: &str) -> Result<ApiResponse, ClientError> {
        self.request_with_retry(Method::GET, url, None::<&()>).await
    }

    pub async fn get_json<T>(&self, url: &str, body: Option<&T>) -> Result<ApiResponse, ClientError>
    where
        T: Serialize + ?Sized,
    {
        self.request_with_retry(Method::GET, url, body).await
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

    pub async fn prune_repo(
        &self,
        manager_url: &str,
        repo: &str,
    ) -> Result<ApiResponse, ClientError> {
        let url = format!("{}/api/v1/prune", manager_url.trim_end_matches('/'));
        let body = PruneRequest { repo };

        self.post_json(&url, &body).await
    }

    pub async fn create_token(
        &self,
        manager_url: &str,
        name: &str,
        sub: &str,
        scope: &[String],
        duration: i64,
    ) -> Result<ApiResponse, ClientError> {
        let url = format!("{}/api/v1/token_subset", manager_url.trim_end_matches('/'));
        let body = CreateTokenRequest {
            name,
            sub,
            scope,
            duration,
        };

        self.post_json(&url, &body).await
    }

    pub async fn wait_for_checks(&self, build_url: &str) -> Result<(), ClientError> {
        let build_url = build_url.trim_end_matches('/');
        let extended_url = format!("{build_url}/extended");

        println!("Waiting for checks, if any...");
        let build = loop {
            match self.get(&extended_url).await {
                Ok(response) => {
                    let build: BuildExtendedResponse = serde_json::from_value(response.body)?;
                    if build.build.repo_state == 1 {
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    break build;
                }
                Err(ClientError::Http { status: 404, .. }) => return Ok(()),
                Err(err) => return Err(err),
            }
        };

        for check in &build.checks {
            println!("Waiting for check: {}", check.check_name);
            let check_job_url = format!("{build_url}/check/{}/job", check.check_name);
            JobPoller::new(self, check_job_url)
                .poll_to_completion()
                .await?;
        }

        match self.get(&extended_url).await {
            Ok(response) => {
                let build: BuildExtendedResponse = serde_json::from_value(response.body)?;
                if let Some(check) = failed_check(&build.checks) {
                    println!("\\ Check {} has failed", check.check_name);
                    return Err(ClientError::FailedJob(serde_json::to_value(check)?));
                }
                Ok(())
            }
            Err(ClientError::Http { status: 404, .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub async fn commit_build(
        &self,
        build_url: &str,
        endoflife: Option<&str>,
        endoflife_rebase: Option<&str>,
        token_type: Option<i32>,
    ) -> Result<Value, ClientError> {
        let build_url = build_url.trim_end_matches('/');
        let url = format!("{build_url}/commit");
        let body = CommitRequest {
            endoflife,
            endoflife_rebase,
            token_type,
        };
        let response = self.post_json(&url, &body).await?;
        let job_url = response
            .location
            .ok_or_else(|| ClientError::Usage("Missing location header for commit job".into()))?;

        println!("Waiting for commit job");
        self.wait_for_checks(build_url).await?;

        let job = JobPoller::new(self, &job_url).poll_to_completion().await?;
        Ok(with_location(job, job_url))
    }

    pub async fn publish_build(
        &self,
        build_url: &str,
        wait: bool,
        wait_update: bool,
    ) -> Result<Value, ClientError> {
        let build_url = build_url.trim_end_matches('/');
        let url = format!("{build_url}/publish");
        let body = json!({});

        let response = match self.post_json(&url, &body).await {
            Ok(response) => response,
            Err(ClientError::Http {
                url,
                status: 400,
                body,
            }) => match publish_current_state(&body).as_deref() {
                Some("published") => {
                    println!("the build has been already published");
                    return Ok(json!({}));
                }
                Some("publishing") => {
                    println!("the build is currently being published");
                    return Ok(json!({}));
                }
                Some("validating") => {
                    println!("the build is still being validated or held for review");
                    return Ok(json!({}));
                }
                Some("failed") => {
                    println!("the build has failed");
                    return Err(ClientError::Http {
                        url,
                        status: 400,
                        body,
                    });
                }
                _ => {
                    return Err(ClientError::Http {
                        url,
                        status: 400,
                        body,
                    });
                }
            },
            Err(err) => return Err(err),
        };

        let job_url = response
            .location
            .ok_or_else(|| ClientError::Usage("Missing location header for publish job".into()))?;

        let mut job = if wait || wait_update {
            println!("Waiting for publish job");
            JobPoller::new(self, &job_url).poll_to_completion().await?
        } else {
            response.body
        };

        // In the non-wait path this is still the initial POST response body, not the
        // already-reparsed terminal job payload returned by JobPoller.
        reparse_job_results(&mut job)?;
        job = with_location(job, job_url);

        if let Some(update_job_id) = update_repo_job_id(&job) {
            println!("Queued repo update job {update_job_id}");
            let update_job_url = format!("{}/job/{update_job_id}", build_url_to_api(build_url)?);

            let mut update_job = if wait_update {
                println!("Waiting for repo update job");
                JobPoller::new(self, &update_job_url)
                    .poll_to_completion()
                    .await?
            } else {
                let empty_body = json!({});
                self.get_json(&update_job_url, Some(&empty_body))
                    .await?
                    .body
            };

            reparse_job_results(&mut update_job)?;
            job["update_job"] = with_location(update_job, update_job_url);
        }

        Ok(job)
    }
}

impl<'a> JobPoller<'a> {
    pub fn new(client: &'a ApiClient, job_url: impl Into<String>) -> Self {
        Self {
            client,
            job_url: job_url.into(),
            printed_len: 0,
            iterations_since_change: 0,
            error_iterations: 0,
            old_status: JobStatus::Queued,
            reported_delay: false,
        }
    }

    pub async fn poll_to_completion(&mut self) -> Result<Value, ClientError> {
        loop {
            let body = JobRequest {
                log_offset: self.printed_len,
            };

            match self.client.get_json(&self.job_url, Some(&body)).await {
                Ok(mut response) => {
                    self.error_iterations = 0;

                    let status: JobStatus = serde_json::from_value(
                        response.body.get("status").cloned().unwrap_or_default(),
                    )?;
                    let log = response
                        .body
                        .get("log")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let start_after: Option<SystemTime> = response
                        .body
                        .get("start_after")
                        .cloned()
                        .map(serde_json::from_value)
                        .transpose()?;

                    if status == JobStatus::Queued && !self.reported_delay {
                        self.reported_delay = true;
                        if let Some(start_after) = start_after {
                            if let Ok(delay) = start_after.duration_since(SystemTime::now()) {
                                if delay.as_secs() > 0 {
                                    println!(
                                        "Waiting {} seconds before starting job",
                                        delay.as_secs()
                                    );
                                }
                            }
                        }
                    }

                    if status != JobStatus::Queued && self.old_status == JobStatus::Queued {
                        println!("/ Job was started");
                    }
                    self.old_status = status;

                    if !log.is_empty() {
                        self.iterations_since_change = 0;
                        for line in log.split_inclusive('\n') {
                            print!("| {line}");
                        }
                        self.printed_len += log.len();
                    } else {
                        self.iterations_since_change += 1;
                    }

                    match status {
                        JobStatus::Queued | JobStatus::Running => {}
                        JobStatus::Completed => {
                            println!("\\ Job completed successfully");
                            reparse_job_results(&mut response.body)?;
                            return Ok(response.body);
                        }
                        JobStatus::Failed => {
                            println!("\\ Job failed");
                            reparse_job_results(&mut response.body)?;
                            return Err(ClientError::FailedJob(response.body));
                        }
                    }
                }
                Err(err @ ClientError::Http { status, .. }) => {
                    self.iterations_since_change = 4;
                    self.error_iterations += 1;
                    if self.error_iterations <= 5 {
                        println!("Unexpected response {status} getting job log, ignoring");
                    } else {
                        return Err(err);
                    }
                }
                Err(ClientError::Reqwest(err)) if is_connection_reset(&err) => {}
                Err(err) => return Err(err),
            }

            sleep(poll_sleep_duration(self.iterations_since_change)).await;
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

    #[test]
    fn serializes_create_token_request_with_sub() {
        let body = serde_json::to_value(CreateTokenRequest {
            name: "subset",
            sub: "build/123",
            scope: &["build".to_string(), "upload".to_string()],
            duration: 3600,
        })
        .unwrap();

        assert_eq!(
            body,
            json!({
                "name": "subset",
                "sub": "build/123",
                "scope": ["build", "upload"],
                "duration": 3600,
            })
        );
    }

    #[test]
    fn serializes_commit_request_like_python() {
        let body = serde_json::to_value(CommitRequest {
            endoflife: None,
            endoflife_rebase: Some("org.example.NewApp"),
            token_type: None,
        })
        .unwrap();

        assert_eq!(
            body,
            json!({
                "endoflife": null,
                "endoflife_rebase": "org.example.NewApp",
            })
        );
    }

    #[test]
    fn extracts_publish_current_state_from_embedded_json_string() {
        assert_eq!(
            publish_current_state(&json!("{\"current-state\":\"validating\"}")).as_deref(),
            Some("validating")
        );
    }

    #[test]
    fn converts_build_url_to_api_base() {
        assert_eq!(
            build_url_to_api("http://host/api/v1/build/42").unwrap(),
            "http://host/api/v1"
        );
    }

    #[test]
    fn converts_build_url_to_api_base_with_trailing_slash() {
        assert_eq!(
            build_url_to_api("http://host/api/v1/build/42/").unwrap(),
            "http://host/api/v1"
        );
    }

    #[test]
    fn deserializes_job_status_from_integer() {
        assert_eq!(
            serde_json::from_value::<JobStatus>(json!(0)).unwrap(),
            JobStatus::Queued
        );
        assert_eq!(
            serde_json::from_value::<JobStatus>(json!(1)).unwrap(),
            JobStatus::Running
        );
        assert_eq!(
            serde_json::from_value::<JobStatus>(json!(2)).unwrap(),
            JobStatus::Completed
        );
        assert_eq!(
            serde_json::from_value::<JobStatus>(json!(3)).unwrap(),
            JobStatus::Failed
        );
    }

    #[test]
    fn reparses_job_results_string() {
        let mut job = json!({
            "results": "{\"update-repo-job\": 42}",
        });

        reparse_job_results(&mut job).unwrap();

        assert_eq!(
            job["results"],
            json!({
                "update-repo-job": 42,
            })
        );
    }

    #[test]
    fn finds_failed_checks() {
        let checks = vec![
            BuildCheck {
                check_name: "lint".into(),
                build_id: 1,
                job_id: 2,
                status: 1,
                status_reason: None,
                results: None,
            },
            BuildCheck {
                check_name: "e2e".into(),
                build_id: 1,
                job_id: 3,
                status: 3,
                status_reason: Some("failed".into()),
                results: None,
            },
        ];

        assert_eq!(
            failed_check(&checks).map(|check| check.check_name.as_str()),
            Some("e2e")
        );
    }

    #[test]
    fn extracts_update_repo_job_id_from_results() {
        let job = json!({
            "results": {
                "update-repo-job": 42,
            }
        });

        assert_eq!(update_repo_job_id(&job), Some(42));
    }

    #[test]
    fn uses_expected_poll_backoff_schedule() {
        assert_eq!(poll_sleep_duration(0), Duration::from_secs(1));
        assert_eq!(poll_sleep_duration(2), Duration::from_secs(3));
        assert_eq!(poll_sleep_duration(5), Duration::from_secs(5));
        assert_eq!(poll_sleep_duration(15), Duration::from_secs(10));
        assert_eq!(poll_sleep_duration(30), Duration::from_secs(60));
    }
}
