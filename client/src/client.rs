use super::PushArgs;
use flat_manager_common::ostree::Delta;
use flate2::{write::GzEncoder, Compression};
use libostree::{self, gio, glib};
use reqwest::{
    header::{CONTENT_ENCODING, CONTENT_TYPE, LOCATION},
    multipart, Method,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    future::Future,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
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

    #[error("OSTree error: {0}")]
    Ostree(String),

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
            ClientError::Reqwest(_)
            | ClientError::Json(_)
            | ClientError::Io(_)
            | ClientError::Ostree(_) => {
                let error_type = match self {
                    ClientError::Reqwest(_) => "reqwest",
                    ClientError::Json(_) => "json",
                    ClientError::Io(_) => "io",
                    ClientError::Ostree(_) => "ostree",
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

impl From<glib::Error> for ClientError {
    fn from(err: glib::Error) -> Self {
        ClientError::Ostree(err.to_string())
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
const UPLOAD_CHUNK_LIMIT: u64 = 4 * 1024 * 1024;

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

#[derive(Serialize)]
struct MissingObjectsRequest<'a> {
    wanted: &'a [String],
}

#[derive(Debug, Deserialize)]
struct MissingObjectsResponse {
    missing: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct CreateBuildRefRequest<'a> {
    #[serde(rename = "ref")]
    ref_name: &'a str,
    commit: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    build_log_url: Option<&'a str>,
}

#[derive(Serialize)]
struct AddExtraIdsRequest<'a> {
    ids: &'a [String],
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "u8")]
#[repr(u8)]
pub enum JobStatus {
    Queued = 0,
    Running = 1,
    Completed = 2,
    Failed = 3,
}

impl TryFrom<u8> for JobStatus {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Queued),
            1 => Ok(Self::Running),
            2 => Ok(Self::Completed),
            3 => Ok(Self::Failed),
            _ => Err(format!("invalid job status {value}")),
        }
    }
}

fn parse_optional_system_time(
    value: Option<&Value>,
) -> Result<Option<SystemTime>, serde_json::Error> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => serde_json::from_value(value.clone()).map(Some),
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

fn build_url_to_manager(build_url: &str) -> Result<String, ClientError> {
    let api_base = build_url_to_api(build_url)?;
    Ok(api_base
        .trim_end_matches("/api/v1")
        .trim_end_matches('/')
        .to_string())
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

async fn parse_error_body(response: reqwest::Response) -> Result<Value, ClientError> {
    let text = response.text().await?;
    Ok(serde_json::from_str(&text).unwrap_or_else(|_| {
        json!({
            "message": format!("Non-json error from server: {text}"),
        })
    }))
}

async fn parse_api_response(
    url: &str,
    response: reqwest::Response,
) -> Result<ApiResponse, ClientError> {
    let status = response.status();
    let location = response
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    if status.as_u16() != 200 {
        return Err(ClientError::Http {
            url: url.to_owned(),
            status: status.as_u16(),
            body: parse_error_body(response).await?,
        });
    }

    let body = response.json().await?;
    Ok(ApiResponse { body, location })
}

fn checksum_from_bytes_variant(
    variant: &glib::Variant,
    field_name: &str,
) -> Result<String, ClientError> {
    let bytes = variant
        .fixed_array::<u8>()
        .map_err(|err| ClientError::Ostree(format!("Invalid {field_name}: {err}")))?;
    Ok(hex::encode(bytes))
}

fn load_dirtree(
    repo: &libostree::Repo,
    checksum: &str,
) -> Result<libostree::TreeVariantType, ClientError> {
    repo.load_variant(libostree::ObjectType::DirTree, checksum)?
        .try_get::<libostree::TreeVariantType>()
        .map_err(|err| ClientError::Ostree(format!("Invalid dirtree variant {checksum}: {err}")))
}

fn local_needed_metadata(
    repo: &libostree::Repo,
    commits: &[String],
) -> Result<HashSet<String>, ClientError> {
    let mut objects = HashSet::new();

    for commit in commits {
        let commit_name = format!("{commit}.commit");
        if !objects.insert(commit_name) {
            continue;
        }

        let (commit_v, _state) = repo.load_commit(commit)?;
        let dirtree_content =
            checksum_from_bytes_variant(&commit_v.child_value(6), "commit root tree checksum")?;
        let dirtree_meta =
            checksum_from_bytes_variant(&commit_v.child_value(7), "commit root meta checksum")?;
        local_needed_metadata_dirtree(repo, &mut objects, &dirtree_content, &dirtree_meta)?;
    }

    Ok(objects)
}

fn local_needed_metadata_dirtree(
    repo: &libostree::Repo,
    objects: &mut HashSet<String>,
    dirtree_content: &str,
    dirtree_meta: &str,
) -> Result<(), ClientError> {
    objects.insert(format!("{dirtree_meta}.dirmeta"));

    let dirtree_name = format!("{dirtree_content}.dirtree");
    if !objects.insert(dirtree_name) {
        return Ok(());
    }

    let (_files, dirs) = load_dirtree(repo, dirtree_content)?;
    for (_name, content_checksum, meta_checksum) in dirs {
        local_needed_metadata_dirtree(
            repo,
            objects,
            &hex::encode(content_checksum),
            &hex::encode(meta_checksum),
        )?;
    }

    Ok(())
}

fn local_needed_files(
    repo: &libostree::Repo,
    metadata_objects: &HashSet<String>,
) -> Result<HashSet<String>, ClientError> {
    let mut objects = HashSet::new();

    for metadata_object in metadata_objects {
        if metadata_object.ends_with(".dirtree") {
            ostree_get_dir_files(repo, &mut objects, metadata_object)?;
        }
    }

    Ok(objects)
}

fn ostree_get_dir_files(
    repo: &libostree::Repo,
    objects: &mut HashSet<String>,
    dirtree: &str,
) -> Result<(), ClientError> {
    let checksum = dirtree
        .strip_suffix(".dirtree")
        .ok_or_else(|| ClientError::Usage(format!("Invalid dirtree object: {dirtree}")))?;
    let (files, _dirs) = load_dirtree(repo, checksum)?;

    for (_name, file_checksum) in files {
        objects.insert(format!("{}.filez", hex::encode(file_checksum)));
    }

    Ok(())
}

fn object_path(repo_path: &str, object_name: &str) -> PathBuf {
    Path::new(repo_path)
        .join("objects")
        .join(&object_name[..2])
        .join(&object_name[2..])
}

fn is_lower_hex_checksum(value: &str) -> bool {
    value.len() == 64
        && value
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
}

fn parse_delta_name(name: &str) -> Result<Delta, ClientError> {
    let parts: Vec<&str> = name.split('-').collect();
    match parts.as_slice() {
        [to] if is_lower_hex_checksum(to) => Ok(Delta::new(None, to)),
        [from, to] if is_lower_hex_checksum(from) && is_lower_hex_checksum(to) => {
            Ok(Delta::new(Some(from), to))
        }
        _ => Delta::from_name(name).map_err(|err| ClientError::Ostree(format!("{name}: {err}"))),
    }
}

fn glob_matches(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let mut pattern_index = 0usize;
    let mut text_index = 0usize;
    let mut star_index = None;
    let mut match_index = 0usize;

    while text_index < text.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == '?' || pattern[pattern_index] == text[text_index])
        {
            pattern_index += 1;
            text_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == '*' {
            star_index = Some(pattern_index);
            match_index = text_index;
            pattern_index += 1;
        } else if let Some(star) = star_index {
            pattern_index = star + 1;
            match_index += 1;
            text_index = match_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == '*' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

fn should_skip_delta(app_id: &str, globs: &[String]) -> bool {
    globs.iter().any(|glob| glob_matches(glob, app_id))
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

    async fn with_retry<F, Fut, T>(&self, mut f: F) -> Result<T, ClientError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ClientError>>,
    {
        let deadline = Instant::now() + Duration::from_secs(300);
        let mut backoff_cap_secs = 1_u64;

        loop {
            match f().await {
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

    async fn request_with_retry<T>(
        &self,
        method: Method,
        url: &str,
        body: Option<&T>,
    ) -> Result<ApiResponse, ClientError>
    where
        T: Serialize + ?Sized,
    {
        self.with_retry(|| async {
            let mut request = self
                .client
                .request(method.clone(), url)
                .bearer_auth(&self.token);
            if let Some(body) = body {
                request = request.json(body);
            }
            let response = request.send().await?;
            parse_api_response(url, response).await
        })
        .await
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
            Err(ClientError::Http { status: 404, .. }) => Ok(ApiResponse {
                body: json!({"status": "build not found"}),
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

    pub async fn missing_objects(
        &self,
        build_url: &str,
        wanted: &[String],
    ) -> Result<Vec<String>, ClientError> {
        let mut missing = Vec::new();

        for chunk in wanted.chunks(2000) {
            missing.extend(self.missing_objects_chunk(build_url, chunk).await?);
        }

        Ok(missing)
    }

    async fn missing_objects_chunk(
        &self,
        build_url: &str,
        chunk: &[String],
    ) -> Result<Vec<String>, ClientError> {
        let url = format!("{}/missing_objects", build_url.trim_end_matches('/'));
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&serde_json::to_vec(&MissingObjectsRequest {
            wanted: chunk,
        })?)?;
        let body = encoder.finish()?;

        self.with_retry(|| async {
            let request = self
                .client
                .request(Method::GET, &url)
                .bearer_auth(&self.token)
                .header(CONTENT_ENCODING, "gzip")
                .header(CONTENT_TYPE, "application/json")
                .body(body.clone())
                .build()?;
            let response = self.client.execute(request).await?;
            let status = response.status();

            if status.as_u16() != 200 {
                return Err(ClientError::Http {
                    url: url.clone(),
                    status: status.as_u16(),
                    body: parse_error_body(response).await?,
                });
            }

            Ok(response.json::<MissingObjectsResponse>().await?.missing)
        })
        .await
    }

    async fn upload_files(
        &self,
        build_url: &str,
        files: Vec<(PathBuf, String)>,
    ) -> Result<(), ClientError> {
        if files.is_empty() {
            return Ok(());
        }

        let total_size = files.iter().try_fold(0u64, |total, (path, _)| {
            Ok::<u64, std::io::Error>(total + std::fs::metadata(path)?.len())
        })?;
        println!("Uploading {} files ({} bytes)", files.len(), total_size);

        let url = format!("{}/upload", build_url.trim_end_matches('/'));
        self.with_retry(|| async {
            let mut form = multipart::Form::new();
            for (index, (path, filename)) in files.iter().enumerate() {
                let part = multipart::Part::bytes(std::fs::read(path)?)
                    .file_name(filename.clone())
                    .mime_str("application/octet-stream")?;
                form = form.part(format!("file{index}"), part);
            }

            let response = self
                .client
                .post(&url)
                .bearer_auth(&self.token)
                .multipart(form)
                .send()
                .await?;
            let status = response.status();

            if status.as_u16() != 200 {
                return Err(ClientError::Http {
                    url: url.clone(),
                    status: status.as_u16(),
                    body: parse_error_body(response).await?,
                });
            }

            let _ = response.bytes().await?;
            Ok(())
        })
        .await
    }

    async fn upload_objects(
        &self,
        repo_path: &str,
        build_url: &str,
        objects: &[String],
    ) -> Result<(), ClientError> {
        let mut batch = Vec::new();
        let mut batch_size = 0u64;

        for object in objects {
            let path = object_path(repo_path, object);
            let file_size = std::fs::metadata(&path)?.len();

            if batch_size + file_size > UPLOAD_CHUNK_LIMIT && !batch.is_empty() {
                self.upload_files(build_url, std::mem::take(&mut batch))
                    .await?;
                batch_size = 0;
            }

            batch.push((path, object.clone()));
            batch_size += file_size;

            if batch_size > UPLOAD_CHUNK_LIMIT {
                self.upload_files(build_url, std::mem::take(&mut batch))
                    .await?;
                batch_size = 0;
            }
        }

        if !batch.is_empty() {
            self.upload_files(build_url, batch).await?;
        }

        Ok(())
    }

    async fn upload_deltas(
        &self,
        repo_path: &str,
        build_url: &str,
        deltas: &[String],
        refs: &[(String, String)],
        ignore_delta: &[String],
    ) -> Result<(), ClientError> {
        if deltas.is_empty() {
            return Ok(());
        }

        let mut req = Vec::new();

        for (ref_name, commit) in refs {
            let ref_parts: Vec<&str> = ref_name.split('/').collect();
            if ref_parts.len() != 4 || (ref_parts[0] != "app" && ref_parts[0] != "runtime") {
                continue;
            }

            if should_skip_delta(ref_parts[1], ignore_delta) {
                continue;
            }

            for delta in deltas {
                let parsed_delta = parse_delta_name(delta)?;
                if parsed_delta.from.is_some() || parsed_delta.to != *commit {
                    continue;
                }

                println!(" {ref_name}: {}", parsed_delta.to);
                let encoded_name = parsed_delta
                    .to_name()
                    .map_err(|err| ClientError::Ostree(err.to_string()))?;
                let delta_dir = Path::new(repo_path)
                    .join("deltas")
                    .join(&encoded_name[..2])
                    .join(&encoded_name[2..]);

                let read_dir = match std::fs::read_dir(&delta_dir) {
                    Ok(read_dir) => read_dir,
                    Err(err) => {
                        log::warn!(
                            "Skipping delta {} at {}: {}",
                            parsed_delta,
                            delta_dir.display(),
                            err
                        );
                        continue;
                    }
                };

                let mut part_names = Vec::new();
                for entry in read_dir {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() {
                        continue;
                    }

                    let part_name = entry.file_name().into_string().map_err(|_| {
                        ClientError::Ostree(format!(
                            "Non-utf8 delta part name in {}",
                            delta_dir.display()
                        ))
                    })?;
                    part_names.push(part_name);
                }
                part_names.sort();

                for part_name in part_names {
                    req.push((
                        delta_dir.join(&part_name),
                        format!("{encoded_name}.{part_name}.delta"),
                    ));
                }
            }
        }

        self.upload_files(build_url, req).await
    }

    pub async fn create_ref(
        &self,
        build_url: &str,
        ref_name: &str,
        commit: &str,
        build_log_url: Option<&str>,
    ) -> Result<ApiResponse, ClientError> {
        println!("Creating ref {ref_name} with commit {commit}");
        let url = format!("{}/build_ref", build_url.trim_end_matches('/'));
        let body = CreateBuildRefRequest {
            ref_name,
            commit,
            build_log_url,
        };
        self.post_json(&url, &body).await
    }

    pub async fn add_extra_ids(
        &self,
        build_url: &str,
        ids: &[String],
    ) -> Result<ApiResponse, ClientError> {
        println!("Adding extra ids {ids:?}");
        let url = format!("{}/add_extra_ids", build_url.trim_end_matches('/'));
        let body = AddExtraIdsRequest { ids };
        self.post_json(&url, &body).await
    }

    pub async fn get_build(&self, build_url: &str) -> Result<Value, ClientError> {
        Ok(self.get(build_url).await?.body)
    }

    pub async fn push_build(&self, args: &PushArgs) -> Result<Value, ClientError> {
        let repo_file = gio::File::for_path(&args.repo_path);
        let repo = libostree::Repo::new(&repo_file);
        repo.open(gio::Cancellable::NONE).map_err(|err| {
            ClientError::Usage(format!("Can't open repo {}: {}", args.repo_path, err))
        })?;

        let refs: HashMap<String, String> = if args.branches.is_empty() {
            repo.list_refs_ext(
                None,
                libostree::RepoListRefsExtFlags::NONE,
                gio::Cancellable::NONE,
            )?
            .into_iter()
            .filter(|(name, _)| {
                name.starts_with("app/")
                    || name.starts_with("runtime/")
                    || name.starts_with("screenshots/")
            })
            .collect()
        } else {
            let mut refs = HashMap::new();
            for branch in &args.branches {
                let rev = repo
                    .resolve_rev(branch, false)?
                    .ok_or_else(|| ClientError::Usage(format!("No such ref: {branch}")))?;
                refs.insert(branch.clone(), rev.to_string());
            }
            refs
        };

        if refs.is_empty() {
            println!("No matching refs to upload");
            return self.get_build(&args.build_url).await;
        }

        let mut ref_pairs: Vec<(String, String)> = refs.into_iter().collect();
        ref_pairs.sort_by(|(left_name, _), (right_name, _)| left_name.cmp(right_name));

        let minimal_upload_client = if args.minimal_token {
            let manager_url = build_url_to_manager(&args.build_url)?;
            let build_id = args
                .build_url
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .filter(|segment| !segment.is_empty())
                .ok_or_else(|| ClientError::Usage("Invalid build URL".into()))?;
            let scope = vec!["upload".to_string()];
            let response = self
                .create_token(
                    &manager_url,
                    "minimal-upload",
                    &format!("build/{build_id}"),
                    &scope,
                    3600,
                )
                .await?;
            let minimal_token = response
                .body
                .get("token")
                .and_then(Value::as_str)
                .ok_or_else(|| ClientError::Usage("No token in create-token response".into()))?
                .to_string();
            Some(ApiClient::new(minimal_token))
        } else {
            None
        };
        let upload_client = minimal_upload_client.as_ref().unwrap_or(self);

        let ref_names = ref_pairs
            .iter()
            .map(|(ref_name, _)| ref_name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        println!("Uploading refs to {}: {ref_names}", args.build_url);

        let commits = ref_pairs
            .iter()
            .map(|(_, commit)| commit.clone())
            .collect::<Vec<_>>();
        let metadata_objects = local_needed_metadata(&repo, &commits)?;
        println!("Refs contain {} metadata objects", metadata_objects.len());

        let mut wanted_metadata = metadata_objects.iter().cloned().collect::<Vec<_>>();
        wanted_metadata.sort();
        let missing_metadata = if args.all_objects {
            println!("Uploading all metadata objects");
            wanted_metadata.clone()
        } else {
            let mut m = upload_client
                .missing_objects(&args.build_url, &wanted_metadata)
                .await?;
            println!("Remote missing {} of those", m.len());
            m.sort();
            m
        };

        let file_objects = local_needed_files(&repo, &metadata_objects)?;
        println!("Has {} file objects for those", file_objects.len());

        let mut wanted_files = file_objects.iter().cloned().collect::<Vec<_>>();
        wanted_files.sort();
        let missing_files = if args.all_objects {
            println!("Uploading all file objects");
            wanted_files.clone()
        } else {
            let mut m = upload_client
                .missing_objects(&args.build_url, &wanted_files)
                .await?;
            println!("Remote missing {} of those", m.len());
            m.sort();
            m
        };

        println!("Uploading file objects");
        upload_client
            .upload_objects(&args.repo_path, &args.build_url, &missing_files)
            .await?;

        println!("Uploading metadata objects");
        upload_client
            .upload_objects(&args.repo_path, &args.build_url, &missing_metadata)
            .await?;

        let deltas = repo
            .list_static_delta_names(gio::Cancellable::NONE)?
            .into_iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        println!("Uploading deltas");
        upload_client
            .upload_deltas(
                &args.repo_path,
                &args.build_url,
                &deltas,
                &ref_pairs,
                &args.ignore_delta,
            )
            .await?;

        for (ref_name, commit) in &ref_pairs {
            upload_client
                .create_ref(
                    &args.build_url,
                    ref_name,
                    commit,
                    args.build_log_url.as_deref(),
                )
                .await?;
        }

        if !args.extra_id.is_empty() {
            upload_client
                .add_extra_ids(&args.build_url, &args.extra_id)
                .await?;
        }

        let commit_job = if args.commit || args.publish {
            Some(
                self.commit_build(
                    &args.build_url,
                    args.end_of_life.as_deref(),
                    args.end_of_life_rebase.as_deref(),
                    args.token_type,
                )
                .await?,
            )
        } else {
            None
        };

        let publish_job = if args.publish {
            Some(
                self.publish_build(
                    &args.build_url,
                    args.wait || args.wait_update,
                    args.wait_update,
                )
                .await?,
            )
        } else {
            None
        };

        let mut data = self.get_build(&args.build_url).await?;
        if let Some(commit_job) = commit_job {
            data["commit_job"] = commit_job;
        }
        if let Some(publish_job) = publish_job {
            data["publish_job"] = publish_job;
        }

        Ok(data)
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
            let mut body = response.body;
            reparse_job_results(&mut body)?;
            body
        };
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
                    let start_after = parse_optional_system_time(response.body.get("start_after"))?;

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
    fn parses_optional_system_time_from_missing_or_null() {
        assert_eq!(parse_optional_system_time(None).unwrap(), None);
        assert_eq!(
            parse_optional_system_time(Some(&Value::Null)).unwrap(),
            None
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

    #[test]
    fn matches_simple_globs() {
        assert!(glob_matches("org.example.*", "org.example.App"));
        assert!(glob_matches("org.?xample.App", "org.example.App"));
        assert!(glob_matches("org.example.App", "org.example.App"));
        assert!(!glob_matches("org.example.App", "org.example.Other"));
    }

    #[test]
    fn skips_deltas_for_matching_globs() {
        assert!(should_skip_delta(
            "org.example.App",
            &["org.example.*".to_string()]
        ));
        assert!(!should_skip_delta(
            "org.example.App",
            &["org.other.*".to_string()]
        ));
        assert!(!should_skip_delta("org.example.App", &[]));
    }

    #[test]
    fn builds_object_paths_like_ostree_layout() {
        let checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let object_name = format!("{checksum}.filez");
        assert_eq!(
            object_path("/repo", &object_name),
            PathBuf::from(format!(
                "/repo/objects/01/23456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.filez"
            ))
        );
    }
}
