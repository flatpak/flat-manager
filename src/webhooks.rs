use aws_lc_rs::hmac;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};

use crate::config::Config;
use crate::errors::ApiError;
use crate::metrics::Metrics;
use crate::models::{Build, CheckJob, CommitJob, Job, JobKind, PublishJob};
use crate::schema::{builds, jobs, webhook_deliveries};
use crate::Pool;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookEventType {
    #[serde(rename = "job.finished")]
    JobFinished,
}

impl WebhookEventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            WebhookEventType::JobFinished => "job.finished",
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct JobFinishedJob {
    pub id: i32,
    pub kind: i16,
    pub status: i16,
    pub repo: Option<String>,
    pub results: Option<String>,
    pub start_after: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct JobFinishedBuild {
    pub id: i32,
    pub repo: String,
    pub app_id: Option<String>,
    pub repo_state: i16,
    pub repo_state_reason: Option<String>,
    pub published_state: i16,
    pub published_state_reason: Option<String>,
    pub commit_job_id: Option<i32>,
    pub publish_job_id: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct JobFinishedData {
    pub job: JobFinishedJob,
    pub build: Option<JobFinishedBuild>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct JobFinishedPayload {
    pub id: i64,
    #[serde(rename = "type")]
    pub event_type: WebhookEventType,
    pub created_at: String,
    pub data: JobFinishedData,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = webhook_deliveries)]
pub struct WebhookDelivery {
    pub event_type: String,
    pub payload: serde_json::Value,
}

pub fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)
}

impl From<&Job> for JobFinishedJob {
    fn from(job: &Job) -> Self {
        JobFinishedJob {
            id: job.id,
            kind: job.kind,
            status: job.status,
            repo: job.repo.clone(),
            results: job.results.clone(),
            start_after: job
                .start_after
                .map(|start_after| format_timestamp(start_after.into())),
        }
    }
}

impl From<&Build> for JobFinishedBuild {
    fn from(build: &Build) -> Self {
        JobFinishedBuild {
            id: build.id,
            repo: build.repo.clone(),
            app_id: build.app_id.clone(),
            repo_state: build.repo_state,
            repo_state_reason: build.repo_state_reason.clone(),
            published_state: build.published_state,
            published_state_reason: build.published_state_reason.clone(),
            commit_job_id: build.commit_job_id,
            publish_job_id: build.publish_job_id,
        }
    }
}

impl JobFinishedPayload {
    pub fn new(id: i64, created_at: DateTime<Utc>, job: &Job, build: Option<&Build>) -> Self {
        JobFinishedPayload {
            id,
            event_type: WebhookEventType::JobFinished,
            created_at: format_timestamp(created_at),
            data: JobFinishedData {
                job: job.into(),
                build: build.map(Into::into),
            },
        }
    }

    fn restamp(&mut self) {
        self.created_at = format_timestamp(Utc::now());
    }
}

const MAX_ATTEMPTS: u32 = 66;
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const RETRY_BUDGET: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
pub struct WebhookQueue(mpsc::UnboundedSender<i64>);

impl WebhookQueue {
    pub fn push(&self, delivery_id: i64) {
        if self.0.send(delivery_id).is_err() {
            error!("Webhook delivery {delivery_id}: Sender is not running");
        }
    }
}

enum Attempt {
    Response(u16),
    Failed(String),
}

impl Attempt {
    fn is_success(&self) -> bool {
        matches!(self, Attempt::Response(200..=299))
    }

    fn is_retryable(&self) -> bool {
        matches!(self, Attempt::Failed(_) | Attempt::Response(500..))
    }
}

impl std::fmt::Display for Attempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Attempt::Response(status) => write!(f, "status {status}"),
            Attempt::Failed(error) => write!(f, "error: {error}"),
        }
    }
}

struct WebhookSender {
    client: reqwest::Client,
    url: String,
    key: hmac::Key,
    semaphore: Semaphore,
    pool: Pool,
    metrics: Arc<Metrics>,
}

fn sign(key: &hmac::Key, body: &[u8]) -> String {
    format!("sha256={}", hex::encode(hmac::sign(key, body).as_ref()))
}

fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(1 << attempt.saturating_sub(1).min(6)).min(MAX_BACKOFF)
}

fn error_chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

async fn run_blocking<T, F>(pool: &Pool, func: F) -> Result<T, String>
where
    F: FnOnce(&mut PgConnection) -> Result<T, DieselError> + Send + 'static,
    T: Send + 'static,
{
    let pool = pool.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        func(&mut conn).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())?
}

impl WebhookSender {
    fn new(
        url: String,
        key: &[u8],
        concurrency: usize,
        timeout: Duration,
        pool: Pool,
        metrics: Arc<Metrics>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .user_agent(concat!("flat-manager/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(timeout)
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("Failed to create webhook HTTP client");

        WebhookSender {
            client,
            url,
            key: hmac::Key::new(hmac::HMAC_SHA256, key),
            semaphore: Semaphore::new(concurrency),
            pool,
            metrics,
        }
    }

    async fn payload_bytes(&self, delivery_id: i64) -> Option<(String, Bytes)> {
        let pool = self.pool.clone();
        let loaded = run_blocking(&pool, move |conn| {
            let row = webhook_deliveries::table
                .find(delivery_id)
                .select(WebhookDelivery::as_select())
                .first::<WebhookDelivery>(conn)?;
            let mut payload = serde_json::from_value::<JobFinishedPayload>(row.payload)
                .map_err(|e| DieselError::DeserializationError(Box::new(e)))?;
            payload.restamp();
            let stored = serde_json::to_value(&payload)
                .map_err(|e| DieselError::SerializationError(Box::new(e)))?;
            diesel::update(webhook_deliveries::table.find(delivery_id))
                .set(webhook_deliveries::payload.eq(stored))
                .execute(conn)?;
            let body = serde_json::to_vec(&payload)
                .map_err(|e| DieselError::SerializationError(Box::new(e)))?;
            Ok((row.event_type, body))
        })
        .await;
        match loaded {
            Ok((event_type, body)) => Some((event_type, Bytes::from(body))),
            Err(e) => {
                error!("Webhook delivery {delivery_id}: Failed to load: {e}");
                None
            }
        }
    }

    async fn send(
        &self,
        delivery_id: i64,
        event_type: &str,
        body: &Bytes,
        signature: &str,
    ) -> Attempt {
        let _permit = self.semaphore.acquire().await;
        let response = self
            .client
            .post(&self.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("X-Flatmanager-Signature", signature)
            .header("X-Flatmanager-Event", event_type)
            .header("X-Flatmanager-Delivery", delivery_id.to_string())
            .header(
                "X-Flatmanager-Timestamp",
                Utc::now().timestamp().to_string(),
            )
            .body(body.clone())
            .send()
            .await;

        match response {
            Ok(response) => Attempt::Response(response.status().as_u16()),
            Err(e) => Attempt::Failed(error_chain(&e.without_url())),
        }
    }

    async fn record(&self, delivery_id: i64, attempt: &Attempt) {
        let (status, error) = match attempt {
            Attempt::Response(status) => (Some(i32::from(*status)), None),
            Attempt::Failed(error) => (None, Some(error.clone())),
        };
        let recorded = run_blocking(&self.pool, move |conn| {
            diesel::update(webhook_deliveries::table.find(delivery_id))
                .set((
                    webhook_deliveries::attempts.eq(webhook_deliveries::attempts + 1),
                    webhook_deliveries::last_attempt_at.eq(diesel::dsl::now),
                    webhook_deliveries::response_status.eq(status),
                    webhook_deliveries::last_error.eq(error),
                ))
                .execute(conn)
        })
        .await;
        if let Err(e) = recorded {
            error!("Webhook delivery {delivery_id}: Failed to record attempt: {e}");
        }
    }

    async fn deliver(&self, delivery_id: i64) {
        let started = tokio::time::Instant::now();

        for attempt_number in 1..=MAX_ATTEMPTS {
            let Some((event_type, body)) = self.payload_bytes(delivery_id).await else {
                return;
            };
            let signature = sign(&self.key, &body);

            let attempt = self.send(delivery_id, &event_type, &body, &signature).await;
            self.record(delivery_id, &attempt).await;

            if attempt.is_success() {
                info!("Webhook delivery {delivery_id}: {attempt} on attempt {attempt_number}");
                return;
            }

            self.metrics.webhook_delivery_failures.inc();
            if !attempt.is_retryable() {
                self.metrics.webhook_deliveries_exhausted.inc();
                error!("Webhook delivery {delivery_id}: {attempt} on attempt {attempt_number}, not retrying");
                return;
            }
            if attempt_number == MAX_ATTEMPTS || started.elapsed() >= RETRY_BUDGET {
                self.metrics.webhook_deliveries_exhausted.inc();
                error!("Webhook delivery {delivery_id}: {attempt} on attempt {attempt_number}, giving up");
                return;
            }

            let delay = backoff(attempt_number).min(RETRY_BUDGET.saturating_sub(started.elapsed()));
            warn!(
                "Webhook delivery {delivery_id}: {attempt} on attempt {attempt_number}, retrying in {}s",
                delay.as_secs()
            );
            tokio::time::sleep(delay).await;
        }
    }
}

pub fn start_webhook_sender(
    config: &Config,
    pool: &Pool,
    metrics: &Arc<Metrics>,
) -> Option<WebhookQueue> {
    let sender = Arc::new(WebhookSender::new(
        config.webhook_url.clone()?,
        config.webhook_hmac_key.as_deref()?,
        config.webhook_concurrency,
        Duration::from_secs(config.webhook_timeout_secs),
        pool.clone(),
        metrics.clone(),
    ));
    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Some(delivery_id) = rx.recv().await {
            let sender = sender.clone();
            tokio::spawn(async move { sender.deliver(delivery_id).await });
        }
    });

    info!("Webhook deliveries enabled");
    Some(WebhookQueue(tx))
}

pub async fn redeliver(
    pool: &Pool,
    queue: &WebhookQueue,
    before: DateTime<Utc>,
) -> Result<usize, ApiError> {
    let mut delivery_ids = run_blocking(pool, move |conn| {
        webhook_deliveries::table
            .filter(webhook_deliveries::created_at.lt(before))
            .filter(
                webhook_deliveries::response_status
                    .is_null()
                    .or(webhook_deliveries::response_status.lt(200))
                    .or(webhook_deliveries::response_status.ge(300)),
            )
            .select(webhook_deliveries::id)
            .get_results::<i64>(conn)
    })
    .await
    .map_err(ApiError::InternalServerError)?;

    delivery_ids.sort_unstable();
    for delivery_id in &delivery_ids {
        queue.push(*delivery_id);
    }
    Ok(delivery_ids.len())
}

fn load_build(job: &Job, conn: &mut PgConnection) -> Result<Option<Build>, DieselError> {
    let linked = match JobKind::from_db(job.kind) {
        Some(JobKind::Commit) => builds::table
            .filter(builds::commit_job_id.eq(job.id))
            .first::<Build>(conn)
            .optional()?,
        Some(JobKind::Publish) => builds::table
            .filter(builds::publish_job_id.eq(job.id))
            .first::<Build>(conn)
            .optional()?,
        _ => None,
    };
    if linked.is_some() {
        return Ok(linked);
    }

    let build_id = match JobKind::from_db(job.kind) {
        Some(JobKind::Commit) => serde_json::from_str::<CommitJob>(&job.contents).map(|c| c.build),
        Some(JobKind::Check) => serde_json::from_str::<CheckJob>(&job.contents).map(|c| c.build),
        Some(JobKind::Publish) => {
            serde_json::from_str::<PublishJob>(&job.contents).map(|p| p.build)
        }
        _ => return Ok(None),
    };
    match build_id {
        Ok(build_id) => builds::table.find(build_id).first::<Build>(conn).optional(),
        Err(e) => {
            warn!("#{}: Can't parse job contents for webhook: {}", job.id, e);
            Ok(None)
        }
    }
}

fn record_job_finished(job_id: i32, conn: &mut PgConnection) -> Result<i64, DieselError> {
    let job = jobs::table.find(job_id).get_result::<Job>(conn)?;
    let build = load_build(&job, conn)?;

    conn.transaction::<_, DieselError, _>(|conn| {
        let (delivery_id, created_at) = diesel::insert_into(webhook_deliveries::table)
            .values((
                webhook_deliveries::event_type.eq(WebhookEventType::JobFinished.as_str()),
                webhook_deliveries::payload.eq(json!({})),
            ))
            .returning((webhook_deliveries::id, webhook_deliveries::created_at))
            .get_result::<(i64, DateTime<Utc>)>(conn)?;

        let payload = serde_json::to_value(JobFinishedPayload::new(
            delivery_id,
            created_at,
            &job,
            build.as_ref(),
        ))
        .map_err(|e| DieselError::SerializationError(Box::new(e)))?;
        diesel::update(webhook_deliveries::table.find(delivery_id))
            .set(webhook_deliveries::payload.eq(payload))
            .execute(conn)?;

        Ok(delivery_id)
    })
}

pub fn emit_job_finished(queue: &Option<WebhookQueue>, job_id: i32, conn: &mut PgConnection) {
    let Some(queue) = queue else {
        return;
    };

    match record_job_finished(job_id, conn) {
        Ok(delivery_id) => queue.push(delivery_id),
        Err(e) => error!("#{}: Failed to record job.finished webhook: {}", job_id, e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{PublishedState, RepoState};
    use base64::{engine::general_purpose, Engine as _};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::UNIX_EPOCH;

    const TEST_BODY: &str = r#"{"id":18231,"type":"job.finished","created_at":"2026-11-16T09:30:00Z","data":{"job":{"id":624335,"kind":0,"status":2,"repo":null,"results":"{\"refs\":{}}","start_after":null},"build":{"id":198765,"repo":"stable","app_id":null,"repo_state":6,"repo_state_reason":null,"published_state":0,"published_state_reason":null,"commit_job_id":624335,"publish_job_id":null}}}"#;

    const TEST_KEY: &str = "ZmxhdC1tYW5hZ2VyLXdlYmhvb2stdGVzdC1zZWNyZXQ=";

    const TEST_SIGNATURE: &str =
        "sha256=02ea48ed7f50e59cc22493cf5983d2f0977e26424fd7b96d08e9c8a7d7ae57a1";

    fn job() -> Job {
        Job {
            id: 624335,
            kind: 0,
            status: 2,
            contents: r#"{"build":198765,"token":"secret-token-material"}"#.to_string(),
            results: Some(r#"{"refs":{}}"#.to_string()),
            log: "full build log".to_string(),
            start_after: None,
            repo: None,
        }
    }

    fn build() -> Build {
        Build {
            id: 198765,
            created: DateTime::from_timestamp(1_700_000_000, 0)
                .unwrap()
                .naive_utc(),
            repo_state: RepoState::Validating.to_db().0,
            repo_state_reason: None,
            published_state: PublishedState::Unpublished.to_db().0,
            published_state_reason: None,
            commit_job_id: Some(624335),
            publish_job_id: None,
            repo: "stable".to_string(),
            extra_ids: vec![Some("extra".to_string())],
            app_id: None,
            public_download: true,
            build_log_url: Some("https://example.org/log".to_string()),
            token_name: Some("upload-token".to_string()),
            token_type: Some("app".to_string()),
        }
    }

    fn created_at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-16T09:30:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn payload() -> JobFinishedPayload {
        JobFinishedPayload::new(18231, created_at(), &job(), Some(&build()))
    }

    fn sorted_keys(value: &serde_json::Value) -> Vec<&str> {
        let mut keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        keys
    }

    #[test]
    fn payload_serializes_to_spec_test_vector() {
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&payload()).unwrap()).unwrap(),
            TEST_BODY
        );
    }

    #[test]
    fn event_type_serializes_as_job_finished() {
        let event_type = WebhookEventType::JobFinished;

        assert_eq!(event_type.as_str(), "job.finished");
        assert_eq!(
            serde_json::to_string(&event_type).unwrap(),
            format!("\"{}\"", event_type.as_str())
        );
    }

    #[test]
    fn projections_only_carry_contract_fields() {
        let value = serde_json::to_value(payload()).unwrap();

        assert_eq!(sorted_keys(&value), ["created_at", "data", "id", "type"]);
        assert_eq!(sorted_keys(&value["data"]), ["build", "job"]);
        assert_eq!(
            sorted_keys(&value["data"]["job"]),
            ["id", "kind", "repo", "results", "start_after", "status"]
        );
        assert_eq!(
            sorted_keys(&value["data"]["build"]),
            [
                "app_id",
                "commit_job_id",
                "id",
                "publish_job_id",
                "published_state",
                "published_state_reason",
                "repo",
                "repo_state",
                "repo_state_reason"
            ]
        );

        let body = serde_json::to_string(&payload()).unwrap();
        for leaked in [
            "secret-token-material",
            "full build log",
            "upload-token",
            "https://example.org/log",
            "extra",
        ] {
            assert!(!body.contains(leaked), "payload contains {leaked}");
        }
    }

    #[test]
    fn optional_fields_serialize_as_explicit_null() {
        let mut job = job();
        job.results = None;
        let value =
            serde_json::to_value(JobFinishedPayload::new(1, created_at(), &job, None)).unwrap();

        assert!(value["data"]["build"].is_null());
        assert_eq!(sorted_keys(&value["data"]).len(), 2);
        for field in ["repo", "results", "start_after"] {
            assert!(value["data"]["job"][field].is_null(), "{field} is not null");
        }
        assert_eq!(sorted_keys(&value["data"]["job"]).len(), 6);

        let value = serde_json::to_value(JobFinishedBuild::from(&build())).unwrap();

        assert_eq!(sorted_keys(&value).len(), 9);
        for field in [
            "app_id",
            "repo_state_reason",
            "published_state_reason",
            "publish_job_id",
        ] {
            assert!(value[field].is_null(), "{field} is not null");
        }
    }

    #[test]
    fn repo_state_uses_database_mapping() {
        let states = [
            (RepoState::Uploading, 0),
            (RepoState::Committing, 1),
            (RepoState::Ready, 2),
            (RepoState::Failed("reason".to_string()), 3),
            (RepoState::Purging, 4),
            (RepoState::Purged, 5),
            (RepoState::Validating, 6),
        ];

        for (state, expected) in states {
            let (repo_state, repo_state_reason) = state.to_db();
            let mut build = build();
            build.repo_state = repo_state;
            build.repo_state_reason = repo_state_reason.clone();

            let projected = JobFinishedBuild::from(&build);

            assert_eq!(projected.repo_state, expected);
            assert_eq!(projected.repo_state_reason, repo_state_reason);
        }
    }

    #[test]
    fn published_state_uses_database_mapping() {
        let states = [
            (PublishedState::Unpublished, 0),
            (PublishedState::Publishing, 1),
            (PublishedState::Published, 2),
            (PublishedState::Failed("reason".to_string()), 3),
        ];

        for (state, expected) in states {
            let (published_state, published_state_reason) = state.to_db();
            let mut build = build();
            build.published_state = published_state;
            build.published_state_reason = published_state_reason.clone();

            let projected = JobFinishedBuild::from(&build);

            assert_eq!(projected.published_state, expected);
            assert_eq!(projected.published_state_reason, published_state_reason);
        }
    }

    #[test]
    fn job_projection_copies_served_integers_and_formats_start_after() {
        let mut job = job();
        job.kind = 2;
        job.status = 3;
        job.repo = Some("stable".to_string());
        job.start_after = Some(UNIX_EPOCH + Duration::new(1_795_000_000, 123_456_000));

        let projected = JobFinishedJob::from(&job);

        assert_eq!(projected.kind, 2);
        assert_eq!(projected.status, 3);
        assert_eq!(projected.repo.as_deref(), Some("stable"));
        assert_eq!(
            projected.start_after.as_deref(),
            Some("2026-11-18T11:06:40Z")
        );
    }

    #[test]
    fn payload_bytes_survive_stored_value_round_trip() {
        let original = serde_json::to_vec(&payload()).unwrap();

        let stored = serde_json::to_value(payload()).unwrap();
        let restored: JobFinishedPayload = serde_json::from_value(stored).unwrap();

        assert_eq!(serde_json::to_vec(&restored).unwrap(), original);
    }

    fn test_key() -> hmac::Key {
        let key = general_purpose::STANDARD.decode(TEST_KEY).unwrap();
        hmac::Key::new(hmac::HMAC_SHA256, &key)
    }

    #[test]
    fn signature_matches_spec_test_vector() {
        let body = serde_json::to_vec(&payload()).unwrap();

        assert_eq!(body, TEST_BODY.as_bytes());
        assert_eq!(sign(&test_key(), &body), TEST_SIGNATURE);
    }

    #[test]
    fn altering_one_byte_of_the_body_changes_the_signature() {
        let key = test_key();
        let body = TEST_BODY.as_bytes();
        let tag = hex::decode(TEST_SIGNATURE.strip_prefix("sha256=").unwrap()).unwrap();

        assert!(hmac::verify(&key, body, &tag).is_ok());
        for index in 0..body.len() {
            let mut altered = body.to_vec();
            altered[index] ^= 1;

            assert_ne!(sign(&key, &altered), TEST_SIGNATURE, "byte {index}");
            assert!(hmac::verify(&key, &altered, &tag).is_err(), "byte {index}");
        }
    }

    #[test]
    fn altering_one_byte_of_the_key_changes_the_signature() {
        let altered = hmac::Key::new(hmac::HMAC_SHA256, b"flat-manager-webhook-test-secreT");

        assert_ne!(sign(&altered, TEST_BODY.as_bytes()), TEST_SIGNATURE);
    }

    #[test]
    fn restamping_keeps_the_event_and_refreshes_the_signature() {
        let mut restamped = payload();
        restamped.created_at = "2026-11-16T09:30:00Z".to_string();
        restamped.restamp();

        assert_eq!(restamped.id, 18231);
        assert_eq!(restamped.event_type, WebhookEventType::JobFinished);
        assert_eq!(restamped.data, payload().data);
        assert_ne!(restamped.created_at, "2026-11-16T09:30:00Z");
        let body = serde_json::to_vec(&restamped).unwrap();
        assert_ne!(sign(&test_key(), &body), TEST_SIGNATURE);
    }

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let delays: Vec<u64> = (1..=10).map(|attempt| backoff(attempt).as_secs()).collect();

        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60, 60, 60]);
    }

    #[test]
    fn retries_cover_an_hour_long_receiver_outage() {
        let waited: u64 = (1..MAX_ATTEMPTS)
            .map(|attempt| backoff(attempt).as_secs())
            .sum();

        assert!(waited >= 60 * 60);
        assert!(waited <= 60 * 60 + 60);
    }

    #[test]
    fn only_transport_errors_and_server_errors_are_retried() {
        for status in [200, 204, 299] {
            assert!(Attempt::Response(status).is_success(), "{status}");
        }
        for status in [100, 199, 300, 302, 400, 401, 404, 408, 429, 499] {
            assert!(!Attempt::Response(status).is_success(), "{status}");
            assert!(!Attempt::Response(status).is_retryable(), "{status}");
        }
        for status in [500, 502, 503, 599] {
            assert!(!Attempt::Response(status).is_success(), "{status}");
            assert!(Attempt::Response(status).is_retryable(), "{status}");
        }

        let failed = Attempt::Failed("connection refused".to_string());
        assert!(!failed.is_success());
        assert!(failed.is_retryable());
    }

    #[derive(thiserror::Error, Debug)]
    #[error("outer")]
    struct Outer(#[source] Inner);

    #[derive(thiserror::Error, Debug)]
    #[error("inner")]
    struct Inner;

    #[test]
    fn error_chain_joins_sources() {
        assert_eq!(error_chain(&Outer(Inner)), "outer: inner");
    }

    struct Received {
        head: String,
        body: Vec<u8>,
    }

    impl Received {
        fn header(&self, name: &str) -> &str {
            self.head
                .lines()
                .skip(1)
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case(name).then_some(value.trim())
                })
                .unwrap_or_else(|| panic!("missing header {name}"))
        }
    }

    fn read_request(stream: &mut TcpStream) -> Received {
        let mut data = Vec::new();
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            if let Some(position) = data.windows(4).position(|window| window == b"\r\n\r\n") {
                break position;
            }
            let read = stream.read(&mut chunk).unwrap();
            assert!(read > 0, "connection closed before the request head ended");
            data.extend_from_slice(&chunk[..read]);
        };

        let head = String::from_utf8(data[..head_end].to_vec()).unwrap();
        let length = head
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        while data.len() < head_end + 4 + length {
            let read = stream.read(&mut chunk).unwrap();
            assert!(read > 0, "connection closed before the request body ended");
            data.extend_from_slice(&chunk[..read]);
        }

        Received {
            head,
            body: data[head_end + 4..head_end + 4 + length].to_vec(),
        }
    }

    fn serve_once(response: &'static str) -> (String, std::thread::JoinHandle<Received>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let received = read_request(&mut stream);
            stream.write_all(response.as_bytes()).unwrap();
            received
        });
        (url, handle)
    }

    fn test_sender(url: String) -> WebhookSender {
        let manager = diesel::r2d2::ConnectionManager::<PgConnection>::new("postgres://unused");
        WebhookSender::new(
            url,
            b"flat-manager-webhook-test-secret",
            1,
            Duration::from_secs(5),
            Pool::builder().build_unchecked(manager),
            Arc::new(Metrics::new()),
        )
    }

    #[tokio::test]
    async fn send_posts_the_exact_body_with_signature_headers() {
        let (url, server) =
            serve_once("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        let sender = test_sender(url);
        let body = Bytes::from(serde_json::to_vec(&payload()).unwrap());
        let signature = sign(&sender.key, &body);

        let attempt = sender.send(18231, "job.finished", &body, &signature).await;

        assert!(matches!(attempt, Attempt::Response(204)), "{attempt}");
        let received = server.join().unwrap();
        assert!(received.head.starts_with("POST /hook HTTP/1.1\r\n"));
        assert_eq!(received.body, TEST_BODY.as_bytes());
        assert_eq!(received.header("content-type"), "application/json");
        assert_eq!(received.header("x-flatmanager-signature"), TEST_SIGNATURE);
        assert_eq!(received.header("x-flatmanager-event"), "job.finished");
        assert_eq!(received.header("x-flatmanager-delivery"), "18231");
        let timestamp: i64 = received.header("x-flatmanager-timestamp").parse().unwrap();
        assert!((Utc::now().timestamp() - timestamp).abs() < 5);
    }

    #[tokio::test]
    async fn send_does_not_follow_redirects() {
        let (url, server) = serve_once(
            "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:9/other\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        let sender = test_sender(url);

        let attempt = sender
            .send(1, "job.finished", &Bytes::from_static(b"{}"), "sha256=00")
            .await;

        assert!(matches!(attempt, Attempt::Response(302)), "{attempt}");
        server.join().unwrap();
    }

    #[tokio::test]
    async fn send_reports_transport_errors_without_the_url() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/secret-path", listener.local_addr().unwrap());
        drop(listener);
        let sender = test_sender(url);

        let attempt = sender
            .send(1, "job.finished", &Bytes::from_static(b"{}"), "sha256=00")
            .await;

        assert!(attempt.is_retryable());
        match attempt {
            Attempt::Failed(error) => assert!(!error.contains("secret-path"), "{error}"),
            Attempt::Response(status) => panic!("unexpected status {status}"),
        }
    }
}
