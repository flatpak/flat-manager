use chrono::{DateTime, SecondsFormat, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::models::{Build, Job};
use crate::schema::webhook_deliveries;

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
    pub id: i64,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{PublishedState, RepoState};
    use std::time::{Duration, UNIX_EPOCH};

    const TEST_BODY: &str = r#"{"id":18231,"type":"job.finished","created_at":"2026-11-16T09:30:00Z","data":{"job":{"id":624335,"kind":0,"status":2,"repo":null,"results":"{\"refs\":{}}","start_after":null},"build":{"id":198765,"repo":"stable","app_id":null,"repo_state":6,"repo_state_reason":null,"published_state":0,"published_state_reason":null,"commit_job_id":624335,"publish_job_id":null}}}"#;

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
}
