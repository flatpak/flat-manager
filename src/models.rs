/* see https://github.com/rust-lang/rust-clippy/issues/9014 */
#![allow(clippy::extra_unused_lifetimes)]

use crate::schema::{build_refs, builds, checks, job_dependencies, jobs, tokens};
use diesel::{Associations, Identifiable, Insertable, Queryable};
use serde::{Deserialize, Serialize};
use std::{mem, time};

#[derive(Deserialize, Insertable, Debug)]
#[diesel(table_name =  builds)]
pub struct NewBuild {
    pub repo: String,
    pub app_id: Option<String>,
    pub public_download: bool,
    pub build_log_url: Option<String>,
    pub builder_id: Option<String>,
}

#[derive(Identifiable, Serialize, Queryable, Debug, Eq, PartialEq)]
pub struct Build {
    pub id: i32,
    pub created: chrono::NaiveDateTime,
    pub repo_state: i16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_state_reason: Option<String>,
    pub published_state: i16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_state_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_job_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_job_id: Option<i32>,
    pub repo: String,
    pub extra_ids: Vec<Option<String>>,
    pub app_id: Option<String>,
    pub public_download: bool,
    pub build_log_url: Option<String>,
    /// The builder_id of the token used to create this build
    pub builder_id: Option<String>,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub enum PublishedState {
    Unpublished,
    Publishing,
    Published,
    Failed(String),
}

impl PublishedState {
    pub fn same_state_as(&self, other: &Self) -> bool {
        mem::discriminant(self) == mem::discriminant(other)
    }

    pub fn to_db(&self) -> (i16, Option<String>) {
        match self {
            PublishedState::Unpublished => (0, None),
            PublishedState::Publishing => (1, None),
            PublishedState::Published => (2, None),
            PublishedState::Failed(s) => (3, Some(s.to_string())),
        }
    }

    pub fn from_db(val: i16, reason: &Option<String>) -> Self {
        match val {
            0 => PublishedState::Unpublished,
            1 => PublishedState::Publishing,
            2 => PublishedState::Published,
            3 => PublishedState::Failed(
                reason
                    .as_ref()
                    .unwrap_or(&"Unknown reason".to_string())
                    .to_string(),
            ),
            _ => PublishedState::Failed("Unknown state".to_string()),
        }
    }
}

#[derive(Deserialize, Debug)]
pub enum RepoState {
    /// The repo is newly created and objects can be uploaded.
    Uploading,
    /// A commit job is queued for this build repo.
    Committing,
    /// The commit job is done and checks are still running or waiting for review.
    Validating,
    /// The commit job is done, any checks jobs have passed, and the build can be published.
    Ready,
    /// One of the build's jobs failed.
    Failed(String),
    /// The build repo is currently being deleted.
    Purging,
    /// The build repo has been deleted.
    Purged,
}

impl RepoState {
    pub fn same_state_as(&self, other: &Self) -> bool {
        mem::discriminant(self) == mem::discriminant(other)
    }

    pub fn to_db(&self) -> (i16, Option<String>) {
        match self {
            RepoState::Uploading => (0, None),
            RepoState::Committing => (1, None),
            RepoState::Ready => (2, None),
            RepoState::Failed(s) => (3, Some(s.to_string())),
            RepoState::Purging => (4, None),
            RepoState::Purged => (5, None),
            RepoState::Validating => (6, None),
        }
    }

    pub fn from_db(val: i16, reason: &Option<String>) -> Self {
        match val {
            0 => RepoState::Uploading,
            1 => RepoState::Committing,
            2 => RepoState::Ready,
            3 => RepoState::Failed(
                reason
                    .as_ref()
                    .unwrap_or(&"Unknown reason".to_string())
                    .to_string(),
            ),
            4 => RepoState::Purging,
            5 => RepoState::Purged,
            6 => RepoState::Validating,
            _ => RepoState::Failed("Unknown state".to_string()),
        }
    }
}

#[derive(Deserialize, Insertable, Debug)]
#[diesel(table_name =  build_refs)]
pub struct NewBuildRef {
    pub build_id: i32,
    pub ref_name: String,
    pub commit: String,
    pub build_log_url: Option<String>,
}

#[derive(Identifiable, Associations, Serialize, Queryable, Eq, PartialEq, Debug)]
#[diesel(belongs_to(Build))]
pub struct BuildRef {
    pub id: i32,
    pub build_id: i32,
    pub ref_name: String,
    pub commit: String,
    pub build_log_url: Option<String>,
}

diesel::table! {
    job_dependencies_with_status (job_id, depends_on) {
        job_id -> Int4,
        depends_on -> Int4,
        dependant_status -> Int2,
    }
}

diesel::allow_tables_to_appear_in_same_query!(jobs, job_dependencies_with_status,);

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub enum JobStatus {
    /// The job is in the queue.
    New,
    /// The job is running.
    Started,
    /// The job has completed successfully.
    Ended,
    /// The job encountered an error, or flat-manager was shut down before it could finish.
    Broken,
}

impl JobStatus {
    pub fn from_db(val: i16) -> Option<Self> {
        match val {
            0 => Some(JobStatus::New),
            1 => Some(JobStatus::Started),
            2 => Some(JobStatus::Ended),
            3 => Some(JobStatus::Broken),
            _ => None,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum JobKind {
    Commit,
    Publish,
    UpdateRepo,
    Republish,
    Check,
}

impl JobKind {
    pub fn to_db(&self) -> i16 {
        match self {
            JobKind::Commit => 0,
            JobKind::Publish => 1,
            JobKind::UpdateRepo => 2,
            JobKind::Republish => 3,
            JobKind::Check => 4,
        }
    }

    pub fn from_db(val: i16) -> Option<Self> {
        match val {
            0 => Some(JobKind::Commit),
            1 => Some(JobKind::Publish),
            2 => Some(JobKind::UpdateRepo),
            3 => Some(JobKind::Republish),
            4 => Some(JobKind::Check),
            _ => None,
        }
    }
}

#[derive(Deserialize, Insertable, Debug)]
#[diesel(table_name =  jobs)]
pub struct NewJob {
    pub kind: i16,
    pub contents: String,
    pub start_after: Option<time::SystemTime>,
    pub repo: Option<String>,
}

#[derive(Identifiable, Serialize, Queryable, Debug, Eq, PartialEq)]
pub struct Job {
    pub id: i32,
    pub kind: i16,
    pub status: i16,
    pub contents: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<String>,
    pub log: String,
    pub start_after: Option<time::SystemTime>,
    pub repo: Option<String>,
}

impl Job {
    // Ideally we'd do this via a SUBSTRING query, but at least do it behind the API
    pub fn apply_log_offset(mut self, log_offset: Option<usize>) -> Self {
        if let Some(log_offset) = log_offset {
            self.log = self
                .log
                .split_off(std::cmp::min(log_offset, self.log.len()))
        }
        self
    }
}

#[derive(Insertable, Debug, Queryable, Identifiable, Associations)]
#[diesel(table_name =  job_dependencies)]
#[diesel(primary_key(job_id, depends_on))]
#[diesel(belongs_to(Job, foreign_key = job_id))]
pub struct JobDependency {
    pub job_id: i32,
    pub depends_on: i32,
}

#[derive(Debug, Queryable, Identifiable, Associations)]
#[diesel(table_name =  job_dependencies_with_status)]
#[diesel(primary_key(job_id, depends_on))]
#[diesel(belongs_to(Job, foreign_key = job_id))]
pub struct JobDependencyWithStatus {
    pub job_id: i32,
    pub depends_on: i32,
    pub dependant_status: i16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "reason")]
pub enum CheckStatus {
    Pending,
    Passed,
    PassedWithWarnings(String),
    Failed(String),
    ReviewRequired(String),
}

impl CheckStatus {
    pub fn to_db(&self) -> (i16, Option<&String>) {
        match self {
            CheckStatus::Pending => (0, None),
            CheckStatus::Passed => (1, None),
            CheckStatus::PassedWithWarnings(s) => (2, Some(s)),
            CheckStatus::Failed(s) => (3, Some(s)),
            CheckStatus::ReviewRequired(s) => (4, Some(s)),
        }
    }

    pub fn from_db(val: i16, msg: Option<String>) -> Option<Self> {
        match val {
            0 => Some(CheckStatus::Pending),
            1 => Some(CheckStatus::Passed),
            2 => Some(CheckStatus::PassedWithWarnings(msg.unwrap_or_default())),
            3 => Some(CheckStatus::Failed(msg.unwrap_or_default())),
            4 => Some(CheckStatus::ReviewRequired(msg.unwrap_or_default())),
            _ => None,
        }
    }

    /// Whether the state is a finish state. When all checks on a build are finished, the build is put into the Ready
    /// or Failed state.
    pub fn is_finished(&self) -> bool {
        match self {
            CheckStatus::Passed | CheckStatus::PassedWithWarnings(_) | CheckStatus::Failed(_) => {
                true
            }
            CheckStatus::Pending | CheckStatus::ReviewRequired(_) => false,
        }
    }

    /// Whether the check failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, CheckStatus::Failed(_))
    }
}

#[derive(Debug, Queryable, Insertable, Identifiable, Associations, Serialize)]
#[diesel(primary_key(check_name, build_id))]
#[diesel(belongs_to(Build, foreign_key = build_id))]
#[diesel(belongs_to(Job, foreign_key = job_id))]
pub struct Check {
    pub check_name: String,
    pub build_id: i32,
    pub job_id: i32,
    pub status: i16,
    pub status_reason: Option<String>,
    pub results: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommitJob {
    pub build: i32,
    pub endoflife: Option<String>,
    pub endoflife_rebase: Option<String>,
    pub token_type: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PublishJob {
    pub build: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RepublishJob {
    pub app: String,
    pub endoflife: Option<String>,
    pub endoflife_rebase: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateRepoJob {
    pub repo: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CheckJob {
    pub build: i32,
    pub name: String,
}

#[derive(Identifiable, Queryable, Debug, Serialize)]
#[diesel(primary_key(token_id))]
pub struct Token {
    pub token_id: String,
    pub expires: Option<chrono::NaiveDateTime>,
    pub last_used: Option<chrono::NaiveDateTime>,
    pub revoked_at: Option<chrono::NaiveDateTime>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = tokens)]
pub struct NewToken {
    pub token_id: String,
    pub expires: chrono::NaiveDateTime,
    pub last_used: chrono::NaiveDateTime,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = tokens)]
pub struct NewRevokedToken {
    pub token_id: String,
    pub revoked_at: chrono::NaiveDateTime,
}
