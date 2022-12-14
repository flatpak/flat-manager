/* see https://github.com/rust-lang/rust-clippy/issues/9014 */
#![allow(clippy::extra_unused_lifetimes)]

use crate::schema::{build_refs, builds, job_dependencies, jobs};
use serde::{Deserialize, Serialize};
use std::{mem, time};

#[derive(Deserialize, Insertable, Debug)]
#[table_name = "builds"]
pub struct NewBuild {
    pub repo: String,
    pub app_id: Option<String>,
    pub public_download: bool,
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
    pub extra_ids: Vec<String>,
    pub app_id: Option<String>,
    pub public_download: bool,
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
    Uploading,
    Verifying,
    Ready,
    Failed(String),
    Purging,
    Purged,
}

impl RepoState {
    pub fn same_state_as(&self, other: &Self) -> bool {
        mem::discriminant(self) == mem::discriminant(other)
    }

    pub fn to_db(&self) -> (i16, Option<String>) {
        match self {
            RepoState::Uploading => (0, None),
            RepoState::Verifying => (1, None),
            RepoState::Ready => (2, None),
            RepoState::Failed(s) => (3, Some(s.to_string())),
            RepoState::Purging => (4, None),
            RepoState::Purged => (5, None),
        }
    }

    pub fn from_db(val: i16, reason: &Option<String>) -> Self {
        match val {
            0 => RepoState::Uploading,
            1 => RepoState::Verifying,
            2 => RepoState::Ready,
            3 => RepoState::Failed(
                reason
                    .as_ref()
                    .unwrap_or(&"Unknown reason".to_string())
                    .to_string(),
            ),
            4 => RepoState::Purging,
            5 => RepoState::Purged,
            _ => RepoState::Failed("Unknown state".to_string()),
        }
    }
}

#[derive(Deserialize, Insertable, Debug)]
#[table_name = "build_refs"]
pub struct NewBuildRef {
    pub build_id: i32,
    pub ref_name: String,
    pub commit: String,
}

#[derive(Identifiable, Associations, Serialize, Queryable, Eq, PartialEq, Debug)]
#[belongs_to(Build)]
pub struct BuildRef {
    pub id: i32,
    pub build_id: i32,
    pub ref_name: String,
    pub commit: String,
}

table! {
    job_dependencies_with_status (job_id, depends_on) {
        job_id -> Int4,
        depends_on -> Int4,
        dependant_status -> Int2,
    }
}

allow_tables_to_appear_in_same_query!(jobs, job_dependencies_with_status,);

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub enum JobStatus {
    New,
    Started,
    Ended,
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
}

impl JobKind {
    pub fn to_db(&self) -> i16 {
        match self {
            JobKind::Commit => 0,
            JobKind::Publish => 1,
            JobKind::UpdateRepo => 2,
            JobKind::Republish => 3,
        }
    }

    pub fn from_db(val: i16) -> Option<Self> {
        match val {
            0 => Some(JobKind::Commit),
            1 => Some(JobKind::Publish),
            2 => Some(JobKind::UpdateRepo),
            3 => Some(JobKind::Republish),
            _ => None,
        }
    }
}

#[derive(Deserialize, Insertable, Debug)]
#[table_name = "jobs"]
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
#[table_name = "job_dependencies"]
#[primary_key(job_id, depends_on)]
#[belongs_to(Job, foreign_key = "job_id")]
pub struct JobDependency {
    pub job_id: i32,
    pub depends_on: i32,
}

#[derive(Debug, Queryable, Identifiable, Associations)]
#[table_name = "job_dependencies_with_status"]
#[primary_key(job_id, depends_on)]
#[belongs_to(Job, foreign_key = "job_id")]
pub struct JobDependencyWithStatus {
    pub job_id: i32,
    pub depends_on: i32,
    pub dependant_status: i16,
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
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateRepoJob {
    pub repo: String,
}
