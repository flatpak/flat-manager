use actix::{Actor, SyncContext};
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use std::mem;

use chrono;
use schema::{ build_refs, };

pub struct DbExecutor(pub Pool<ConnectionManager<PgConnection>>);

impl Actor for DbExecutor {
    type Context = SyncContext<Self>;
}

#[derive(Serialize, Queryable, Debug)]
pub struct Build {
    pub id: i32,
    pub created: chrono::NaiveDateTime,
    pub repo_state: i16,
    pub repo_state_reason: Option<String>,
    pub published_state: i16,
    pub published_state_reason: Option<String>,
}

#[derive(Deserialize, Debug,PartialEq)]
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
            PublishedState::Failed(s) => (3, Some(s.to_string()))
        }
    }

    pub fn from_db(val: i16, reason: &Option<String>) -> Self {
        match val {
            0 => PublishedState::Unpublished,
            1 => PublishedState::Publishing,
            2 => PublishedState::Published,
            3 => PublishedState::Failed(reason.as_ref().unwrap_or(&"Unknown reason".to_string()).to_string()),
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
            RepoState::Purged => (4, None),
        }
    }

    pub fn from_db(val: i16, reason: &Option<String>) -> Self {
        match val {
            0 => RepoState::Uploading,
            1 => RepoState::Verifying,
            2 => RepoState::Ready,
            3 => RepoState::Failed(reason.as_ref().unwrap_or(&"Unknown reason".to_string()).to_string()),
            4 => RepoState::Purged,
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

#[derive(Serialize, Queryable, Debug)]
pub struct BuildRef {
    pub id: i32,
    pub build_id: i32,
    pub ref_name: String,
    pub commit: String,
}
