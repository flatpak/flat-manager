use actix::{Actor, SyncContext};
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};

use chrono;
use schema::{ build_refs, };

pub struct DbExecutor(pub Pool<ConnectionManager<PgConnection>>);

impl Actor for DbExecutor {
    type Context = SyncContext<Self>;
}

#[derive(Serialize, Queryable, Debug)]
pub struct Build {
    pub id: i32,
    pub is_published: bool,
    pub created: chrono::NaiveDateTime,
    pub repo_state: i16
}

pub enum RepoState {
    Uploading,
    Verifying,
    _Failed,
    _Ready,
    _Purged,
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
