use actix::prelude::*;
use actix_web::*;
use diesel;
use diesel::prelude::*;
use diesel::result::{Error as DieselError};

use models::*;
use errors::ApiError;
use schema;

pub trait DbRequest : std::marker::Send + std::marker::Sized + 'static {
    type DbType: 'static + std::marker::Send;
}
pub struct DbRequestWrapper<T>(pub T);

impl <T: DbRequest> Message for DbRequestWrapper<T> {
    type Result = Result<T::DbType, ApiError>;
}

impl DbRequest for NewBuild {
    type DbType = Build;
}

impl Handler<DbRequestWrapper<NewBuild>> for DbExecutor {
    type Result = Result<<NewBuild as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<NewBuild>, _: &mut Self::Context) -> Self::Result {
        use self::schema::builds::dsl::*;
        let conn = &self.0.get().unwrap();
        diesel::insert_into(builds)
            .values(&msg.0)
            .get_result::<Build>(conn)
            .map_err(|e| {
                From::from(e)
            })
    }
}

impl DbRequest for NewBuildRef {
    type DbType = BuildRef;
}

impl Handler<DbRequestWrapper<NewBuildRef>> for DbExecutor {
    type Result = Result<<NewBuildRef as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<NewBuildRef>, _: &mut Self::Context) -> Self::Result {
        use self::schema::build_refs::dsl::*;
        let conn = &self.0.get().unwrap();
        diesel::insert_into(build_refs)
            .values(&msg.0)
            .get_result::<BuildRef>(conn)
            .map_err(|e| {
                From::from(e)
            })
    }
}

#[derive(Deserialize, Debug)]
pub struct LookupJob {
    pub id: i32,
    pub log_offset: Option<usize>,
}

impl DbRequest for LookupJob {
    type DbType = Job;
}

// Ideally we'd do this via a SUBSTRING query, but at least do it behind the API
fn handle_log_offset(mut job: Job, log_offset: Option<usize>) -> Job {
    if let Some(log_offset) = log_offset {
        job.log = job.log.split_off(log_offset)
    }
    job
}

impl Handler<DbRequestWrapper<LookupJob>> for DbExecutor {
    type Result = Result<<LookupJob as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<LookupJob>, _: &mut Self::Context) -> Self::Result {
        use schema::jobs::dsl::*;
        let conn = &self.0.get().unwrap();
        jobs
            .filter(id.eq(msg.0.id))
            .get_result::<Job>(conn)
            .map(|job| handle_log_offset (job, msg.0.log_offset))
            .map_err(|e| {
                From::from(e)
            })
    }
}

#[derive(Deserialize, Debug)]
pub struct LookupCommitJob {
    pub build_id: i32,
    pub log_offset: Option<usize>,
}

impl DbRequest for LookupCommitJob {
    type DbType = Job;
}

impl Handler<DbRequestWrapper<LookupCommitJob>> for DbExecutor {
    type Result = Result<<LookupCommitJob as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<LookupCommitJob>, _: &mut Self::Context) -> Self::Result {
        use schema::jobs::dsl::*;
        use schema::builds::dsl::*;
        let conn = &self.0.get().unwrap();
        jobs
            .inner_join(builds.on(commit_job_id.eq(schema::jobs::dsl::id.nullable())))
            .select(schema::jobs::all_columns)
            .filter(schema::builds::dsl::id.eq(msg.0.build_id))
            .get_result::<Job>(conn)
            .map(|job| handle_log_offset (job, msg.0.log_offset))
            .map_err(|e| {
                From::from(e)
            })
    }
}

#[derive(Deserialize, Debug)]
pub struct LookupPublishJob {
    pub build_id: i32,
    pub log_offset: Option<usize>,
}

impl DbRequest for LookupPublishJob {
    type DbType = Job;
}

impl Handler<DbRequestWrapper<LookupPublishJob>> for DbExecutor {
    type Result = Result<<LookupPublishJob as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<LookupPublishJob>, _: &mut Self::Context) -> Self::Result {
        use schema::jobs::dsl::*;
        use schema::builds::dsl::*;
        let conn = &self.0.get().unwrap();
        jobs
            .inner_join(builds.on(publish_job_id.eq(schema::jobs::dsl::id.nullable())))
            .select(schema::jobs::all_columns)
            .filter(schema::builds::dsl::id.eq(msg.0.build_id))
            .get_result::<Job>(conn)
            .map(|job| handle_log_offset (job, msg.0.log_offset))
            .map_err(|e| {
                From::from(e)
            })
    }
}

#[derive(Deserialize, Debug)]
pub struct LookupBuild {
    pub id: i32
}

impl DbRequest for LookupBuild {
    type DbType = Build;
}

impl Handler<DbRequestWrapper<LookupBuild>> for DbExecutor {
    type Result = Result<<LookupBuild as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<LookupBuild>, _: &mut Self::Context) -> Self::Result {
        use schema::builds::dsl::*;
        let conn = &self.0.get().unwrap();
        builds
            .filter(id.eq(msg.0.id))
            .get_result::<Build>(conn)
            .map_err(|e| {
                From::from(e)
            })
    }
}

#[derive(Deserialize, Debug)]
pub struct LookupBuildRef {
    pub id: i32,
    pub ref_id: i32,
}

impl DbRequest for LookupBuildRef {
    type DbType = BuildRef;
}

impl Handler<DbRequestWrapper<LookupBuildRef>> for DbExecutor {
    type Result = Result<<LookupBuildRef as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<LookupBuildRef>, _: &mut Self::Context) -> Self::Result {
        use schema::build_refs::dsl::*;
        let conn = &self.0.get().unwrap();
        build_refs
            .filter(build_id.eq(msg.0.id))
            .filter(id.eq(msg.0.ref_id))
            .get_result::<BuildRef>(conn)
            .map_err(|e| From::from(e))
    }
}

#[derive(Deserialize, Debug)]
pub struct LookupBuildRefs {
    pub id: i32,
}

impl DbRequest for LookupBuildRefs {
    type DbType = Vec<BuildRef>;
}

impl Handler<DbRequestWrapper<LookupBuildRefs>> for DbExecutor {
    type Result = Result<<LookupBuildRefs as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<LookupBuildRefs>, _: &mut Self::Context) -> Self::Result {
        use schema::build_refs::dsl::*;
        let conn = &self.0.get().unwrap();
        build_refs
            .filter(build_id.eq(msg.0.id))
            .get_results::<BuildRef>(conn)
            .map_err(|e| From::from(e))
    }
}

#[derive(Deserialize, Debug)]
pub struct ListBuilds {
}

impl DbRequest for ListBuilds {
    type DbType = Vec<Build>;
}

impl Handler<DbRequestWrapper<ListBuilds>> for DbExecutor {
    type Result = Result<<ListBuilds as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, _msg: DbRequestWrapper<ListBuilds>, _: &mut Self::Context) -> Self::Result {
        use schema::builds::dsl::*;
        let conn = &self.0.get().unwrap();
        let (val, _) = RepoState::Purged.to_db();
        builds
            .filter(repo_state.ne(val))
            .get_results::<Build>(conn)
            .map_err(|e| {
                From::from(e)
            })
    }
}


#[derive(Deserialize, Debug)]
pub struct StartCommitJob {
    pub id: i32,
    pub endoflife: Option<String>,
}

impl DbRequest for StartCommitJob {
    type DbType = Job;
}

impl Handler<DbRequestWrapper<StartCommitJob>> for DbExecutor {
    type Result = Result<<StartCommitJob as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<StartCommitJob>, _: &mut Self::Context) -> Self::Result {
        let conn = &self.0.get().unwrap();
        conn.transaction::<Job, DieselError, _>(|| {
            let current_build = schema::builds::table
                .filter(schema::builds::id.eq(msg.0.id))
                .get_result::<Build>(conn)?;
            let current_repo_state = RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            if !current_repo_state.same_state_as(&RepoState::Uploading) {
                return Err(DieselError::RollbackTransaction)
            };
            let (val, reason) = RepoState::to_db(&RepoState::Verifying);
            let job =
            diesel::insert_into(schema::jobs::table)
                .values(NewJob {
                    kind: JobKind::Commit.to_db(),
                    contents: json!(CommitJob {
                        build: msg.0.id,
                        endoflife: msg.0.endoflife
                    }).to_string(),
                })
                .get_result::<Job>(conn)?;
            diesel::update(schema::builds::table)
                .filter(schema::builds::id.eq(msg.0.id))
                .set((schema::builds::commit_job_id.eq(job.id),
                      schema::builds::repo_state.eq(val),
                      schema::builds::repo_state_reason.eq(reason)))
                .get_result::<Build>(conn)?;
            Ok(job)
        })
            .map_err(|e| {
                match e {
                    DieselError::RollbackTransaction => ApiError::BadRequest("Build is already commited".to_string()),
                    _ => From::from(e)
                }
            })
    }
}


#[derive(Deserialize, Debug)]
pub struct StartPublishJob {
    pub id: i32,
    pub subsets: Option<Vec<String>>
}

impl DbRequest for StartPublishJob {
    type DbType = Job;
}

impl Handler<DbRequestWrapper<StartPublishJob>> for DbExecutor {
    type Result = Result<<StartPublishJob as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<StartPublishJob>, _: &mut Self::Context) -> Self::Result {
        let conn = &self.0.get().unwrap();
        conn.transaction::<Job, DieselError, _>(|| {
            let current_build = schema::builds::table
                .filter(schema::builds::id.eq(msg.0.id))
                .get_result::<Build>(conn)?;
            let current_published_state = PublishedState::from_db(current_build.published_state, &current_build.published_state_reason);
            if !current_published_state.same_state_as(&PublishedState::Unpublished) {
                error!("Unexpected publishing state {:?}", current_published_state);
                return Err(DieselError::RollbackTransaction) // Already published
            };
            let current_repo_state = RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            if !current_repo_state.same_state_as(&RepoState::Ready) {
                error!("Unexpected repo state {:?}", current_repo_state);
                return Err(DieselError::RollbackTransaction) // Not commited correctly
            };
            let (val, reason) = PublishedState::to_db(&PublishedState::Publishing);
            let job =
                diesel::insert_into(schema::jobs::table)
                .values(NewJob {
                    kind: JobKind::Publish.to_db(),
                    contents: json!(PublishJob {
                        build: msg.0.id,
                        subsets: msg.0.subsets,
                    }).to_string(),
                })
                .get_result::<Job>(conn)?;
            diesel::update(schema::builds::table)
                .filter(schema::builds::id.eq(msg.0.id))
                .set((schema::builds::publish_job_id.eq(job.id),
                      schema::builds::published_state.eq(val),
                      schema::builds::published_state_reason.eq(reason)))
                .get_result::<Build>(conn)?;
            Ok(job)
        })
            .map_err(|e| {
                match e {
                    DieselError::RollbackTransaction => ApiError::BadRequest("Invalid build state for publish".to_string()),
                    _ => From::from(e)
                }
            })
    }
}

#[derive(Deserialize, Debug)]
pub struct InitPurge {
    pub id: i32,
}

impl DbRequest for InitPurge {
    type DbType = ();
}

impl Handler<DbRequestWrapper<InitPurge>> for DbExecutor {
    type Result = Result<<InitPurge as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<InitPurge>, _: &mut Self::Context) -> Self::Result {
        use schema::builds::dsl::*;
        let conn = &self.0.get().unwrap();
        conn.transaction::<(), DieselError, _>(|| {
            let current_build = builds
                .filter(id.eq(msg.0.id))
                .get_result::<Build>(conn)?;
            let current_repo_state = RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            let current_published_state = PublishedState::from_db(current_build.published_state, &current_build.published_state_reason);
            if current_repo_state.same_state_as(&RepoState::Verifying) ||
                current_repo_state.same_state_as(&RepoState::Purging) ||
                current_published_state.same_state_as(&PublishedState::Publishing) {
                    /* Only allow pruning when we're not working on the build repo */
                return Err(DieselError::RollbackTransaction)
            };
            let (val, reason) = RepoState::to_db(&RepoState::Purging);
            diesel::update(builds)
                .filter(id.eq(msg.0.id))
                .set((repo_state.eq(val),
                      repo_state_reason.eq(reason)))
                .execute(conn)?;
            Ok(())
        })
            .map_err(|e| {
                match e {
                    DieselError::RollbackTransaction => ApiError::BadRequest("Can't prune build while in use".to_string()),
                    _ => From::from(e)
                }
            })
    }
}

#[derive(Deserialize, Debug)]
pub struct FinishPurge {
    pub id: i32,
    pub error: Option<String>,
}

impl DbRequest for FinishPurge {
    type DbType = Build;
}

impl Handler<DbRequestWrapper<FinishPurge>> for DbExecutor {
    type Result = Result<<FinishPurge as DbRequest>::DbType, ApiError>;

    fn handle(&mut self, msg: DbRequestWrapper<FinishPurge>, _: &mut Self::Context) -> Self::Result {
        use schema::builds::dsl::*;
        let conn = &self.0.get().unwrap();
        conn.transaction::<Build, DieselError, _>(|| {
            let current_build = builds
                .filter(id.eq(msg.0.id))
                .get_result::<Build>(conn)?;
            let current_repo_state = RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            if !current_repo_state.same_state_as(&RepoState::Purging) {
                return Err(DieselError::RollbackTransaction)
            };
            let new_state = match msg.0.error {
                None => RepoState::Purged,
                Some(err_string) => RepoState::Failed(format!("Failed to Purge build: {}", err_string)),
            };
            let (val, reason) = RepoState::to_db(&new_state);
            let new_build =
                diesel::update(builds)
                .filter(id.eq(msg.0.id))
                .set((repo_state.eq(val),
                      repo_state_reason.eq(reason)))
                .get_result::<Build>(conn)?;
            Ok(new_build)
        })
            .map_err(|e| {
                match e {
                    DieselError::RollbackTransaction => ApiError::BadRequest("Unexpected repo state, was not purging".to_string()),
                    _ => From::from(e)
                }
            })
    }
}
