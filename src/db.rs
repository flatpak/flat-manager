use actix::prelude::*;
use actix_web::*;
use diesel::prelude::*;
use futures3::compat::Compat01As03;
use serde_json::json;

use crate::errors::ApiError;
use crate::models::*;
use crate::schema;
use crate::Pool;

pub struct Db(pub Pool);

impl Db {
    async fn run<Func, T>(&self, func: Func) -> Result<T, ApiError>
    where
        Func: FnOnce(
            &mut r2d2::PooledConnection<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
        ) -> Result<T, ApiError>,
        Func: Send + 'static,
        T: Send + 'static,
    {
        let p = self.0.clone();
        Compat01As03::new(
            web::block(move || {
                let mut conn = p.get()?;
                func(&mut conn)
            })
            .map_err(ApiError::from),
        )
        .await
    }

    async fn run_in_transaction<Func, T>(&self, func: Func) -> Result<T, ApiError>
    where
        Func: FnOnce(
            &mut r2d2::PooledConnection<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
        ) -> Result<T, ApiError>,
        Func: Send + 'static,
        T: Send + 'static,
    {
        self.run(move |conn| conn.transaction::<T, ApiError, _>(|conn| func(conn)))
            .await
    }

    /* Jobs */

    pub async fn lookup_job(
        &self,
        job_id: i32,
        log_offset: Option<usize>,
    ) -> Result<Job, ApiError> {
        self.run(move |conn| {
            use schema::jobs::dsl::*;
            Ok(jobs
                .filter(id.eq(job_id))
                .get_result::<Job>(conn)?
                .apply_log_offset(log_offset))
        })
        .await
    }

    pub async fn list_active_jobs(&self) -> Result<Vec<Job>, ApiError> {
        self.run(move |conn| {
            use schema::jobs::dsl::*;
            Ok(jobs
                .order(id)
                .filter(status.le(JobStatus::Started as i16))
                .get_results::<Job>(conn)?)
        })
        .await
    }

    pub async fn start_commit_job(
        &self,
        build_id: i32,
        endoflife: Option<String>,
        endoflife_rebase: Option<String>,
        token_type: Option<i32>,
    ) -> Result<Job, ApiError> {
        self.run_in_transaction(move |conn| {
            let current_build = schema::builds::table
                .filter(schema::builds::id.eq(build_id))
                .get_result::<Build>(conn)?;
            let current_repo_state =
                RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            match current_repo_state {
                RepoState::Uploading => (),
                RepoState::Committing => {
                    return Err(ApiError::WrongRepoState(
                        "Build is currently being commited".to_string(),
                        "uploading".to_string(),
                        "committing".to_string(),
                    ))
                }
                RepoState::Ready | RepoState::Validating => {
                    return Err(ApiError::WrongRepoState(
                        "Build is already commited".to_string(),
                        "uploading".to_string(),
                        "ready".to_string(),
                    ))
                }
                RepoState::Failed(s) => {
                    return Err(ApiError::WrongRepoState(
                        format!("Commit already failed: {s}"),
                        "uploading".to_string(),
                        "failed".to_string(),
                    ))
                }
                RepoState::Purging | RepoState::Purged => {
                    return Err(ApiError::WrongRepoState(
                        "Build has been purged".to_string(),
                        "uploading".to_string(),
                        "purged".to_string(),
                    ))
                }
            }
            let (val, reason) = RepoState::to_db(&RepoState::Committing);
            let job = diesel::insert_into(schema::jobs::table)
                .values(NewJob {
                    kind: JobKind::Commit.to_db(),
                    start_after: None,
                    repo: None,
                    contents: json!(CommitJob {
                        build: build_id,
                        endoflife,
                        endoflife_rebase,
                        token_type,
                    })
                    .to_string(),
                })
                .get_result::<Job>(conn)?;
            diesel::update(schema::builds::table)
                .filter(schema::builds::id.eq(build_id))
                .set((
                    schema::builds::commit_job_id.eq(job.id),
                    schema::builds::repo_state.eq(val),
                    schema::builds::repo_state_reason.eq(reason),
                ))
                .get_result::<Build>(conn)?;
            Ok(job)
        })
        .await
    }

    pub async fn start_publish_job(&self, build_id: i32, repo: String) -> Result<Job, ApiError> {
        self.run_in_transaction(move |conn| {
            let current_build = schema::builds::table
                .filter(schema::builds::id.eq(build_id))
                .get_result::<Build>(conn)?;
            let current_published_state = PublishedState::from_db(
                current_build.published_state,
                &current_build.published_state_reason,
            );

            match current_published_state {
                PublishedState::Unpublished => (),
                PublishedState::Publishing => {
                    return Err(ApiError::WrongPublishedState(
                        "Build is currently being published".to_string(),
                        "unpublished".to_string(),
                        "publishing".to_string(),
                    ))
                }
                PublishedState::Published => {
                    return Err(ApiError::WrongPublishedState(
                        "Build has already been published".to_string(),
                        "unpublished".to_string(),
                        "published".to_string(),
                    ))
                }
                PublishedState::Failed(s) => {
                    return Err(ApiError::WrongPublishedState(
                        format!("Previous publish failed: {s}"),
                        "unpublished".to_string(),
                        "failed".to_string(),
                    ))
                }
            }

            let current_repo_state =
                RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            match current_repo_state {
                RepoState::Uploading => {
                    return Err(ApiError::WrongRepoState(
                        "Build is not commited".to_string(),
                        "ready".to_string(),
                        "uploading".to_string(),
                    ))
                }
                RepoState::Committing => {
                    return Err(ApiError::WrongRepoState(
                        "Build is not commited".to_string(),
                        "ready".to_string(),
                        "committing".to_string(),
                    ))
                }
                RepoState::Validating => {
                    return Err(ApiError::WrongRepoState(
                        "Build is still validating".to_string(),
                        "ready".to_string(),
                        "validating".to_string(),
                    ))
                }
                RepoState::Ready => (),
                RepoState::Failed(s) => {
                    return Err(ApiError::WrongRepoState(
                        format!("Build failed: {s}"),
                        "ready".to_string(),
                        "failed".to_string(),
                    ))
                }
                RepoState::Purging | RepoState::Purged => {
                    return Err(ApiError::WrongRepoState(
                        "Build has been purged".to_string(),
                        "ready".to_string(),
                        "purged".to_string(),
                    ))
                }
            }

            let (val, reason) = PublishedState::to_db(&PublishedState::Publishing);
            let job = diesel::insert_into(schema::jobs::table)
                .values(NewJob {
                    kind: JobKind::Publish.to_db(),
                    start_after: None,
                    repo: Some(repo),
                    contents: json!(PublishJob { build: build_id }).to_string(),
                })
                .get_result::<Job>(conn)?;
            diesel::update(schema::builds::table)
                .filter(schema::builds::id.eq(build_id))
                .set((
                    schema::builds::publish_job_id.eq(job.id),
                    schema::builds::published_state.eq(val),
                    schema::builds::published_state_reason.eq(reason),
                ))
                .get_result::<Build>(conn)?;
            Ok(job)
        })
        .await
    }

    pub async fn start_republish_job(&self, repo: String, app: String) -> Result<Job, ApiError> {
        self.run_in_transaction(move |conn| {
            let job = diesel::insert_into(schema::jobs::table)
                .values(NewJob {
                    kind: JobKind::Republish.to_db(),
                    start_after: None,
                    repo: Some(repo.clone()),
                    contents: json!(RepublishJob { app }).to_string(),
                })
                .get_result::<Job>(conn)?;

            Ok(job)
        })
        .await
    }

    /* Checks */

    pub async fn get_check_by_job_id(&self, job: i32) -> Result<Check, ApiError> {
        self.run(move |conn| {
            use schema::checks::dsl::*;
            Ok(checks.filter(job_id.eq(job)).get_result::<Check>(conn)?)
        })
        .await
    }

    pub async fn set_check_status(
        &self,
        job: i32,
        new_status: CheckStatus,
        new_results: Option<String>,
    ) -> Result<(), ApiError> {
        self.run(move |conn| {
            use schema::checks::dsl;
            let (status, status_reason) = new_status.to_db();

            diesel::update(dsl::checks)
                .filter(dsl::job_id.eq(job))
                .set((
                    dsl::status.eq(status),
                    dsl::status_reason.eq(status_reason),
                    new_results.map(|r| dsl::results.eq(r)),
                ))
                .execute(conn)?;
            Ok(())
        })
        .await
    }

    /* Builds */

    pub async fn new_build(&self, a_build: NewBuild) -> Result<Build, ApiError> {
        self.run(move |conn| {
            use schema::builds::dsl::*;
            Ok(diesel::insert_into(builds)
                .values(&a_build)
                .get_result::<Build>(conn)?)
        })
        .await
    }

    pub async fn lookup_build(&self, build_id: i32) -> Result<Build, ApiError> {
        self.run(move |conn| {
            use schema::builds::dsl::*;
            Ok(builds.filter(id.eq(build_id)).get_result::<Build>(conn)?)
        })
        .await
    }

    pub async fn list_builds(&self) -> Result<Vec<Build>, ApiError> {
        self.run(move |conn| {
            use schema::builds::dsl::*;
            let (val, _) = RepoState::Purged.to_db();
            Ok(builds
                .filter(repo_state.ne(val))
                .filter(app_id.is_null())
                .get_results::<Build>(conn)?)
        })
        .await
    }

    pub async fn list_builds_for_app(&self, for_app_id: String) -> Result<Vec<Build>, ApiError> {
        self.run(move |conn| {
            use schema::builds::dsl::*;
            let (val, _) = RepoState::Purged.to_db();
            Ok(builds
                .filter(repo_state.ne(val))
                .filter(app_id.eq(for_app_id))
                .get_results::<Build>(conn)?)
        })
        .await
    }

    pub async fn add_extra_ids(&self, build_id: i32, ids: Vec<String>) -> Result<Build, ApiError> {
        self.run_in_transaction(move |conn| {
            let current_build = schema::builds::table
                .filter(schema::builds::id.eq(build_id))
                .get_result::<Build>(conn)?;

            let mut new_ids = current_build.extra_ids;
            for new_id in ids.iter() {
                if !new_ids.contains(new_id) {
                    new_ids.push(new_id.to_string())
                }
            }
            Ok(diesel::update(schema::builds::table)
                .filter(schema::builds::id.eq(build_id))
                .set(schema::builds::extra_ids.eq(new_ids))
                .get_result::<Build>(conn)?)
        })
        .await
    }

    pub async fn init_purge(&self, build_id: i32) -> Result<(), ApiError> {
        self.run_in_transaction(move |conn| {
            use schema::builds::dsl::*;
            let current_build = builds.filter(id.eq(build_id)).get_result::<Build>(conn)?;
            let current_repo_state =
                RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            let current_published_state = PublishedState::from_db(
                current_build.published_state,
                &current_build.published_state_reason,
            );
            if matches!(
                current_repo_state,
                RepoState::Committing | RepoState::Purging | RepoState::Validating
            ) || matches!(current_published_state, PublishedState::Publishing)
            {
                /* Only allow pruning when we're not working on the build repo */
                return Err(ApiError::BadRequest(
                    "Can't prune build while in use".to_string(),
                ));
            };
            let (val, reason) = RepoState::to_db(&RepoState::Purging);
            diesel::update(builds)
                .filter(id.eq(build_id))
                .set((repo_state.eq(val), repo_state_reason.eq(reason)))
                .execute(conn)?;
            Ok(())
        })
        .await
    }

    pub async fn finish_purge(
        &self,
        build_id: i32,
        error: Option<String>,
    ) -> Result<Build, ApiError> {
        self.run_in_transaction(move |conn| {
            use schema::builds::dsl::*;
            let current_build = builds.filter(id.eq(build_id)).get_result::<Build>(conn)?;
            let current_repo_state =
                RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            if !current_repo_state.same_state_as(&RepoState::Purging) {
                return Err(ApiError::BadRequest(
                    "Unexpected repo state, was not purging".to_string(),
                ));
            };
            let new_state = match error {
                None => RepoState::Purged,
                Some(err_string) => {
                    RepoState::Failed(format!("Failed to Purge build: {err_string}"))
                }
            };
            let (val, reason) = RepoState::to_db(&new_state);
            let new_build = diesel::update(builds)
                .filter(id.eq(build_id))
                .set((repo_state.eq(val), repo_state_reason.eq(reason)))
                .get_result::<Build>(conn)?;
            Ok(new_build)
        })
        .await
    }

    /* Build refs */

    pub async fn new_build_ref(&self, a_build_ref: NewBuildRef) -> Result<BuildRef, ApiError> {
        self.run(move |conn| {
            use self::schema::build_refs::dsl::*;
            Ok(diesel::insert_into(build_refs)
                .values(&a_build_ref)
                .get_result::<BuildRef>(conn)?)
        })
        .await
    }

    pub async fn lookup_build_ref(
        &self,
        the_build_id: i32,
        ref_id: i32,
    ) -> Result<BuildRef, ApiError> {
        self.run(move |conn| {
            use schema::build_refs::dsl::*;
            Ok(build_refs
                .filter(build_id.eq(the_build_id))
                .filter(id.eq(ref_id))
                .get_result::<BuildRef>(conn)?)
        })
        .await
    }

    #[allow(dead_code)]
    pub async fn lookup_build_refs(&self, the_build_id: i32) -> Result<Vec<BuildRef>, ApiError> {
        self.run(move |conn| {
            use schema::build_refs::dsl::*;
            Ok(build_refs
                .filter(build_id.eq(the_build_id))
                .get_results::<BuildRef>(conn)?)
        })
        .await
    }
}
