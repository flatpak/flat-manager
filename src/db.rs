use actix::prelude::*;
use actix_web::*;
use diesel::prelude::*;
use serde_json::json;

use crate::errors::ApiError;
use crate::models::*;
use crate::schema;
use crate::Pool;

pub struct Db(pub Pool);

impl Db {
    fn run<Func, T>(&self, func: Func) -> impl Future<Item = T, Error = ApiError>
    where
        Func: FnOnce(
            &r2d2::PooledConnection<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
        ) -> Result<T, ApiError>,
        Func: Send + 'static,
        T: Send + 'static,
    {
        let p = self.0.clone();
        web::block(move || {
            let conn = p.get()?;
            func(&conn)
        })
        .map_err(ApiError::from)
    }

    fn run_in_transaction<Func, T>(&self, func: Func) -> impl Future<Item = T, Error = ApiError>
    where
        Func: FnOnce(
            &r2d2::PooledConnection<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
        ) -> Result<T, ApiError>,
        Func: Send + 'static,
        T: Send + 'static,
    {
        self.run(move |conn| conn.transaction::<T, ApiError, _>(|| func(conn)))
    }

    /* Jobs */

    pub fn lookup_job(
        &self,
        job_id: i32,
        log_offset: Option<usize>,
    ) -> impl Future<Item = Job, Error = ApiError> {
        self.run(move |conn| {
            use schema::jobs::dsl::*;
            Ok(jobs
                .filter(id.eq(job_id))
                .get_result::<Job>(conn)?
                .apply_log_offset(log_offset))
        })
    }

    pub fn list_active_jobs(&self) -> impl Future<Item = Vec<Job>, Error = ApiError> {
        self.run(move |conn| {
            use schema::jobs::dsl::*;
            Ok(jobs
                .order(id)
                .filter(status.le(JobStatus::Started as i16))
                .get_results::<Job>(conn)?)
        })
    }

    pub fn lookup_commit_job(
        &self,
        build_id: i32,
        log_offset: Option<usize>,
    ) -> impl Future<Item = Job, Error = ApiError> {
        self.run(move |conn| {
            use schema::builds::dsl::*;
            use schema::jobs::dsl::*;

            Ok(jobs
                .inner_join(builds.on(commit_job_id.eq(schema::jobs::dsl::id.nullable())))
                .select(schema::jobs::all_columns)
                .filter(schema::builds::dsl::id.eq(build_id))
                .get_result::<Job>(conn)?
                .apply_log_offset(log_offset))
        })
    }

    pub fn lookup_publish_job(
        &self,
        build_id: i32,
        log_offset: Option<usize>,
    ) -> impl Future<Item = Job, Error = ApiError> {
        self.run(move |conn| {
            use schema::builds::dsl::*;
            use schema::jobs::dsl::*;

            Ok(jobs
                .inner_join(builds.on(publish_job_id.eq(schema::jobs::dsl::id.nullable())))
                .select(schema::jobs::all_columns)
                .filter(schema::builds::dsl::id.eq(build_id))
                .get_result::<Job>(conn)?
                .apply_log_offset(log_offset))
        })
    }

    pub fn start_commit_job(
        &self,
        build_id: i32,
        endoflife: Option<String>,
        endoflife_rebase: Option<String>,
        token_type: Option<i32>,
    ) -> impl Future<Item = Job, Error = ApiError> {
        self.run_in_transaction(move |conn| {
            let current_build = schema::builds::table
                .filter(schema::builds::id.eq(build_id))
                .get_result::<Build>(conn)?;
            let current_repo_state =
                RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            match current_repo_state {
                RepoState::Uploading => (),
                RepoState::Verifying => {
                    return Err(ApiError::WrongRepoState(
                        "Build is currently being commited".to_string(),
                        "uploading".to_string(),
                        "verifying".to_string(),
                    ))
                }
                RepoState::Ready => {
                    return Err(ApiError::WrongRepoState(
                        "Build is already commited".to_string(),
                        "uploading".to_string(),
                        "ready".to_string(),
                    ))
                }
                RepoState::Failed(s) => {
                    return Err(ApiError::WrongRepoState(
                        format!("Commit already failed: {}", s),
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
            let (val, reason) = RepoState::to_db(&RepoState::Verifying);
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
    }

    pub fn start_publish_job(
        &self,
        build_id: i32,
        repo: String,
    ) -> impl Future<Item = Job, Error = ApiError> {
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
                        format!("Previous publish failed: {}", s),
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
                RepoState::Verifying => {
                    return Err(ApiError::WrongRepoState(
                        "Build is not commited".to_string(),
                        "ready".to_string(),
                        "verifying".to_string(),
                    ))
                }
                RepoState::Ready => (),
                RepoState::Failed(s) => {
                    return Err(ApiError::WrongRepoState(
                        format!("Build failed: {}", s),
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
    }

    /* Builds */

    pub fn new_build(&self, a_build: NewBuild) -> impl Future<Item = Build, Error = ApiError> {
        self.run(move |conn| {
            use schema::builds::dsl::*;
            Ok(diesel::insert_into(builds)
                .values(&a_build)
                .get_result::<Build>(conn)?)
        })
    }

    pub fn lookup_build(&self, build_id: i32) -> impl Future<Item = Build, Error = ApiError> {
        self.run(move |conn| {
            use schema::builds::dsl::*;
            Ok(builds.filter(id.eq(build_id)).get_result::<Build>(conn)?)
        })
    }

    pub fn list_builds(&self) -> impl Future<Item = Vec<Build>, Error = ApiError> {
        self.run(move |conn| {
            use schema::builds::dsl::*;
            let (val, _) = RepoState::Purged.to_db();
            Ok(builds
                .filter(repo_state.ne(val))
                .get_results::<Build>(conn)?)
        })
    }

    pub fn add_extra_ids(
        &self,
        build_id: i32,
        ids: Vec<String>,
    ) -> impl Future<Item = Build, Error = ApiError> {
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
    }

    pub fn init_purge(&self, build_id: i32) -> impl Future<Item = (), Error = ApiError> {
        self.run_in_transaction(move |conn| {
            use schema::builds::dsl::*;
            let current_build = builds.filter(id.eq(build_id)).get_result::<Build>(conn)?;
            let current_repo_state =
                RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            let current_published_state = PublishedState::from_db(
                current_build.published_state,
                &current_build.published_state_reason,
            );
            if current_repo_state.same_state_as(&RepoState::Verifying)
                || current_repo_state.same_state_as(&RepoState::Purging)
                || current_published_state.same_state_as(&PublishedState::Publishing)
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
    }

    pub fn finish_purge(
        &self,
        build_id: i32,
        error: Option<String>,
    ) -> impl Future<Item = Build, Error = ApiError> {
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
                    RepoState::Failed(format!("Failed to Purge build: {}", err_string))
                }
            };
            let (val, reason) = RepoState::to_db(&new_state);
            let new_build = diesel::update(builds)
                .filter(id.eq(build_id))
                .set((repo_state.eq(val), repo_state_reason.eq(reason)))
                .get_result::<Build>(conn)?;
            Ok(new_build)
        })
    }

    /* Build refs */

    pub fn new_build_ref(
        &self,
        a_build_ref: NewBuildRef,
    ) -> impl Future<Item = BuildRef, Error = ApiError> {
        self.run(move |conn| {
            use self::schema::build_refs::dsl::*;
            Ok(diesel::insert_into(build_refs)
                .values(&a_build_ref)
                .get_result::<BuildRef>(conn)?)
        })
    }

    pub fn lookup_build_ref(
        &self,
        the_build_id: i32,
        ref_id: i32,
    ) -> impl Future<Item = BuildRef, Error = ApiError> {
        self.run(move |conn| {
            use schema::build_refs::dsl::*;
            Ok(build_refs
                .filter(build_id.eq(the_build_id))
                .filter(id.eq(ref_id))
                .get_result::<BuildRef>(conn)?)
        })
    }

    #[allow(dead_code)]
    pub fn lookup_build_refs(
        &self,
        the_build_id: i32,
    ) -> impl Future<Item = Vec<BuildRef>, Error = ApiError> {
        self.run(move |conn| {
            use schema::build_refs::dsl::*;
            Ok(build_refs
                .filter(build_id.eq(the_build_id))
                .get_results::<BuildRef>(conn)?)
        })
    }
}
