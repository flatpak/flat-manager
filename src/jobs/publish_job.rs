use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use log::{error, info};
use serde_json::json;
use std::collections::HashMap;
use std::ffi::{CString, OsStr, OsString};
use std::fs::{self, File};
use std::io::{self, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};
use std::process::Command;
use walkdir::WalkDir;

use crate::config::{Config, RepoConfig};
use crate::errors::{JobError, JobResult};
use crate::models;
use crate::models::{Job, PublishJob, PublishedState};
use crate::ostree;
use crate::schema::*;

use super::job_executor::JobExecutor;
use super::job_instance::{InvalidJobInstance, JobInstance};
use super::utils::{
    add_gpg_args, do_command, generate_flatpakref, load_build_and_config, load_build_refs,
    schedule_update_job,
};

#[derive(Debug)]
pub struct PublishJobInstance {
    pub job_id: i32,
    pub build_id: i32,
}

impl PublishJobInstance {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(job: Job) -> Box<dyn JobInstance> {
        if let Ok(publish_job) = serde_json::from_str::<PublishJob>(&job.contents) {
            Box::new(PublishJobInstance {
                job_id: job.id,
                build_id: publish_job.build,
            })
        } else {
            InvalidJobInstance::new(job, JobError::new("Can't parse publish job"))
        }
    }

    fn do_publish(
        &self,
        build: &models::Build,
        build_refs: &[models::BuildRef],
        config: &Config,
        repoconfig: &RepoConfig,
        executor: &JobExecutor,
        conn: &mut PgConnection,
    ) -> JobResult<serde_json::Value> {
        let build_repo_path = config.build_repo_base.join(self.build_id.to_string());

        // Run the publish hook, if any
        if let Some(mut hook) = repoconfig
            .hooks
            .publish
            .as_ref()
            .and_then(|x| x.build_command(&build_repo_path))
        {
            job_log_and_info!(self.job_id, conn, "Running publish hook");

            hook.env("FLAT_MANAGER_IS_REPUBLISH", "false");
            hook.env("FLAT_MANAGER_BUILD_ID", build.id.to_string());

            do_command(hook)?;
        }

        let mut src_repo_arg = OsString::from("--src-repo=");
        src_repo_arg.push(&build_repo_path);

        // Import commit and modify refs

        let mut cmd = Command::new("flatpak");
        cmd.arg("build-commit-from")
            .arg("--force") // Always generate a new commit even if nothing changed
            .arg("--no-update-summary"); // We update it separately

        add_gpg_args(&mut cmd, &repoconfig.gpg_key, &config.gpg_homedir);

        if let Some(collection_id) = &repoconfig.collection_id {
            for extra_id in build.extra_ids.iter() {
                let extra_id = extra_id.as_ref().expect("extra_id is null");
                cmd.arg(format!("--extra-collection-id={collection_id}.{extra_id}"));
            }
        }

        cmd.arg(&src_repo_arg).arg(&repoconfig.path);

        job_log_and_info!(
            self.job_id,
            conn,
            &format!("Importing build to repo {}", repoconfig.name),
        );
        do_command(cmd)?;

        let appstream_dir = repoconfig.path.join("appstream");
        fs::create_dir_all(&appstream_dir)?;

        let media_dir = repoconfig.path.join("media");
        fs::create_dir_all(&media_dir)?;

        let mut commits = HashMap::new();
        for build_ref in build_refs.iter() {
            if build_ref.ref_name.starts_with("app/") || build_ref.ref_name.starts_with("runtime/")
            {
                let commit = ostree::parse_ref(&repoconfig.path, &build_ref.ref_name)?;
                commits.insert(build_ref.ref_name.to_string(), commit);
            }

            if build_ref.ref_name.starts_with("app/") {
                let (filename, contents) =
                    generate_flatpakref(&build_ref.ref_name, None, config, repoconfig);
                let path = appstream_dir.join(&filename);
                job_log_and_info!(self.job_id, conn, &format!("generating {}", &filename));
                let old_contents = fs::read_to_string(&path).unwrap_or_default();
                if contents != old_contents {
                    File::create(&path)?.write_all(contents.as_bytes())?;
                }
            }
        }

        for build_ref in build_refs.iter() {
            if build_ref.ref_name.starts_with("screenshots/") {
                job_log_and_info!(
                    self.job_id,
                    conn,
                    &format!("extracting {}", build_ref.ref_name),
                );
                extract_screenshot_ref(&build_repo_path, &build_ref.ref_name, &media_dir)?;
            }
        }

        let update_job = schedule_update_job(
            config,
            repoconfig,
            conn,
            self.job_id,
            Some(&executor.metrics),
        )?;

        let path = Path::new(&build_repo_path);
        job_log_and_info!(
            self.job_id,
            conn,
            &format!("Removing build {}", self.build_id),
        );

        fs::remove_dir_all(path).unwrap_or_else(|_| {
            job_log_and_info!(self.job_id, conn, "Failed to remove build");
        });

        Ok(json!({
            "refs": commits,
            "update-repo-job": update_job.id,
        }))
    }
}

fn extract_screenshot_ref(
    build_repo_path: &Path,
    ref_name: &str,
    media_dir: &Path,
) -> JobResult<()> {
    let staged_dir = tempfile::tempdir().map_err(|err| {
        JobError::InternalError(format!(
            "Failed to create temporary screenshot checkout: {err}"
        ))
    })?;

    ostree::checkout_ref(build_repo_path, ref_name, staged_dir.path())?;
    install_staged_screenshot_tree(staged_dir.path(), media_dir)
}

fn validate_staged_screenshot_tree(staged_dir: &Path) -> JobResult<()> {
    let metadata = fs::symlink_metadata(staged_dir).map_err(|err| {
        unsafe_screenshot_source(staged_dir, format!("failed to inspect path: {err}"))
    })?;
    if !metadata.file_type().is_dir() {
        return Err(unsafe_screenshot_source(
            staged_dir,
            "is not a real directory",
        ));
    }

    for entry in WalkDir::new(staged_dir).follow_links(false) {
        let entry = entry.map_err(|err| {
            let path = err.path().unwrap_or(staged_dir);
            unsafe_screenshot_source(path, format!("failed to read path: {err}"))
        })?;
        let path = entry.path();
        let relative_path = relative_screenshot_path(staged_dir, path)?;
        validate_relative_screenshot_path(relative_path)?;

        let metadata = fs::symlink_metadata(path).map_err(|err| {
            unsafe_screenshot_source(path, format!("failed to inspect path: {err}"))
        })?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            return Err(unsafe_screenshot_source(path, "is a symlink"));
        }
        if !file_type.is_dir() && !file_type.is_file() {
            return Err(unsafe_screenshot_source(
                path,
                "is not a regular file or directory",
            ));
        }
    }

    Ok(())
}

fn install_staged_screenshot_tree(staged_dir: &Path, media_dir: &Path) -> JobResult<()> {
    validate_staged_screenshot_tree(staged_dir)?;
    copy_staged_screenshot_tree(staged_dir, media_dir)
}

fn copy_staged_screenshot_tree(staged_dir: &Path, media_dir: &Path) -> JobResult<()> {
    let media_root = open_media_dir(media_dir)?;

    for entry in WalkDir::new(staged_dir).follow_links(false) {
        let entry = entry.map_err(|err| {
            let path = err.path().unwrap_or(staged_dir);
            unsafe_screenshot_source(path, format!("failed to read path: {err}"))
        })?;
        let source_path = entry.path();
        let relative_path = relative_screenshot_path(staged_dir, source_path)?;
        validate_relative_screenshot_path(relative_path)?;

        let metadata = fs::symlink_metadata(source_path).map_err(|err| {
            unsafe_screenshot_source(source_path, format!("failed to inspect path: {err}"))
        })?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            return Err(unsafe_screenshot_source(source_path, "is a symlink"));
        } else if file_type.is_dir() {
            let _dir = open_destination_directory(&media_root, media_dir, relative_path)?;
        } else if file_type.is_file() {
            copy_screenshot_file(source_path, &media_root, media_dir, relative_path)?;
        } else {
            return Err(unsafe_screenshot_source(
                source_path,
                "is not a regular file or directory",
            ));
        }
    }

    Ok(())
}

fn relative_screenshot_path<'a>(base: &Path, path: &'a Path) -> JobResult<&'a Path> {
    path.strip_prefix(base).map_err(|err| {
        unsafe_screenshot_source(path, format!("failed to calculate relative path: {err}"))
    })
}

fn validate_relative_screenshot_path(path: &Path) -> JobResult<()> {
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(JobError::InternalError(format!(
                "Unsafe screenshot path {}: contains non-normal component",
                path.display()
            )));
        }
    }

    Ok(())
}

fn copy_screenshot_file(
    source_path: &Path,
    media_root: &File,
    media_dir: &Path,
    relative_path: &Path,
) -> JobResult<()> {
    let parent_path = relative_path.parent().unwrap_or_else(|| Path::new(""));
    let file_name = relative_path.file_name().ok_or_else(|| {
        unsafe_screenshot_source(source_path, "regular file has no destination file name")
    })?;
    let parent_dir = open_destination_directory(media_root, media_dir, parent_path)?;
    let destination_path = media_dir.join(relative_path);

    let mut source_file = open_source_file(source_path)?;
    let mut destination_file = create_destination_file(&parent_dir, file_name, &destination_path)?;
    io::copy(&mut source_file, &mut destination_file).map_err(|err| {
        JobError::InternalError(format!(
            "Failed to copy screenshot file {} to {}: {err}",
            source_path.display(),
            destination_path.display()
        ))
    })?;

    Ok(())
}

fn open_media_dir(media_dir: &Path) -> JobResult<File> {
    let media_dir = media_dir.canonicalize().map_err(|err| {
        unsafe_screenshot_destination(
            media_dir,
            format!("is not a real directory or could not be resolved: {err}"),
        )
    })?;

    open_dir_path_no_follow(&media_dir).map_err(|err| {
        unsafe_screenshot_destination(
            &media_dir,
            format!("is not a real directory or could not be opened: {err}"),
        )
    })
}

fn open_destination_directory(
    media_root: &File,
    media_dir: &Path,
    relative_path: &Path,
) -> JobResult<File> {
    validate_relative_screenshot_path(relative_path)?;

    let mut directory = media_root.try_clone().map_err(|err| {
        unsafe_screenshot_destination(
            media_dir,
            format!("failed to duplicate media directory handle: {err}"),
        )
    })?;
    let mut destination_path = media_dir.to_path_buf();

    for component in relative_path.components() {
        let Component::Normal(name) = component else {
            return Err(JobError::InternalError(format!(
                "Unsafe screenshot path {}: contains non-normal component",
                relative_path.display()
            )));
        };

        destination_path.push(name);
        directory =
            open_or_create_destination_directory(directory.as_raw_fd(), name, &destination_path)?;
    }

    Ok(directory)
}

fn open_or_create_destination_directory(
    parent_fd: RawFd,
    name: &OsStr,
    destination_path: &Path,
) -> JobResult<File> {
    match openat_dir_no_follow(parent_fd, name) {
        Ok(directory) => Ok(directory),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let name = c_string_for_path_component(name, destination_path)?;
            let result = unsafe { libc::mkdirat(parent_fd, name.as_ptr(), 0o755) };
            if result != 0 {
                let err = io::Error::last_os_error();
                if err.kind() != io::ErrorKind::AlreadyExists {
                    return Err(unsafe_screenshot_destination(
                        destination_path,
                        format!("failed to create directory: {err}"),
                    ));
                }
            }

            openat_dir_no_follow(parent_fd, destination_path.file_name().unwrap()).map_err(|err| {
                destination_directory_error(
                    parent_fd,
                    destination_path.file_name().unwrap(),
                    destination_path,
                    err,
                )
            })
        }
        Err(err) => Err(destination_directory_error(
            parent_fd,
            name,
            destination_path,
            err,
        )),
    }
}

fn destination_directory_error(
    parent_fd: RawFd,
    name: &OsStr,
    destination_path: &Path,
    err: io::Error,
) -> JobError {
    if err.raw_os_error() == Some(libc::ELOOP) {
        return unsafe_screenshot_destination(destination_path, "is a symlink");
    }

    if let Ok(existing) = openat_path_no_follow(parent_fd, name) {
        if let Ok(metadata) = existing.metadata() {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                return unsafe_screenshot_destination(destination_path, "is a symlink");
            }
            if !file_type.is_dir() {
                return unsafe_screenshot_destination(destination_path, "is not a directory");
            }
        }
    }

    unsafe_screenshot_destination(
        destination_path,
        format!("is not a real directory or could not be opened: {err}"),
    )
}

fn open_source_file(path: &Path) -> JobResult<File> {
    let path_name = c_string_for_path(path)?;
    let fd = unsafe {
        libc::open(
            path_name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ELOOP) {
            return Err(unsafe_screenshot_source(path, "is a symlink"));
        }
        return Err(unsafe_screenshot_source(
            path,
            format!("failed to open file without following symlinks: {err}"),
        ));
    }

    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata().map_err(|err| {
        unsafe_screenshot_source(path, format!("failed to inspect opened file: {err}"))
    })?;
    if !metadata.file_type().is_file() {
        return Err(unsafe_screenshot_source(path, "is not a regular file"));
    }

    Ok(file)
}

fn create_destination_file(
    parent_dir: &File,
    name: &OsStr,
    destination_path: &Path,
) -> JobResult<File> {
    for _ in 0..2 {
        match openat_create_new(parent_dir.as_raw_fd(), name) {
            Ok(file) => return Ok(file),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                remove_existing_destination_file(parent_dir, name, destination_path)?;
            }
            Err(err) => {
                return Err(unsafe_screenshot_destination(
                    destination_path,
                    format!("failed to create file without following symlinks: {err}"),
                ));
            }
        }
    }

    Err(unsafe_screenshot_destination(
        destination_path,
        "failed to replace existing file safely",
    ))
}

fn remove_existing_destination_file(
    parent_dir: &File,
    name: &OsStr,
    destination_path: &Path,
) -> JobResult<()> {
    let existing = openat_path_no_follow(parent_dir.as_raw_fd(), name).map_err(|err| {
        if err.raw_os_error() == Some(libc::ELOOP) {
            unsafe_screenshot_destination(destination_path, "is a symlink")
        } else {
            unsafe_screenshot_destination(
                destination_path,
                format!("failed to inspect existing destination: {err}"),
            )
        }
    })?;
    let metadata = existing.metadata().map_err(|err| {
        unsafe_screenshot_destination(
            destination_path,
            format!("failed to inspect existing destination: {err}"),
        )
    })?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(unsafe_screenshot_destination(
            destination_path,
            "is a symlink",
        ));
    }
    if file_type.is_dir() {
        return Err(unsafe_screenshot_destination(
            destination_path,
            "is a directory",
        ));
    }
    if !file_type.is_file() {
        return Err(unsafe_screenshot_destination(
            destination_path,
            "is not a regular file",
        ));
    }

    drop(existing);

    let name = c_string_for_path_component(name, destination_path)?;
    let result = unsafe { libc::unlinkat(parent_dir.as_raw_fd(), name.as_ptr(), 0) };
    if result != 0 {
        return Err(unsafe_screenshot_destination(
            destination_path,
            format!(
                "failed to remove existing file: {}",
                io::Error::last_os_error()
            ),
        ));
    }

    Ok(())
}

fn open_dir_path_no_follow(path: &Path) -> Result<File, io::Error> {
    let path_name = c_string_for_path(path)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
    let fd = unsafe {
        libc::open(
            path_name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn openat_dir_no_follow(parent_fd: RawFd, name: &OsStr) -> Result<File, io::Error> {
    let name = CString::new(name.as_bytes())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
    let fd = unsafe {
        libc::openat(
            parent_fd,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn openat_path_no_follow(parent_fd: RawFd, name: &OsStr) -> Result<File, io::Error> {
    let name = CString::new(name.as_bytes())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
    let fd = unsafe {
        libc::openat(
            parent_fd,
            name.as_ptr(),
            libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn openat_create_new(parent_fd: RawFd, name: &OsStr) -> Result<File, io::Error> {
    let name = CString::new(name.as_bytes())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
    let fd = unsafe {
        libc::openat(
            parent_fd,
            name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o644,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn c_string_for_path(path: &Path) -> JobResult<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        JobError::InternalError(format!(
            "Unsafe screenshot path {}: contains a NUL byte",
            path.display()
        ))
    })
}

fn c_string_for_path_component(name: &OsStr, path: &Path) -> JobResult<CString> {
    CString::new(name.as_bytes()).map_err(|_| {
        JobError::InternalError(format!(
            "Unsafe screenshot path {}: contains a NUL byte",
            path.display()
        ))
    })
}

fn unsafe_screenshot_source(path: &Path, reason: impl std::fmt::Display) -> JobError {
    JobError::InternalError(format!(
        "Unsafe screenshot source path {}: {reason}",
        path.display()
    ))
}

fn unsafe_screenshot_destination(path: &Path, reason: impl std::fmt::Display) -> JobError {
    JobError::InternalError(format!(
        "Unsafe screenshot destination path {}: {reason}",
        path.display()
    ))
}

impl JobInstance for PublishJobInstance {
    fn get_job_id(&self) -> i32 {
        self.job_id
    }

    fn order(&self) -> i32 {
        1 /* Delay publish after commits (and other normal ops). because the
          commits may generate more publishes. */
    }

    fn handle_job(
        &mut self,
        executor: &JobExecutor,
        conn: &mut PgConnection,
    ) -> JobResult<serde_json::Value> {
        info!(
            "#{}: Handling Job Publish: build: {}",
            &self.job_id, &self.build_id
        );

        let config = &executor.config;

        let loaded = load_build_and_config(self.build_id, config, conn)?;
        let build_data = loaded.build;
        let repoconfig = loaded.repoconfig;
        let build_refs = load_build_refs(self.build_id, conn)?;

        // Do the actual work
        let res = self.do_publish(&build_data, &build_refs, config, repoconfig, executor, conn);

        // Update the publish repo state in db

        let new_published_state = match &res {
            Ok(_) => PublishedState::Published,
            Err(e) => PublishedState::Failed(e.to_string()),
        };

        conn.transaction::<models::Build, DieselError, _>(|conn| {
            let current_build = builds::table
                .filter(builds::id.eq(self.build_id))
                .get_result::<models::Build>(conn)?;
            let current_published_state = PublishedState::from_db(
                current_build.published_state,
                &current_build.published_state_reason,
            );
            let is_restart_recovery = res.is_ok()
                && current_published_state
                    == PublishedState::Failed("Server was restarted during publish".to_string());

            if !current_published_state.same_state_as(&PublishedState::Publishing)
                && !is_restart_recovery
            {
                error!("Unexpected publishing state {:?}", current_published_state);
                return Err(DieselError::RollbackTransaction);
            };
            let (val, reason) = PublishedState::to_db(&new_published_state);
            diesel::update(builds::table)
                .filter(builds::id.eq(self.build_id))
                .set((
                    builds::published_state.eq(val),
                    builds::published_state_reason.eq(reason),
                ))
                .get_result::<models::Build>(conn)
        })?;

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener;

    fn write_file(path: &Path, contents: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    fn assert_error_contains(result: JobResult<()>, expected: &str) {
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains(expected),
            "expected error to contain {expected:?}, got {err:?}"
        );
    }

    #[test]
    fn copies_nested_screenshot_files() {
        let staged = tempfile::tempdir().unwrap();
        let media = tempfile::tempdir().unwrap();
        write_file(
            &staged.path().join("nested/screenshots/flat-manager.json"),
            r#"{"ok":true}"#,
        );

        install_staged_screenshot_tree(staged.path(), media.path()).unwrap();

        assert_eq!(
            fs::read_to_string(media.path().join("nested/screenshots/flat-manager.json")).unwrap(),
            r#"{"ok":true}"#
        );
    }

    #[test]
    fn replaces_existing_regular_destination_file() {
        let staged = tempfile::tempdir().unwrap();
        let media = tempfile::tempdir().unwrap();
        let relative_path = Path::new("screenshots/flat-manager.json");
        write_file(&staged.path().join(relative_path), "new");
        write_file(&media.path().join(relative_path), "old");

        install_staged_screenshot_tree(staged.path(), media.path()).unwrap();

        assert_eq!(
            fs::read_to_string(media.path().join(relative_path)).unwrap(),
            "new"
        );
    }

    #[test]
    fn rejects_staged_symlink_before_copying_anything() {
        let staged = tempfile::tempdir().unwrap();
        let media = tempfile::tempdir().unwrap();
        let safe_path = Path::new("screenshots/ok.json");
        let link_path = Path::new("screenshots/link.json");
        write_file(&staged.path().join(safe_path), "new");
        fs::create_dir_all(staged.path().join("screenshots")).unwrap();
        symlink("target", staged.path().join(link_path)).unwrap();
        write_file(&media.path().join(safe_path), "old");

        assert_error_contains(
            install_staged_screenshot_tree(staged.path(), media.path()),
            "symlink",
        );
        assert_eq!(
            fs::read_to_string(media.path().join(safe_path)).unwrap(),
            "old"
        );
    }

    #[test]
    fn rejects_staged_unix_socket() {
        let staged = tempfile::tempdir().unwrap();
        let media = tempfile::tempdir().unwrap();
        let socket_path = staged.path().join("screenshots/socket");
        fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
        let _socket = UnixListener::bind(&socket_path).unwrap();

        assert_error_contains(
            install_staged_screenshot_tree(staged.path(), media.path()),
            "not a regular file or directory",
        );
    }

    #[test]
    fn rejects_existing_destination_symlink_file_target() {
        let staged = tempfile::tempdir().unwrap();
        let media = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        fs::write(outside.path(), "outside").unwrap();
        let relative_path = Path::new("screenshots/flat-manager.json");
        write_file(&staged.path().join(relative_path), "new");
        fs::create_dir_all(media.path().join("screenshots")).unwrap();
        symlink(outside.path(), media.path().join(relative_path)).unwrap();

        assert_error_contains(
            install_staged_screenshot_tree(staged.path(), media.path()),
            "symlink",
        );
        assert_eq!(fs::read_to_string(outside.path()).unwrap(), "outside");
    }

    #[test]
    fn rejects_destination_symlink_pivot() {
        let repo = tempfile::tempdir().unwrap();
        let staged = tempfile::tempdir().unwrap();
        let media = repo.path().join("media");
        fs::create_dir_all(&media).unwrap();
        write_file(
            &staged.path().join("pivot/screenshots/flat-manager.json"),
            "new",
        );
        symlink("..", media.join("pivot")).unwrap();

        assert_error_contains(
            install_staged_screenshot_tree(staged.path(), &media),
            "symlink",
        );
        assert!(!repo.path().join("screenshots/flat-manager.json").exists());
    }

    #[test]
    fn allows_configured_media_directory_symlink() {
        let repo = tempfile::tempdir().unwrap();
        let media_target = tempfile::tempdir().unwrap();
        let staged = tempfile::tempdir().unwrap();
        let media = repo.path().join("media");
        let relative_path = Path::new("screenshots/flat-manager.json");
        symlink(media_target.path(), &media).unwrap();
        write_file(&staged.path().join(relative_path), "new");

        install_staged_screenshot_tree(staged.path(), &media).unwrap();

        assert_eq!(
            fs::read_to_string(media_target.path().join(relative_path)).unwrap(),
            "new"
        );
    }
}
