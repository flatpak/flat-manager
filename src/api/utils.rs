use actix_web::http::header::{self, ContentDisposition};
use actix_web::{error, http, web};
use actix_web::{HttpRequest, HttpResponse, Result};

use futures::StreamExt;
use log::warn;
use serde::Serialize;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path;
use tempfile::{NamedTempFile, TempDir};
use tokio::io::AsyncWriteExt;

use crate::errors::ApiError;

pub fn respond_with_url<T>(
    data: &T,
    req: &HttpRequest,
    name: &str,
    elements: &[String],
) -> Result<HttpResponse, ApiError>
where
    T: Serialize,
{
    match req.url_for(name, elements) {
        Ok(url) => Ok(HttpResponse::Ok()
            .insert_header((http::header::LOCATION, url.to_string()))
            .json(data)),
        Err(e) => Err(ApiError::InternalServerError(format!(
            "Can't get url for {name} {elements:?}: {e}"
        ))),
    }
}

fn is_all_lower_hexdigits(s: &str) -> bool {
    s.chars()
        .all(|c: char| c.is_ascii_hexdigit() && !c.is_uppercase())
}

#[derive(Clone)]
enum UploadTarget {
    Object {
        subpath: path::PathBuf,
        object_type: libostree::ObjectType,
        checksum: String,
    },
    Delta {
        subpath: path::PathBuf,
    },
}

impl UploadTarget {
    fn subpath(&self) -> &path::Path {
        match self {
            Self::Object { subpath, .. } | Self::Delta { subpath } => subpath,
        }
    }

    fn filename(&self) -> Option<String> {
        match self {
            Self::Object {
                checksum,
                object_type,
                ..
            } => {
                let suffix = match object_type {
                    libostree::ObjectType::File => "filez",
                    libostree::ObjectType::DirMeta => "dirmeta",
                    libostree::ObjectType::DirTree => "dirtree",
                    libostree::ObjectType::Commit => "commit",
                    _ => return None,
                };
                Some(format!("{checksum}.{suffix}"))
            }
            Self::Delta { .. } => None,
        }
    }
}

fn filename_parse_object(filename: &str) -> Option<UploadTarget> {
    let v: Vec<&str> = filename.split('.').collect();

    if v.len() != 2 {
        return None;
    }

    if v[0].len() != 64 || !is_all_lower_hexdigits(v[0]) {
        return None;
    }

    let object_type = match v[1] {
        "dirmeta" => libostree::ObjectType::DirMeta,
        "dirtree" => libostree::ObjectType::DirTree,
        "filez" => libostree::ObjectType::File,
        "commit" => libostree::ObjectType::Commit,
        _ => return None,
    };

    Some(UploadTarget::Object {
        subpath: path::Path::new("objects")
            .join(&filename[..2])
            .join(&filename[2..]),
        object_type,
        checksum: v[0].to_string(),
    })
}

fn is_all_digits(s: &str) -> bool {
    !s.contains(|c: char| !c.is_ascii_digit())
}

/* Delta part filenames with no slashes, ending with .{part}.delta.
 * Examples:
 * oS6QiSBxQF5nJZBVS6MJ6tCk_KN63I72Y7QipgUTh5w-sdm_iU8hHZYwDpmzYBAP6cJQ5MX5VLxoGF+j+Q1OGPQ.superblock.delta
 * oS6QiSBxQF5nJZBVS6MJ6tCk_KN63I72Y7QipgUTh5w-sdm_iU8hHZYwDpmzYBAP6cJQ5MX5VLxoGF+j+Q1OGPQ.0.delta
 * sdm_iU8hHZYwDpmzYBAP6cJQ5MX5VLxoGF+j+Q1OGPQ.superblock.delta
 * sdm_iU8hHZYwDpmzYBAP6cJQ5MX5VLxoGF+j+Q1OGPQ.0.delta
 */
fn filename_parse_delta(name: &str) -> Option<path::PathBuf> {
    let v: Vec<&str> = name.split('.').collect();

    if v.len() != 3 {
        return None;
    }

    if v[2] != "delta" {
        return None;
    }

    if v[1] != "superblock" && !is_all_digits(v[1]) {
        return None;
    }

    if !(v[0].len() == 43 || (v[0].len() == 87 && v[0].chars().nth(43) == Some('-'))) {
        return None;
    }

    Some(
        path::Path::new("deltas")
            .join(&v[0][..2])
            .join(&v[0][2..])
            .join(v[1]),
    )
}

fn get_upload_target(
    field: &actix_multipart::Field,
    state: &UploadState,
) -> error::Result<UploadTarget, ApiError> {
    // actix-multipart filters content_disposition() to only return FormData dispositions,
    // but the client sends "attachment". Fall back to parsing the raw header.
    let cd_owned;
    let cd = match field.content_disposition() {
        Some(cd) => cd,
        None => {
            let raw = field
                .headers()
                .get(&header::CONTENT_DISPOSITION)
                .ok_or_else(|| {
                    ApiError::BadRequest("No content disposition for multipart item".to_string())
                })?;
            cd_owned = ContentDisposition::from_raw(raw).map_err(|_| {
                ApiError::BadRequest("Invalid content disposition for multipart item".to_string())
            })?;
            &cd_owned
        }
    };
    let filename = cd
        .get_filename()
        .ok_or_else(|| ApiError::BadRequest("No filename for multipart item".to_string()))?;
    // We verify the format below, but just to make sure we never allow anything like a path
    if filename.contains('/') {
        return Err(ApiError::BadRequest("Invalid upload filename".to_string()));
    }

    if !state.only_deltas {
        if let Some(target) = filename_parse_object(filename) {
            return Ok(target);
        }
    }

    if let Some(subpath) = filename_parse_delta(filename) {
        return Ok(UploadTarget::Delta { subpath });
    }

    Err(ApiError::BadRequest("Invalid upload filename".to_string()))
}

pub struct UploadState {
    pub repo_path: path::PathBuf,
    pub only_deltas: bool,
    validation_repo: libostree::Repo,
    validation_tmp_dir: TempDir,
}

impl UploadState {
    pub fn new(repo_path: path::PathBuf, only_deltas: bool) -> Result<Self, ApiError> {
        let tmp_dir = repo_path.join("deltas/.tmp");
        fs::create_dir_all(&tmp_dir).map_err(|e| {
            ApiError::InternalServerError(format!("Can't create upload temporary directory: {e}"))
        })?;
        let validation_tmp_dir = TempDir::new_in(&tmp_dir).map_err(|e| {
            ApiError::InternalServerError(format!("Can't create upload validation directory: {e}"))
        })?;
        let validation_repo = libostree::Repo::new_for_path(validation_tmp_dir.path());
        validation_repo
            .create(
                libostree::RepoMode::Archive,
                libostree::gio::Cancellable::NONE,
            )
            .map_err(|e| {
                ApiError::InternalServerError(format!("Can't create upload validation repository: {e}"))
            })?;
        Ok(Self {
            repo_path,
            only_deltas,
            validation_repo,
            validation_tmp_dir,
        })
    }
}

fn start_save(
    target: &UploadTarget,
    state: &UploadState,
) -> Result<(NamedTempFile, path::PathBuf)> {
    let absolute_path = state.repo_path.join(target.subpath());
    if let Some(parent) = absolute_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_dir = state.repo_path.join("deltas/.tmp");
    fs::create_dir_all(&tmp_dir)?;

    let named_file = NamedTempFile::new_in(&tmp_dir)?;
    Ok((named_file, absolute_path))
}

fn set_upload_permissions(uploaded_file: &path::Path) {
    if let Ok(metadata) = fs::metadata(uploaded_file) {
        let mut perms = metadata.permissions();
        perms.set_mode(0o644);
        if let Err(_e) = fs::set_permissions(uploaded_file, perms) {
            warn!("Can't change permissions on uploaded file");
        }
    } else {
        warn!("Can't get permissions on uploaded file");
    }
}

fn persist_upload_blocking(
    named_file: NamedTempFile,
    target: &UploadTarget,
    repo_path: &path::Path,
    validation_repo: &libostree::Repo,
    validation_tmp_dir: &path::Path,
) -> Result<(), ApiError> {
    let object_file = repo_path.join(target.subpath());
    match target {
        UploadTarget::Object {
            object_type,
            checksum,
            ..
        } => {
            let staged_object = validation_tmp_dir.join(target.subpath());
            if let Some(parent) = staged_object.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
            }
            named_file
                .persist(&staged_object)
                .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
            if let Err(e) = validation_repo.fsck_object(
                *object_type,
                checksum,
                libostree::gio::Cancellable::NONE,
            ) {
                let filename = target.filename().ok_or_else(|| {
                    ApiError::InternalServerError("Invalid upload object type".to_string())
                })?;
                return Err(ApiError::InternalServerError(format!(
                    "Uploaded OSTree object {filename} failed validation: {e}"
                )));
            }
            fs::rename(&staged_object, &object_file)
                .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
            set_upload_permissions(&object_file);
        }
        UploadTarget::Delta { .. } => {
            named_file
                .persist(&object_file)
                .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
            set_upload_permissions(&object_file);
        }
    }
    Ok(())
}

#[cfg(test)]
fn persist_upload(
    named_file: NamedTempFile,
    target: &UploadTarget,
    state: &UploadState,
) -> Result<(), ApiError> {
    persist_upload_blocking(
        named_file,
        target,
        &state.repo_path,
        &state.validation_repo,
        state.validation_tmp_dir.path(),
    )
}

pub async fn save_file(
    mut field: actix_multipart::Field,
    state: &UploadState,
) -> Result<i64, ApiError> {
    let target = get_upload_target(&field, state)?;

    let (named_file, _object_file) = start_save(&target, state)
        .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
    let mut async_file = tokio::fs::File::from_std(
        named_file
            .as_file()
            .try_clone()
            .map_err(|e| ApiError::InternalServerError(e.to_string()))?,
    );

    let mut size = 0i64;
    while let Some(chunk) = field.next().await {
        let chunk = chunk.map_err(|e| ApiError::InternalServerError(e.to_string()))?;
        async_file
            .write_all(chunk.as_ref())
            .await
            .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
        size += chunk.len() as i64;
    }
    async_file
        .flush()
        .await
        .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
    drop(async_file);

    let repo_path = state.repo_path.clone();
    let validation_repo = state.validation_repo.clone();
    let validation_tmp_dir = state.validation_tmp_dir.path().to_path_buf();
    web::block(move || {
        persist_upload_blocking(
            named_file,
            &target,
            &repo_path,
            &validation_repo,
            &validation_tmp_dir,
        )
    })
    .await
    .map_err(|e| ApiError::InternalServerError(e.to_string()))??;

    Ok(size)
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use super::*;

    #[test]
    fn test_is_all_lower_hexdigits() {
        assert!(is_all_lower_hexdigits("0123456789abcdef"));
        assert!(!is_all_lower_hexdigits("0123456789Abcdef"));
        assert!(!is_all_lower_hexdigits("?"));
    }

    #[test]
    fn object_filenames_map_to_types_and_subpaths() {
        let checksum = "0123456789abcdef".repeat(4);
        let object_types = [
            ("filez", libostree::ObjectType::File),
            ("dirmeta", libostree::ObjectType::DirMeta),
            ("dirtree", libostree::ObjectType::DirTree),
            ("commit", libostree::ObjectType::Commit),
        ];

        for (suffix, expected_type) in object_types {
            let filename = format!("{checksum}.{suffix}");
            let expected_subpath = path::Path::new("objects")
                .join(&checksum[..2])
                .join(format!("{}.{suffix}", &checksum[2..]));
            match filename_parse_object(&filename) {
                Some(UploadTarget::Object {
                    subpath,
                    object_type,
                    checksum: parsed_checksum,
                }) => {
                    assert_eq!(subpath, expected_subpath);
                    assert_eq!(object_type, expected_type);
                    assert_eq!(parsed_checksum, checksum);
                }
                _ => panic!("Expected object target for {filename}"),
            }
        }
    }

    #[test]
    fn object_filename_rejections() {
        let checksum = "0123456789abcdef".repeat(4);
        let invalid_filenames = [
            format!("{}.filez", "A".repeat(64)),
            format!("{}.filez", "a".repeat(63)),
            format!("{checksum}.unknown"),
            format!("{checksum}/object.filez"),
        ];

        for filename in invalid_filenames {
            assert!(filename_parse_object(&filename).is_none(), "{filename}");
        }
    }

    #[test]
    fn invalid_object_is_not_published() {
        let repo_dir = tempfile::tempdir().unwrap();
        let state = UploadState::new(repo_dir.path().to_path_buf(), false).unwrap();
        let checksum = "0123456789abcdef".repeat(4);
        let filename = format!("{checksum}.filez");
        let target = filename_parse_object(&filename).unwrap();
        let (mut named_file, final_path) = start_save(&target, &state).unwrap();
        named_file.write_all(b"invalid object").unwrap();

        let result = persist_upload(named_file, &target, &state);
        match result {
            Err(ApiError::InternalServerError(message)) => {
                assert!(message.starts_with(&format!(
                    "Uploaded OSTree object {filename} failed validation: "
                )));
            }
            other => panic!("Expected validation error, got {other:?}"),
        }
        assert!(!final_path.exists());
    }

    #[test]
    fn invalid_object_does_not_replace_existing_file() {
        let repo_dir = tempfile::tempdir().unwrap();
        let state = UploadState::new(repo_dir.path().to_path_buf(), false).unwrap();
        let checksum = "0123456789abcdef".repeat(4);
        let filename = format!("{checksum}.filez");
        let target = filename_parse_object(&filename).unwrap();
        let final_path = state.repo_path.join(target.subpath());
        fs::create_dir_all(final_path.parent().unwrap()).unwrap();
        fs::write(&final_path, b"existing object").unwrap();
        let (mut named_file, _) = start_save(&target, &state).unwrap();
        named_file.write_all(b"invalid object").unwrap();

        assert!(persist_upload(named_file, &target, &state).is_err());
        assert_eq!(fs::read(final_path).unwrap(), b"existing object");
    }

    #[test]
    fn valid_archive_object_is_published_and_valid() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_repo = libostree::Repo::new_for_path(source_dir.path());
        source_repo
            .create(
                libostree::RepoMode::Archive,
                libostree::gio::Cancellable::NONE,
            )
            .unwrap();
        let checksum = source_repo
            .write_regfile_inline(
                None,
                0,
                0,
                0o100644,
                None,
                b"valid object",
                libostree::gio::Cancellable::NONE,
            )
            .unwrap()
            .to_string();
        let source_object = source_dir
            .path()
            .join("objects")
            .join(&checksum[..2])
            .join(format!("{}.filez", &checksum[2..]));

        let destination_dir = tempfile::tempdir().unwrap();
        let destination_repo = libostree::Repo::new_for_path(destination_dir.path());
        destination_repo
            .create(
                libostree::RepoMode::Archive,
                libostree::gio::Cancellable::NONE,
            )
            .unwrap();
        let state = UploadState::new(destination_dir.path().to_path_buf(), false).unwrap();
        let filename = format!("{checksum}.filez");
        let target = filename_parse_object(&filename).unwrap();
        let (mut named_file, final_path) = start_save(&target, &state).unwrap();
        named_file
            .write_all(&fs::read(source_object).unwrap())
            .unwrap();

        persist_upload(named_file, &target, &state).unwrap();

        assert!(final_path.exists());
        destination_repo
            .fsck_object(
                libostree::ObjectType::File,
                &checksum,
                libostree::gio::Cancellable::NONE,
            )
            .unwrap();
    }
}
