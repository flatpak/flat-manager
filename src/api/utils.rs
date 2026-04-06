use actix_web::http::header::{self, ContentDisposition};
use actix_web::{error, http};
use actix_web::{HttpRequest, HttpResponse, Result};

use futures::StreamExt;
use log::warn;
use serde::Serialize;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path;
use std::sync::Arc;
use tempfile::NamedTempFile;

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

fn filename_parse_object(filename: &str) -> Option<path::PathBuf> {
    let v: Vec<&str> = filename.split('.').collect();

    if v.len() != 2 {
        return None;
    }

    if v[0].len() != 64 || !is_all_lower_hexdigits(v[0]) {
        return None;
    }

    if v[1] != "dirmeta" && v[1] != "dirtree" && v[1] != "filez" && v[1] != "commit" {
        return None;
    }

    Some(
        path::Path::new("objects")
            .join(&filename[..2])
            .join(&filename[2..]),
    )
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

fn get_upload_subpath(
    field: &actix_multipart::Field,
    state: &Arc<UploadState>,
) -> error::Result<path::PathBuf, ApiError> {
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
        if let Some(path) = filename_parse_object(filename) {
            return Ok(path);
        }
    }

    if let Some(path) = filename_parse_delta(filename) {
        return Ok(path);
    }

    Err(ApiError::BadRequest("Invalid upload filename".to_string()))
}

pub struct UploadState {
    pub repo_path: path::PathBuf,
    pub only_deltas: bool,
}

pub fn start_save(
    subpath: &path::Path,
    state: &Arc<UploadState>,
) -> Result<(NamedTempFile, path::PathBuf)> {
    let absolute_path = state.repo_path.join(subpath);

    if let Some(parent) = absolute_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_dir = state.repo_path.join("deltas/.tmp");
    fs::create_dir_all(&tmp_dir)?;

    let named_file = NamedTempFile::new_in(&tmp_dir)?;
    Ok((named_file, absolute_path))
}

pub async fn save_file(
    mut field: actix_multipart::Field,
    state: &Arc<UploadState>,
) -> Result<(path::PathBuf, i64), ApiError> {
    let repo_subpath = get_upload_subpath(&field, state)?;

    let (mut named_file, object_file) = start_save(&repo_subpath, state)
        .map_err(|e| ApiError::InternalServerError(e.to_string()))?;

    let mut size = 0i64;
    while let Some(chunk) = field.next().await {
        let chunk = chunk.map_err(|e| ApiError::InternalServerError(e.to_string()))?;
        named_file
            .write_all(chunk.as_ref())
            .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
        size += chunk.len() as i64;
    }

    let persisted_file = named_file
        .persist(&object_file)
        .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
    if let Ok(metadata) = persisted_file.metadata() {
        let mut perms = metadata.permissions();
        perms.set_mode(0o644);
        if let Err(_e) = fs::set_permissions(&object_file, perms) {
            warn!("Can't change permissions on uploaded file");
        }
    } else {
        warn!("Can't get permissions on uploaded file");
    };

    Ok((repo_subpath, size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_all_lower_hexdigits() {
        assert!(is_all_lower_hexdigits("0123456789abcdef"));
        assert!(!is_all_lower_hexdigits("0123456789Abcdef"));
        assert!(!is_all_lower_hexdigits("?"));
    }
}
