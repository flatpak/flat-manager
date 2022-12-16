use actix::prelude::*;
use actix_web::{error, http};
use actix_web::{HttpRequest, HttpResponse, Result};

use futures::future;
use futures::future::Future;
use log::warn;
use serde::Serialize;
use std::cell::RefCell;
use std::clone::Clone;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path;
use std::rc::Rc;
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
            .header(http::header::LOCATION, url.to_string())
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
    let cd = field.content_disposition().ok_or_else(|| {
        ApiError::BadRequest("No content disposition for multipart item".to_string())
    })?;
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

    let tmp_dir = state.repo_path.join("tmp");
    fs::create_dir_all(&tmp_dir)?;

    let named_file = NamedTempFile::new_in(&tmp_dir)?;
    Ok((named_file, absolute_path))
}

pub fn save_file(
    field: actix_multipart::Field,
    state: &Arc<UploadState>,
) -> Box<dyn Future<Item = i64, Error = ApiError>> {
    let repo_subpath = match get_upload_subpath(&field, state) {
        Ok(subpath) => subpath,
        Err(e) => return Box::new(future::err(e)),
    };

    let (named_file, object_file) = match start_save(&repo_subpath, state) {
        Ok((named_file, object_file)) => (named_file, object_file),
        Err(e) => return Box::new(future::err(ApiError::InternalServerError(e.to_string()))),
    };

    // We need file in two continuations below, so put it in a Rc+RefCell
    let shared_file = Rc::new(RefCell::new(named_file));
    let shared_file2 = shared_file.clone();
    Box::new(
        field
            .fold(0i64, move |acc, bytes| {
                let rt = shared_file
                    .borrow_mut()
                    .write_all(bytes.as_ref())
                    .map(|_| acc + bytes.len() as i64)
                    .map_err(|e| {
                        actix_multipart::MultipartError::Payload(error::PayloadError::Io(e))
                    });
                future::result(rt)
            })
            .map_err(|e| ApiError::InternalServerError(e.to_string()))
            .and_then(move |res| {
                // persist consumes the named file, so we need to
                // completely move it out of the shared Rc+RefCell
                let named_file = Rc::try_unwrap(shared_file2).unwrap().into_inner();
                match named_file.persist(&object_file) {
                    Ok(persisted_file) => {
                        if let Ok(metadata) = persisted_file.metadata() {
                            let mut perms = metadata.permissions();
                            perms.set_mode(0o644);
                            if let Err(_e) = fs::set_permissions(&object_file, perms) {
                                warn!("Can't change permissions on uploaded file");
                            }
                        } else {
                            warn!("Can't get permissions on uploaded file");
                        };
                        future::result(Ok(res))
                    }
                    Err(e) => future::err(ApiError::InternalServerError(e.to_string())),
                }
            }),
    )
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
