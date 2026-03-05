use base64::{engine::general_purpose, Engine as _};
use libostree::{gio, glib};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{self, Path, PathBuf};
use std::time::Duration;
use std::{fs, io};
use thiserror::Error;
use tokio::process::Command;
use tokio::time::sleep;
use walkdir::WalkDir;

use crate::jobs::SetsidCommandExt;

#[derive(Error, Debug, Clone, Eq, PartialEq)]
pub enum OstreeError {
    #[error("No such ref: {0}")]
    NoSuchRef(String),
    #[error("No such commit: {0}")]
    NoSuchCommit(String),
    #[error("No such object: {0}")]
    NoSuchObject(String),
    #[error("Invalid utf8 string")]
    InvalidUtf8,
    #[error("Command {0} failed to start: {1}")]
    ExecFailed(String, String),
    #[error("Command {0} exited unsuccessfully with stderr: {1}")]
    CommandFailed(String, String),
    #[error("Internal Error: {0}")]
    InternalError(String),
}

pub type OstreeResult<T> = Result<T, OstreeError>;

#[derive(Debug)]
pub struct OstreeCommit {
    pub metadata: HashMap<String, Variant>,
    pub parent: Option<String>,
    pub subject: String,
    pub body: String,
    pub timestamp: u64,
    pub root_tree: String,
    pub root_metadata: String,
}

#[derive(Debug)]
pub struct OstreeDeltaSuperblock {
    pub metadata: HashMap<String, Variant>,
    pub commit: OstreeCommit,
}

#[derive(Debug)]
pub struct Variant(pub glib::Variant);

impl Variant {
    pub fn as_string(&self) -> OstreeResult<String> {
        self.0
            .str()
            .map(ToString::to_string)
            .ok_or_else(|| OstreeError::InternalError("Variant is not a string".to_string()))
    }

    pub fn as_string_vec(&self) -> OstreeResult<Vec<String>> {
        self.0.get::<Vec<String>>().ok_or_else(|| {
            OstreeError::InternalError("Variant is not an array of strings".to_string())
        })
    }

    pub fn as_i32_le(&self) -> OstreeResult<i32> {
        self.0
            .get::<i32>()
            .ok_or_else(|| OstreeError::InternalError("Variant is not an i32".to_string()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.fixed_array::<u8>().unwrap_or(&[])
    }
}

pub fn open_repo(repo_path: &Path) -> OstreeResult<libostree::Repo> {
    let repo_dir = std::env::current_dir()
        .map_err(|e| OstreeError::InternalError(format!("Failed to get cwd: {e}")))?
        .join(repo_path);
    let repo = libostree::Repo::new_for_path(&repo_dir);
    repo.open(gio::Cancellable::NONE).map_err(|e| {
        OstreeError::InternalError(format!("Failed to open repo {repo_path:?}: {e}"))
    })?;
    Ok(repo)
}

fn bytes_to_object(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

fn object_to_bytes(object: &str) -> OstreeResult<Vec<u8>> {
    hex::decode(object)
        .map_err(|e| OstreeError::InternalError(format!("Invalid object '{object}: {e}'")))
}

fn get_deltas_path(repo_path: &path::Path) -> path::PathBuf {
    let mut ref_dir = std::env::current_dir().unwrap_or_default();
    ref_dir.push(repo_path);
    ref_dir.push("deltas");
    ref_dir
}

fn get_tmp_deltas_path(repo_path: &path::Path) -> path::PathBuf {
    let mut ref_dir = std::env::current_dir().unwrap_or_default();
    ref_dir.push(repo_path);
    ref_dir.push("deltas/.tmp");
    ref_dir
}

/* This is like basename, but also includes the parent dir because in
 * ostree object and delta part filenames that is the first to letters
 * of the ID, which we don't want to miss.
 * Also, this is mainly used for debug/errors, so converts to string.
 */
fn get_dir_and_basename(path: &path::Path) -> String {
    let mut res = String::new();
    if let Some(parent_name) = path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|file_name| file_name.to_str())
    {
        res.push_str(parent_name);
        res.push('/');
    }
    res.push_str(path.file_name().and_then(|s| s.to_str()).unwrap_or("?"));
    res
}

fn as_metadata(metadata_variant: &glib::Variant) -> OstreeResult<HashMap<String, Variant>> {
    let metadata: HashMap<String, glib::Variant> = metadata_variant.get().ok_or_else(|| {
        OstreeError::InternalError("Commit metadata has unexpected type".to_string())
    })?;

    Ok(metadata
        .into_iter()
        .map(|(key, value)| (key, Variant(value)))
        .collect())
}

fn as_byte_array<'a>(variant: &'a glib::Variant, field_name: &str) -> OstreeResult<&'a [u8]> {
    variant.fixed_array::<u8>().map_err(|e| {
        OstreeError::InternalError(format!(
            "Commit field '{field_name}' has unexpected type: {e}"
        ))
    })
}

fn as_child_string(
    variant: &glib::Variant,
    index: usize,
    field_name: &str,
) -> OstreeResult<String> {
    variant
        .child_value(index)
        .str()
        .map(ToString::to_string)
        .ok_or_else(|| {
            OstreeError::InternalError(format!("Commit field '{field_name}' has unexpected type"))
        })
}

fn ostree_commit_from_variant(commit_variant: &glib::Variant) -> OstreeResult<OstreeCommit> {
    let metadata = as_metadata(&commit_variant.child_value(0))?;
    let root_tree_variant = commit_variant.child_value(6);
    let root_metadata_variant = commit_variant.child_value(7);

    Ok(OstreeCommit {
        metadata,
        parent: libostree::commit_get_parent(commit_variant).map(|parent| parent.to_string()),
        subject: as_child_string(commit_variant, 3, "subject")?,
        body: as_child_string(commit_variant, 4, "body")?,
        timestamp: libostree::commit_get_timestamp(commit_variant),
        root_tree: bytes_to_object(as_byte_array(&root_tree_variant, "root_tree")?),
        root_metadata: bytes_to_object(as_byte_array(&root_metadata_variant, "root_metadata")?),
    })
}

pub fn get_commit(repo_path: &path::Path, commit: &str) -> OstreeResult<OstreeCommit> {
    let repo = open_repo(repo_path)?;
    let (commit_variant, _state) = repo
        .load_commit(commit)
        .map_err(|e| OstreeError::NoSuchCommit(format!("{commit}: {e}")))?;
    ostree_commit_from_variant(&commit_variant)
}

fn variant_type(type_string: &'static str) -> &'static glib::VariantTy {
    glib::VariantTy::new(type_string).expect("static variant type must be valid")
}

pub fn load_delta_superblock_file(path: &path::Path) -> OstreeResult<OstreeDeltaSuperblock> {
    let contents = fs::read(path)
        .map_err(|e| OstreeError::NoSuchObject(format!("{}: {e}", get_dir_and_basename(path))))?;

    let superblock_variant = glib::Variant::from_data_with_type(
        contents,
        variant_type("(a{sv}tayay(a{sv}aya(say)sstayay)aya(uayttay)a(yaytt))"),
    );
    let metadata = as_metadata(&superblock_variant.child_value(0))?;
    let commit_variant = superblock_variant.child_value(4);
    let commit = ostree_commit_from_variant(&commit_variant)?;

    Ok(OstreeDeltaSuperblock { metadata, commit })
}

pub fn get_delta_superblock(
    repo_path: &path::Path,
    delta: &str,
) -> OstreeResult<OstreeDeltaSuperblock> {
    let mut path = get_deltas_path(repo_path);
    path.push(&delta[0..2]);
    path.push(&delta[2..]);
    path.push("superblock");

    load_delta_superblock_file(&path)
}

pub fn parse_ref(repo_path: &path::Path, ref_name: &str) -> OstreeResult<String> {
    let repo = open_repo(repo_path)?;
    repo.resolve_rev(ref_name, false)
        .map_err(|e| OstreeError::NoSuchRef(format!("{ref_name}: {e}")))?
        .map(|commit| commit.to_string())
        .ok_or_else(|| OstreeError::NoSuchRef(ref_name.to_string()))
}

pub fn list_refs(repo_path: &path::Path, prefix: &str) -> Vec<String> {
    let repo = match open_repo(repo_path) {
        Ok(repo) => repo,
        Err(_e) => return Vec::new(),
    };
    let ref_prefix = if prefix.is_empty() {
        None
    } else {
        Some(prefix)
    };
    let refs = match repo.list_refs_ext(
        ref_prefix,
        libostree::RepoListRefsExtFlags::NONE,
        gio::Cancellable::NONE,
    ) {
        Ok(refs) => refs,
        Err(_e) => return Vec::new(),
    };

    let mut names: Vec<String> = refs.into_keys().collect();
    names.sort();
    names
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
pub struct Delta {
    pub from: Option<String>,
    pub to: String,
}

fn delta_part_to_hex(part: &str) -> OstreeResult<String> {
    let bytes = general_purpose::STANDARD_NO_PAD
        .decode(part.replace('_', "/"))
        .map_err(|err| {
            OstreeError::InternalError(format!("Invalid delta part name '{part}': {err}"))
        })?;
    Ok(bytes_to_object(&bytes))
}

fn hex_to_delta_part(hex: &str) -> OstreeResult<String> {
    let bytes = object_to_bytes(hex)?;
    let part = general_purpose::STANDARD_NO_PAD.encode(bytes);
    Ok(part.replace('/', "_"))
}

impl Delta {
    pub fn new(from: Option<&str>, to: &str) -> Delta {
        Delta {
            from: from.map(|s| s.to_string()),
            to: to.to_string(),
        }
    }
    pub fn from_name(name: &str) -> OstreeResult<Delta> {
        let parts: Vec<&str> = name.split('-').collect();
        if parts.len() == 1 {
            Ok(Delta {
                from: None,
                to: delta_part_to_hex(parts[0])?,
            })
        } else {
            Ok(Delta {
                from: Some(delta_part_to_hex(parts[0])?),
                to: delta_part_to_hex(parts[1])?,
            })
        }
    }

    pub fn to_name(&self) -> OstreeResult<String> {
        let mut name = String::new();

        if let Some(ref from) = self.from {
            name.push_str(&hex_to_delta_part(from)?);
            name.push('-');
        }
        name.push_str(&hex_to_delta_part(&self.to)?);
        Ok(name)
    }

    pub fn delta_path(&self, repo_path: &path::Path) -> OstreeResult<path::PathBuf> {
        let mut path = get_deltas_path(repo_path);
        let name = self.to_name()?;
        path.push(&name[0..2]);
        path.push(&name[2..]);
        Ok(path)
    }

    pub fn tmp_delta_path(&self, repo_path: &path::Path) -> OstreeResult<path::PathBuf> {
        let mut path = get_tmp_deltas_path(repo_path);
        let name = self.to_name()?;
        path.push(&name[0..2]);
        path.push(&name[2..]);
        Ok(path)
    }
}

impl std::fmt::Display for Delta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}",
            self.from.as_deref().unwrap_or("nothing"),
            self.to
        )
    }
}

pub fn list_deltas(repo_path: &path::Path) -> Vec<Delta> {
    let deltas_dir = get_deltas_path(repo_path);

    WalkDir::new(deltas_dir)
        .min_depth(2)
        .max_depth(2)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_dir())
        .map(|e| {
            format!(
                "{}{}",
                e.path()
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_string_lossy(),
                e.file_name().to_string_lossy()
            )
        })
        .filter_map(|name| Delta::from_name(&name).ok())
        .collect()
}

pub fn calc_deltas_for_ref(repo_path: &path::Path, ref_name: &str, depth: u32) -> Vec<Delta> {
    let repo = match open_repo(repo_path) {
        Ok(repo) => repo,
        Err(_e) => return Vec::new(),
    };

    let mut res = Vec::new();
    let to_commit = match repo.resolve_rev(ref_name, false).ok().flatten() {
        Some(commit) => commit.to_string(),
        None => return res,
    };

    let mut from_commit: Option<String> = None;
    for _i in 0..depth {
        let commit_id = from_commit.as_ref().unwrap_or(&to_commit);
        if let Ok((commit_variant, _state)) = repo.load_commit(commit_id) {
            res.push(Delta::new(from_commit.as_deref(), &to_commit));
            from_commit =
                libostree::commit_get_parent(&commit_variant).map(|parent| parent.to_string());
            if from_commit.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    res
}

pub fn checkout_ref(repo_path: &Path, ref_name: &str, dest_dir: &Path) -> OstreeResult<()> {
    let repo = open_repo(repo_path)?;
    fs::create_dir_all(dest_dir).map_err(|e| {
        OstreeError::InternalError(format!(
            "Failed to create checkout destination {dest_dir:?}: {e}"
        ))
    })?;
    let dest_fd = fs::File::open(dest_dir).map_err(|e| {
        OstreeError::InternalError(format!(
            "Failed to open checkout destination {dest_dir:?}: {e}"
        ))
    })?;

    let commit = repo
        .resolve_rev(ref_name, false)
        .map_err(|e| OstreeError::NoSuchRef(format!("{ref_name}: {e}")))?
        .ok_or_else(|| OstreeError::NoSuchRef(ref_name.to_string()))?;

    let checkout_options = libostree::RepoCheckoutAtOptions {
        mode: libostree::RepoCheckoutMode::User,
        overwrite_mode: libostree::RepoCheckoutOverwriteMode::UnionFiles,
        bareuseronly_dirs: true,
        ..Default::default()
    };
    repo.checkout_at(
        Some(&checkout_options),
        dest_fd.as_raw_fd(),
        ".",
        &commit,
        gio::Cancellable::NONE,
    )
    .map_err(|e| OstreeError::InternalError(format!("Failed to checkout ref {ref_name}: {e}")))
}

fn result_from_output(output: std::process::Output, command: &str) -> Result<(), OstreeError> {
    if !output.status.success() {
        Err(OstreeError::CommandFailed(
            command.to_string(),
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ))
    } else {
        Ok(())
    }
}

pub async fn pull_commit_async(
    n_retries: i32,
    repo_path: PathBuf,
    url: String,
    commit: String,
) -> Result<(), OstreeError> {
    let mut retries_left = n_retries;
    loop {
        let mut cmd = Command::new("ostree");
        SetsidCommandExt::setsid(&mut cmd);

        cmd.arg(format!("--repo={}", &repo_path.to_str().unwrap()))
            .arg("pull")
            .arg(format!("--url={url}"))
            .arg("upstream")
            .arg(&commit);

        log::info!("Pulling commit {}", commit);
        let output = cmd
            .output()
            .await
            .map_err(|e| OstreeError::ExecFailed("ostree pull".to_string(), e.to_string()))?;

        match result_from_output(output, "ostree pull") {
            Ok(()) => return Ok(()),
            Err(e) => {
                if retries_left > 1 {
                    log::warn!("Pull error, retrying commit {}: {}", commit, e);
                    retries_left -= 1;
                    sleep(Duration::from_secs(5)).await;
                } else {
                    return Err(e);
                }
            }
        }
    }
}

pub async fn pull_delta_async(
    n_retries: i32,
    repo_path: &Path,
    url: &str,
    delta: &Delta,
) -> Result<(), OstreeError> {
    let url_clone = url.to_string();
    let repo_path_clone = repo_path.to_path_buf();

    if let Some(ref from) = delta.from {
        pull_commit_async(
            n_retries,
            repo_path.to_path_buf(),
            url_clone.clone(),
            from.clone(),
        )
        .await?;
    }

    pull_commit_async(n_retries, repo_path_clone, url_clone, delta.to.clone()).await
}

pub async fn generate_delta_async(repo_path: &Path, delta: &Delta) -> Result<(), OstreeError> {
    let mut cmd = Command::new("timeout");
    cmd.arg("3600").arg("flatpak");
    SetsidCommandExt::setsid(&mut cmd);

    cmd.arg("build-update-repo")
        .arg("--generate-static-delta-to")
        .arg(delta.to.clone());

    if let Some(ref from) = delta.from {
        cmd.arg("--generate-static-delta-from").arg(from.clone());
    };

    cmd.arg(repo_path);

    log::info!("Generating delta {}", delta);
    let output = cmd.output().await.map_err(|e| {
        OstreeError::ExecFailed("flatpak build-update-repo".to_string(), e.to_string())
    })?;
    result_from_output(output, "flatpak build-update-repo")
}

pub async fn prune_async(repo_path: &Path) -> Result<(), OstreeError> {
    let mut cmd = Command::new("ostree");
    SetsidCommandExt::setsid(&mut cmd);

    cmd.arg("prune")
        .arg(format!("--repo={}", repo_path.to_string_lossy()))
        .arg("--keep-younger-than=3 days ago");

    let output = cmd
        .output()
        .await
        .map_err(|e| OstreeError::ExecFailed("ostree prune".to_string(), e.to_string()))?;
    result_from_output(output, "ostree prune")
}

pub fn init_ostree_repo(
    repo_path: &path::Path,
    parent_repo_path: &path::Path,
    opt_collection_id: &Option<(String, i32)>,
) -> io::Result<()> {
    let parent_repo_absolute_path = std::env::current_dir()?.join(parent_repo_path);

    for &d in [
        "extensions",
        "objects",
        "refs/heads",
        "refs/mirrors",
        "refs/remotes",
        "state",
        "tmp/cache",
    ]
    .iter()
    {
        fs::create_dir_all(repo_path.join(d))?;
    }

    std::os::unix::fs::symlink(&parent_repo_absolute_path, repo_path.join("parent"))?;

    let mut file = fs::File::create(repo_path.join("config"))?;
    file.write_all(
        format!(
            r#"[core]
repo_version=1
mode=archive-z2
min-free-space-size=500MB
{}parent={}"#,
            match opt_collection_id {
                Some((collection_id, build_id)) =>
                    format!("collection-id={collection_id}.Build{build_id}\n"),
                _ => "".to_string(),
            },
            parent_repo_absolute_path.display()
        )
        .as_bytes(),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_delta_name() {
        assert_eq!(
            delta_part_to_hex("OkiocD9GLq_Nt660BvWyrH8G62dAvtLv7RPqngWqf5c"),
            Ok("3a48a8703f462eafcdb7aeb406f5b2ac7f06eb6740bed2efed13ea9e05aa7f97".to_string())
        );
        assert_eq!(
            hex_to_delta_part("3a48a8703f462eafcdb7aeb406f5b2ac7f06eb6740bed2efed13ea9e05aa7f97"),
            Ok("OkiocD9GLq_Nt660BvWyrH8G62dAvtLv7RPqngWqf5c".to_string())
        );
        assert_eq!(
            Delta::from_name("OkiocD9GLq_Nt660BvWyrH8G62dAvtLv7RPqngWqf5c"),
            Ok(Delta {
                from: None,
                to: "3a48a8703f462eafcdb7aeb406f5b2ac7f06eb6740bed2efed13ea9e05aa7f97".to_string()
            })
        );
        assert_eq!(Delta::from_name("OkiocD9GLq_Nt660BvWyrH8G62dAvtLv7RPqngWqf5c-3dpOrJG4MNyKHDDGXHpH_zd9NXugnexr5jpvSFQ77S4"),
                   Ok(Delta { from: Some("3a48a8703f462eafcdb7aeb406f5b2ac7f06eb6740bed2efed13ea9e05aa7f97".to_string()), to: "ddda4eac91b830dc8a1c30c65c7a47ff377d357ba09dec6be63a6f48543bed2e".to_string() }));
    }

    #[test]
    fn test_delta_display_has_no_trailing_newline() {
        let no_from = Delta {
            from: None,
            to: "tohash".to_string(),
        };
        assert_eq!(no_from.to_string(), "nothing-tohash");
        assert!(!no_from.to_string().ends_with('\n'));

        let with_from = Delta {
            from: Some("fromhash".to_string()),
            to: "tohash".to_string(),
        };
        assert_eq!(with_from.to_string(), "fromhash-tohash");
        assert!(!with_from.to_string().ends_with('\n'));
    }
}
