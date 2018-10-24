use actix_web::{dev, error, multipart, http};
use actix_web::{AsyncResponder, FutureResponse, HttpMessage, HttpRequest, HttpResponse, Json, Path, Result, State,};
use actix_web::error::{ErrorBadRequest,};

use futures::future;
use futures::{Future, Stream};
use std::cell::RefCell;
use std::env;
use std::ffi::OsString;
use std::fs::File;
use std::fs;
use std::io::Write;
use std::io;
use std::path;
use std::process::{Command};
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use tempfile::NamedTempFile;

use app::{AppState};
use db::{CreateBuild, CreateBuildRef, LookupBuild, LookupBuildRef, LookupBuildRefs, ChangeRepoState};
use models::{NewBuildRef, RepoState};
use actix_web::ResponseError;

pub fn create_build(
    (state, req): (State<AppState>, HttpRequest<AppState>)
) -> FutureResponse<HttpResponse> {
    state
        .db
        .send(CreateBuild { })
        .from_err()
        .and_then(move |res| match res {
            Ok(build) => {
                match req.url_for("show_build", &[build.id.to_string()]) {
                    Ok(url) => Ok(HttpResponse::Ok()
                                  .header(http::header::LOCATION, url.to_string())
                                  .json(build)),
                    Err(e) => Ok(e.error_response())
                }
            },
            Err(e) => Ok(e.error_response())
        })
        .responder()
}

#[derive(Deserialize)]
pub struct BuildPathParams {
    id: i32,
}

pub fn get_build(
    (params, state): (Path<BuildPathParams>, State<AppState>),
) -> FutureResponse<HttpResponse> {
    state
        .db
        .send(LookupBuild { id: params.id })
        .from_err()
        .and_then(|res| match res {
            Ok(build) => Ok(HttpResponse::Ok().json(build)),
            Err(e) => Ok(e.error_response())
        })
        .responder()
}

#[derive(Deserialize)]
pub struct RefPathParams {
    id: i32,
    ref_id: i32,
}

pub fn get_build_ref(
    (params, state): (Path<RefPathParams>, State<AppState>),
) -> FutureResponse<HttpResponse> {
    state
        .db
        .send(LookupBuildRef {
            id: params.id,
            ref_id: params.ref_id,
        })
        .from_err()
        .and_then(|res| match res {
            Ok(build_ref) => Ok(HttpResponse::Ok().json(build_ref)),
            Err(e) => Ok(e.error_response())
        })
        .responder()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MissingObjectsArgs {
    wanted: Vec<String>
}

fn has_object (build_id: i32, object: &str, state: &State<AppState>) -> bool
{
    let subpath: path::PathBuf = ["objects", &object[..2], &object[2..]].iter().collect();
    let build_path = state.build_repo_base_path.join(build_id.to_string()).join(&subpath);
    if build_path.exists() {
        true
    } else {
        let main_path = state.repo_path.join(&subpath);
        main_path.exists()
    }
}

pub fn missing_objects(
    (missing_objects, params, state): (Json<MissingObjectsArgs>, Path<BuildPathParams>, State<AppState>),
) -> HttpResponse {
    let mut missing = vec![];
    for object in &missing_objects.wanted {
        if ! has_object (params.id, object, &state) {
            missing.push(object);
        }
    }
    HttpResponse::Ok()
        .content_encoding(http::ContentEncoding::Gzip)
        .json(&missing)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateBuildRefArgs {
    #[serde(rename = "ref")] ref_name: String,
    commit: String,
}

pub fn create_build_ref (
    (args, params, state, req): (Json<CreateBuildRefArgs>, Path<BuildPathParams>, State<AppState>, HttpRequest<AppState>),
) -> FutureResponse<HttpResponse> {
    state
        .db
        .send(CreateBuildRef {
            data: NewBuildRef {
                build_id: params.id,
                ref_name: args.ref_name.clone(),
                commit: args.commit.clone(),
            }
        })
        .from_err()
        .and_then(move |res| match res {
            Ok(buildref) =>  {
                match req.url_for("show_build_ref", &[params.id.to_string(), buildref.id.to_string()]) {
                    Ok(url) => Ok(HttpResponse::Ok()
                                  .header(http::header::LOCATION, url.to_string())
                                  .json(buildref)),
                    Err(e) => Ok(e.error_response())
                }
            },
            Err(e) => Ok(e.error_response())
        })
        .responder()
}

fn objectname_is_valid(name: &str) -> bool {
    let v: Vec<&str> = name.splitn(2, ".").collect();

    v.len() == 2 &&
        v[0].len() == 64  &&
        !v[0].contains(|c: char| !(c.is_digit(16) && !c.is_uppercase())) &&
        (v[1] == "dirmeta" ||
         v[1] == "dirtree" ||
         v[1] == "filez" ||
         v[1] == "commit")
}

fn get_object_name(field: &multipart::Field<dev::Payload>) -> error::Result<String, io::Error> {
    let cd = field.content_disposition().ok_or(
        io::Error::new(io::ErrorKind::InvalidInput,
                       "No content disposition for multipart item"))?;
    let filename = cd.get_filename().ok_or(
        io::Error::new(io::ErrorKind::InvalidInput,
                       "No filename for multipart item"))?;
    if !objectname_is_valid(filename) {
        Err(io::Error::new(io::ErrorKind::InvalidInput,
                           "Invalid object name"))
    } else {
        Ok(filename.to_string())
    }
}

struct UploadState {
    repo_path: path::PathBuf,
}

fn start_save(
    object: String,
    state: &Arc<UploadState>,
) -> Result<(NamedTempFile,path::PathBuf)> {
    let objects_dir = state.repo_path.join("objects");
    let object_dir = objects_dir.join(&object[..2]);
    let object_file = object_dir.join(&object[2..]);

    fs::create_dir_all(object_dir)?;

    let tmp_dir = state.repo_path.join("tmp");
    fs::create_dir_all(&tmp_dir)?;

    let named_file = NamedTempFile::new_in(&tmp_dir)?;
    Ok((named_file, object_file))
}

fn save_file(
    field: multipart::Field<dev::Payload>,
    state: &Arc<UploadState>
) -> Box<Future<Item = i64, Error = error::Error>> {
    let object = match  get_object_name (&field) {
        Ok(name) => name,
        Err(e) => return Box::new(future::err(ErrorBadRequest(e))),
    };

    let (named_file, object_file) = match start_save (object, state) {
        Ok((named_file, object_file)) => (named_file, object_file),
        Err(e) => return Box::new(future::err(error::ErrorInternalServerError(e))),
    };

    // We need file in two continuations below, so put it in a Rc+RefCell
    let shared_file = Rc::new(RefCell::new(named_file));
    let shared_file2 = shared_file.clone();
    Box::new(
        field
            .fold(0i64, move |acc, bytes| {
                let rt = shared_file.borrow_mut()
                    .write_all(bytes.as_ref())
                    .map(|_| acc + bytes.len() as i64)
                    .map_err(|e| {
                        println!("file.write_all failed: {:?}", e);
                        error::MultipartError::Payload(error::PayloadError::Io(e))
                    });
                future::result(rt)
            })
            .map_err(|e| {
                println!("save_file failed, {:?}", e);
                error::ErrorInternalServerError(e)
            }).and_then (move |res| {
                // persist consumes the named file, so we need to
                // completely move it out of the shared Rc+RefCell
                let named_file = Rc::try_unwrap(shared_file2).unwrap().into_inner();
                match named_file.persist(object_file) {
                    Ok(_persisted_file) => future::result(Ok(res)),
                    Err(e) => future::err(error::ErrorInternalServerError(e))
                }
            }),
    )
}

fn handle_multipart_item(
    item: multipart::MultipartItem<dev::Payload>,
    state: &Arc<UploadState>
) -> Box<Stream<Item = i64, Error = error::Error>> {
    match item {
        multipart::MultipartItem::Field(field) => {
            Box::new(save_file(field, state).into_stream())
        }
        multipart::MultipartItem::Nested(mp) => {
            let s = state.clone();
            Box::new(mp.map_err(error::ErrorInternalServerError)
                     .map(move |item| { handle_multipart_item (item, &s) })
                     .flatten())
        }
    }
}

pub fn upload(
    (params, req): (Path<BuildPathParams>, HttpRequest<AppState>)
) -> FutureResponse<HttpResponse> {
    let state = req.state();
    let uploadstate = Arc::new(UploadState { repo_path: state.build_repo_base_path.join(params.id.to_string()).join("upload") });
    Box::new(
        req.multipart()
            .map_err(error::ErrorInternalServerError)
            .map(move |item| { handle_multipart_item (item, &uploadstate) })
            .flatten()
            .collect()
            .map(|sizes| HttpResponse::Ok().json(sizes))
            .map_err(|e| {
                println!("failed: {}", e);
                e
            }),
    )
}

fn init_repo(repo_path: &path::PathBuf, parent_repo_path: &path::PathBuf, build_id: i32, opt_collection_id: &Option<String>) -> io::Result<()> {
    let parent_repo_absolute_path = env::current_dir()?.join(parent_repo_path);

    for &d in ["extensions",
               "objects",
               "refs/heads",
               "refs/mirrors",
               "refs/remotes",
               "state",
               "tmp/cache"].iter() {
        fs::create_dir_all(repo_path.join(d))?;
    }

    let mut file = File::create(repo_path.join("config"))?;
    file.write_all(format!(r#"
[core]
repo_version=1
mode=archive-z2
{}parent={}"#,
                           match opt_collection_id {
                               Some(collection_id) => format!("collection-id={}.Build{}\n", collection_id, build_id),
                               _ => "".to_string(),
                           },
                           parent_repo_absolute_path.display()).as_bytes())?;
    Ok(())
}

fn commit_build(state: &AppState, build_id: i32, base_url: &String, args: &CommitArgs) -> io::Result<()> {
    let build_refs = state
        .db
        .send(LookupBuildRefs {
            id: build_id
        })
        .wait()
    // wait error
        .or_else(|_e| Err(io::Error::new(io::ErrorKind::Other, "Can't load build refs")))?
    // send error
        .or_else(|_e| Err(io::Error::new(io::ErrorKind::Other, "Can't load build refs")))?;

    let build_repo_path = state.build_repo_base_path.join(build_id.to_string());
    let upload_path = build_repo_path.join("upload");

    init_repo (&build_repo_path, &state.repo_path, build_id, &state.collection_id)?;
    init_repo (&upload_path, &state.repo_path, build_id, &None)?;

    let mut src_repo_arg = OsString::from("--src-repo=");
    src_repo_arg.push(&upload_path);

    for build_ref in build_refs.iter() {
        let mut src_ref_arg = String::from("--src-ref=");
        src_ref_arg.push_str(&build_ref.commit);

        let mut cmd = Command::new("flatpak");
        cmd
            .arg("build-commit-from")
            .arg("--timestamp=NOW")     // All builds have the same timestamp, not when the individual builds finished
            .arg("--no-update-summary") // We update it once at the end
            .arg("--untrusted")         // Verify that the uploaded objects are correct
            .arg("--disable-fsync");    // There is a sync in flatpak build-update-repo, so avoid it here

        if let Some(gpg_homedir) = &state.gpg_homedir {
            cmd
                .arg(format!("--gpg-homedir=={}", gpg_homedir));
        };

        if let Some(key) = &state.build_gpg_key {
            cmd
                .arg(format!("--gpg-sign=={}", key));
        };

        if let Some(endoflife) = &args.endoflife {
            cmd
                .arg("--force")         // Even if the content is the same, the metadata is not, so ensure its updated
                .arg(format!("--end-of-life={}", endoflife));
        };

        let output = cmd
            .arg(&src_repo_arg)
            .arg(&src_ref_arg)
            .arg(&build_repo_path)
            .arg(&build_ref.ref_name)
            .output()?;
        if !output.status.success() {
            return Err(io::Error::new(io::ErrorKind::Other, format!("Failed to build commit for ref {}: {}", &build_ref.ref_name, String::from_utf8_lossy(&output.stderr))));
        }

        if build_ref.ref_name.starts_with("app/") {
            let parts: Vec<&str> = build_ref.ref_name.split('/').collect();
            let mut file = File::create(build_repo_path.join(format!("{}.flatpakref", parts[1])))?;
            // TODO: We should also add GPGKey here if state.build_gpg_key is set
            file.write_all(format!(r#"
[Flatpak Ref]
Name={}
Branch={}
Url={}/build-repo/{}
RuntimeRepo=https://dl.flathub.org/repo/flathub.flatpakrepo
IsRuntime=false
"#,
                                   parts[1],
                                   parts[3],
                                   base_url,
                                   build_id).as_bytes())?;
        }
    }

    let output = Command::new("flatpak")
        .arg("build-update-repo")
        .arg(&build_repo_path)
        .output()?;
    if !output.status.success() {
        return Err(io::Error::new(io::ErrorKind::Other, format!("Failed to update repo: {}", String::from_utf8_lossy(&output.stderr))));
    }

    fs::remove_dir_all(&upload_path)?;

    state.db
        .send(ChangeRepoState {
            id: build_id,
            new: RepoState::Ready,
            expected: Some(RepoState::Verifying),
        })
        .wait()
    // wait error
        .or_else(|_e| Err(io::Error::new(io::ErrorKind::Other, "Can't change repo state")))?
    // send error
        .or_else(|_e| Err(io::Error::new(io::ErrorKind::Other, "Can't change repo state")))?;

    Ok(())
}

fn commit_thread(state: AppState, build_id: i32, base_url: String, args: CommitArgs) {
    if let Err(e) = commit_build(&state, build_id, &base_url, &args) {
        println!("commit failed");
        let res =
            &state.db
            .send(ChangeRepoState {
                id: build_id,
                new: RepoState::Failed(e.to_string()),
                expected: None,
            })
            .wait();
        if res.is_err() {
            println!("Failed to mark operation failed");
        }
    }
}

#[derive(Deserialize)]
pub struct CommitArgs {
    endoflife: Option<String>,
}

fn request_base_url(req: &HttpRequest<AppState>) -> String {
    let conn_info = req.connection_info();
    format!("{}://{}",
            conn_info.scheme(),
            conn_info.host())
}

use std::clone::Clone;
pub fn commit(
    (args, params, state, req): (Json<CommitArgs>, Path<BuildPathParams>, State<AppState>, HttpRequest<AppState>),
) -> FutureResponse<HttpResponse> {
    let s = (*state).clone();
    let id = params.id;
    let base_url = request_base_url(&req);
    state
        .db
        .send(ChangeRepoState {
            id: params.id,
            new: RepoState::Verifying,
            expected: Some(RepoState::Uploading),
        })
        .from_err()
        .and_then(move |res| match res {
            Ok(build) => {
                thread::spawn(move || commit_thread (s, id, base_url, args.into_inner()) );
                Ok(HttpResponse::Ok().json(build))
            },
            Err(e) => Ok(e.error_response())
        })
        .responder()
}
