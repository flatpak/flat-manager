extern crate flatmanager;
extern crate actix;
extern crate actix_web;
extern crate dotenv;
extern crate env_logger;
extern crate futures;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_json;
extern crate num_cpus;
extern crate mpart_async;
extern crate tokio;
extern crate futures_fs;

use actix::*;
use actix_web::http::header;
use actix_web::http;
use actix_web::client;
use actix_web::ws::{Client, ClientWriter, Message, ProtocolError};
use dotenv::dotenv;
use std::path::{Path,PathBuf};
use std::env;
use std::fs;
use std::io;
use std::io::Write;
use std::time::{Instant, Duration};
use futures::Future;
use mpart_async::MultipartRequest;
use futures::Stream;
use futures::future;
use futures_fs::FsPool;

use flatmanager::{RemoteClientMessage,RemoteServerMessage};
use flatmanager::ostree;
use flatmanager::errors::DeltaGenerationError;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const SERVER_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(10);

fn init_ostree_repo(repo_path: &PathBuf) -> io::Result<()> {
    for &d in ["extensions",
               "objects",
               "refs/heads",
               "refs/mirrors",
               "refs/remotes",
               "state",
               "tmp/cache"].iter() {
        fs::create_dir_all(repo_path.join(d))?;
    }

    let mut file = fs::File::create(repo_path.join("config"))?;
    file.write_all(format!(
r#"[core]
repo_version=1
mode=bare-user
# We use one single upstream remote which we pass the url to manually each tim
[remote "upstream"]
url=
gpg-verify=false
gpg-verify-summary=false
"#).as_bytes())?;
    Ok(())
}

struct Manager {
    fs_pool: FsPool,
    url: String,
    token: String,
    client: Option<Addr<DeltaClient>>,
    capacity: u32,
    repo: PathBuf,
}

impl Manager {
    fn retry(&mut self, ctx: &mut Context<Self>) {
        info!("Retrying connect in {} seconds", CONNECTION_RETRY_DELAY.as_secs());
        ctx.run_later(CONNECTION_RETRY_DELAY, move |manager, ctx| {
            manager.connect(ctx);
        });
    }

    fn connect(&mut self, ctx: &mut Context<Self>) {
        info!("Connecting to server at {}...", self.url);
        ctx.spawn(
            Client::new(&format!("{}/api/v1/delta/worker", self.url))
                .header(header::AUTHORIZATION, format!("Bearer {}", self.token))
                .connect()
                .into_actor(self)
                .map_err(|e, manager, ctx| {
                    error!("Error connecting: {}", e);
                    manager.retry(ctx);
                    ()
                })
                .map(|(reader, writer), manager, ctx| {
                    let addr = ctx.address();
                    let capacity = manager.capacity;
                    let repo = manager.repo.clone();
                    let url = manager.url.clone();
                    let token = manager.token.clone();
                    let fs_pool = manager.fs_pool.clone();
                    manager.client = Some(DeltaClient::create(move |ctx| {
                        DeltaClient::add_stream(reader, ctx);
                        DeltaClient {
                            fs_pool: fs_pool,
                            writer: writer,
                            manager: addr,
                            url: url,
                            token: token,
                            capacity: capacity,
                            repo: repo,
                            last_recieved_pong: Instant::now(),
                        }
                    }));
                    ()
                }));
    }
}

impl Actor for Manager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.connect(ctx);
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        System::current().stop();
    }
}

#[derive(Message)]
struct ClientClosed;

impl Handler<ClientClosed> for Manager {
    type Result = ();

    fn handle(&mut self, _msg: ClientClosed, ctx: &mut Context<Self>) {
        self.client = None;
        self.retry(ctx);
    }
}

struct DeltaClient {
    fs_pool: FsPool,
    writer: ClientWriter,
    manager: Addr<Manager>,
    url: String,
    token: String,
    capacity: u32,
    repo: PathBuf,
    last_recieved_pong: Instant,
}

impl Actor for DeltaClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Connected");
        // Kick off heartbeat process
        self.run_heartbeat(ctx);
        self.register();
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        info!("Disconnected");
    }
}

fn pull_and_generate_delta_async(repo_path: &PathBuf,
                                 url: &String,
                                 delta: &ostree::Delta) -> Box<Future<Item=(), Error=DeltaGenerationError>> {
    let url = url.clone();
    let repo_path2 = repo_path.clone();
    let delta_clone = delta.clone();
    Box::new(
        /* We do 5 retries, because pull is sometimes not super stable */
        ostree::pull_delta_async(5, &repo_path, &url, &delta_clone)
            .and_then(move |_| ostree::generate_delta_async(&repo_path2, &delta_clone))
            .from_err()
            )
}

fn add_delta_parts(fs_pool: &FsPool,
                   repo_path: &PathBuf,
                   delta: &ostree::Delta,
                   mpart: &mut MultipartRequest<futures_fs::FsReadStream>) -> Result<(),DeltaGenerationError>
{
    let delta_path = delta.delta_path(repo_path)?;

    for entry in fs::read_dir(delta_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let filename = path.file_name().unwrap();
            let deltafilename = format!("{}.{}.delta", delta.to_name().unwrap(), filename.to_string_lossy());
            mpart.add_stream("content", &deltafilename,"application/octet-stream",
                             fs_pool.read(path.clone(), Default::default()));
        }
    }
    Ok(())
}


pub fn upload_delta(fs_pool: &FsPool,
                    base_url: &String,
                    token: &String,
                    repo: &String,
                    repo_path: &PathBuf,
                    delta: &ostree::Delta) -> Box<Future<Item=(), Error=DeltaGenerationError>> {
    let url = format!("{}/api/v1/delta/upload/{}", base_url, repo);
    let token = token.clone();

    let mut mpart = MultipartRequest::default();

    info!("Uploading delta {}", delta.to_name().unwrap_or("??".to_string()));
    if let Err(e) = add_delta_parts(fs_pool, repo_path, delta, &mut mpart) {
        return Box::new(future::err(e))
    }

    Box::new(
        client::ClientRequest::build()
            .header(header::CONTENT_TYPE, format!("multipart/form-data; boundary={}", mpart.get_boundary()))
            .header(header::AUTHORIZATION, format!("Bearer {}", token))
            .uri(&url)
            .method(http::Method::POST)
            .body(actix_web::Body::Streaming(Box::new(mpart.from_err())))
            .unwrap()
            .send()
            .then(|r| {
                match r {
                    Ok(response) => {
                        if response.status().is_success() {
                            Ok(())
                        } else {
                            error!("Unexpected upload response: {:?}", response);
                            Err(DeltaGenerationError::new(&format!("Delta upload failed with error {}", response.status())))
                        }
                    },
                    Err(e) => {
                        error!("Unexpected upload error: {:?}", e);
                        Err(DeltaGenerationError::new(&format!("Delta upload failed with error {}", e)))
                    },
                }
            }))
}

impl DeltaClient {
    fn run_heartbeat(&self, ctx: &mut Context<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |client, ctx| {
            client.writer.ping("");
            if Instant::now().duration_since(client.last_recieved_pong) > SERVER_TIMEOUT {
                warn!("Server heartbeat missing, disconnecting!");
                client.close();
                ctx.stop()
            }
        });
    }

    fn register(&mut self) {
        self.writer.text(json!(
            RemoteClientMessage::Register {
                capacity: self.capacity,
            }
        ).to_string());
    }

    fn finished(&mut self, id: u32, errmsg: Option<String>) {
        info!("Sending finished for request {}", id);
        self.writer.text(json!(
            RemoteClientMessage::Finished {
                id: id,
                errmsg: errmsg.clone(),
            }
        ).to_string());
    }

    fn msg_request_delta(&mut self,
                         id: u32,
                         url: String,
                         repo: String,
                         delta: ostree::Delta,
                         ctx: &mut Context<Self>)
    {
        info!("Got delta request {}: {} {}", id, repo, delta.to_string());
        let path = self.repo.clone();
        let path2 = self.repo.clone();
        let delta2 = delta.clone();
        let base_url = self.url.clone();
        let token = self.token.clone();
        let reponame = repo.clone();
        let fs_pool = self.fs_pool.clone();
        ctx.spawn(
            pull_and_generate_delta_async(&path, &url, &delta)
                .and_then(move |_| upload_delta(&fs_pool, &base_url, &token, &reponame, &path2, &delta2))
                .into_actor(self)
                .then(move |r, client, _ctx| {
                    client.finished(id, r.err().map(|e| e.to_string()));
                    actix::fut::ok(())
                }));

        // TODO: GC delta-repo now and then
    }

    fn close(&mut self) {
        self.manager.do_send(ClientClosed);
    }

    fn message(&mut self, message: RemoteServerMessage, ctx: &mut Context<Self>) {
        match message {
            RemoteServerMessage::RequestDelta { id, url, repo, delta } => self.msg_request_delta(id, url, repo, delta, ctx),
        }
    }
}

impl StreamHandler<Message, ProtocolError> for DeltaClient {
    fn handle(&mut self, msg: Message, ctx: &mut Context<Self>) {
        match msg {
            Message::Text(text) => {
                match serde_json::from_str::<RemoteServerMessage>(&text) {
                    Ok(message) => self.message(message, ctx),
                    Err(e) => error!("Got invalid websocket message: {}", e),
                }
            },
            Message::Pong(_) => {
                self.last_recieved_pong = Instant::now();
            },
            _ => (),
        }
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        // websocket got closed
        self.close();
        ctx.stop()
    }
}


fn main() {
    env::set_var("RUST_LOG", "info");
    let _ = env_logger::init();
    let sys = actix::System::new("delta-generator-client");

    let cwd = PathBuf::from(std::env::current_dir().expect("Can't get cwd"));

    dotenv().ok();

    let token = env::var("REPO_TOKEN").expect("No token, set REPO_TOKEN in env or .env");
    let url = env::var("MANAGER_URL").unwrap_or ("http://127.0.0.1:8080".to_string());
    let capacity: u32 = env::var("CAPACITY")
        .map(|s| s.parse().expect("Failed to parse $CAPACITY"))
        .unwrap_or (num_cpus::get() as u32);
    let workdir = env::var("WORKDIR")
        .map(|s| cwd.join(Path::new(&s)))
        .unwrap_or_else(|_e| cwd.clone());

    let repodir = workdir.join("delta-repo");

    if !repodir.exists() {
        init_ostree_repo(&repodir).expect("Failed to create repo");
    }

    let _addr = Manager {
        fs_pool: FsPool::default(),
        url: url,
        token: token,
        client: None,
        capacity: capacity,
        repo: repodir,
    }.start();

    let r = sys.run();
    info!("System run returned {:?}", r);
}
