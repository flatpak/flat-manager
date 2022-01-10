extern crate actix;
extern crate actix_codec;
extern crate actix_http;
extern crate actix_web;
extern crate actix_web_actors;
extern crate awc;
extern crate dotenv;
extern crate env_logger;
extern crate flatmanager;
extern crate futures;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_json;
extern crate futures_fs;
extern crate futures_locks;
extern crate mpart_async;
extern crate num_cpus;
extern crate tokio;

use actix::io::{SinkWrite, WriteHandler};
use actix::*;
use actix_codec::Framed;
use actix_web::http;
use actix_web::http::header;
use awc::{
    error::WsProtocolError,
    ws::{Codec, Frame, Message},
    Client,
};
use dotenv::dotenv;
use futures::stream::SplitSink;
use futures::{Future, Stream};
use futures_fs::FsPool;
use mpart_async::MultipartRequest;
use std::env;
use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use flatmanager::errors::DeltaGenerationError;
use flatmanager::ostree;
use flatmanager::{RemoteClientMessage, RemoteServerMessage};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const SERVER_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(10);

const UPLOAD_BUFFER_CAPACITY_BYTES: usize = 512 * 1024;
// Delta uploads can be slow, so don't timeout unnecessary
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(10 * 60);

// Prune once a day
const PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);

fn init_ostree_repo(repo_path: &PathBuf) -> io::Result<()> {
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

    let mut file = fs::File::create(repo_path.join("config"))?;
    file.write_all(
        format!(
            r#"[core]
repo_version=1
mode=bare-user
# We use one single upstream remote which we pass the url to manually each tim
[remote "upstream"]
url=
gpg-verify=false
gpg-verify-summary=false
"#
        )
        .as_bytes(),
    )?;
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
        info!(
            "Retrying connect in {} seconds",
            CONNECTION_RETRY_DELAY.as_secs()
        );
        ctx.run_later(CONNECTION_RETRY_DELAY, move |manager, ctx| {
            manager.connect(ctx);
        });
    }

    fn connect(&mut self, ctx: &mut Context<Self>) {
        info!("Connecting to server at {}...", self.url);
        ctx.spawn(
            Client::new()
                .ws(&format!("{}/api/v1/delta/worker", self.url))
                .header(header::AUTHORIZATION, format!("Bearer {}", self.token))
                .connect()
                .into_actor(self)
                .map_err(|e, manager, ctx| {
                    error!("Error connecting: {}", e);
                    manager.retry(ctx);
                    ()
                })
                .map(|(_response, framed), manager, ctx| {
                    info!("Connected");
                    let addr = ctx.address();
                    let capacity = manager.capacity;
                    let repo = manager.repo.clone();
                    let url = manager.url.clone();
                    let token = manager.token.clone();
                    let fs_pool = manager.fs_pool.clone();
                    let (sink, stream) = framed.split();
                    manager.client = Some(DeltaClient::create(move |ctx| {
                        DeltaClient::add_stream(stream, ctx);
                        DeltaClient {
                            fs_pool: fs_pool,
                            writer: SinkWrite::new(sink, ctx),
                            manager: addr,
                            url: url,
                            token: token,
                            capacity: capacity,
                            repo: repo,
                            repo_lock: futures_locks::RwLock::new(1),
                            last_recieved_pong: Instant::now(),
                        }
                    }))
                }),
        );
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
    writer: SinkWrite<SplitSink<Framed<awc::BoxedSocket, Codec>>>,
    manager: Addr<Manager>,
    url: String,
    token: String,
    capacity: u32,
    repo: PathBuf,
    repo_lock: futures_locks::RwLock<i32>,
    last_recieved_pong: Instant,
}

impl Actor for DeltaClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Connected");
        // Kick off heartbeat process
        self.run_heartbeat(ctx);
        self.run_regular_prune(ctx);
        self.register();
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        info!("Disconnected");
    }
}

fn pull_and_generate_delta_async(
    repo_path: &PathBuf,
    url: &String,
    delta: &ostree::Delta,
) -> Box<dyn Future<Item = (), Error = DeltaGenerationError>> {
    let url = url.clone();
    let repo_path2 = repo_path.clone();
    let delta_clone = delta.clone();
    Box::new(
        // We do 5 retries, because pull is sometimes not super stable
        ostree::pull_delta_async(5, &repo_path, &url, &delta_clone)
            .and_then(move |_| ostree::generate_delta_async(&repo_path2, &delta_clone))
            .from_err(),
    )
}

fn add_delta_parts(
    fs_pool: &FsPool,
    repo_path: &PathBuf,
    delta: &ostree::Delta,
    mpart: &mut MultipartRequest<futures_fs::FsReadStream>,
) -> Result<(), DeltaGenerationError> {
    let delta_path = delta.delta_path(repo_path)?;

    for entry in fs::read_dir(delta_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let filename = path.file_name().unwrap();
            let deltafilename = format!(
                "{}.{}.delta",
                delta.to_name().unwrap(),
                filename.to_string_lossy()
            );
            mpart.add_stream(
                "content",
                &deltafilename,
                "application/octet-stream",
                fs_pool.read(
                    path.clone(),
                    futures_fs::ReadOptions::default().buffer_size(UPLOAD_BUFFER_CAPACITY_BYTES),
                ),
            )
        }
    }
    Ok(())
}

pub fn upload_delta(
    fs_pool: &FsPool,
    base_url: &String,
    token: &String,
    repo: &String,
    repo_path: &PathBuf,
    delta: &ostree::Delta,
) -> impl Future<Item = (), Error = DeltaGenerationError> {
    let url = format!("{}/api/v1/delta/upload/{}", base_url, repo);
    let token = token.clone();

    let mut mpart = MultipartRequest::default();

    info!(
        "Uploading delta {}",
        delta.to_name().unwrap_or("??".to_string())
    );
    futures::done(add_delta_parts(fs_pool, repo_path, delta, &mut mpart)).and_then(move |_| {
        Client::new()
            .post(&url)
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={}", mpart.get_boundary()),
            )
            .header(header::AUTHORIZATION, format!("Bearer {}", token))
            .timeout(UPLOAD_TIMEOUT)
            .method(http::Method::POST)
            .send_body(actix_http::body::BodyStream::new(mpart))
            .then(|r| match r {
                Ok(response) => {
                    if response.status().is_success() {
                        Ok(())
                    } else {
                        error!("Unexpected upload response: {:?}", response);
                        Err(DeltaGenerationError::new(&format!(
                            "Delta upload failed with error {}",
                            response.status()
                        )))
                    }
                }
                Err(e) => {
                    error!("Unexpected upload error: {:?}", e);
                    Err(DeltaGenerationError::new(&format!(
                        "Delta upload failed with error {}",
                        e
                    )))
                }
            })
    })
}

impl DeltaClient {
    fn run_heartbeat(&self, ctx: &mut Context<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |client, ctx| {
            info!("Pinging server");
            if let Err(e) = client.writer.write(Message::Ping(String::new())) {
                warn!("Failed to ping server: {:?}, disconnecting", e);
                client.close();
                ctx.stop();
                return;
            }
            if Instant::now().duration_since(client.last_recieved_pong) > SERVER_TIMEOUT {
                warn!("Server heartbeat missing, disconnecting!");
                client.close();
                ctx.stop()
            }
        });
    }

    fn register(&mut self) {
        if let Err(e) = self.writer.write(Message::Text(
            json!(RemoteClientMessage::Register {
                capacity: self.capacity,
            })
            .to_string(),
        )) {
            warn!("Failed to register with server: {:?}", e);
        }
    }

    fn finished(&mut self, id: u32, errmsg: Option<String>) {
        info!("Sending finished for request {}", id);
        if let Err(e) = self.writer.write(Message::Text(
            json!(RemoteClientMessage::Finished {
                id: id,
                errmsg: errmsg.clone(),
            })
            .to_string(),
        )) {
            warn!("Failed to call finished: {:?}", e);
        }
    }

    fn prune_repo(&mut self, ctx: &mut Context<Self>) {
        info!("Waiting to prune repo");
        let path = self.repo.clone();
        ctx.spawn(
            // We take a write lock across the entire operation to
            // block out all delta request handling during the prune
            self.repo_lock
                .write()
                .into_actor(self)
                .then(move |guard, client, _ctx| {
                    info!("Pruning repo");
                    ostree::prune_async(&path)
                        .into_actor(client)
                        .then(move |r, _client, _ctx| {
                            match r {
                                Err(e) => error!("Failed to prune repo: {}", e.to_string()),
                                _ => info!("Pruning repo done"),
                            };
                            let _ = &guard;
                            actix::fut::ok(())
                        })
                }),
        );
    }

    fn run_regular_prune(&mut self, ctx: &mut Context<Self>) {
        self.prune_repo(ctx);
        ctx.run_interval(PRUNE_INTERVAL, |client, ctx| client.prune_repo(ctx));
    }

    fn msg_request_delta(
        &mut self,
        id: u32,
        url: String,
        repo: String,
        delta: ostree::Delta,
        ctx: &mut Context<Self>,
    ) {
        info!("Got delta request {}: {} {}", id, repo, delta.to_string());
        let path = self.repo.clone();
        let path2 = self.repo.clone();
        let delta2 = delta.clone();
        let base_url = self.url.clone();
        let token = self.token.clone();
        let reponame = repo.clone();
        let fs_pool = self.fs_pool.clone();
        ctx.spawn(
            // We take a read lock across the entire operation to
            // protect against the regular GC:ing the repo, which
            // takes a write lock
            self.repo_lock
                .read()
                .into_actor(self)
                .then(move |guard, client, _ctx| {
                    pull_and_generate_delta_async(&path, &url, &delta)
                        .and_then(move |_| {
                            upload_delta(&fs_pool, &base_url, &token, &reponame, &path2, &delta2)
                        })
                        .into_actor(client)
                        .then(move |r, client, _ctx| {
                            let _ = &guard;
                            client.finished(id, r.err().map(|e| e.to_string()));
                            actix::fut::ok(())
                        })
                }),
        );
    }

    fn close(&mut self) {
        self.manager.do_send(ClientClosed);
    }

    fn message(&mut self, message: RemoteServerMessage, ctx: &mut Context<Self>) {
        match message {
            RemoteServerMessage::RequestDelta {
                id,
                url,
                repo,
                delta,
            } => self.msg_request_delta(id, url, repo, delta, ctx),
        }
    }
}

impl StreamHandler<Frame, WsProtocolError> for DeltaClient {
    fn handle(&mut self, msg: Frame, ctx: &mut Context<Self>) {
        match msg {
            Frame::Text(Some(bytes)) => match std::str::from_utf8(&bytes) {
                Ok(text) => match serde_json::from_str::<RemoteServerMessage>(text) {
                    Ok(message) => self.message(message, ctx),
                    Err(e) => error!("Got invalid websocket message: {}", e),
                },
                Err(e) => error!("Got invalid websocket message: {}", e),
            },
            Frame::Pong(_) => {
                self.last_recieved_pong = Instant::now();
            }
            _ => (),
        }
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {}

    fn finished(&mut self, ctx: &mut Context<Self>) {
        // websocket got closed
        self.close();
        ctx.stop()
    }
}

impl WriteHandler<WsProtocolError> for DeltaClient {}

fn main() {
    env::set_var("RUST_LOG", "info");
    let _ = env_logger::init();
    let sys = actix::System::new("delta-generator-client");

    let cwd = PathBuf::from(std::env::current_dir().expect("Can't get cwd"));

    dotenv().ok();

    let token = env::var("REPO_TOKEN").expect("No token, set REPO_TOKEN in env or .env");
    let url = env::var("MANAGER_URL").unwrap_or("http://127.0.0.1:8080".to_string());
    let capacity: u32 = env::var("CAPACITY")
        .map(|s| s.parse().expect("Failed to parse $CAPACITY"))
        .unwrap_or(num_cpus::get() as u32);
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
    }
    .start();

    let r = sys.run();
    info!("System run returned {:?}", r);
}
