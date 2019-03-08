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

use actix::*;
use actix_web::http::header;
use actix_web::ws::{Client, ClientWriter, Message, ProtocolError};
use dotenv::dotenv;
use std::path::{Path,PathBuf};
use std::env;
use std::time::Duration;
use flatmanager::{RemoteClientMessage,RemoteServerMessage};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(10);

struct Manager {
    url: String,
    token: String,
    client: Option<Addr<DeltaClient>>,
    capacity: u32,
}

impl Manager {
    fn retry(&mut self, ctx: &mut Context<Self>) {
        info!("Retrying connect in {} seconds", CONNECTION_RETRY_DELAY.as_secs());
        ctx.run_later(CONNECTION_RETRY_DELAY, move |manager, ctx| {
            manager.connect(ctx);
        });
    }

    fn connect(&mut self, ctx: &mut Context<Self>) {
        println!("Connecting to server at {}...", self.url);
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
                    manager.client = Some(DeltaClient::create(move |ctx| {
                        DeltaClient::add_stream(reader, ctx);
                        DeltaClient {
                            writer: writer,
                            manager: addr,
                            capacity: capacity,
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
    writer: ClientWriter,
    manager: Addr<Manager>,
    capacity: u32,
}

impl Actor for DeltaClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Connected");
        // Kick off heartbeat process
        self.heartbeat(ctx);
        self.register();
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        info!("Disconnected");
    }
}

impl DeltaClient {
    fn heartbeat(&self, ctx: &mut Context<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |client, _ctx| {
            client.writer.ping("");
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
        self.writer.text(json!(
            RemoteClientMessage::Finished {
                id: id,
                errmsg: errmsg.clone(),
            }
        ).to_string());
    }

    fn message(&mut self, message: RemoteServerMessage, ctx: &mut Context<Self>) {
        info!("Got client message: {:?}", message);
        match message {
            RemoteServerMessage::RequestDelta { id, url, repo, delta } => {
                // TODO: download, etc
                info!("PROCESSING REQUEST...");
                ctx.run_later(Duration::new(30, 0), move |client, ctx| {
                    info!("DONE...");
                    client.finished(id, Some("This needs to be implemented".to_string()));
                });
            }
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
            _ => (),
        }
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        self.manager.do_send(ClientClosed);
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
    let _workdir = env::var("WORKDIR")
        .map(|s| cwd.join(Path::new(&s)))
        .unwrap_or_else(|_e| cwd.clone());

    let _addr = Manager {
        url: url,
        token: token,
        client: None,
        capacity: capacity,
    }.start();

    let _ = sys.run();
}
