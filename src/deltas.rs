use actix::prelude::*;
use actix::Actor;
use app::Config;
use errors::{DeltaGenerationError};
use futures::Future;
use ostree;
use std::process::Command;
use std::os::unix::process::CommandExt;
use std::collections::{VecDeque,HashMap};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Instant, Duration};
use actix_web::ws;
use serde_json;

use app::AppState;
use delayed::DelayedResult;

#[derive(Debug,Clone,PartialEq)]
pub struct DeltaRequest {
    pub repo: String,
    pub delta: ostree::Delta,
}

impl DeltaRequest {
    fn to_string(&self) -> String {
        format!("{}/{}", self.repo, self.delta.to_string())
    }
}


impl Message for DeltaRequest {
    type Result = Result<(), DeltaGenerationError>;
}

trait WorkerWrapper : std::fmt::Debug {
    fn forward_request(&self, request: DeltaRequest) -> Box<Future<Item=Result<(), DeltaGenerationError>, Error=MailboxError>>;
}

#[derive(Debug)]
struct QueuedRequest {
    request: DeltaRequest,
    delayed_result: DelayedResult<(),DeltaGenerationError>,
}

impl QueuedRequest {
    fn new(request: &DeltaRequest) -> Self {
        let delayed = DelayedResult::new();
        QueuedRequest {
            request: request.clone(),
            delayed_result: delayed.clone(),
        }
    }
}

#[derive(Debug)]
struct WorkerInfo {
    name: String,
    id: usize,
    prio: u32,
    available: Cell<u32>,
    wrapper: Box<WorkerWrapper>,
}

/* The DeltaGenerator is an actor handling the DeltaRequest message, but
 * it then fronts a number of workers that it queues the request onto.
 */

#[derive(Debug)]
pub struct DeltaGenerator {
    config: Arc<Config>,
    outstanding: VecDeque<QueuedRequest>,
    workers: Vec<Rc<WorkerInfo>>,
    next_worker_id: usize,
}

impl Actor for DeltaGenerator {
    type Context = Context<Self>;
}

impl DeltaGenerator {
    fn add_worker(&mut self, name: &str, prio: u32, available: u32, worker: Box<WorkerWrapper>) -> usize {
        let id = self.next_worker_id;
        self.next_worker_id += 1;
        self.workers.push(Rc::new(WorkerInfo {
            name: name.to_string(),
            id: id,
            prio: prio,
            available: Cell::new(available),
            wrapper: worker,
        }));
        info!("New delta worker {} registred as #{} ", name, id);
        id
    }

    fn remove_worker(&mut self, id: usize) {
        if let Some(index) = self.workers.iter().position(|w| w.id == id) {
            let w = self.workers.remove(index);
            info!("Delta worker {} #{} unregistred", w.name, w.id);
        } else {
            error!("Trying to remove worker #{} which doesn't exist", id);
        }
    }

    fn start_request(&mut self, worker: Rc<WorkerInfo>, mut queued_request: QueuedRequest, ctx: &mut Context<Self>) {
        worker.available.set(worker.available.get() - 1);
        ctx.spawn(
            worker.wrapper.forward_request(queued_request.request.clone())
                .into_actor(self)
                .then(move |msg_send_res, generator, ctx| {
                    match msg_send_res {
                        Ok(Ok(_)) => {
                            queued_request.delayed_result.set(Ok(()));
                        }
                        Ok(Err(job_err)) => {
                            queued_request.delayed_result.set(Err(job_err));
                        }
                        Err(send_err) => {
                            // This typically happens when a worker has disconnected, such errors
                            // are transient and we retry with a new worker.
                            warn!("Failed to send delta worker request to worker: {}. Probably disconnected, retrying.", send_err);
                            generator.outstanding.push_front(queued_request);
                        },
                    };

                    worker.available.set(worker.available.get() + 1);
                    generator.run_queue(ctx);
                    actix::fut::ok(())
                }));
    }

    fn run_queue(&mut self, ctx: &mut Context<Self>) {
        /* Use workers with the highest prio available */
        let max_prio = self.workers.iter().fold(0, |acc, w| u32::max(acc, w.prio));

        while let Some(request) = self.outstanding.pop_front() {
            let worker_maybe = self.workers.iter().find(|w| w.prio == max_prio && w.available.get() > 0).map(|w| w.clone());
            if let Some(worker) = worker_maybe {
                info!("Assigned delta {} to worker {} #{}", request.request.to_string(), worker.name, worker.id);
                self.start_request(worker.clone(), request, ctx);
            } else {
                /* No worker available, return to queue */
                self.outstanding.push_front(request);
                break;
            }
        }
    }
}

impl Handler<DeltaRequest> for DeltaGenerator {
    type Result = ResponseActFuture<Self, (), DeltaGenerationError>;

    fn handle(&mut self, msg: DeltaRequest, ctx: &mut Self::Context) -> Self::Result {
        let r =
            self.outstanding.iter()
            .find(|req| req.request == msg)
            .map(|req| req.delayed_result.clone())
            .unwrap_or_else( || {
                let req = QueuedRequest::new(&msg);
                let r = req.delayed_result.clone();
                self.outstanding.push_back(req);

                /* Maybe a worker can handle it directly? */
                self.run_queue(ctx);
                r
            });

        Box::new(r.into_actor(self))
    }
}

#[derive(Debug)]
pub struct StopDeltaGenerator();

impl Message for StopDeltaGenerator {
    type Result = ();
}

impl Handler<StopDeltaGenerator> for DeltaGenerator {
    type Result = ();

    fn handle(&mut self, _msg: StopDeltaGenerator, ctx: &mut Self::Context) {
        ctx.stop();
    }
}


#[derive(Debug)]
pub struct RegisterRemoteWorker {
    name: String,
    addr: Addr<RemoteWorker>,
    capacity: u32,
}

impl Message for RegisterRemoteWorker {
    type Result = usize;
}

impl Handler<RegisterRemoteWorker> for DeltaGenerator {
    type Result = usize;

    fn handle(&mut self, msg: RegisterRemoteWorker, _ctx: &mut Self::Context) -> usize {
        self.add_worker( &msg.name, 1, msg.capacity, Box::new(RemoteWorkerWrapper(msg.addr)))
    }
}


#[derive(Debug)]
pub struct UnregisterRemoteWorker {
    id: usize,
}

impl Message for UnregisterRemoteWorker {
    type Result = ();
}

impl Handler<UnregisterRemoteWorker> for DeltaGenerator {
    type Result = ();

    fn handle(&mut self, msg: UnregisterRemoteWorker, _ctx: &mut Self::Context) {
        self.remove_worker(msg.id);
    }
}


#[derive(Debug)]
pub struct LocalWorker {
    pub config: Arc<Config>,
}

impl Actor for LocalWorker {
    type Context = SyncContext<Self>;
}

#[derive(Debug)]
pub struct LocalWorkerWrapper(Addr<LocalWorker>);

impl WorkerWrapper for LocalWorkerWrapper {
    fn forward_request(&self, request: DeltaRequest) -> Box<Future<Item=Result<(), DeltaGenerationError>, Error=MailboxError>> {
        Box::new(self
                 .0
                 .send(request))
    }
}

impl Handler<DeltaRequest> for LocalWorker {
    type Result = Result<(), DeltaGenerationError>;

    fn handle(&mut self, msg: DeltaRequest, _ctx: &mut Self::Context) -> Self::Result {
        let repoconfig = self.config.get_repoconfig(&msg.repo)
            .map_err(|_e| DeltaGenerationError::new(&format!("No repo named: {}", &msg.repo)))?;
        let repo_path = repoconfig.get_abs_repo_path();
        let delta = msg.delta;

        let mut cmd = Command::new("flatpak");
        cmd
            .before_exec (|| {
                // Setsid in the child to avoid SIGINT on server killing
                // child and breaking the graceful shutdown
                unsafe { libc::setsid() };
                Ok(())
            });

        cmd
            .arg("build-update-repo")
            .arg("--generate-static-delta-to")
            .arg(delta.to.clone());

        if let Some(ref from) = delta.from {
            cmd
                .arg("--generate-static-delta-from")
                .arg(from.clone());
        };

        cmd
            .arg(&repo_path);

        let o = cmd.output()
            .map_err(move |e| DeltaGenerationError::new(&format!("build-update-repo failed: {}", e)))?;

        if !o.status.success() {
            return Err(DeltaGenerationError::new("delta generation exited unsuccesfully"));
        }
        Ok(())
    }
}

pub fn start_delta_generator(config: Arc<Config>) -> Addr<DeltaGenerator> {

    let n_threads = config.local_delta_threads;
    let config_copy = config.clone();
    let threaded_worker = SyncArbiter::start(n_threads as usize, move || LocalWorker {
        config: config_copy.clone(),
    });


    let mut generator = DeltaGenerator {
        config: config,
        //fallback: fallback_addr,
        outstanding: VecDeque::new(),
        workers: Vec::new(),
        next_worker_id: 0,
    };

    generator.add_worker("local", 0, n_threads, Box::new(LocalWorkerWrapper(threaded_worker)));
    generator.start()
}

const CLIENT_TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub struct RemoteWorkerItem {
    id: u32,
    request: DeltaRequest,
    delayed_result: DelayedResult<(),DeltaGenerationError>,
}


#[derive(Debug)]
pub struct RemoteWorker {
    remote: Option<String>,
    id: Option<usize>,
    unregistered: bool,
    last_item_id: u32,
    outstanding: HashMap<u32, RemoteWorkerItem>,
    config: Arc<Config>,
    heartbeat: Instant,
    generator: Addr<DeltaGenerator>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RemoteClientMessage {
    Register { capacity: u32 },
    Unregister,
    Finished { id: u32,
               errmsg: Option<String> },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RemoteServerMessage {
    RequestDelta {
        id: u32,
        url: String,
        repo: String,
        delta: ostree::Delta,
    },
}

#[derive(Debug)]
pub struct RemoteWorkerWrapper(Addr<RemoteWorker>);

impl WorkerWrapper for RemoteWorkerWrapper {
    fn forward_request(&self, request: DeltaRequest) -> Box<Future<Item=Result<(), DeltaGenerationError>, Error=MailboxError>> {
        Box::new(self
                 .0
                 .send(request))
    }
}

impl RemoteWorker {
    pub fn new(config: &Arc<Config>, generator: &Addr<DeltaGenerator>) -> Self {
        RemoteWorker {
            remote: None,
            id: None,
            unregistered: false,
            last_item_id: 0,
            outstanding: HashMap::new(),
            config: config.clone(),
            heartbeat: Instant::now(),
            generator: generator.clone(),
        }
    }

    fn allocate_item_id(&mut self) -> u32 {
        self.last_item_id +=1;
        self.last_item_id
    }

    fn new_item(&mut self, msg: &DeltaRequest) -> RemoteWorkerItem {
        RemoteWorkerItem {
            id: self.allocate_item_id(),
            request: msg.clone(),
            delayed_result: DelayedResult::new(),
        }
    }

    fn msg_register(&mut self, capacity: u32, ctx: &mut ws::WebsocketContext<Self, AppState>) {
        let addr = ctx.address();
        ctx.spawn(
            self.generator
                .send(RegisterRemoteWorker {
                    name: self.remote.clone().unwrap_or("Unknown".to_string()),
                    addr, capacity,
                })
                .into_actor(self)
                .then(move |msg_send_res, worker, ctx| {
                    if let Ok(id) = msg_send_res {
                        worker.id = Some(id);

                        /* We might have already unregistered before
                         * we got the register response, do it now */
                        if worker.unregistered {
                            ctx.spawn(
                                worker.generator
                                    .send(UnregisterRemoteWorker {
                                        id,
                                    })
                                    .into_actor(worker)
                                    .then(|_msg_send_res, _worker, _ctx| actix::fut::ok(()) ));
                        }
                    } else {
                        error!("Unable to register Remote Worker {:?}", msg_send_res);
                    }
                    actix::fut::ok(())
                }));
    }

    fn msg_unregister(&mut self, ctx: &mut ws::WebsocketContext<Self, AppState>) {
        /* This stops assigning jobs for the worker, but keeps
         * outstanding jobs running */

        self.unregistered = true;
        if let Some(id) = self.id {
            ctx.spawn(
                self.generator
                    .send(UnregisterRemoteWorker {
                        id,
                    })
                    .into_actor(self)
                    .then(move |_msg_send_res, worker, _ctx| {
                        worker.id = None;
                        actix::fut::ok(())
                    }));
        }
    }

    fn msg_finished(&mut self, id: u32, errmsg: Option<String>, _ctx: &mut ws::WebsocketContext<Self, AppState>) {
        match self.outstanding.remove(&id) {
            Some(mut item) => {
                item.delayed_result.set(match errmsg {
                    None => Ok(()),
                    Some(msg) => Err(DeltaGenerationError::new(&format!("Remote worked id {} failed to generate delta: {}", id, &msg))),
                })
            },
            None => error!("Got finished message for unexpected handle {}", id),
        }
    }

    fn message(&mut self, message: RemoteClientMessage, ctx: &mut ws::WebsocketContext<Self, AppState>) {
        match message {
            RemoteClientMessage::Register { capacity } => self.msg_register(capacity, ctx),
            RemoteClientMessage::Unregister => self.msg_unregister(ctx),
            RemoteClientMessage::Finished { id, errmsg } => self.msg_finished(id, errmsg, ctx),
        }
    }

    fn heartbeat(&self, ctx: &mut ws::WebsocketContext<Self, AppState>) {
        ctx.run_interval(CLIENT_TIMEOUT_CHECK_INTERVAL, |worker, ctx| {
            if Instant::now().duration_since(worker.heartbeat) > CLIENT_TIMEOUT {
                warn!("Delta worker heartbeat missing, disconnecting!");
                ctx.stop();
                return;
            }
        });
    }
}

impl Handler<DeltaRequest> for RemoteWorker {
    type Result = ResponseActFuture<Self, (), DeltaGenerationError>;

    fn handle(&mut self, msg: DeltaRequest, ctx: &mut Self::Context) -> Self::Result {
        let url = {
            let repoconfig =
                match self.config.get_repoconfig(&msg.repo) {
                    Ok(c) => c,
                    Err(e) => return Box::new(
                        DelayedResult::err(DeltaGenerationError::new(&format!("Can't get repoconfig: {}", e)))
                            .into_actor(self)),
                };
            repoconfig.get_base_url(&self.config)
        };

        let item = self.new_item(&msg);

        ctx.text(json!(RemoteServerMessage::RequestDelta {
            url: url,
            id: item.id,
            repo: msg.repo,
            delta: msg.delta,
        }).to_string());

        let fut = item.delayed_result.clone();
        self.outstanding.insert(item.id, item);
        Box::new(fut.into_actor(self))
    }
}

impl Actor for RemoteWorker {
    type Context = ws::WebsocketContext<Self, AppState>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Kick off heartbeat process
        self.remote = ctx.request().connection_info().remote().map(|s| s.to_string());
        info!("Remote delta worker from {} connected", self.remote.clone().unwrap_or("Unknown".to_string()));
        self.heartbeat(ctx);
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        if let Some(id) = self.id {
            /* We send this with Arbiter::spawn, not cxt.spawn() as this context is shutting down */
            Arbiter::spawn(
                self.generator
                    .send(UnregisterRemoteWorker {
                        id,
                    })
                    .then(|_msg_send_res| Ok(())));
        }

        Running::Stop
    }
}

impl StreamHandler<ws::Message, ws::ProtocolError> for RemoteWorker {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        match msg {
            ws::Message::Ping(msg) => {
                self.heartbeat = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.heartbeat = Instant::now();
            }
            ws::Message::Text(text) => {
                match serde_json::from_str::<RemoteClientMessage>(&text) {
                    Ok(message) => self.message(message, ctx),
                    Err(e) => error!("Got invalid websocket message: {}", e),
                }
            }
            ws::Message::Binary(_bin) => error!("Unexpected binary ws message"),
            ws::Message::Close(_) => {
                ctx.stop();
            },
        }
    }
}
