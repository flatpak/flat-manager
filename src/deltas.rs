use actix::prelude::*;
use actix::Actor;
use actix::dev::ToEnvelope;
use actix_web::web::Data;
use app::Config;
use errors::{DeltaGenerationError};
use futures::Future;
use futures::future;
use ostree;
use std::collections::{VecDeque,HashMap};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Instant, Duration};
use actix_web_actors::ws;
use serde_json;
use rand;
use rand::prelude::IteratorRandom;
use std::sync::mpsc;

use delayed::DelayedResult;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

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

#[derive(Debug,Clone)]
pub struct DeltaRequestSync {
    pub delta_request: DeltaRequest,
    pub tx: mpsc::Sender<(ostree::Delta, Result<(), DeltaGenerationError>)>,
}

impl Message for DeltaRequestSync {
    type Result = Result<(), ()>;
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
struct WorkerInfo<A: actix::Actor> {
    name: String,
    id: usize,
    available: Cell<u32>,
    addr: Addr<A>,
}

impl<A: Actor> WorkerInfo<A> {
    pub fn is_available(&self) -> bool {
        self.available.get() > 0
    }
    pub fn claim(&self) {
        let current = self.available.get();
        assert!(current > 0);
        self.available.set(current - 1);
    }

    pub fn unclaim(&self) {
        self.available.set(self.available.get() + 1);
    }
}


/* The DeltaGenerator is an actor handling the DeltaRequest message, but
 * it then fronts a number of workers that it queues the request onto.
 */

#[derive(Debug)]
pub struct DeltaGenerator {
    config: Arc<Config>,
    outstanding: VecDeque<QueuedRequest>,
    local_worker: Rc<WorkerInfo<LocalWorker>>,
    remote_workers: Vec<Rc<WorkerInfo<RemoteWorker>>>,
    next_worker_id: usize,
}

impl Actor for DeltaGenerator {
    type Context = Context<Self>;
}

impl DeltaGenerator {
    fn add_worker(&mut self, name: &str, available: u32, addr: Addr<RemoteWorker>) -> usize {
        let id = self.next_worker_id;
        self.next_worker_id += 1;
        self.remote_workers.push(Rc::new(WorkerInfo {
            name: name.to_string(),
            id: id,
            available: Cell::new(available),
            addr: addr,
        }));
        info!("New delta worker {} registred as #{} ", name, id);
        id
    }

    fn remove_worker(&mut self, id: usize) {
        if let Some(index) = self.remote_workers.iter().position(|w| w.id == id) {
            let w = self.remote_workers.remove(index);
            info!("Delta worker {} #{} unregistred", w.name, w.id);
        } else {
            error!("Trying to remove worker #{} which doesn't exist", id);
        }
    }

    fn start_request<A>(&self, worker: Rc<WorkerInfo<A>>, mut queued_request: QueuedRequest, ctx: &mut Context<Self>)
        where A: Handler<DeltaRequest>,
              A::Context: ToEnvelope<A, DeltaRequest>  {
        info!("Assigned delta {} to worker {} #{}", queued_request.request.to_string(), worker.name, worker.id);
        worker.claim();
        ctx.spawn(
            worker.addr
                .send(queued_request.request.clone())
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

                    worker.unclaim();
                    generator.run_queue(ctx);
                    actix::fut::ok(())
                })
        );
    }

    fn run_queue(&mut self, ctx: &mut Context<Self>) {
        while let Some(request) = self.outstanding.pop_front() {
            if self.remote_workers.is_empty() {
                /* No remotes, fallback to local worker */
                if self.local_worker.is_available() {
                    self.start_request(self.local_worker.clone(), request, ctx);
                } else {
                    /* No worker available, return to queue */
                    self.outstanding.push_front(request);
                    break;
                }
            } else {
                /* Find available worker */
                if let Some(available_worker) = self.remote_workers.iter().filter(|w| w.is_available()).choose(&mut rand::thread_rng()) {
                    self.start_request(available_worker.clone(), request, ctx);
                } else {
                    /* No worker available, return to queue */
                    self.outstanding.push_front(request);
                    break;
                }
            }
        }
    }

    fn handle_request(&mut self, request: DeltaRequest, ctx: &mut Context<Self>) -> DelayedResult<(),DeltaGenerationError> {
        self.outstanding.iter()
            .find(|req| req.request == request)
            .map(|req| req.delayed_result.clone())
            .unwrap_or_else( || {
                let req = QueuedRequest::new(&request);
                let r = req.delayed_result.clone();
                self.outstanding.push_back(req);

                /* Maybe a worker can handle it directly? */
                self.run_queue(ctx);
                r
            })
    }
}

impl Handler<DeltaRequest> for DeltaGenerator {
    type Result = ResponseActFuture<Self, (), DeltaGenerationError>;

    fn handle(&mut self, msg: DeltaRequest, ctx: &mut Self::Context) -> Self::Result {
        Box::new(self.handle_request(msg, ctx).into_actor(self))
    }
}

impl Handler<DeltaRequestSync> for DeltaGenerator {
    type Result = ResponseActFuture<Self, (), ()>;

    fn handle(&mut self, msg: DeltaRequestSync, ctx: &mut Self::Context) -> Self::Result {
        let request = msg.delta_request.clone();
        let delta = request.delta.clone();
        let tx = msg.tx.clone();
        let r = self.handle_request(request, ctx);
        ctx.spawn(Box::new(r
                           .then(move |r| {
                               if let Err(_e) = tx.send((delta, r.clone())) {
                                   error!("Failed to reply to sync delta request");
                               }
                               r
                           })
                           .map_err(|_e| ())
                           .into_actor(self)
        ));
        Box::new(actix::fut::ok(()))
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
        self.add_worker( &msg.name, msg.capacity, msg.addr)
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
    type Context = Context<Self>;
}

impl Handler<DeltaRequest> for LocalWorker {
    type Result = ResponseActFuture<Self, (), DeltaGenerationError>;

    fn handle(&mut self, msg: DeltaRequest, _ctx: &mut Self::Context) -> Self::Result {
        let repoconfig = match self.config.get_repoconfig(&msg.repo) {
            Err(_e) => {
                return Box::new(
                    future::err(DeltaGenerationError::new(&format!("No repo named: {}", &msg.repo)))
                        .into_actor(self))
            },
            Ok(r) => r,
        };

        let repo_path = repoconfig.get_abs_repo_path();
        let delta = msg.delta;

        Box::new(
            ostree::generate_delta_async(&repo_path, &delta)
                .from_err()
                .into_actor(self))
    }
}

pub fn start_delta_generator(config: Arc<Config>) -> Addr<DeltaGenerator> {

    let n_threads = config.local_delta_threads;
    let config_copy = config.clone();
    let local_worker = LocalWorker {
        config: config_copy.clone(),
    }.start();

    let generator = DeltaGenerator {
        config: config,
        outstanding: VecDeque::new(),
        local_worker: Rc::new(WorkerInfo {
            name: "local".to_string(),
            id: 0,
            available: Cell::new(n_threads),
            addr: local_worker,
        }),
        remote_workers: Vec::new(),
        next_worker_id: 1,
    };

    generator.start()
}

#[derive(Debug)]
pub struct RemoteWorkerItem {
    id: u32,
    request: DeltaRequest,
    delayed_result: DelayedResult<(),DeltaGenerationError>,
}


#[derive(Debug)]
pub struct RemoteWorker {
    remote: String,
    id: Option<usize>,
    unregistered: bool,
    last_item_id: u32,
    outstanding: HashMap<u32, RemoteWorkerItem>,
    config: Data<Config>,
    last_recieved_ping: Instant,
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

impl RemoteWorker {
    pub fn new(config: &Data<Config>, generator: &Addr<DeltaGenerator>, remote: String) -> Self {
        RemoteWorker {
            remote: remote,
            id: None,
            unregistered: false,
            last_item_id: 0,
            outstanding: HashMap::new(),
            config: config.clone(),
            last_recieved_ping: Instant::now(),
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

    fn msg_register(&mut self, capacity: u32, ctx: &mut ws::WebsocketContext<Self>) {
        let addr = ctx.address();
        ctx.spawn(
            self.generator
                .send(RegisterRemoteWorker {
                    name: self.remote.clone(),
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

    fn msg_unregister(&mut self, ctx: &mut ws::WebsocketContext<Self>) {
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

    fn msg_finished(&mut self, id: u32, errmsg: Option<String>, _ctx: &mut ws::WebsocketContext<Self>) {
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

    fn message(&mut self, message: RemoteClientMessage, ctx: &mut ws::WebsocketContext<Self>) {
        match message {
            RemoteClientMessage::Register { capacity } => self.msg_register(capacity, ctx),
            RemoteClientMessage::Unregister => self.msg_unregister(ctx),
            RemoteClientMessage::Finished { id, errmsg } => self.msg_finished(id, errmsg, ctx),
        }
    }

    fn run_heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |worker, ctx| {
            if Instant::now().duration_since(worker.last_recieved_ping) > CLIENT_TIMEOUT {
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
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Kick off heartbeat process
        info!("Remote delta worker from {} connected", self.remote);
        self.run_heartbeat(ctx);
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
                info!("Got ping");
                self.last_recieved_ping = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
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
            ws::Message::Nop => {
            },
        }
    }
}
