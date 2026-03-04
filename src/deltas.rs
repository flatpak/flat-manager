use crate::config::Config;
use crate::delayed::DelayedResult;
use crate::errors::DeltaGenerationError;
use crate::ostree;
use actix::dev::ToEnvelope;
use actix::prelude::*;
use actix::Actor;
use log::{error, info, warn};
use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::mpsc;
use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeltaRequest {
    pub repo: String,
    pub delta: ostree::Delta,
}

impl std::fmt::Display for DeltaRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}/{}", self.repo, self.delta)
    }
}

impl Message for DeltaRequest {
    type Result = Result<(), DeltaGenerationError>;
}

#[derive(Debug, Clone)]
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
    delayed_result: DelayedResult<(), DeltaGenerationError>,
}

impl QueuedRequest {
    fn new(request: &DeltaRequest) -> Self {
        let delayed = DelayedResult::new();
        QueuedRequest {
            request: request.clone(),
            delayed_result: delayed,
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
    outstanding: VecDeque<QueuedRequest>,
    local_worker: Rc<WorkerInfo<LocalWorker>>,
}

impl Actor for DeltaGenerator {
    type Context = Context<Self>;
}

impl DeltaGenerator {
    fn start_request<A>(
        &self,
        worker: Rc<WorkerInfo<A>>,
        mut queued_request: QueuedRequest,
        ctx: &mut Context<Self>,
    ) where
        A: Handler<DeltaRequest>,
        A::Context: ToEnvelope<A, DeltaRequest>,
    {
        info!(
            "Assigned delta {} to worker {} #{}",
            queued_request.request, worker.name, worker.id
        );
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
                    actix::fut::ready(())
                })
        );
    }

    fn run_queue(&mut self, ctx: &mut Context<Self>) {
        while let Some(request) = self.outstanding.pop_front() {
            if self.local_worker.is_available() {
                self.start_request(self.local_worker.clone(), request, ctx);
            } else {
                self.outstanding.push_front(request);
                break;
            }
        }
    }

    fn handle_request(
        &mut self,
        request: DeltaRequest,
        ctx: &mut Context<Self>,
    ) -> DelayedResult<(), DeltaGenerationError> {
        self.outstanding
            .iter()
            .find(|req| req.request == request)
            .map(|req| req.delayed_result.clone())
            .unwrap_or_else(|| {
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
    type Result = ResponseActFuture<Self, Result<(), DeltaGenerationError>>;

    fn handle(&mut self, msg: DeltaRequest, ctx: &mut Self::Context) -> Self::Result {
        Box::pin(self.handle_request(msg, ctx).into_actor(self))
    }
}

impl Handler<DeltaRequestSync> for DeltaGenerator {
    type Result = ResponseActFuture<Self, Result<(), ()>>;

    fn handle(&mut self, msg: DeltaRequestSync, ctx: &mut Self::Context) -> Self::Result {
        let request = msg.delta_request.clone();
        let delta = request.delta.clone();
        let tx = msg.tx;
        let r = self.handle_request(request, ctx);
        ctx.spawn(Box::pin(
            async move {
                let result = r.await;
                if tx.send((delta, result)).is_err() {
                    error!("Failed to reply to sync delta request");
                }
            }
            .into_actor(self),
        ));
        Box::pin(actix::fut::ready(Ok(())))
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
pub struct LocalWorker {
    pub config: Arc<Config>,
}

impl Actor for LocalWorker {
    type Context = Context<Self>;
}

impl Handler<DeltaRequest> for LocalWorker {
    type Result = ResponseActFuture<Self, Result<(), DeltaGenerationError>>;

    fn handle(&mut self, msg: DeltaRequest, _ctx: &mut Self::Context) -> Self::Result {
        let repoconfig = match self.config.get_repoconfig(&msg.repo) {
            Err(_e) => {
                return Box::pin(
                    actix::fut::ready(Err(DeltaGenerationError::new(&format!(
                        "No repo named: {}",
                        &msg.repo
                    ))))
                    .into_actor(self),
                )
            }
            Ok(r) => r,
        };

        let repo_path = repoconfig.get_abs_repo_path();
        let delta = msg.delta;

        Box::pin(
            async move {
                ostree::generate_delta_async(&repo_path, &delta)
                    .await
                    .map_err(DeltaGenerationError::from)
            }
            .into_actor(self),
        )
    }
}

pub fn start_delta_generator(config: Arc<Config>) -> Addr<DeltaGenerator> {
    let n_threads = config.local_delta_threads;
    let local_worker = LocalWorker { config }.start();

    let generator = DeltaGenerator {
        outstanding: VecDeque::new(),
        local_worker: Rc::new(WorkerInfo {
            name: "local".to_string(),
            id: 0,
            available: Cell::new(n_threads),
            addr: local_worker,
        }),
    };

    generator.start()
}
