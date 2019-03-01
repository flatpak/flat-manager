use actix::prelude::*;
use actix::Actor;
use app::Config;
use errors::{JobError};
use futures::{task, Future, Async, Poll};
use ostree;
use std::process::Command;
use std::os::unix::process::CommandExt;
use std::collections::{VecDeque};
use std::cell::{Cell,RefCell};
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug,Clone,PartialEq)]
pub struct DeltaRequest {
    pub repo: String,
    pub delta: ostree::Delta,
}

impl Message for DeltaRequest {
    type Result = Result<(), JobError>;
}

trait WorkerWrapper : std::fmt::Debug {
    fn forward_request(&self, request: DeltaRequest) -> Box<Future<Item=Result<(), JobError>, Error=MailboxError>>;
}

#[derive(Debug)]
struct QueuedRequest {
    request: DeltaRequest,
    result: RefCell<Option<Result<(),JobError>>>,
    blocking_tasks: RefCell<Vec<task::Task>>,
}

impl QueuedRequest {
    fn new(request: &DeltaRequest) -> Rc<Self> {
        Rc::new(QueuedRequest {
            request: request.clone(),
            result: RefCell::new(None),
            blocking_tasks: RefCell::new(Vec::new()),
        })
    }
    fn wake_me_later(&self, t: task::Task) {
        self.blocking_tasks.borrow_mut().push(t);
    }
}

#[derive(Debug)]
struct WorkerInfo {
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
    outstanding: VecDeque<Rc<QueuedRequest>>,
    workers: Vec<Rc<WorkerInfo>>,
}

impl Actor for DeltaGenerator {
    type Context = Context<Self>;
}

impl DeltaGenerator {
    fn add_worker(&mut self, prio: u32, available: u32, worker: Box<WorkerWrapper>) {
        self.workers.push(Rc::new(WorkerInfo {
            prio: prio,
            available: Cell::new(available),
            wrapper: worker,
        }));
    }

    fn start_request(&mut self, worker: Rc<WorkerInfo>, queued_request: Rc<QueuedRequest>, ctx: &mut Context<Self>) {
        worker.available.set(worker.available.get() - 1);
        ctx.spawn(
            worker.wrapper.forward_request(queued_request.request.clone())
                .into_actor(self)
                .then(move |msg_send_res, generator, ctx| {
                    let res = match msg_send_res {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(job_err)) => Err(job_err),
                        Err(send_err) => Err(JobError::new(&format!("Failed to send request to worker: {}", send_err))),
                    };

                    // TODO: On failure, should we try to re-queue on another worker instead of reporting back?
                    queued_request.result.replace(Some(res));
                    for task in queued_request.blocking_tasks.borrow_mut().iter() {
                        task.notify();
                    }
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
    type Result = ResponseActFuture<Self, (), JobError>;

    fn handle(&mut self, msg: DeltaRequest, ctx: &mut Self::Context) -> Self::Result {
        let request =
            match self.outstanding.iter().find(|req| req.request == msg) {
                Some(existing_request) => existing_request.clone(),
                None => QueuedRequest::new(&msg),
            };
        self.outstanding.push_back(request.clone());
        self.run_queue(ctx);
        Box::new(DeltaRequestFut::new(request))
    }
}

#[derive(Debug)]
struct DeltaRequestFut {
    queued_request: Rc<QueuedRequest>,
}

impl DeltaRequestFut {
    pub fn new(queued_request: Rc<QueuedRequest>) -> Self {
        Self {
            queued_request: queued_request,
        }
    }
}

impl ActorFuture for DeltaRequestFut {
    type Item = ();
    type Error = JobError;
    type Actor = DeltaGenerator;

    fn poll(
        &mut self, _: &mut DeltaGenerator, _: &mut Context<DeltaGenerator>,
    ) -> Poll<Self::Item, Self::Error> {
        if let Some(ref res) = *self.queued_request.result.borrow() {
            match res {
                Err(e) => Err(e.clone()),
                Ok(r) => Ok(Async::Ready(r.clone())),
            }
        } else {
            self.queued_request.wake_me_later(task::current().clone());
            Ok(Async::NotReady)
        }
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
    fn forward_request(&self, request: DeltaRequest) -> Box<Future<Item=Result<(), JobError>, Error=MailboxError>> {
        Box::new(self
                 .0
                 .send(request))
    }
}

impl Handler<DeltaRequest> for LocalWorker {
    type Result = Result<(), JobError>;

    fn handle(&mut self, msg: DeltaRequest, _ctx: &mut Self::Context) -> Self::Result {
        let repoconfig = self.config.get_repoconfig(&msg.repo)
            .map_err(|_e| JobError::new(&format!("No repo named: {}", &msg.repo)))?;
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
            .map_err(|e| JobError::new(&format!("Failed to generate delta: {}", e)))?;

        if !o.status.success() {
            return Err(JobError::new("delta generation exited unsuccesfully"));
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
    };

    generator.add_worker(0, n_threads, Box::new(LocalWorkerWrapper(threaded_worker)));
    generator.start()
}
