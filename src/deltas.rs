use crate::config::Config;
use crate::delayed::DelayedResult;
use crate::errors::DeltaGenerationError;
use crate::ostree;
use actix::prelude::*;
use actix::Actor;
use log::{error, info};
use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeltaRequest {
    pub repo: String,
    pub delta: ostree::Delta,
}

impl std::fmt::Display for DeltaRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.repo, self.delta)
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

/* The DeltaGenerator is an actor handling DeltaRequest and dispatching work
 * directly from its queue.
 */

#[derive(Debug)]
pub struct DeltaGenerator {
    outstanding: VecDeque<QueuedRequest>,
    config: Arc<Config>,
    available: u32,
}

impl Actor for DeltaGenerator {
    type Context = Context<Self>;
}

impl DeltaGenerator {
    fn start_request(&mut self, mut queued_request: QueuedRequest, ctx: &mut Context<Self>) {
        info!("Generating delta {}", queued_request.request);
        self.available -= 1;

        let repoconfig = match self.config.get_repoconfig(&queued_request.request.repo) {
            Ok(r) => r,
            Err(_) => {
                self.available += 1;
                queued_request
                    .delayed_result
                    .set(Err(DeltaGenerationError::new(&format!(
                        "No repo named: {}",
                        &queued_request.request.repo
                    ))));
                return;
            }
        };

        let repo_path = repoconfig.get_abs_repo_path();
        let delta = queued_request.request.delta.clone();

        ctx.spawn(
            async move {
                ostree::generate_delta_async(&repo_path, &delta)
                    .await
                    .map_err(DeltaGenerationError::from)
            }
            .into_actor(self)
            .then(move |result, generator, ctx| {
                queued_request.delayed_result.set(result);
                generator.available += 1;
                generator.run_queue(ctx);
                actix::fut::ready(())
            }),
        );
    }

    fn run_queue(&mut self, ctx: &mut Context<Self>) {
        while let Some(request) = self.outstanding.pop_front() {
            if self.available > 0 {
                self.start_request(request, ctx);
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

                /* Maybe a queued task can start immediately? */
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

pub fn start_delta_generator(config: Arc<Config>) -> Addr<DeltaGenerator> {
    let n_threads = config.local_delta_threads;

    DeltaGenerator {
        outstanding: VecDeque::new(),
        config,
        available: n_threads,
    }
    .start()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_request_display_has_no_trailing_newline() {
        let request = DeltaRequest {
            repo: "stable".to_string(),
            delta: ostree::Delta {
                from: None,
                to: "tohash".to_string(),
            },
        };

        assert_eq!(request.to_string(), "stable/nothing-tohash");
        assert!(!request.to_string().ends_with('\n'));
    }
}
