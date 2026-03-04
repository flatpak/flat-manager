use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
struct InnerDelayedResult<T, E> {
    next_clone_id: Cell<usize>,
    result: RefCell<Option<Result<T, E>>>,
    waiters: RefCell<HashMap<usize, Waker>>,
}

impl<T, E> InnerDelayedResult<T, E> {
    fn new() -> Rc<Self> {
        Rc::new(InnerDelayedResult {
            next_clone_id: Cell::new(0),
            result: RefCell::new(None),
            waiters: RefCell::new(HashMap::new()),
        })
    }
}

#[derive(Debug)]
pub struct DelayedResult<T, E> {
    inner: Rc<InnerDelayedResult<T, E>>,
    waiter: usize,
}

impl<T, E> Clone for DelayedResult<T, E> {
    fn clone(&self) -> Self {
        let next_id = self.inner.next_clone_id.get() + 1;
        self.inner.next_clone_id.replace(next_id);

        Self {
            inner: self.inner.clone(),
            waiter: next_id,
        }
    }
}

impl<T, E> Drop for DelayedResult<T, E> {
    fn drop(&mut self) {
        let mut waiters = self.inner.waiters.borrow_mut();
        waiters.remove(&self.waiter);
    }
}

impl<T, E> Future for DelayedResult<T, E>
where
    T: std::fmt::Debug + std::clone::Clone,
    E: std::fmt::Debug + std::clone::Clone,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let res_ref = this.inner.result.borrow().clone();
        if let Some(res) = res_ref {
            Poll::Ready(res)
        } else {
            this.inner
                .waiters
                .borrow_mut()
                .insert(this.waiter, cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T, E> DelayedResult<T, E>
where
    T: std::fmt::Debug,
    E: std::fmt::Debug,
{
    pub fn new() -> Self {
        let inner = InnerDelayedResult::new();
        DelayedResult {
            waiter: inner.next_clone_id.get(),
            inner,
        }
    }
    pub fn set(&mut self, res: Result<T, E>) {
        self.inner.result.replace(Some(res));

        let waiters = self.inner.waiters.replace(HashMap::new());

        for (_, waiter) in waiters {
            waiter.wake();
        }
    }
}
