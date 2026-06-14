//! Runtime-independent context carrier for Déjà causal attribution.
//!
//! This crate intentionally does not depend on Tokio. Runtime integrations can call
//! these functions from task hooks to capture, enter, and adopt business/request
//! context without creating dependency cycles with framework-specific integration
//! crates.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

/// A captured causal context.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContextSnapshot {
    correlation_id: Option<String>,
}

impl ContextSnapshot {
    /// Create an empty context snapshot.
    pub fn empty() -> Self {
        Self {
            correlation_id: None,
        }
    }

    /// Create a context snapshot containing the provided correlation ID.
    pub fn new(correlation_id: impl Into<String>) -> Self {
        Self {
            correlation_id: Some(correlation_id.into()),
        }
    }

    /// Return the correlation ID, if present.
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    /// Return true when no correlation ID is present.
    pub fn is_empty(&self) -> bool {
        self.correlation_id.is_none()
    }
}

thread_local! {
    /// Thread-visible context read by syscall/preload hooks.
    static CURRENT_CONTEXT: RefCell<Option<String>> = const { RefCell::new(None) };

    /// Tokio task currently being polled on this OS thread, if Tokio has called
    /// the runtime task-hook entry point.
    static CURRENT_TASK_ID: RefCell<Option<String>> = const { RefCell::new(None) };

    /// Stack used to restore previous thread-visible context around nested poll
    /// hook calls.
    static POLL_STACK: RefCell<Vec<PollFrame>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Debug)]
struct PollFrame {
    previous_task_id: Option<String>,
    previous_context: Option<String>,
}

static TASK_CONTEXTS: OnceLock<Mutex<HashMap<String, ContextSnapshot>>> = OnceLock::new();

fn task_contexts() -> &'static Mutex<HashMap<String, ContextSnapshot>> {
    TASK_CONTEXTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn set_current_context(snapshot: &ContextSnapshot) {
    CURRENT_CONTEXT.with(|cell| {
        *cell.borrow_mut() = snapshot.correlation_id.clone();
    });
}

fn clear_current_context() {
    CURRENT_CONTEXT.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Return the current thread-visible correlation ID.
pub fn current_correlation_id() -> Option<String> {
    CURRENT_CONTEXT.with(|cell| cell.borrow().clone())
}

/// Capture the current thread-visible context.
pub fn capture_current() -> ContextSnapshot {
    ContextSnapshot {
        correlation_id: current_correlation_id(),
    }
}

/// Enter a context for the lifetime of the returned guard.
pub fn enter(snapshot: ContextSnapshot) -> ContextGuard {
    let previous = capture_current();
    set_current_context(&snapshot);
    ContextGuard { previous }
}

/// Enter a correlation ID for the lifetime of the returned guard.
pub fn enter_correlation_id(correlation_id: impl Into<String>) -> ContextGuard {
    enter(ContextSnapshot::new(correlation_id))
}

/// Guard that restores the previous thread-visible context on drop.
#[derive(Debug)]
pub struct ContextGuard {
    previous: ContextSnapshot,
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        set_current_context(&self.previous);
    }
}

pin_project! {
    /// Future wrapper that enters a context for each poll only.
    pub struct ContextScopeFuture<F> {
        context: ContextSnapshot,
        #[pin]
        inner: F,
    }
}

impl<F> ContextScopeFuture<F> {
    /// Create a new context-scoped future.
    pub fn new(context: ContextSnapshot, inner: F) -> Self {
        Self { context, inner }
    }
}

impl<F: Future> Future for ContextScopeFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = enter(this.context.clone());
        this.inner.poll(cx)
    }
}

/// Scope a future with a correlation ID for each poll.
pub fn scope<F>(correlation_id: impl Into<String>, inner: F) -> ContextScopeFuture<F> {
    ContextScopeFuture::new(ContextSnapshot::new(correlation_id), inner)
}

/// Scope a future with an existing snapshot for each poll.
pub fn scope_snapshot<F>(context: ContextSnapshot, inner: F) -> ContextScopeFuture<F> {
    ContextScopeFuture::new(context, inner)
}

/// Run synchronous code inside a context.
pub fn scope_sync<F, R>(context: ContextSnapshot, f: F) -> R
where
    F: FnOnce() -> R,
{
    let _guard = enter(context);
    f()
}

/// Tokio hook entry point: a task was spawned.
pub fn tokio_task_spawn(task_id: impl ToString) {
    let task_id = task_id.to_string();
    let context = capture_current();
    if let Ok(mut contexts) = task_contexts().lock() {
        if context.is_empty() {
            contexts.remove(&task_id);
        } else {
            contexts.insert(task_id, context);
        }
    }
}

/// Tokio hook entry point: a task is about to be polled.
pub fn tokio_task_poll_start(task_id: impl ToString) {
    let task_id = task_id.to_string();
    let previous_task_id = CURRENT_TASK_ID.with(|cell| {
        let previous = cell.borrow().clone();
        *cell.borrow_mut() = Some(task_id.clone());
        previous
    });

    let previous_context = current_correlation_id();

    POLL_STACK.with(|stack| {
        stack.borrow_mut().push(PollFrame {
            previous_task_id,
            previous_context,
        });
    });

    let context = task_contexts()
        .lock()
        .ok()
        .and_then(|contexts| contexts.get(&task_id).cloned())
        .unwrap_or_else(ContextSnapshot::empty);

    set_current_context(&context);
}

/// Tokio hook entry point: a task poll finished.
pub fn tokio_task_poll_stop(_task_id: impl ToString) {
    let frame = POLL_STACK.with(|stack| stack.borrow_mut().pop());

    if let Some(frame) = frame {
        CURRENT_TASK_ID.with(|cell| {
            *cell.borrow_mut() = frame.previous_task_id;
        });
        CURRENT_CONTEXT.with(|cell| {
            *cell.borrow_mut() = frame.previous_context;
        });
    } else {
        CURRENT_TASK_ID.with(|cell| {
            *cell.borrow_mut() = None;
        });
        clear_current_context();
    }
}

/// Tokio hook entry point: a task terminated.
pub fn tokio_task_terminate(task_id: impl ToString) {
    let task_id = task_id.to_string();
    if let Ok(mut contexts) = task_contexts().lock() {
        contexts.remove(&task_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_and_enter_restore_context() {
        assert_eq!(current_correlation_id(), None);
        {
            let _guard = enter_correlation_id("req-1");
            assert_eq!(current_correlation_id().as_deref(), Some("req-1"));
        }
        assert_eq!(current_correlation_id(), None);
    }
}
