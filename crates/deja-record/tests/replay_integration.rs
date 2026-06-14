#![allow(clippy::unwrap_used)] // tests panic on failure by design

//! Integration test: verify that `ReplayHook` intercepts calls and returns
//! recorded results instead of hitting the real implementation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use deja_record::{read_events, RecordingHook, ReplayHook};

// --- Define a trait ---

#[deja_derive::recordable]
#[async_trait::async_trait]
pub trait CounterService {
    async fn get_value(&self) -> Result<u64, String>;
    async fn increment(&self, delta: u64) -> Result<u64, String>;
    async fn tag(&self, name: String) -> Result<String, String>;
    async fn reset(&self) -> Result<(), String>;
}

// --- Real implementation that tracks invocations ---

#[derive(Clone)]
struct RealCounter {
    calls: Arc<AtomicUsize>,
}

impl RealCounter {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl CounterService for RealCounter {
    async fn get_value(&self) -> Result<u64, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(42)
    }

    async fn increment(&self, delta: u64) -> Result<u64, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(42 + delta)
    }

    async fn tag(&self, name: String) -> Result<String, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(format!("tag:{name}"))
    }

    async fn reset(&self) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// --- Déjà wrapper ---

struct DejaCounter {
    inner: Box<dyn CounterService + Send + Sync>,
    hook: Arc<dyn deja_record::DejaHook>,
}

delegate_counter_service_with_replay!(DejaCounter, inner, hook, "service");

// --- Tests ---

#[tokio::test]
async fn replay_returns_recorded_value_without_calling_real_impl() {
    // Phase 1: Record.
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let real = RealCounter::new();
    let store = DejaCounter {
        inner: Box::new(real.clone()),
        hook: record_hook.clone(),
    };
    let v1 = store.get_value().await.unwrap();
    assert_eq!(v1, 42);
    drop(store);
    drop(record_hook);

    let events = read_events(record_dir.path()).expect("read");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].result["Ok"], 42);

    // Phase 2: Replay — real impl is behind the wrapper but never called.
    let replay_real = RealCounter::new();
    let replay_hook = Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay"));
    let replay_store = DejaCounter {
        inner: Box::new(replay_real.clone()),
        hook: replay_hook.clone(),
    };

    let v2 = replay_store.get_value().await.unwrap();
    assert_eq!(v2, 42); // recorded, not from real
    assert_eq!(
        replay_real.call_count(),
        0,
        "real impl should not be called"
    );
}

#[tokio::test]
async fn replay_sliding_window_recovers_skipped_calls() {
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let real = RealCounter::new();
    let store = DejaCounter {
        inner: Box::new(real.clone()),
        hook: record_hook.clone(),
    };
    let _ = store.increment(1).await.unwrap();
    let _ = store.increment(2).await.unwrap();
    let _ = store.get_value().await.unwrap();
    drop(store);
    drop(record_hook);

    // Replay but only call get_value — sliding window should skip the increments.
    let replay_real = RealCounter::new();
    let replay_hook = Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay"));
    let replay_store = DejaCounter {
        inner: Box::new(replay_real.clone()),
        hook: replay_hook.clone(),
    };

    let v = replay_store.get_value().await.unwrap();
    assert_eq!(v, 42);
    assert_eq!(replay_real.call_count(), 0);

    let report = replay_hook.take_report();
    assert!(report.has_divergences());
    assert_eq!(
        report.divergences[0].kind,
        deja_record::DivergenceKind::OmittedCall
    );
}

#[tokio::test]
async fn replay_logs_novel_call_and_falls_through() {
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let real = RealCounter::new();
    let store = DejaCounter {
        inner: Box::new(real.clone()),
        hook: record_hook.clone(),
    };
    let _ = store.get_value().await.unwrap();
    drop(store);
    drop(record_hook);

    // Replay but call reset (novel — not in recording).
    let replay_real = RealCounter::new();
    let replay_hook = Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay"));
    let replay_store = DejaCounter {
        inner: Box::new(replay_real.clone()),
        hook: replay_hook.clone(),
    };

    let r = replay_store.reset().await;
    assert!(r.is_ok());
    assert_eq!(
        replay_real.call_count(),
        1,
        "real impl should be called for novel calls"
    );

    let report = replay_hook.take_report();
    assert_eq!(report.divergences.len(), 1);
    assert_eq!(
        report.divergences[0].kind,
        deja_record::DivergenceKind::NovelCall
    );
}

#[tokio::test]
async fn replay_arg_mismatch_returns_recorded_result_anyway() {
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let real = RealCounter::new();
    let store = DejaCounter {
        inner: Box::new(real.clone()),
        hook: record_hook.clone(),
    };
    let v = store.increment(5).await.unwrap();
    assert_eq!(v, 47); // 42 + 5
    drop(store);
    drop(record_hook);

    // Replay with different args.
    let replay_real = RealCounter::new();
    let replay_hook = Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay"));
    let replay_store = DejaCounter {
        inner: Box::new(replay_real.clone()),
        hook: replay_hook.clone(),
    };

    // Arg mismatch but skip_arg_mismatch = true → return recorded result.
    let v = replay_store.increment(99).await.unwrap();
    assert_eq!(v, 47); // recorded result for increment(5)
    assert_eq!(replay_real.call_count(), 0);

    let report = replay_hook.take_report();
    assert_eq!(report.divergences.len(), 1);
    assert_eq!(
        report.divergences[0].kind,
        deja_record::DivergenceKind::FieldMismatch
    );
}

#[tokio::test]
async fn replay_with_owned_arg_does_not_move_before_fallthrough() {
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let real = RealCounter::new();
    let store = DejaCounter {
        inner: Box::new(real.clone()),
        hook: record_hook.clone(),
    };
    let v = store.tag("alpha".to_string()).await.unwrap();
    assert_eq!(v, "tag:alpha");
    drop(store);
    drop(record_hook);

    let replay_real = RealCounter::new();
    let replay_hook = Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay"));
    let replay_store = DejaCounter {
        inner: Box::new(replay_real.clone()),
        hook: replay_hook,
    };

    let v = replay_store.tag("alpha".to_string()).await.unwrap();
    assert_eq!(v, "tag:alpha");
    assert_eq!(replay_real.call_count(), 0);
}
