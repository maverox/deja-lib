//! Semantic recording primitives for Déjà.
//!
//! Captures trait-level operations (DB, Redis, HTTP, gRPC) with full call-site
//! tracking, correlation IDs, atomic sequencing, and timestamps.
//!
//! Replay is fully wired: the orchestrator renders a `LookupTable` from a
//! recording, and the candidate runs a `LookupTableHook` (installed via
//! `RuntimeHook`/`DEJA_MODE=replay`) that substitutes recorded results
//! per-boundary and emits an `ObservedCall` per lookup for post-hoc
//! divergence scoring.
//!
//! # Architecture
//!
//! The recording layer sits at the trait-object DI boundary:
//!
//! ```text
//! Handler → DejaStore (decorator) → Real Store
//!              │
//!              └─ DejaHook::record(SemanticEvent)
//!                    │
//!                    └─ semantic-events.jsonl
//! ```
//!
//! Each event carries:
//! - `global_sequence`: monotonic atomic counter across all requests
//! - `request_sequence`: per-correlation-id ordering
//! - `call_file:call_line:call_column`: from `#[track_caller]`
//! - `correlation_id`: from `deja_context::current_correlation_id()`
//! - `timestamp_ns`: nanoseconds since UNIX epoch

use std::collections::HashMap;
use std::future::Future;
use std::panic::Location;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub mod correlation_layer;
pub mod graph;
pub mod replay;
pub mod writer;
pub use correlation_layer::{current_logical_span_path, DejaCorrelationLayer};
pub use deja_core::DEJA_GRAPH_DIR_ENV_VAR;
pub use graph::{
    current_execution_graph_context, execution_graph_path, read_execution_graph_records,
    ExecutionGraphLayer,
};
pub use replay::{
    ArgMismatchPolicy, Divergence, DivergenceKind, ReplayConfig, ReplayHook, ReplayReport,
};
pub use writer::{
    AsyncRecordWriter, CompositeSink, JsonlSink, MarkerKind, RecordSink, SinkPolicy, WriterConfig,
    WriterStatsSnapshot, DEJA_BATCH_SIZE_ENV_VAR, DEJA_FLUSH_INTERVAL_MS_ENV_VAR,
    DEJA_QUEUE_CAPACITY_ENV_VAR, DEJA_SINK_POLICY_ENV_VAR,
};

/// Optional stable identifier for one process/run inside an appended artifact.
pub const DEJA_RUN_ID_ENV_VAR: &str = "DEJA_RUN_ID";

pub(crate) fn current_recording_run_id() -> Option<String> {
    std::env::var(DEJA_RUN_ID_ENV_VAR)
        .ok()
        .filter(|value| !value.is_empty())
}

// ---------------------------------------------------------------------------
// Core event type
// ---------------------------------------------------------------------------

/// A single semantic operation captured at the trait boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEvent {
    /// Monotonically increasing counter across all requests (no gaps).
    pub global_sequence: u64,
    /// Per-request ordering (1st, 2nd, 3rd call within this correlation scope).
    pub request_sequence: u64,
    /// Correlation ID from `deja_context::current_correlation_id()`.
    pub correlation_id: Option<String>,
    /// Nanoseconds since UNIX epoch.
    pub timestamp_ns: u64,
    /// Process/run identity for append-only recordings that contain many router runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_run_id: Option<String>,
    /// Active execution graph node id, when the execution graph layer is installed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_node_id: Option<u64>,
    /// Active `tracing` span id. Useful for diagnosing missing graph-node joins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing_span_id: Option<u64>,
    /// Boundary layer: "storage", "redis", "http_client", "grpc".
    pub boundary: String,
    /// Trait name: "PaymentIntentInterface", "AddressInterface", etc.
    pub trait_name: String,
    /// Method name: "find_payment_intent_by_id", "insert_address", etc.
    pub method_name: String,
    /// Source file of the caller (from `#[track_caller]`).
    pub call_file: String,
    /// Source line of the caller.
    pub call_line: u32,
    /// Source column of the caller.
    pub call_column: u32,
    /// Receiver/decorator context captured before dispatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receiver: Option<serde_json::Value>,
    /// Request-like method input payload. Kept alongside `args` for readability.
    #[serde(default)]
    pub request: serde_json::Value,
    /// Serialized key arguments (JSON).
    pub args: serde_json::Value,
    /// Response-like method output payload. Kept alongside `result` for readability.
    #[serde(default)]
    pub response: serde_json::Value,
    /// Serialized result (JSON). For errors, contains `{"error": "..."}`.
    pub result: serde_json::Value,
    /// Whether the operation returned an error.
    pub is_error: bool,
    /// Wall-clock duration in microseconds.
    pub duration_us: u64,
    /// Wire-format schema version for this event. Bump when adding fields that
    /// require special handling by downstream readers.
    #[serde(default = "default_event_schema_version")]
    pub event_schema_version: u16,
    /// Optional structured call-site identity (syntactic hash, lexical path,
    /// operation occurrence, etc.) used for stable replay matching when source
    /// line/column information shifts.
    #[serde(default)]
    pub callsite_identity: Option<CallsiteIdentity>,
}

/// Default `event_schema_version` for back-compat with older records.
fn default_event_schema_version() -> u16 {
    1
}

// ---------------------------------------------------------------------------
// Call-site identity
// ---------------------------------------------------------------------------

/// Source kind for a `CallsiteIdentity`. Indicates how the identity was
/// derived (explicit annotation, syntactic hash, lexical path, operation
/// occurrence index, or legacy file/line/column).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CallsiteSource {
    /// User-supplied callsite identity (explicit annotation).
    Explicit,
    /// Hash derived from surrounding syntax tokens.
    SyntacticHash,
    /// Stable module path / item path.
    LexicalPath,
    /// Per-operation occurrence index within a correlation scope.
    OperationOccurrence,
    /// Legacy file:line:column captured by `#[track_caller]`.
    LegacyLocation,
}

/// Structured identity describing a call-site for stable replay matching.
///
/// Carries enough metadata to disambiguate distinct logical call sites even
/// when source file/line numbers shift across recordings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallsiteIdentity {
    /// Wire-format version for this `CallsiteIdentity`.
    pub version: u16,
    /// How this identity was derived.
    pub source: CallsiteSource,
    /// Stable identifier (e.g. hash digest or explicit tag).
    pub id: Option<String>,
    /// Logical scope (module path, function path, etc.).
    pub scope: Option<String>,
    /// Per-source occurrence index within `scope`.
    pub occurrence: u32,
    /// Enclosing function name when known.
    pub caller_function: Option<String>,
    /// Lexical path (e.g. `crate::module::function`) when known.
    pub lexical_path: Option<String>,
    /// Syntactic hash of surrounding tokens when known.
    ///
    /// Deserialized leniently (number OR string): a `u64` hash can exceed 2^53,
    /// and JSON transports that route through JS-based tooling (e.g. Vector in the
    /// Kafka→S3 recording path) serialize such values as STRINGS to preserve
    /// precision. Accepting both keeps the recording round-trippable regardless of
    /// which sink wrote it.
    #[serde(default, deserialize_with = "de_u64_opt_lenient")]
    pub syntax_hash: Option<u64>,
    /// Logical span-path (root→leaf `tracing` span names, joined by `>`) the call
    /// fired within — the SOURCE for the rank-2 `Address::LogicalContext`. Stable
    /// across benign V2 edits (line shifts, signature tweaks) that leave the span
    /// structure intact, and DISTINCT for concurrent same-callsite calls in
    /// different spans — which is what stops the positional `occurrence` from
    /// swapping under async interleaving. `None` when no span was entered (the call
    /// then degrades to weaker ranks — never worse than before this field existed).
    #[serde(default)]
    pub logical_context: Option<String>,
}

/// Deserialize an `Option<u64>` from either a JSON number or a JSON string,
/// tolerating transports that stringify large (>2^53) integers.
fn de_u64_opt_lenient<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    match Option::<serde_json::Value>::deserialize(deserializer)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => Ok(n.as_u64()),
        Some(serde_json::Value::String(s)) => {
            s.parse::<u64>().map(Some).map_err(serde::de::Error::custom)
        }
        Some(other) => Err(serde::de::Error::custom(format!(
            "syntax_hash: expected u64 number or string, got {other}"
        ))),
    }
}

/// Lookup query carrying replay context (boundary, args, optional callsite
/// identity, optional caller location).
///
/// Hooks that opt into context-aware replay implement
/// [`DejaHook::try_replay_with_context`].
pub struct ReplayLookup<'a> {
    /// Boundary tag (e.g. `"storage"`, `"redis"`, `"http_client"`).
    pub boundary: &'a str,
    /// Trait name at the boundary.
    pub trait_name: &'a str,
    /// Method name being invoked.
    pub method_name: &'a str,
    /// Serialized arguments to match against.
    pub args: &'a serde_json::Value,
    /// Optional structured callsite identity for stable matching.
    pub callsite_identity: Option<&'a CallsiteIdentity>,
    /// Optional caller location for legacy file:line:column matching.
    pub caller_location: Option<&'a std::panic::Location<'a>>,
}

// ---------------------------------------------------------------------------
// Call-site helper
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Hook trait
// ---------------------------------------------------------------------------

/// Trait for receiving semantic events from the decorator layer.
///
/// Implementations handle recording (write to file) or replay (match + return).
pub trait DejaHook: Send + Sync {
    /// Return true when the hook is active (recording or replay is enabled).
    ///
    /// When false, the generated delegation skips all recording overhead
    /// (no JSON serialization, no file writes, no sequencing).
    fn is_active(&self) -> bool {
        true
    }

    /// Attempt to replay a previously recorded result without calling the
    /// real implementation.
    ///
    /// Returns `Some(result_json)` if a matching recorded event is found.
    /// Returns `None` if no match — the delegation should fall through to
    /// the real implementation.
    ///
    /// Default returns `None`, making this opt-in for replay-enabled hooks.
    fn try_replay(
        &self,
        _boundary: &str,
        _trait_name: &str,
        _method_name: &str,
        _args: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        None
    }

    /// Record a completed semantic event.
    fn record(&self, event: SemanticEvent);

    /// Allocate the next global sequence number.
    fn next_global_sequence(&self) -> u64;

    /// Allocate the next per-request sequence number for the given correlation ID.
    fn next_request_sequence(&self, correlation_id: Option<&str>) -> u64;

    /// Attempt a context-aware replay lookup.
    ///
    /// Default delegation falls back to [`DejaHook::try_replay`] using the
    /// boundary/trait/method/args carried in `query`. Replay-capable hooks
    /// override this to consult the structured `callsite_identity` and
    /// `caller_location` for stable matching across source-line shifts.
    fn try_replay_with_context(&self, query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        self.try_replay(
            query.boundary,
            query.trait_name,
            query.method_name,
            query.args,
        )
    }

    /// Allocate the next per-callsite occurrence index within a correlation
    /// scope.
    ///
    /// Replay/recording hooks use this to disambiguate repeated calls at the
    /// same logical call-site. The default returns `0` for hooks that do not
    /// track occurrences.
    fn next_callsite_occurrence(
        &self,
        _correlation_id: Option<&str>,
        _source: CallsiteSource,
        _scope: Option<&str>,
    ) -> u32 {
        0
    }

    /// Optional stable identifier for the current recording run.
    ///
    /// Recording hooks return `Some(&str)` to attach a stable run id to every
    /// emitted event. Non-recording hooks return `None` (the default).
    fn recording_run_id(&self) -> Option<&str> {
        None
    }
}

// ---------------------------------------------------------------------------
// No-op hook — pass-through with zero overhead
// ---------------------------------------------------------------------------

/// A `DejaHook` that does nothing.
///
/// Used when Déjà recording is disabled. The delegation macro's fast-path
/// (`self.hook.is_active()`) avoids even entering the async block that
/// contains this hook, so `NoOpHook` is typically never instantiated at
/// runtime — but it is useful for type-system wiring and tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOpHook;

impl DejaHook for NoOpHook {
    fn is_active(&self) -> bool {
        false
    }

    fn record(&self, _event: SemanticEvent) {}

    fn next_global_sequence(&self) -> u64 {
        0
    }

    fn next_request_sequence(&self, _correlation_id: Option<&str>) -> u64 {
        0
    }

    fn try_replay_with_context(&self, _query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        None
    }

    fn next_callsite_occurrence(
        &self,
        _correlation_id: Option<&str>,
        _source: CallsiteSource,
        _scope: Option<&str>,
    ) -> u32 {
        0
    }

    fn recording_run_id(&self) -> Option<&str> {
        None
    }
}

// ---------------------------------------------------------------------------
// Timestamp helper
// ---------------------------------------------------------------------------

/// Current wall-clock time as nanoseconds since UNIX epoch.
#[inline]
pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64
}

// ---------------------------------------------------------------------------
// Recording implementation
// ---------------------------------------------------------------------------

/// Records semantic events to a JSONL file with atomic sequencing.
pub struct RecordingHook {
    writer: AsyncRecordWriter<SemanticEvent>,
    global_counter: AtomicU64,
    request_counters: Mutex<HashMap<String, u64>>,
    /// Counter for events with no correlation ID.
    uncorrelated_counter: AtomicU64,
    /// Stable identifier for this recording run, attached to every event.
    recording_run_id: String,
    /// Per-callsite occurrence counters keyed by
    /// `(correlation_id, source, scope)`.
    callsite_occurrence: Mutex<CallsiteOccurrenceMap>,
}

/// Per-callsite occurrence counters keyed by `(correlation_id, source, scope)`.
pub(crate) type CallsiteOccurrenceMap =
    HashMap<(Option<String>, CallsiteSource, Option<String>), u32>;

impl RecordingHook {
    /// Resolve `recording_run_id` from the environment, falling back to a
    /// time-based id when neither `DEJA_RECORDING_RUN_ID` nor
    /// `DEJA_RUN_ID_ENV_VAR` is set.
    fn resolve_recording_run_id() -> String {
        std::env::var("DEJA_RECORDING_RUN_ID")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| {
                std::env::var(DEJA_RUN_ID_ENV_VAR)
                    .ok()
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| format!("run-{}", now_ns()))
    }

    /// Create a new recording hook writing to the given directory.
    ///
    /// Creates `semantic-events.jsonl` in the specified directory. This is
    /// the JSONL-only convenience constructor; applications that want to
    /// fan out to additional transports (e.g. Kafka) should construct a
    /// `CompositeSink` and use [`RecordingHook::with_sink`] instead.
    pub fn new(artifact_dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(artifact_dir)?;
        let path = artifact_dir.join("semantic-events.jsonl");
        let sink = JsonlSink::new(&path)?;
        Ok(Self::with_sink(sink, Self::resolve_recording_run_id()))
    }

    /// Create a recording hook backed by a caller-supplied sink.
    ///
    /// This is the dependency-inversion entry point: the application owns
    /// transport choice (JSONL alone, Kafka, S3, a fan-out via
    /// [`crate::writer::CompositeSink`], etc.) and hands the resulting sink
    /// to `deja-record`. The library no longer needs to know about Kafka,
    /// S3, or any other transport.
    ///
    /// `recording_run_id` is the stable identifier attached to every event
    /// emitted through this hook. Callers that want the standard env-var
    /// resolution can pass `RecordingHook::resolve_recording_run_id_default()`.
    pub fn with_sink<S>(sink: S, recording_run_id: String) -> Self
    where
        S: RecordSink<SemanticEvent>,
    {
        Self {
            // The seq extractor lets the writer account drops and stamp the
            // sink markers (checkpoint/eof/dropped) with real global
            // sequences (`DEJA_SINK_POLICY` fail-open accounting).
            writer: AsyncRecordWriter::with_seq_of(
                sink,
                WriterConfig::from_env(),
                Some(std::sync::Arc::new(|e: &SemanticEvent| e.global_sequence)),
            ),
            global_counter: AtomicU64::new(0),
            request_counters: Mutex::new(HashMap::new()),
            uncorrelated_counter: AtomicU64::new(0),
            recording_run_id,
            callsite_occurrence: Mutex::new(HashMap::new()),
        }
    }

    /// Convenience wrapper around [`Self::resolve_recording_run_id`] for
    /// callers of [`Self::with_sink`] that want the default env-var
    /// resolution without duplicating logic.
    pub fn resolve_recording_run_id_default() -> String {
        Self::resolve_recording_run_id()
    }

    /// Stable identifier for this recording run, attached to every emitted
    /// event.
    pub fn recording_run_id(&self) -> &str {
        &self.recording_run_id
    }

    /// Flush all queued records through the configured sink.
    ///
    /// Recording errors are intentionally not surfaced to request handlers, but
    /// tests and harnesses can call this to force JSONL visibility before
    /// reading artifact files.
    pub fn flush(&self) -> std::io::Result<()> {
        self.writer.flush()
    }

    /// Snapshot health counters for the async writer.
    pub fn writer_stats(&self) -> WriterStatsSnapshot {
        self.writer.stats()
    }
}

impl DejaHook for RecordingHook {
    fn is_active(&self) -> bool {
        self.writer.is_active()
    }

    fn record(&self, event: SemanticEvent) {
        let _ = self.writer.record(event);
    }

    fn next_global_sequence(&self) -> u64 {
        self.global_counter.fetch_add(1, Ordering::SeqCst)
    }

    fn next_request_sequence(&self, correlation_id: Option<&str>) -> u64 {
        match correlation_id {
            Some(id) => {
                if let Ok(mut map) = self.request_counters.lock() {
                    let counter = map.entry(id.to_string()).or_insert(0);
                    let seq = *counter;
                    *counter += 1;
                    seq
                } else {
                    0
                }
            }
            None => self.uncorrelated_counter.fetch_add(1, Ordering::SeqCst),
        }
    }

    fn next_callsite_occurrence(
        &self,
        correlation_id: Option<&str>,
        source: CallsiteSource,
        scope: Option<&str>,
    ) -> u32 {
        let key = (
            correlation_id.map(String::from),
            source,
            scope.map(String::from),
        );
        let mut guard = self
            .callsite_occurrence
            .lock()
            .expect("callsite_occurrence poisoned");
        let entry = guard.entry(key).or_insert(0);
        let value = *entry;
        *entry += 1;
        value
    }

    fn recording_run_id(&self) -> Option<&str> {
        Some(&self.recording_run_id)
    }
}

// ---------------------------------------------------------------------------
// Runtime hook enum (record / replay / no-op)
// ---------------------------------------------------------------------------

/// Polymorphic runtime hook that selects between recording, replay, and
/// no-op behavior based on environment configuration.
// One value per process, built at boot — variant size imbalance is irrelevant
// and boxing would churn every constructor.
#[allow(clippy::large_enum_variant)]
pub enum RuntimeHook {
    /// Writes every event to a JSONL artifact.
    ///
    /// Held behind an `Arc` so the SAME recorder can be shared with
    /// `GLOBAL_RECORDING_HOOK` via [`global_hook_from_env`]. Without sharing,
    /// boundaries that resolve through the runtime hook (e.g. id generation)
    /// and boundaries that resolve through `global_hook_from_env` (e.g. db,
    /// http, redis) would use two independent `RecordingHook`s — two
    /// `global_sequence` counters and two sink sets — corrupting the recording
    /// (duplicate sequences, torn JSONL lines) and splitting any
    /// application-supplied secondary sink (e.g. Kafka) across only half the
    /// boundaries.
    Recording(Arc<RecordingHook>),
    /// Replays a previously recorded artifact using the in-process cascade
    /// (matching policy lives in `deja-record`).
    Replay(ReplayHook),
    /// Replays from a pre-rendered `LookupTable` (matching policy lives at
    /// the orchestrator). The hot path is O(1) lookup; divergence detection
    /// runs post-hoc by the orchestrator over the emitted
    /// `ObservedCall` stream.
    LookupReplay(crate::replay::LookupTableHook),
    /// No-op pass-through.
    NoOp(NoOpHook),
}

impl RuntimeHook {
    /// Flush any buffered writes. No-op for non-recording variants.
    pub fn flush(&self) -> std::io::Result<()> {
        match self {
            RuntimeHook::Recording(h) => h.flush(),
            RuntimeHook::LookupReplay(h) => h.flush(),
            RuntimeHook::Replay(_) | RuntimeHook::NoOp(_) => Ok(()),
        }
    }

    /// Snapshot writer stats when the underlying hook is a recorder.
    pub fn writer_stats(&self) -> Option<WriterStatsSnapshot> {
        match self {
            RuntimeHook::Recording(h) => Some(h.writer_stats()),
            _ => None,
        }
    }

    /// Returns a stable identifier for the hook variant for logging.
    pub fn variant_name(&self) -> &'static str {
        match self {
            RuntimeHook::Recording(_) => "recording",
            RuntimeHook::Replay(_) => "replay",
            RuntimeHook::LookupReplay(_) => "lookup_replay",
            RuntimeHook::NoOp(_) => "noop",
        }
    }
}

impl DejaHook for RuntimeHook {
    fn is_active(&self) -> bool {
        match self {
            RuntimeHook::Recording(h) => h.is_active(),
            RuntimeHook::Replay(h) => h.is_active(),
            RuntimeHook::LookupReplay(h) => h.is_active(),
            RuntimeHook::NoOp(h) => h.is_active(),
        }
    }

    fn try_replay(
        &self,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
        args: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        match self {
            RuntimeHook::Recording(h) => h.try_replay(boundary, trait_name, method_name, args),
            RuntimeHook::Replay(h) => h.try_replay(boundary, trait_name, method_name, args),
            RuntimeHook::LookupReplay(h) => h.try_replay(boundary, trait_name, method_name, args),
            RuntimeHook::NoOp(h) => h.try_replay(boundary, trait_name, method_name, args),
        }
    }

    fn try_replay_with_context(&self, query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        match self {
            RuntimeHook::Recording(h) => h.try_replay_with_context(query),
            RuntimeHook::Replay(h) => h.try_replay_with_context(query),
            RuntimeHook::LookupReplay(h) => h.try_replay_with_context(query),
            RuntimeHook::NoOp(h) => h.try_replay_with_context(query),
        }
    }

    fn record(&self, event: SemanticEvent) {
        match self {
            RuntimeHook::Recording(h) => h.record(event),
            RuntimeHook::Replay(h) => h.record(event),
            RuntimeHook::LookupReplay(h) => h.record(event),
            RuntimeHook::NoOp(h) => h.record(event),
        }
    }

    fn next_global_sequence(&self) -> u64 {
        match self {
            RuntimeHook::Recording(h) => h.next_global_sequence(),
            RuntimeHook::Replay(h) => h.next_global_sequence(),
            RuntimeHook::LookupReplay(h) => h.next_global_sequence(),
            RuntimeHook::NoOp(h) => h.next_global_sequence(),
        }
    }

    fn next_request_sequence(&self, correlation_id: Option<&str>) -> u64 {
        match self {
            RuntimeHook::Recording(h) => h.next_request_sequence(correlation_id),
            RuntimeHook::Replay(h) => h.next_request_sequence(correlation_id),
            RuntimeHook::LookupReplay(h) => h.next_request_sequence(correlation_id),
            RuntimeHook::NoOp(h) => h.next_request_sequence(correlation_id),
        }
    }

    fn next_callsite_occurrence(
        &self,
        correlation_id: Option<&str>,
        source: CallsiteSource,
        scope: Option<&str>,
    ) -> u32 {
        match self {
            RuntimeHook::Recording(h) => h.next_callsite_occurrence(correlation_id, source, scope),
            RuntimeHook::Replay(h) => h.next_callsite_occurrence(correlation_id, source, scope),
            RuntimeHook::LookupReplay(h) => {
                h.next_callsite_occurrence(correlation_id, source, scope)
            }
            RuntimeHook::NoOp(h) => h.next_callsite_occurrence(correlation_id, source, scope),
        }
    }

    fn recording_run_id(&self) -> Option<&str> {
        match self {
            RuntimeHook::Recording(h) => Some(h.recording_run_id()),
            RuntimeHook::Replay(h) => DejaHook::recording_run_id(h),
            RuntimeHook::LookupReplay(h) => DejaHook::recording_run_id(h),
            RuntimeHook::NoOp(h) => DejaHook::recording_run_id(h),
        }
    }
}

/// Construct an `Option<RuntimeHook::LookupReplay>` from
/// `DEJA_LOOKUP_TABLE` (path to a JSONL or JSON `LookupTable`) and
/// optionally `DEJA_OBSERVED_SINK` (path to a JSONL file the candidate
/// writes per-call `ObservedCall` records to). When the sink env var is
/// unset, observations accumulate in memory and are lost unless the
/// application drains them explicitly via the hook's underlying sink.
fn lookup_replay_hook_from_env() -> Option<RuntimeHook> {
    let table_path = std::env::var("DEJA_LOOKUP_TABLE").ok()?;
    let source = crate::replay::LocalFileLookupSource::new(&table_path);
    let hook = match std::env::var("DEJA_OBSERVED_SINK").ok() {
        Some(observed_path) => match crate::replay::FileObservedSink::create(&observed_path) {
            Ok(sink) => crate::replay::LookupTableHook::from_source(source, sink),
            Err(err) => {
                eprintln!("deja: failed to open DEJA_OBSERVED_SINK={observed_path}: {err}");
                return None;
            }
        },
        None => crate::replay::LookupTableHook::from_source(
            source,
            crate::replay::InMemoryObservedSink::new(),
        ),
    };
    match hook {
        Ok(h) => Some(RuntimeHook::LookupReplay(h)),
        Err(err) => {
            eprintln!("deja: failed to load DEJA_LOOKUP_TABLE={table_path}: {err}");
            None
        }
    }
}

/// Construct a [`RuntimeHook`] from environment variables.
///
/// Reads `DEJA_MODE` (`record` | `replay` | `disabled`) and
/// `DEJA_ARTIFACT_DIR`. Returns `None` when disabled or misconfigured.
pub fn runtime_hook_from_env() -> Option<RuntimeHook> {
    let mode = std::env::var("DEJA_MODE").ok();
    let artifact_dir = std::env::var("DEJA_ARTIFACT_DIR").ok();

    match mode.as_deref() {
        Some("record") => artifact_dir.and_then(|dir| {
            RecordingHook::new(Path::new(&dir))
                .ok()
                .map(|h| RuntimeHook::Recording(Arc::new(h)))
        }),
        Some("replay") => {
            // Prefer the lookup-table path when DEJA_LOOKUP_TABLE is set
            // (harness-driven runs); fall back to the classic in-process
            // ReplayHook for standalone use (local development loops).
            if let Some(hook) = lookup_replay_hook_from_env() {
                Some(hook)
            } else {
                artifact_dir.and_then(|dir| {
                    ReplayHook::from_artifact_dir(Path::new(&dir))
                        .ok()
                        .map(RuntimeHook::Replay)
                })
            }
        }
        Some("disabled") | Some("off") | Some("none") => None,
        None => artifact_dir.and_then(|dir| {
            // Default: if artifact dir is set but mode isn't, assume record.
            RecordingHook::new(Path::new(&dir))
                .ok()
                .map(|h| RuntimeHook::Recording(Arc::new(h)))
        }),
        Some(other) => {
            eprintln!(
                "deja: unknown DEJA_MODE='{}', expected record|replay|disabled",
                other
            );
            None
        }
    }
}

static GLOBAL_RUNTIME_HOOK: OnceLock<Option<Arc<RuntimeHook>>> = OnceLock::new();

/// Process-wide [`RuntimeHook`] initialized once from environment configuration.
pub fn global_runtime_hook_from_env() -> Option<Arc<RuntimeHook>> {
    GLOBAL_RUNTIME_HOOK
        .get_or_init(|| runtime_hook_from_env().map(Arc::new))
        .clone()
}

/// Install an explicitly-constructed [`RuntimeHook`] as the process-wide hook.
///
/// This is the injection point applications use when they want to construct
/// the hook with a custom sink (typically a [`crate::writer::CompositeSink`]
/// fanning a JSONL primary out to one or more secondaries supplied by the
/// application — e.g. Hyperswitch's Kafka producer). Must be called BEFORE
/// any code path invokes [`global_runtime_hook_from_env`]; returns
/// `Err` if the hook has already been initialized.
pub fn set_global_runtime_hook(hook: Option<RuntimeHook>) -> Result<(), &'static str> {
    GLOBAL_RUNTIME_HOOK
        .set(hook.map(Arc::new))
        .map_err(|_| "global runtime hook already initialized")
}

/// Flush the global [`RuntimeHook`] when one is configured.
pub fn flush_global_runtime_hook() -> std::io::Result<()> {
    if let Some(hook) = global_runtime_hook_from_env() {
        hook.flush()
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Builder for SemanticEvent (used by generated delegation code)
// ---------------------------------------------------------------------------

/// Builder used by generated delegation macros to construct events.
///
/// Captures the "before" state (call site, args, timestamp), then finalizes
/// with the result after the inner call completes.
pub struct EventBuilder {
    pub global_sequence: u64,
    pub request_sequence: u64,
    pub correlation_id: Option<String>,
    pub start_ns: u64,
    pub boundary: &'static str,
    pub trait_name: &'static str,
    pub method_name: &'static str,
    pub call_file: &'static str,
    pub call_line: u32,
    pub call_column: u32,
    pub graph_node_id: Option<u64>,
    pub tracing_span_id: Option<u64>,
    pub receiver: Option<serde_json::Value>,
    pub args: serde_json::Value,
    /// Optional structured call-site identity attached to the emitted event.
    pub callsite_identity: Option<CallsiteIdentity>,
}

impl EventBuilder {
    /// Start building an event. Call this before the inner method.
    ///
    /// `caller` must be `'static` — always satisfied by `Location::caller()`
    /// inside a `#[track_caller]` function.
    pub fn start(
        hook: &dyn DejaHook,
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
        caller: &'static Location<'static>,
        args: serde_json::Value,
    ) -> Self {
        Self::start_with_receiver(hook, boundary, trait_name, method_name, caller, None, args)
    }

    /// Start building an event with an explicit correlation ID.
    ///
    /// The explicit value is used when present; otherwise the ambient
    /// `deja_context` correlation ID is used.
    pub fn start_with_correlation_id(
        hook: &dyn DejaHook,
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
        caller: &'static Location<'static>,
        correlation_id: Option<String>,
        args: serde_json::Value,
    ) -> Self {
        Self::start_with_receiver_and_correlation_id(
            hook,
            boundary,
            trait_name,
            method_name,
            caller,
            None,
            correlation_id,
            args,
        )
    }

    /// Start building an event with receiver/decorator context.
    pub fn start_with_receiver(
        hook: &dyn DejaHook,
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
        caller: &'static Location<'static>,
        receiver: Option<serde_json::Value>,
        args: serde_json::Value,
    ) -> Self {
        Self::start_with_receiver_and_correlation_id(
            hook,
            boundary,
            trait_name,
            method_name,
            caller,
            receiver,
            None,
            args,
        )
    }

    /// Start building an event with receiver context and optional explicit correlation.
    #[allow(clippy::too_many_arguments)] // mirrors the macro-generated call shape
    pub fn start_with_receiver_and_correlation_id(
        hook: &dyn DejaHook,
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
        caller: &'static Location<'static>,
        receiver: Option<serde_json::Value>,
        explicit_correlation_id: Option<String>,
        args: serde_json::Value,
    ) -> Self {
        let correlation_id = explicit_correlation_id.or_else(deja_context::current_correlation_id);
        let (tracing_span_id, graph_node_id) = current_execution_graph_context();
        let global_sequence = hook.next_global_sequence();
        let request_sequence = hook.next_request_sequence(correlation_id.as_deref());

        Self {
            global_sequence,
            request_sequence,
            correlation_id,
            start_ns: now_ns(),
            boundary,
            trait_name,
            method_name,
            call_file: caller.file(),
            call_line: caller.line(),
            call_column: caller.column(),
            graph_node_id,
            tracing_span_id,
            receiver,
            args,
            callsite_identity: None,
        }
    }

    /// Attach a structured call-site identity to the event under construction.
    pub fn with_callsite_identity(mut self, identity: CallsiteIdentity) -> Self {
        self.callsite_identity = Some(identity);
        self
    }

    /// Finalize the event with the result and send it to the hook.
    pub fn finish(self, hook: &dyn DejaHook, result: serde_json::Value, is_error: bool) {
        let end_ns = now_ns();
        let duration_us = end_ns.saturating_sub(self.start_ns) / 1_000;

        let recording_run_id = hook
            .recording_run_id()
            .map(String::from)
            .or_else(current_recording_run_id);

        let event = SemanticEvent {
            global_sequence: self.global_sequence,
            request_sequence: self.request_sequence,
            correlation_id: self.correlation_id,
            timestamp_ns: self.start_ns,
            recording_run_id,
            graph_node_id: self.graph_node_id,
            tracing_span_id: self.tracing_span_id,
            boundary: self.boundary.to_string(),
            trait_name: self.trait_name.to_string(),
            method_name: self.method_name.to_string(),
            call_file: self.call_file.to_string(),
            call_line: self.call_line,
            call_column: self.call_column,
            receiver: self.receiver,
            request: self.args.clone(),
            args: self.args,
            response: result.clone(),
            result,
            is_error,
            duration_us,
            event_schema_version: 1,
            callsite_identity: self.callsite_identity,
        };

        hook.record(event);
    }
}

/// Inject captured body bytes into a result JSON object under the
/// `response_body` key.
fn inject_body_json(result: &mut serde_json::Value, bytes: Vec<u8>) {
    let body_json = if bytes.is_empty() {
        serde_json::json!({
            "captured": false,
            "reason": "empty body or stream incomplete",
        })
    } else {
        let bytes_len = bytes.len();
        let text = std::str::from_utf8(&bytes).ok().map(str::to_string);
        let parsed = text
            .as_deref()
            .and_then(|t| serde_json::from_str::<serde_json::Value>(t).ok());
        serde_json::json!({
            "captured": true,
            "bytes_len": bytes_len,
            "utf8": text.is_some(),
            "text": text,
            "json": parsed,
            "raw_bytes": bytes,
        })
    };

    if let serde_json::Value::Object(ref mut map) = result {
        map.insert("response_body".to_string(), body_json);
    }
}

/// A deferred event finalizer for boundaries where the complete result
/// is not known until after an async stream (e.g. HTTP response body)
/// has been fully consumed.
///
/// Typical usage:
/// 1. Start the event with `EventBuilder::start(...)`
/// 2. Create a `LazyEventFinalizer` with the partial result you know
///    at boundary-start time (status code, headers, etc.)
/// 3. Append streamed chunks via `finalizer.capture_chunk(...)`
/// 4. When the stream completes, call `finalizer.finalize()` to emit
///    the event with the full result including the buffered bytes.
pub struct LazyEventFinalizer {
    builder: Option<EventBuilder>,
    hook: Option<Arc<dyn DejaHook>>,
    partial_result: serde_json::Value,
    is_error: bool,
    body: Vec<u8>,
}

impl LazyEventFinalizer {
    /// Create a new lazy finalizer.
    pub fn new(
        builder: EventBuilder,
        hook: Arc<dyn DejaHook>,
        partial_result: serde_json::Value,
        is_error: bool,
    ) -> Self {
        Self {
            builder: Some(builder),
            hook: Some(hook),
            partial_result,
            is_error,
            body: Vec::new(),
        }
    }

    /// Append a response-body chunk to the full-fidelity capture buffer.
    pub fn capture_chunk(&mut self, chunk: &[u8]) {
        self.body.extend_from_slice(chunk);
    }

    /// Consume the finalizer, build the complete result (partial result +
    /// captured body bytes), and emit the event.
    pub fn finalize(mut self) {
        let builder = self.builder.take().expect("already finalized");
        let hook = self.hook.take().expect("already finalized");

        let mut result = self.partial_result.clone();
        inject_body_json(&mut result, std::mem::take(&mut self.body));

        builder.finish(&*hook, result, self.is_error);
    }
}

impl Drop for LazyEventFinalizer {
    fn drop(&mut self) {
        if self.builder.is_some() {
            if let (Some(builder), Some(hook)) = (self.builder.take(), self.hook.take()) {
                let mut result = self.partial_result.clone();
                inject_body_json(&mut result, std::mem::take(&mut self.body));

                builder.finish(&*hook, result, self.is_error);
            }
        }
    }
}

/// Infer whether a serialized return payload represents an error.
///
/// Serde serializes `Result<T, E>` as `{"Ok": ...}` or `{"Err": ...}`. Using
/// that shape keeps the generated recording path independent of a concrete
/// `Result` return type while preserving error reporting for normal results.
pub fn serialized_result_is_error(result: &serde_json::Value) -> bool {
    matches!(
        result,
        serde_json::Value::Object(map) if map.contains_key("Err")
    )
}

// ---------------------------------------------------------------------------
// Environment-based factory
// ---------------------------------------------------------------------------

/// Initialize a recording hook from environment variables.
///
/// Reads:
/// - `DEJA_MODE`: "record" to enable recording (default: disabled)
/// - `DEJA_ARTIFACT_DIR`: directory for output files (required when recording)
///
/// Returns `None` if disabled or misconfigured.
pub fn hook_from_env() -> Option<RecordingHook> {
    let mode = std::env::var("DEJA_MODE").unwrap_or_default();
    if mode != "record" {
        return None;
    }
    let dir = std::env::var("DEJA_ARTIFACT_DIR").ok()?;
    RecordingHook::new(Path::new(&dir)).ok()
}

static GLOBAL_RECORDING_HOOK: OnceLock<Option<Arc<RecordingHook>>> = OnceLock::new();

/// Shared recording hook initialized once from `DEJA_MODE` and `DEJA_ARTIFACT_DIR`.
///
/// If an application has installed a recording [`RuntimeHook`] via
/// [`set_global_runtime_hook`] (e.g. Hyperswitch's `deja_boot`, which composes a
/// Kafka secondary onto the JSONL primary), this returns that hook's SHARED
/// `RecordingHook` so callers of this getter use the exact same recorder as
/// callers of [`global_runtime_hook_from_env`]. That unification is what keeps a
/// single `global_sequence` counter and a single sink set across every boundary
/// — regardless of which resolver a given boundary happens to call.
///
/// The runtime hook is only PEEKED (`get`, never `get_or_init`): we must not
/// pre-empt the install-before-getter ordering contract documented on
/// [`set_global_runtime_hook`]. When no runtime hook is installed (standalone
/// recording, tests), this falls back to the env-derived `GLOBAL_RECORDING_HOOK`
/// exactly as before.
pub fn global_hook_from_env() -> Option<Arc<RecordingHook>> {
    if let Some(Some(runtime)) = GLOBAL_RUNTIME_HOOK.get() {
        if let RuntimeHook::Recording(hook) = runtime.as_ref() {
            return Some(Arc::clone(hook));
        }
    }
    GLOBAL_RECORDING_HOOK
        .get_or_init(|| hook_from_env().map(Arc::new))
        .as_ref()
        .cloned()
}

/// Flush the global recording hook, when one is configured.
///
/// The recording hook may live in EITHER `GLOBAL_RECORDING_HOOK` (when
/// resolved standalone) OR inside `GLOBAL_RUNTIME_HOOK` as
/// `RuntimeHook::Recording` (when the runtime hook was initialized first — e.g.
/// because a boundary allocated a callsite occurrence via the runtime hook
/// before any event was recorded). Flush whichever holds it so events are not
/// silently left unflushed.
pub fn flush_global_hook() -> std::io::Result<()> {
    if let Some(hook) = GLOBAL_RECORDING_HOOK.get().and_then(|hook| hook.as_ref()) {
        return hook.flush();
    }
    if let Some(Some(runtime)) = GLOBAL_RUNTIME_HOOK.get() {
        if let RuntimeHook::Recording(hook) = runtime.as_ref() {
            return hook.flush();
        }
    }
    Ok(())
}

/// Record a completed semantic event from hand-written boundary hooks.
#[track_caller]
pub fn record_semantic_event(
    boundary: &'static str,
    trait_name: &'static str,
    method_name: &'static str,
    request: serde_json::Value,
    response: serde_json::Value,
    is_error: bool,
) {
    let Some(hook) = global_hook_from_env() else {
        return;
    };

    if !hook.is_active() {
        return;
    }

    let caller = Location::caller();
    let builder = EventBuilder::start(&*hook, boundary, trait_name, method_name, caller, request);
    builder.finish(&*hook, response, is_error);
}

/// Static semantic boundary metadata used by generated boundary wrappers.
#[derive(Debug, Clone, Copy)]
pub struct BoundarySpec {
    pub boundary: &'static str,
    pub trait_name: &'static str,
    pub method_name: &'static str,
}

impl BoundarySpec {
    pub const fn new(
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
    ) -> Self {
        Self {
            boundary,
            trait_name,
            method_name,
        }
    }
}

/// Record an async function boundary without changing the function body.
pub async fn record_boundary_async<F, T, R>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: serde_json::Value,
    future: F,
    result: R,
) -> T
where
    F: Future<Output = T>,
    R: FnOnce(&T) -> (serde_json::Value, bool),
{
    let event = start_boundary_event(caller, spec, correlation_id, args, None);
    let output = future.await;
    finish_boundary_event(event, &output, result);
    output
}

/// Record an async function boundary while constructing args only when active.
pub async fn record_boundary_async_lazy<F, T, A, R>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: A,
    future: F,
    result: R,
) -> T
where
    F: Future<Output = T>,
    A: FnOnce() -> serde_json::Value,
    R: FnOnce(&T) -> (serde_json::Value, bool),
{
    let event = start_boundary_event_lazy(caller, spec, correlation_id, args, None);
    let output = future.await;
    finish_boundary_event(event, &output, result);
    output
}

/// Record a synchronous function boundary without changing the function body.
pub fn record_boundary_sync<F, T, R>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: serde_json::Value,
    function: F,
    result: R,
) -> T
where
    F: FnOnce() -> T,
    R: FnOnce(&T) -> (serde_json::Value, bool),
{
    let event = start_boundary_event(caller, spec, correlation_id, args, None);
    let output = function();
    finish_boundary_event(event, &output, result);
    output
}

/// Record a synchronous function boundary while constructing args only when active.
pub fn record_boundary_sync_lazy<F, T, A, R>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: A,
    function: F,
    result: R,
) -> T
where
    F: FnOnce() -> T,
    A: FnOnce() -> serde_json::Value,
    R: FnOnce(&T) -> (serde_json::Value, bool),
{
    let event = start_boundary_event_lazy(caller, spec, correlation_id, args, None);
    let output = function();
    finish_boundary_event(event, &output, result);
    output
}

/// Allocate the next per-callsite occurrence index for a boundary invocation.
///
/// Resolves the process-wide runtime hook (which uniformly implements
/// [`DejaHook::next_callsite_occurrence`] across record / replay / lookup
/// modes) and bumps the counter keyed by `(correlation_id, source, scope)`.
/// In record mode the runtime hook shares the SAME `RecordingHook` the
/// recording path uses, so a single call here yields one consistent occurrence
/// for both the replay-lookup key and the recorded event. Returns `0` when no
/// hook is configured (inactive / no-op), which is harmless because nothing is
/// recorded or replayed in that state.
///
/// MUST be called EXACTLY ONCE per boundary invocation; the result is reused
/// for both the replay prelude and the recording path to keep record/replay
/// occurrence numbering aligned (rank-4 `Address::LexicalPath`).
pub fn next_boundary_occurrence(
    correlation_id: Option<&str>,
    source: CallsiteSource,
    scope: Option<&str>,
) -> u32 {
    match global_runtime_hook_from_env() {
        Some(hook) => hook.next_callsite_occurrence(correlation_id, source, scope),
        None => 0,
    }
}

/// Replay substitution for an instrumented boundary (the macro `replay` flag).
///
/// Resolves the process runtime hook and asks it to replay this call from the
/// recorded lookup table. In replay mode (a lookup-table hook) this returns
/// `Some(result_json)` — the macro deserializes it into the function's return
/// type and skips the live call. In record / no-op mode the hook does not
/// replay, so this returns `None` and the caller executes + records as usual.
/// The correlation id is read from the ambient `deja_context` inside the hook,
/// so only the structured args + call site are passed here.
/// `identity` is the structured [`CallsiteIdentity`] computed ONCE by the
/// boundary macro (or hand-built caller) for this invocation. It is threaded
/// into the [`ReplayLookup`] so the candidate hook can resolve at the stable
/// content/identity ranks (logical-context / syntactic-hash / lexical-path)
/// rather than the rank-6 positional fallback.
/// The SAME identity value must be reused for the recording path so the
/// renderer and the hook stamp identical occurrence keys.
#[track_caller]
pub fn replay_boundary(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    args: &serde_json::Value,
    identity: Option<&CallsiteIdentity>,
) -> Option<serde_json::Value> {
    let hook = global_runtime_hook_from_env()?;
    if !hook.is_active() {
        return None;
    }
    hook.try_replay_with_context(ReplayLookup {
        boundary: spec.boundary,
        trait_name: spec.trait_name,
        method_name: spec.method_name,
        args,
        callsite_identity: identity,
        caller_location: Some(caller),
    })
}

fn start_boundary_event(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: serde_json::Value,
    identity: Option<CallsiteIdentity>,
) -> Option<(Arc<RecordingHook>, EventBuilder)> {
    start_boundary_event_lazy(caller, spec, correlation_id, || args, identity)
}

pub fn start_boundary_event_lazy<A>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: A,
    identity: Option<CallsiteIdentity>,
) -> Option<(Arc<RecordingHook>, EventBuilder)>
where
    A: FnOnce() -> serde_json::Value,
{
    let hook = global_hook_from_env()?;
    if !hook.is_active() {
        return None;
    }

    let mut event = EventBuilder::start_with_correlation_id(
        &*hook,
        spec.boundary,
        spec.trait_name,
        spec.method_name,
        caller,
        correlation_id,
        args(),
    );
    if let Some(identity) = identity {
        event = event.with_callsite_identity(identity);
    }
    Some((hook, event))
}

pub fn finish_boundary_event<T, R>(
    event: Option<(Arc<RecordingHook>, EventBuilder)>,
    output: &T,
    result: R,
) where
    R: FnOnce(&T) -> (serde_json::Value, bool),
{
    if let Some((hook, event)) = event {
        let (response, is_error) = result(output);
        event.finish(&*hook, response, is_error);
    }
}

/// Path to the semantic events file within an artifact directory.
pub fn semantic_events_path(artifact_dir: &Path) -> PathBuf {
    artifact_dir.join("semantic-events.jsonl")
}

// ---------------------------------------------------------------------------
// Artifact reader (for analysis and future replay)
// ---------------------------------------------------------------------------

/// Read all semantic events from a JSONL file.
pub fn read_events(artifact_dir: &Path) -> std::io::Result<Vec<SemanticEvent>> {
    let path = semantic_events_path(artifact_dir);
    let content = std::fs::read_to_string(path)?;
    let mut events = Vec::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(event) = serde_json::from_str::<SemanticEvent>(line) {
            events.push(event);
        }
    }
    Ok(events)
}

// ---------------------------------------------------------------------------
// Replay/deviation lookup
// ---------------------------------------------------------------------------

/// Replay match strictness for a recorded semantic event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayConfidence {
    /// Same boundary, trait, method, call file, call line, and args.
    Exact,
    /// Same boundary, trait, method, call file, and args, but a different line.
    LineShifted,
    /// Same boundary, trait, method, call file, and line, but different args.
    ArgsChanged,
}

/// Query used to find a recorded semantic operation during replay analysis.
#[derive(Debug, Clone, Copy)]
pub struct ReplayQuery<'a> {
    pub correlation_id: Option<&'a str>,
    pub boundary: &'a str,
    pub trait_name: &'a str,
    pub method_name: &'a str,
    pub call_file: &'a str,
    pub call_line: u32,
    pub args: &'a serde_json::Value,
}

/// A replay/deviation match against a recorded event.
#[derive(Debug, Clone, Copy)]
pub struct ReplayMatch<'a> {
    pub event: &'a SemanticEvent,
    pub confidence: ReplayConfidence,
    pub reason: &'static str,
}

/// In-memory index over recorded semantic events.
///
/// This is a diagnostic and matching primitive. It deliberately does not
/// implement `DejaHook` because returning typed application values from JSON is
/// boundary-specific work, not something the generic recorder can do safely.
#[derive(Debug, Clone)]
pub struct ReplayIndex {
    events: Vec<SemanticEvent>,
}

impl ReplayIndex {
    pub fn new(events: Vec<SemanticEvent>) -> Self {
        Self { events }
    }

    pub fn from_artifact_dir(artifact_dir: &Path) -> std::io::Result<Self> {
        read_events(artifact_dir).map(Self::new)
    }

    pub fn events(&self) -> &[SemanticEvent] {
        &self.events
    }

    /// Find the best match for a replay query using a strict-to-loose cascade.
    pub fn find(&self, query: ReplayQuery<'_>) -> Option<ReplayMatch<'_>> {
        self.find_by(query, |event| {
            event.call_file == query.call_file
                && event.call_line == query.call_line
                && event.args == *query.args
        })
        .map(|event| ReplayMatch {
            event,
            confidence: ReplayConfidence::Exact,
            reason: "exact call-site and args match",
        })
        .or_else(|| {
            self.find_by(query, |event| {
                event.call_file == query.call_file && event.args == *query.args
            })
            .map(|event| ReplayMatch {
                event,
                confidence: ReplayConfidence::LineShifted,
                reason: "same call file and args, line shifted",
            })
        })
        .or_else(|| {
            self.find_by(query, |event| {
                event.call_file == query.call_file && event.call_line == query.call_line
            })
            .map(|event| ReplayMatch {
                event,
                confidence: ReplayConfidence::ArgsChanged,
                reason: "same call-site, args changed",
            })
        })
    }

    fn find_by(
        &self,
        query: ReplayQuery<'_>,
        predicate: impl Fn(&SemanticEvent) -> bool,
    ) -> Option<&SemanticEvent> {
        self.events.iter().find(|event| {
            correlation_matches(event, query.correlation_id)
                && event.boundary == query.boundary
                && event.trait_name == query.trait_name
                && event.method_name == query.method_name
                && predicate(event)
        })
    }

    /// Deterministic call-graph fingerprint for one correlation scope.
    ///
    /// The hash includes operation order, boundary, trait/method name, and call
    /// location. It is intended for change detection, not cryptographic use.
    pub fn call_graph_fingerprint(&self, correlation_id: Option<&str>) -> u64 {
        let mut hash = FNV_OFFSET_BASIS;
        for event in self
            .events
            .iter()
            .filter(|event| correlation_matches(event, correlation_id))
        {
            hash = fnv1a_u64(hash, event.request_sequence);
            hash = fnv1a_str(hash, &event.boundary);
            hash = fnv1a_str(hash, &event.trait_name);
            hash = fnv1a_str(hash, &event.method_name);
            hash = fnv1a_str(hash, &event.call_file);
            hash = fnv1a_u64(hash, event.call_line as u64);
            hash = fnv1a_u64(hash, event.call_column as u64);
        }
        hash
    }
}

fn correlation_matches(event: &SemanticEvent, correlation_id: Option<&str>) -> bool {
    match correlation_id {
        Some(id) => event.correlation_id.as_deref() == Some(id),
        None => event.correlation_id.is_none(),
    }
}

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn fnv1a_str(hash: u64, value: &str) -> u64 {
    let hash = fnv1a_bytes(hash, value.as_bytes());
    fnv1a_bytes(hash, &[0xff])
}

fn fnv1a_u64(hash: u64, value: u64) -> u64 {
    fnv1a_bytes(hash, &value.to_le_bytes())
}

/// Deterministic FNV-1a hash of a string, used to derive a stable
/// `CallsiteIdentity::syntax_hash` (rank-3 `Address::SyntacticHash`).
///
/// The boundary proc-macro replicates this exact algorithm at expansion time
/// to emit a `syntax_hash` literal from the boundary/component/operation
/// tuple; the hand-written DB boundary calls this at runtime. Using one shared,
/// version-stable algorithm guarantees the same input string always yields the
/// same hash regardless of rustc/syn version, so record and replay agree.
pub fn stable_callsite_hash(input: &str) -> u64 {
    fnv1a_str(FNV_OFFSET_BASIS, input)
}

// ---------------------------------------------------------------------------
// Metrics summary
// ---------------------------------------------------------------------------

/// Summary metrics from a recording session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingMetrics {
    pub total_events: u64,
    pub correlated_events: u64,
    pub uncorrelated_events: u64,
    pub unique_correlation_ids: u64,
    pub unique_traits: u64,
    pub unique_methods: u64,
    pub unique_call_sites: u64,
    pub error_events: u64,
    pub boundaries: HashMap<String, u64>,
}

/// Compute metrics from a set of recorded events.
pub fn compute_metrics(events: &[SemanticEvent]) -> RecordingMetrics {
    use std::collections::HashSet;

    let mut correlation_ids = HashSet::new();
    let mut traits = HashSet::new();
    let mut methods = HashSet::new();
    let mut call_sites = HashSet::new();
    let mut boundaries: HashMap<String, u64> = HashMap::new();
    let mut correlated = 0u64;
    let mut uncorrelated = 0u64;
    let mut errors = 0u64;

    for event in events {
        if let Some(ref id) = event.correlation_id {
            correlation_ids.insert(id.clone());
            correlated += 1;
        } else {
            uncorrelated += 1;
        }
        traits.insert(event.trait_name.clone());
        methods.insert(format!("{}::{}", event.trait_name, event.method_name));
        call_sites.insert(format!(
            "{}:{}:{}",
            event.call_file, event.call_line, event.call_column
        ));
        *boundaries.entry(event.boundary.clone()).or_insert(0) += 1;
        if event.is_error {
            errors += 1;
        }
    }

    RecordingMetrics {
        total_events: events.len() as u64,
        correlated_events: correlated,
        uncorrelated_events: uncorrelated,
        unique_correlation_ids: correlation_ids.len() as u64,
        unique_traits: traits.len() as u64,
        unique_methods: methods.len() as u64,
        unique_call_sites: call_sites.len() as u64,
        error_events: errors,
        boundaries,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::Location;

    #[test]
    fn now_ns_returns_reasonable_value() {
        let ts = now_ns();
        // Should be after 2024-01-01 (in nanoseconds)
        assert!(ts > 1_704_067_200_000_000_000);
    }

    #[test]
    fn recording_hook_sequences_atomically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = RecordingHook::new(dir.path()).expect("hook");

        assert_eq!(hook.next_global_sequence(), 0);
        assert_eq!(hook.next_global_sequence(), 1);
        assert_eq!(hook.next_global_sequence(), 2);

        assert_eq!(hook.next_request_sequence(Some("req-1")), 0);
        assert_eq!(hook.next_request_sequence(Some("req-1")), 1);
        assert_eq!(hook.next_request_sequence(Some("req-2")), 0);
        assert_eq!(hook.next_request_sequence(Some("req-1")), 2);
    }

    #[test]
    fn event_builder_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = RecordingHook::new(dir.path()).expect("hook");

        // Simulate what generated delegation code does
        #[track_caller]
        fn simulate_call(hook: &RecordingHook) {
            let caller = Location::caller();
            let builder = EventBuilder::start(
                hook,
                "storage",
                "AddressInterface",
                "find_address_by_address_id",
                caller,
                serde_json::json!({"address_id": "addr_123"}),
            );
            builder.finish(
                hook,
                serde_json::json!({"id": "addr_123", "city": "Mumbai"}),
                false,
            );
        }

        simulate_call(&hook);
        simulate_call(&hook);

        // Flush and read back
        drop(hook);
        let events = read_events(dir.path()).expect("read");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].global_sequence, 0);
        assert_eq!(events[1].global_sequence, 1);
        assert_eq!(events[0].trait_name, "AddressInterface");
        assert_eq!(events[0].method_name, "find_address_by_address_id");
        assert_eq!(events[0].boundary, "storage");
        assert!(!events[0].is_error);
        assert!(events[0].call_file.contains("lib.rs"));
    }

    #[test]
    fn compute_metrics_from_events() {
        let events = vec![
            SemanticEvent {
                global_sequence: 0,
                request_sequence: 0,
                correlation_id: Some("req-1".into()),
                timestamp_ns: now_ns(),
                recording_run_id: None,
                graph_node_id: None,
                tracing_span_id: None,
                boundary: "storage".into(),
                trait_name: "PaymentIntentInterface".into(),
                method_name: "find".into(),
                call_file: "payments.rs".into(),
                call_line: 42,
                call_column: 9,
                receiver: None,
                request: serde_json::json!({}),
                args: serde_json::json!({}),
                response: serde_json::json!({}),
                result: serde_json::json!({}),
                is_error: false,
                duration_us: 100,
                event_schema_version: 1,
                callsite_identity: None,
            },
            SemanticEvent {
                global_sequence: 1,
                request_sequence: 0,
                correlation_id: None,
                timestamp_ns: now_ns(),
                recording_run_id: None,
                graph_node_id: None,
                tracing_span_id: None,
                boundary: "redis".into(),
                trait_name: "RedisPool".into(),
                method_name: "get_key".into(),
                call_file: "cache.rs".into(),
                call_line: 10,
                call_column: 5,
                receiver: None,
                request: serde_json::json!({}),
                args: serde_json::json!({}),
                response: serde_json::json!({"error": "not found"}),
                result: serde_json::json!({"error": "not found"}),
                is_error: true,
                duration_us: 50,
                event_schema_version: 1,
                callsite_identity: None,
            },
        ];

        let metrics = compute_metrics(&events);
        assert_eq!(metrics.total_events, 2);
        assert_eq!(metrics.correlated_events, 1);
        assert_eq!(metrics.uncorrelated_events, 1);
        assert_eq!(metrics.unique_correlation_ids, 1);
        assert_eq!(metrics.unique_traits, 2);
        assert_eq!(metrics.error_events, 1);
        assert_eq!(metrics.boundaries.get("storage"), Some(&1));
        assert_eq!(metrics.boundaries.get("redis"), Some(&1));
    }

    #[test]
    fn replay_index_uses_strict_to_loose_matching() {
        let events = vec![
            SemanticEvent {
                global_sequence: 0,
                request_sequence: 0,
                correlation_id: Some("req-1".into()),
                timestamp_ns: now_ns(),
                recording_run_id: None,
                graph_node_id: None,
                tracing_span_id: None,
                boundary: "storage".into(),
                trait_name: "AddressInterface".into(),
                method_name: "find_address_by_address_id".into(),
                call_file: "payments.rs".into(),
                call_line: 42,
                call_column: 9,
                receiver: None,
                request: serde_json::json!({"address_id": "addr_1"}),
                args: serde_json::json!({"address_id": "addr_1"}),
                response: serde_json::json!({"ok": true}),
                result: serde_json::json!({"ok": true}),
                is_error: false,
                duration_us: 100,
                event_schema_version: 1,
                callsite_identity: None,
            },
            SemanticEvent {
                global_sequence: 1,
                request_sequence: 1,
                correlation_id: Some("req-1".into()),
                timestamp_ns: now_ns(),
                recording_run_id: None,
                graph_node_id: None,
                tracing_span_id: None,
                boundary: "storage".into(),
                trait_name: "AddressInterface".into(),
                method_name: "find_address_by_address_id".into(),
                call_file: "payments.rs".into(),
                call_line: 50,
                call_column: 9,
                receiver: None,
                request: serde_json::json!({"address_id": "addr_2"}),
                args: serde_json::json!({"address_id": "addr_2"}),
                response: serde_json::json!({"ok": true}),
                result: serde_json::json!({"ok": true}),
                is_error: false,
                duration_us: 100,
                event_schema_version: 1,
                callsite_identity: None,
            },
        ];
        let index = ReplayIndex::new(events);

        let exact_args = serde_json::json!({"address_id": "addr_1"});
        let exact = index
            .find(ReplayQuery {
                correlation_id: Some("req-1"),
                boundary: "storage",
                trait_name: "AddressInterface",
                method_name: "find_address_by_address_id",
                call_file: "payments.rs",
                call_line: 42,
                args: &exact_args,
            })
            .expect("exact match");
        assert_eq!(exact.confidence, ReplayConfidence::Exact);

        let shifted = index
            .find(ReplayQuery {
                correlation_id: Some("req-1"),
                boundary: "storage",
                trait_name: "AddressInterface",
                method_name: "find_address_by_address_id",
                call_file: "payments.rs",
                call_line: 43,
                args: &exact_args,
            })
            .expect("line-shifted match");
        assert_eq!(shifted.confidence, ReplayConfidence::LineShifted);

        let changed_args = serde_json::json!({"address_id": "addr_changed"});
        let changed = index
            .find(ReplayQuery {
                correlation_id: Some("req-1"),
                boundary: "storage",
                trait_name: "AddressInterface",
                method_name: "find_address_by_address_id",
                call_file: "payments.rs",
                call_line: 42,
                args: &changed_args,
            })
            .expect("args-changed match");
        assert_eq!(changed.confidence, ReplayConfidence::ArgsChanged);

        assert_ne!(
            index.call_graph_fingerprint(Some("req-1")),
            FNV_OFFSET_BASIS
        );
    }
}
