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
    /// How this event entered the artifact: a primary recording capture, or a
    /// shadow capture written while an execute-mode dispatch ran the REAL
    /// boundary during replay. Lets the post-hoc tally pair recorded vs shadow
    /// events to classify [`ValueDiverged`](crate::DivergenceKind::ValueDiverged).
    #[serde(default)]
    pub provenance: Provenance,
    /// Reconstructability of `result`: whether it round-trips losslessly, only
    /// structurally, or is opaque. Inert in M1 (always [`Recon::Lossless`]);
    /// carried so later stages can mark partial captures.
    #[serde(default)]
    pub recon: Recon,
    /// Post-image of the affected state after this operation, when an
    /// execute-mode dispatch observed it (e.g. the row a WRITE produced). `None`
    /// for ordinary lookup-mode events.
    #[serde(default)]
    pub result_image: Option<serde_json::Value>,
    /// Pre-image of the affected state before this operation, when known. `None`
    /// for ordinary lookup-mode events.
    #[serde(default)]
    pub pre_image: Option<serde_json::Value>,
    /// State keys this crossing READ, derived from args for State-channel reads.
    /// Best-effort (the leading string key); lets a seed/template builder
    /// reproduce preconditions and lets replay detect a diverged read — a key
    /// requested under `x'` that is absent here is not reconstructable. Captured
    /// at record time because it cannot be back-filled later.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read_set: Vec<String>,
    /// State keys this crossing WROTE, derived from args for State-channel writes.
    /// Best-effort, same shape as [`read_set`](Self::read_set).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write_set: Vec<String>,
    /// Stable content digest over `(args, result)` — the cheapest dataflow hint
    /// (a write whose digest matches an upstream read's is a probable read→write
    /// edge). Reuses the canonical args hashing, never a second hash function.
    /// An FNV-1a u64 routinely exceeds `i64::MAX`; the Kafka→Vector→MinIO record
    /// pipeline stringifies such integers (to dodge JSON float-precision loss),
    /// so deserialize leniently (accept number OR string) — otherwise every event
    /// carrying a large digest fails to parse and is dropped from replay.
    #[serde(
        default,
        deserialize_with = "de_u64_opt_lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub value_digest: Option<u64>,
    /// For Entropy/Time crossings, the generator family that produced the value
    /// ("id", "time"). A classification PRIMITIVE recorded for replay to read;
    /// never a replay verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entropy_source: Option<String>,
    /// DECLARED channel for this boundary (declarative boundary model). `None`
    /// when the boundary declared nothing (the current vendor). Additive: an old
    /// reader tolerates a newer tape via `#[serde(default)]`, and a new reader
    /// tolerates an old tape the same way. Read by the decision matrix at replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<Channel>,
    /// DECLARED effect for this boundary, or `None` (undeclared).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<Effect>,
    /// DECLARED per-op strategy override (REQUIRED for declared RMW), or `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<Strategy>,
    /// Resampleable pre-transform draw for Entropy/Clock crossings where the
    /// boundary is a direct source. Reserved (usually `None`): black-box wrapping
    /// observes only the post-transform `result`, so this is populated only when
    /// the boundary returns the raw draw itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_draw: Option<serde_json::Value>,
    /// Wall-clock completion time (ns since epoch). Paired with `timestamp_ns`
    /// it gives the true span without collapsing it into `duration_us`;
    /// un-back-fillable, so captured now for latency/interleaving replay modes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_timestamp_ns: Option<u64>,
}

/// Default `event_schema_version` for records that predate the field entirely
/// (legacy v1). New events are stamped with [`CURRENT_EVENT_SCHEMA_VERSION`].
fn default_event_schema_version() -> u16 {
    1
}

/// Wire-format schema version stamped on freshly recorded events. Bumped in
/// lock-step with changes to the captured field set so tapes are distinguishable;
/// older readers tolerate newer tapes via `#[serde(default)]` on each added field,
/// and newer readers tolerate older tapes the same way. v2 adds the
/// forward-looking handler-completeness fields (read_set/write_set/value_digest/
/// entropy_source/raw_draw/end_timestamp_ns).
pub const CURRENT_EVENT_SCHEMA_VERSION: u16 = 2;

/// How a [`SemanticEvent`] entered the artifact.
///
/// `Recorded` is the ordinary capture path (record mode, or a lookup-mode
/// replay that substitutes). `ExecuteShadow` marks a shadow event written while
/// an execute-mode dispatch ran the REAL boundary during replay — the post-hoc
/// tally joins recorded ↔ shadow by args-free identity + occurrence to classify
/// value divergences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    /// Primary capture (record mode or substituted lookup replay).
    #[default]
    Recorded,
    /// Shadow capture from an execute-mode dispatch running the real boundary.
    ExecuteShadow,
}

/// Reconstructability of a captured `result`.
///
/// Inert in M1 (always [`Recon::Lossless`]); carried additively so later stages
/// can flag captures that only round-trip structurally or not at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Recon {
    /// Result round-trips byte-for-byte / value-for-value.
    #[default]
    Lossless,
    /// Result round-trips structurally but not losslessly.
    Structured,
    /// Result cannot be reconstructed from the capture.
    Opaque,
}

// ---------------------------------------------------------------------------
// Declarative boundary model (additive foundation slice)
//
// A boundary author DECLARES the intrinsic semantics of an op once per wrapper
// (design `docs/design/declarative-boundary-model.md`); the runtime decision
// becomes a pure table over (declarations × replay policy) — see
// [`crate::replay::decide_strategy`]. These enums are the declared primitives.
//
// FORWARD-COMPAT: every wire enum that may grow carries a `#[serde(other)]
// Unknown` unit variant so an OLD reader tolerates a NEW tape (an unknown
// discriminant deserializes to `Unknown` instead of failing the whole record).
// `EntropySource::Other(String)` (carried inside `Channel::Entropy`) plays the
// same role for the entropy family.
//
// ADDITIVE: a boundary that declares NOTHING (every current vendor wrapper)
// carries `None` for each declaration, and the runtime falls back to the
// existing string heuristics so behavior is byte-identical. The value activates
// only once a boundary declares.
// ---------------------------------------------------------------------------

/// Where a boundary's effect goes. Replaces the `is_state_channel` string match
/// (only [`Channel::State`] is execute-eligible). [`Channel::Entropy`] folds the
/// generator family in as a payload ([`EntropySource`]) — time/id/rng are
/// reconstructed by lookup, never executed. [`Channel::Egress`] (outbound HTTP /
/// gRPC) is never re-executed or re-issued.
///
/// Carries [`EntropySource`] data inside [`Channel::Entropy`], so it is NOT
/// `Copy` — use [`Clone`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    /// db / redis — the only channel that opts into execute-mode. [`Effect`] is a
    /// STATE-ONLY concept, declared only for this channel.
    State,
    /// Time / id / rng — reconstructable by lookup, never executed. The
    /// [`EntropySource`] payload names the generator family.
    Entropy(EntropySource),
    /// Outbound HTTP / gRPC — never re-executed or re-issued.
    Egress,
    /// Forward-compat: an unknown channel on a newer tape.
    #[serde(other)]
    Unknown,
}

/// What a boundary does to its [`Channel::State`] channel — a STATE-ONLY concept
/// (`None`/absent for Entropy/Egress). Replaces the `is_read_op` verb match.
/// [`Effect::Read`] covers `count`/`filter`/`aggregate` the verb list misses;
/// [`Effect::ReadModifyWrite`] is the keystone (INCR/SETNX/SADD); [`Effect::Append`]
/// is an append-only log (XADD); [`Effect::VolatileRead`] is a read whose value
/// decays with wall-clock (`get_ttl`) or iterates non-deterministically — never
/// reproducible by execution, so always served from lookup; [`Effect::Opaque`] is
/// an arbitrary read+write whose return is unrelated to the mutation (EVAL/Lua).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// Read-only (find/get/list/count/filter/aggregate).
    Read,
    /// Overwrites state (insert/update/set).
    Write,
    /// Reads then conditionally writes; return depends on prior state
    /// (INCR/SETNX/SADD/`get_or_create`). MUST declare a [`Strategy`].
    ReadModifyWrite,
    /// Append-only log; the return is an assigned id, never re-read for diff (XADD).
    Append,
    /// A read whose value is time-decaying (`get_ttl`/`set_expire_at`) or whose
    /// iteration order is non-deterministic — never reproducible by execution, so
    /// always served from lookup (never executed even under SelectiveExecute).
    VolatileRead,
    /// Arbitrary read+write whose return is unrelated to what it mutated
    /// (EVAL/Lua) → always served from lookup, never executed.
    Opaque,
    /// Forward-compat: an unknown effect on a newer tape.
    #[serde(other)]
    Unknown,
}

/// The generator family behind a [`Channel::Entropy`] value. A classification
/// PRIMITIVE recorded for replay to read; never a replay verdict.
/// [`EntropySource::Other`] absorbs new families for forward-compat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntropySource {
    /// Wall-clock time.
    Clock,
    /// Identifier generator (uuid, nanoid, ...).
    Id,
    /// A random-number generator.
    Rng,
    /// Any other named source (forward-compat growth absorber).
    Other(String),
}

/// The runtime strategy a boundary resolves to under a [`Policy`]. The output of
/// the decision matrix ([`crate::replay::decide_strategy`]).
///
/// - [`Strategy::Lookup`] — serve the recorded value, never touch the live
///   boundary (maps to [`ExecuteMode::Lookup`]).
/// - [`Strategy::SeedAndExecute`] — seed the pre-image, run the real op, diff;
///   catches TOTAL derivatives (maps to [`ExecuteMode::Execute`]).
/// - [`Strategy::LookupAndSeed`] — serve the recorded return + seed the post-state
///   (full-mock for RMW/Append; never double-applies). The live "serve recorded
///   return + seed post-state" mechanism is STUBBED in this slice (see
///   [`crate::replay::decide_strategy`]); declared boundaries resolving to it map
///   to a safe [`ExecuteMode::Lookup`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    /// Serve recorded, never touch live (→ [`ExecuteMode::Lookup`]).
    Lookup,
    /// Seed pre-image, run real op, diff (→ [`ExecuteMode::Execute`]).
    SeedAndExecute,
    /// Serve recorded return + seed post-state (no pre-image). STUBBED → Lookup.
    LookupAndSeed,
    /// Forward-compat: an unknown strategy on a newer tape.
    #[serde(other)]
    Unknown,
}

/// The declared intrinsic semantics of a boundary, carried alongside the
/// [`BoundarySpec`]. Every field is `Option`: `None` means UNDECLARED (the
/// current vendor), and the runtime falls back to the existing heuristics so
/// behavior is byte-identical. A populated field is an author declaration the
/// decision matrix reads directly (no strings).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BoundarySemantics {
    /// Declared channel (`is_state_channel` replacement), or `None` (undeclared).
    pub channel: Option<Channel>,
    /// Declared effect (`is_read_op` replacement), or `None` (undeclared).
    pub effect: Option<Effect>,
    /// Declared per-op strategy override (REQUIRED for declared RMW), or `None`.
    pub strategy: Option<Strategy>,
}

impl BoundarySemantics {
    /// All-undeclared semantics (the additive default).
    pub const fn undeclared() -> Self {
        Self {
            channel: None,
            effect: None,
            strategy: None,
        }
    }

    /// True when nothing is declared — the runtime must use the string-heuristic
    /// fallback for this boundary.
    pub fn is_undeclared(&self) -> bool {
        self.channel.is_none() && self.effect.is_none() && self.strategy.is_none()
    }
}

// ---------------------------------------------------------------------------
// Replay policy + per-boundary execute mode
// ---------------------------------------------------------------------------

/// Process-wide replay policy, parsed once from `DEJA_POLICY`.
///
/// `AllLookup` is the default and reproduces today's behavior exactly: every
/// boundary is served from the recorded lookup table (the PARTIAL derivative —
/// direct side effects are caught, transitive ones flowing through substituted
/// results are masked). `SelectiveExecute` opts the State (db/redis) channel
/// into running the REAL boundary during replay (the TOTAL derivative — a
/// shadow event is recorded and the post-hoc tally classifies
/// [`crate::DivergenceKind::ValueDiverged`] on a value diff).
///
/// With `DEJA_POLICY` unset the policy is `AllLookup` and every
/// [`DejaHook::execute_mode`] returns [`ExecuteMode::Lookup`], so behavior is
/// byte-identical to before this enum existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    /// Full-mock: every boundary resolves from the recorded lookup table.
    #[default]
    AllLookup,
    /// Run the REAL State (db/redis) boundary during replay; lookup elsewhere.
    SelectiveExecute,
}

impl Policy {
    /// Parse a `DEJA_POLICY` value. Unrecognized / empty values fall back to the
    /// default ([`Policy::AllLookup`]) so a typo can never silently flip a run
    /// into the (more invasive) execute path. Accepts a few spellings for
    /// ergonomics (`all_lookup`/`all-lookup`/`lookup`, `selective_execute`/
    /// `selective-execute`/`execute`), case-insensitively.
    pub fn from_env_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "selective_execute" | "selective-execute" | "selectiveexecute" | "execute" => {
                Policy::SelectiveExecute
            }
            "all_lookup" | "all-lookup" | "alllookup" | "lookup" | "" => Policy::AllLookup,
            other => {
                eprintln!(
                    "deja: unknown DEJA_POLICY='{other}', expected all_lookup|selective_execute; \
                     defaulting to all_lookup"
                );
                Policy::AllLookup
            }
        }
    }

    /// Resolve the policy from the `DEJA_POLICY` environment variable, defaulting
    /// to [`Policy::AllLookup`] when unset or empty.
    pub fn from_env() -> Self {
        std::env::var("DEJA_POLICY")
            .ok()
            .filter(|v| !v.is_empty())
            .map(|v| Policy::from_env_value(&v))
            .unwrap_or_default()
    }
}

/// Per-boundary dispatch mode chosen for one replay call.
///
/// [`ExecuteMode::Lookup`] (the default) serves the call from the recorded
/// table; [`ExecuteMode::Execute`] runs the REAL boundary and shadow-records the
/// result. A hook returns `Execute` only when its [`Policy`] AND the boundary's
/// channel both opt in (see [`DejaHook::execute_mode`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteMode {
    /// Serve the call from the recorded lookup table (full mock).
    #[default]
    Lookup,
    /// Run the REAL boundary and shadow-record the fresh result.
    Execute,
}

/// Opaque handle returned by [`DejaHook::execute_shadow_peek`] and consumed by
/// [`DejaHook::execute_shadow_observe`].
///
/// The macro's execute-mode arm runs in two steps so the live boundary call can
/// sit BETWEEN them: `execute_shadow_peek` resolves the recorded baseline (and
/// advances the lookup-table stamper / occurrence counters in the SAME lock-step
/// the lookup path does, so numbering never drifts) WITHOUT substituting and
/// WITHOUT emitting a `Recorded` observation; the macro then runs the real
/// `self.$inner.$method()` against the live boundary; finally
/// `execute_shadow_observe` stamps the real result onto the carried
/// [`ObservedCall`] (provenance [`Provenance::ExecuteShadow`]) and emits it.
///
/// The token holds the fully-built `ObservedCall` with `observed_result` left
/// `None`; `observe` fills it. It is intentionally opaque to the macro — the
/// macro only moves it from `peek` into `observe`.
pub struct ExecuteShadowToken {
    /// The observation to emit once the real result is known. `observed_result`
    /// is `None` here and filled by [`DejaHook::execute_shadow_observe`].
    observed: crate::replay::ObservedCall,
}

impl ExecuteShadowToken {
    /// Build a token from a pre-resolved [`ObservedCall`]. The call should carry
    /// `provenance = Provenance::ExecuteShadow`, the resolved `recorded_result`
    /// (or `None` + `seed_gap = true` when no baseline was found), and a `None`
    /// `observed_result` (filled at observe time).
    pub fn new(observed: crate::replay::ObservedCall) -> Self {
        Self { observed }
    }

    /// Consume the token, attaching the real boundary's `observed_result`, and
    /// return the completed [`ObservedCall`] ready to be emitted.
    pub fn into_observed(mut self, observed_result: serde_json::Value) -> crate::replay::ObservedCall {
        self.observed.observed_result = Some(observed_result);
        self.observed
    }
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

    /// Decide whether a boundary call should be served from the recorded table
    /// ([`ExecuteMode::Lookup`]) or run against the REAL boundary and
    /// shadow-recorded ([`ExecuteMode::Execute`]).
    ///
    /// The default returns [`ExecuteMode::Lookup`] for every call, so a hook
    /// that does not override this (and the no-policy [`Policy::AllLookup`] path)
    /// behaves byte-identically to before this method existed. Only a
    /// replay hook running under [`Policy::SelectiveExecute`] returns `Execute`,
    /// and only for the State (db/redis) channel.
    fn execute_mode(
        &self,
        _boundary: &str,
        _trait_name: &str,
        _method_name: &str,
    ) -> ExecuteMode {
        ExecuteMode::Lookup
    }

    /// The active replay [`Policy`] for this hook. Read by the declarative
    /// decision matrix ([`crate::replay::boundary_execute_mode_for`]) for a
    /// DECLARED boundary. The default reports [`Policy::AllLookup`] so a hook that
    /// does not track a policy (record / no-op hooks) decides exactly as before
    /// this method existed — a declared boundary on an `AllLookup` hook resolves
    /// to `Substitute`/`Lookup`, byte-identical to the undeclared path.
    fn replay_policy(&self) -> Policy {
        Policy::AllLookup
    }

    /// First half of an execute-mode dispatch: resolve the recorded baseline for
    /// this call WITHOUT substituting and WITHOUT emitting a `Recorded`
    /// observation, returning a token the macro carries across the real boundary
    /// call. The token records the resolved `recorded_result` (or a seed gap when
    /// none is found) for the post-hoc value-divergence join.
    ///
    /// Implementations MUST advance the same per-call occurrence / sequence /
    /// stamper state the lookup path advances, so a run that mixes lookup and
    /// execute boundaries keeps identical numbering. The default returns `None`,
    /// meaning the hook does not support shadowing — the macro then falls back to
    /// ordinary lookup behavior, so any hook that does not override this (and the
    /// no-policy [`Policy::AllLookup`] path) is byte-identical to before this
    /// method existed.
    fn execute_shadow_peek(&self, _query: ReplayLookup<'_>) -> Option<ExecuteShadowToken> {
        None
    }

    /// Second half of an execute-mode dispatch: stamp the REAL boundary's
    /// `observed_result` onto the token's carried observation and emit it
    /// (provenance [`Provenance::ExecuteShadow`]). Called by the macro AFTER the
    /// real `self.$inner.$method()` completes. The default is a no-op.
    fn execute_shadow_observe(
        &self,
        _token: ExecuteShadowToken,
        _observed_result: serde_json::Value,
    ) {
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
        // Process-level recording state AND the per-request sampling gate. The
        // gate defaults to `true` (record) when no sampler is engaged, so a host
        // that never pushes a decision is byte-for-byte unaffected; an explicit
        // `false` for this request's correlation makes every boundary a no-op
        // (gate-before-allocation, since callers fast-path on `is_active`). Every
        // boundary — db, instrument id/time/crypto/http, redis, and the
        // `RuntimeHook::Recording` delegation — funnels through here.
        self.writer.is_active()
            && deja_context::recording_decision_for_current().unwrap_or(true)
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
        // SHADOW GUARANTEE: recover a poisoned lock instead of panicking — a
        // recording-side panic must never propagate into the real request.
        let mut guard = self
            .callsite_occurrence
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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

    /// Whether this hook is replaying recorded results (either the standalone
    /// `Replay` hook or the harness-driven `LookupReplay` hook).
    pub fn is_replay(&self) -> bool {
        matches!(self, RuntimeHook::Replay(_) | RuntimeHook::LookupReplay(_))
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

    fn execute_mode(
        &self,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
    ) -> ExecuteMode {
        match self {
            RuntimeHook::Recording(h) => h.execute_mode(boundary, trait_name, method_name),
            RuntimeHook::Replay(h) => h.execute_mode(boundary, trait_name, method_name),
            RuntimeHook::LookupReplay(h) => h.execute_mode(boundary, trait_name, method_name),
            RuntimeHook::NoOp(h) => h.execute_mode(boundary, trait_name, method_name),
        }
    }

    fn execute_shadow_peek(&self, query: ReplayLookup<'_>) -> Option<ExecuteShadowToken> {
        match self {
            RuntimeHook::Recording(h) => h.execute_shadow_peek(query),
            RuntimeHook::Replay(h) => h.execute_shadow_peek(query),
            RuntimeHook::LookupReplay(h) => h.execute_shadow_peek(query),
            RuntimeHook::NoOp(h) => h.execute_shadow_peek(query),
        }
    }

    fn execute_shadow_observe(
        &self,
        token: ExecuteShadowToken,
        observed_result: serde_json::Value,
    ) {
        match self {
            RuntimeHook::Recording(h) => h.execute_shadow_observe(token, observed_result),
            RuntimeHook::Replay(h) => h.execute_shadow_observe(token, observed_result),
            RuntimeHook::LookupReplay(h) => h.execute_shadow_observe(token, observed_result),
            RuntimeHook::NoOp(h) => h.execute_shadow_observe(token, observed_result),
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
    let policy = Policy::from_env();
    // Op-scope for execute mode: comma-separated operation/method names; entries
    // are trimmed and empties dropped. Empty/unset => the original "all State
    // executes" behavior under SelectiveExecute.
    let execute_ops: std::collections::HashSet<String> = std::env::var("DEJA_EXECUTE_OPS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    let source = crate::replay::LocalFileLookupSource::new(&table_path);
    let hook = match std::env::var("DEJA_OBSERVED_SINK").ok() {
        Some(observed_path) => match crate::replay::FileObservedSink::create(&observed_path) {
            Ok(sink) => crate::replay::LookupTableHook::from_source_with_policy(
                source,
                sink,
                policy,
                execute_ops,
            ),
            Err(err) => {
                eprintln!("deja: failed to open DEJA_OBSERVED_SINK={observed_path}: {err}");
                return None;
            }
        },
        None => crate::replay::LookupTableHook::from_source_with_policy(
            source,
            crate::replay::InMemoryObservedSink::new(),
            policy,
            execute_ops,
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
    /// DECLARED boundary semantics (declarative boundary model). Defaults to
    /// undeclared (all `None`), so a builder constructed without declarations
    /// stamps the same event it did before this slice. Populated from the
    /// [`BoundarySpec`] in `start_boundary_event_lazy` and written into the
    /// emitted [`SemanticEvent`] by [`Self::finish`].
    pub semantics: BoundarySemantics,
}

/// Best-effort extraction of the primary state KEY from a captured args payload,
/// for `read_set`/`write_set`. Returns the first string scalar in argument order
/// (the common shape: the key is the leading string arg of a redis/db call) as a
/// one-element vec. A HINT, not authoritative — multi-key ops (MGET, SADD members)
/// and non-string keys are out of scope for this heuristic and are refined
/// per-boundary later. Replay never trusts it blindly; it only ever tightens a
/// match it already made on the recorded `args`.
fn extract_primary_state_key(args: &serde_json::Value) -> Vec<String> {
    fn first_string(v: &serde_json::Value) -> Option<String> {
        match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Array(a) => a.iter().find_map(first_string),
            serde_json::Value::Object(m) => m.values().find_map(first_string),
            _ => None,
        }
    }
    first_string(args).into_iter().collect()
}

/// Stable content digest over `(args, result)`, reusing the same canonical
/// hashing as args-pairing so the value is byte-identical across binaries and is
/// never a second hash function. The cheapest dataflow hint — a write whose
/// digest matches an upstream read's is a probable read→write edge.
fn value_digest_of(args: &serde_json::Value, result: &serde_json::Value) -> u64 {
    let a = crate::replay::canonical_args_hash(args);
    let r = crate::replay::canonical_args_hash(result);
    fnv1a_u64(a, r)
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
            semantics: BoundarySemantics::undeclared(),
        }
    }

    /// Attach a structured call-site identity to the event under construction.
    pub fn with_callsite_identity(mut self, identity: CallsiteIdentity) -> Self {
        self.callsite_identity = Some(identity);
        self
    }

    /// Attach DECLARED boundary semantics so they are stamped onto the emitted
    /// event. Additive: the default ([`BoundarySemantics::undeclared`]) leaves
    /// every declared field `None`, stamping the same event as before.
    pub fn with_semantics(mut self, semantics: BoundarySemantics) -> Self {
        self.semantics = semantics;
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

        // Forward-looking handler-completeness capture. Derived purely from what
        // this boundary already exposes — its DECLARED semantics (preferred), or
        // its kind/method name (the undeclared fallback) plus args/result — so it
        // adds no I/O and changes no existing field (the args/result bytes, and
        // thus `canonical_args_hash`, are untouched). `extract_primary_state_key`
        // is KEPT: it extracts the key, it does not classify the channel/effect.
        //
        // DECLARATIVE PREFERENCE: when this boundary DECLARED its semantics, the
        // State-channel verdict reads `self.semantics.channel == Some(State)` and
        // the read/write verdict reads `self.semantics.effect == Some(Read)` —
        // replacing `is_state_channel(self.boundary)` / `is_read_op(self.method_name)`.
        // FALLBACK: an UNDECLARED boundary (channel `None`) keeps the EXACT name
        // heuristics, so the stamped `read_set`/`write_set`/`entropy_source` are
        // byte-identical to before this slice. (The db seam — a State channel —
        // declares an effect that mirrors the `is_read_op` verdict, so even the
        // declared db path produces the same sets the fallback would have.)
        let is_state = match self.semantics.channel {
            Some(_) => self.semantics.channel.as_ref() == Some(&Channel::State),
            None => crate::replay::is_state_channel(self.boundary),
        };
        let is_read = match self.semantics.channel {
            // Declared: the read verdict is the declared effect.
            Some(_) => self.semantics.effect == Some(Effect::Read),
            // Undeclared: the name heuristic.
            None => crate::replay::is_read_op(self.method_name),
        };
        let (read_set, write_set) = if is_state {
            let keys = extract_primary_state_key(&self.args);
            if is_read {
                (keys, Vec::new())
            } else {
                (Vec::new(), keys)
            }
        } else {
            (Vec::new(), Vec::new())
        };
        let value_digest = Some(value_digest_of(&self.args, &result));
        // DECLARATIVE PREFERENCE: derive the entropy source from a declared
        // `Channel::Entropy(EntropySource::*)`; an UNDECLARED boundary falls back
        // to the boundary-name heuristic (byte-identical: id→"id", time→"time").
        // The declared spelling maps to the SAME wire strings the heuristic used
        // (`Id`→"id", `Clock`→"time").
        let entropy_source = match &self.semantics.channel {
            Some(Channel::Entropy(EntropySource::Id)) => Some("id".to_string()),
            Some(Channel::Entropy(EntropySource::Clock)) => Some("time".to_string()),
            // Any other DECLARED channel (State/Egress/Entropy::Rng/Other/Unknown)
            // is not the legacy id/time entropy boundary → no entropy source.
            Some(_) => None,
            // UNDECLARED → the original boundary-name heuristic.
            None => match self.boundary {
                "id" => Some("id".to_string()),
                "time" => Some("time".to_string()),
                _ => None,
            },
        };

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
            event_schema_version: CURRENT_EVENT_SCHEMA_VERSION,
            callsite_identity: self.callsite_identity,
            provenance: Provenance::default(),
            recon: Recon::default(),
            result_image: None,
            pre_image: None,
            read_set,
            write_set,
            value_digest,
            entropy_source,
            // DECLARED semantics (declarative boundary model). All `None` for an
            // undeclared boundary (the current vendor), so the stamped event is
            // byte-identical; populated only when the macro declared them.
            channel: self.semantics.channel,
            effect: self.semantics.effect,
            strategy: self.semantics.strategy,
            raw_draw: None,
            end_timestamp_ns: Some(end_ns),
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
        // SHADOW GUARANTEE: never finalize while the thread is already unwinding.
        // If the real call panicked, this finalizer is dropped mid-unwind; running
        // `finish` (which can itself panic on serialization/locks) during an unwind
        // escalates to `abort()` and kills the whole process. Drop the event instead.
        if std::thread::panicking() {
            return;
        }
        if self.builder.is_some() {
            if let (Some(builder), Some(hook)) = (self.builder.take(), self.hook.take()) {
                let mut result = self.partial_result.clone();
                inject_body_json(&mut result, std::mem::take(&mut self.body));

                // And firewall the normal-path finalize too, so a serialization
                // panic here never escapes into the caller.
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    builder.finish(&*hook, result, self.is_error);
                }));
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

/// Whether ANY hook that consumes a boundary's args is active — the runtime
/// (replay/execute) hook OR the standalone recording hook. The boundary macro
/// uses this to decide whether to EAGERLY evaluate the args expression into an
/// owned `serde_json::Value` *before* forming the run closure.
///
/// Why eager-when-active rather than a lazy thunk: a lazy args *thunk* and the
/// run *thunk* are handed to `dispatch` simultaneously, so a thunk that borrows
/// a value (e.g. an http boundary whose `args`/`correlation` borrow `&request`)
/// cannot coexist with a run thunk that MOVES that same value (the body sends
/// `request`). Evaluating args to an owned value first ends the borrow before
/// the move. Gating on this keeps the inactive path zero-cost: when nothing is
/// capturing, the macro never runs the args expression at all.
///
/// This mirrors `dispatch`'s own fast-path test exactly: `dispatch` evaluates
/// args only when the runtime hook is active, and `record_only_path` (its
/// inactive-runtime branch) evaluates args only when the recording hook is
/// active — so this disjunction is precisely "will args be consumed".
pub fn capture_is_active() -> bool {
    global_runtime_hook_from_env()
        .map(|hook| hook.is_active())
        .unwrap_or(false)
        || global_hook_from_env()
            .map(|hook| hook.is_active())
            .unwrap_or(false)
}

/// Whether this process is replaying recorded results (`DEJA_MODE=replay`).
///
/// The library owns the record-vs-replay determination; instrumented call
/// sites ask this instead of inspecting `DEJA_MODE` directly, so the recording
/// surface carries no replay-mode env logic of its own. Mirrors
/// [`capture_is_active`].
pub fn replay_is_active() -> bool {
    global_runtime_hook_from_env()
        .map(|hook| hook.is_replay())
        .unwrap_or(false)
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
///
/// Carries the matching tuple (boundary/trait/method) AND the optional DECLARED
/// semantics ([`channel`](Self::channel)/[`effect`](Self::effect)/
/// [`strategy`](Self::strategy)). The declarations default to `None` (undeclared)
/// so the legacy [`Self::new`] constructor — and every current vendor wrapper —
/// keeps the exact behavior it had before this slice (the runtime falls back to
/// string heuristics).
#[derive(Debug, Clone)]
pub struct BoundarySpec {
    pub boundary: &'static str,
    pub trait_name: &'static str,
    pub method_name: &'static str,
    /// DECLARED channel, or `None` (undeclared → heuristic fallback).
    pub channel: Option<Channel>,
    /// DECLARED effect, or `None` (undeclared → heuristic fallback).
    pub effect: Option<Effect>,
    /// DECLARED per-op strategy override (REQUIRED for declared RMW), or `None`.
    pub strategy: Option<Strategy>,
}

impl BoundarySpec {
    /// Construct an UNDECLARED boundary spec (no semantics). Kept identical to the
    /// pre-declarative signature so every existing call site compiles unchanged
    /// and behaves byte-identically (the runtime uses the string heuristics).
    pub const fn new(
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
    ) -> Self {
        Self {
            boundary,
            trait_name,
            method_name,
            channel: None,
            effect: None,
            strategy: None,
        }
    }

    /// Construct a boundary spec carrying DECLARED semantics. Used by the
    /// declarative macro path; the matching tuple is unchanged, only the declared
    /// fields are populated.
    pub fn with_semantics(
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
        semantics: BoundarySemantics,
    ) -> Self {
        Self {
            boundary,
            trait_name,
            method_name,
            channel: semantics.channel,
            effect: semantics.effect,
            strategy: semantics.strategy,
        }
    }

    /// The declared semantics carried by this spec, as a [`BoundarySemantics`].
    pub fn semantics(&self) -> BoundarySemantics {
        BoundarySemantics {
            channel: self.channel.clone(),
            effect: self.effect,
            strategy: self.strategy,
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
///
/// Deprecated: replay branching now lives behind [`dispatch`], which calls this
/// internally. Direct callers (the pre-`dispatch` macro shape) are being
/// removed; new code routes through [`dispatch`] so the macro names no
/// replay-only operation.
#[deprecated(
    since = "0.1.0",
    note = "internal replay seam; route boundary instrumentation through `dispatch` instead"
)]
#[track_caller]
pub fn replay_boundary(
    caller: &'static Location<'static>,
    spec: &BoundarySpec,
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

/// Resolve whether an instrumented boundary should run in execute mode (the
/// boundary-macro analogue of the delegate macro's [`DejaHook::execute_mode`]
/// call). Returns [`ExecuteMode::Lookup`] when no hook is configured / inactive,
/// so a run with no `DEJA_POLICY` set is byte-identical to before.
///
/// DECLARATIVE PATH: when `spec` carries declared semantics, the decision is the
/// pure matrix [`crate::replay::decide_strategy`] mapped to an [`ExecuteMode`].
/// UNDECLARED PATH: when `spec` declares nothing (every current vendor wrapper),
/// `decide_strategy` falls back to the hook's existing string-heuristic
/// [`DejaHook::execute_mode`], so the decision is byte-identical to before this
/// slice. The hook is consulted for the active [`Policy`] either way.
///
/// Deprecated: the execute/shadow lifecycle now lives inside [`dispatch`].
#[deprecated(
    since = "0.1.0",
    note = "internal execute-mode seam; route boundary instrumentation through `dispatch` instead"
)]
pub fn boundary_execute_mode(spec: &BoundarySpec) -> ExecuteMode {
    match global_runtime_hook_from_env() {
        Some(hook) if hook.is_active() => {
            crate::replay::boundary_execute_mode_for(&*hook, spec)
        }
        _ => ExecuteMode::Lookup,
    }
}

/// First half of an execute-mode dispatch for an instrumented boundary: peek the
/// recorded baseline WITHOUT substituting and WITHOUT emitting a `Recorded`
/// observation, returning a token to carry across the live block. Mirrors
/// [`DejaHook::execute_shadow_peek`]; returns `None` when no hook is configured,
/// the hook is inactive, or the hook does not support shadowing (so the macro
/// falls back to lookup behavior).
///
/// Deprecated: the execute/shadow lifecycle now lives inside [`dispatch`].
#[deprecated(
    since = "0.1.0",
    note = "internal execute-shadow seam; route boundary instrumentation through `dispatch` instead"
)]
#[track_caller]
pub fn execute_shadow_peek_boundary(
    caller: &'static Location<'static>,
    spec: &BoundarySpec,
    args: &serde_json::Value,
    identity: Option<&CallsiteIdentity>,
) -> Option<ExecuteShadowToken> {
    let hook = global_runtime_hook_from_env()?;
    if !hook.is_active() {
        return None;
    }
    hook.execute_shadow_peek(ReplayLookup {
        boundary: spec.boundary,
        trait_name: spec.trait_name,
        method_name: spec.method_name,
        args,
        callsite_identity: identity,
        caller_location: Some(caller),
    })
}

/// Second half of an execute-mode dispatch for an instrumented boundary: emit
/// the shadow observation with the real block's `observed_result`. Mirrors
/// [`DejaHook::execute_shadow_observe`]. No-op when no hook is configured.
///
/// Deprecated: the execute/shadow lifecycle now lives inside [`dispatch`].
#[deprecated(
    since = "0.1.0",
    note = "internal execute-shadow seam; route boundary instrumentation through `dispatch` instead"
)]
pub fn execute_shadow_observe_boundary(
    token: ExecuteShadowToken,
    observed_result: serde_json::Value,
) {
    if let Some(hook) = global_runtime_hook_from_env() {
        hook.execute_shadow_observe(token, observed_result);
    }
}

// ---------------------------------------------------------------------------
// The single boundary-crossing seam (`dispatch`)
// ---------------------------------------------------------------------------

/// Matching-only inputs the boundary macro hands to [`dispatch`].
///
/// This carries ONLY what is needed to ADDRESS / MATCH a crossing against a
/// recording: the boundary tuple ([`BoundarySpec`]), the call-site
/// [`CallsiteIdentity`] (with its `occurrence` allocated exactly once by the
/// caller), and the `#[track_caller]` invocation [`Location`]. It deliberately
/// carries NO classification verdict (`boundary_kind` / `effect_hint` /
/// `strategy_hint`): those are written into the recorded event only and read
/// by replay off the tape, never fed into this live decision seam (design §3,
/// decoupling blocker #2).
pub struct CrossingObservation {
    /// Boundary tuple (boundary / trait / method) — to match a recording.
    pub spec: BoundarySpec,
    /// Structured call-site identity; `occurrence` allocated ONCE by the caller
    /// and reused for both the replay lookup and the recorded event.
    ///
    /// Held BY VALUE (not borrowed): the boxed-future macro shape returns the
    /// `dispatch_async` future from a sync fn, so the future — and everything it
    /// captures, including `obs` — must own its data rather than borrow a local.
    /// The internal seams that want `Option<&CallsiteIdentity>` borrow it from
    /// here; nothing is cloned on the hot lookup/execute paths.
    pub identity: CallsiteIdentity,
    /// `#[track_caller]` invocation address for legacy file:line:column matching.
    pub caller: &'static Location<'static>,
    /// Explicit correlation id for the recorded event, when the call site set
    /// one (`correlation = ...`). `None` falls back to the ambient
    /// `deja_context` correlation inside the record seam — the same fallback the
    /// pre-`dispatch` macro used. This is a recording address, not a replay
    /// verdict (it is the test-case isolation key, project memory).
    pub correlation_id: Option<String>,
}

impl CrossingObservation {
    /// Build a crossing observation from its matching inputs (no explicit
    /// correlation — the record seam falls back to the ambient one).
    pub fn new(
        spec: BoundarySpec,
        identity: CallsiteIdentity,
        caller: &'static Location<'static>,
    ) -> Self {
        Self {
            spec,
            identity,
            caller,
            correlation_id: None,
        }
    }

    /// Build a crossing observation with an explicit correlation id.
    pub fn with_correlation(
        spec: BoundarySpec,
        identity: CallsiteIdentity,
        caller: &'static Location<'static>,
        correlation_id: Option<String>,
    ) -> Self {
        Self {
            spec,
            identity,
            caller,
            correlation_id,
        }
    }
}

/// The ONE replay-facing seam the boundary macro calls.
///
/// Recording captures raw observations; replay performs all interpretation
/// (design §1). This function owns ALL of the run/skip/shadow/record control
/// flow internally, so the macro emits a single mode-agnostic shape and names
/// ZERO replay-only operations. Removing every replay hook would leave this a
/// plain "run + (maybe) record" function and change the macro's emitted tokens
/// by zero.
///
/// The four closures the macro supplies:
/// - `args` — LAZY structured-args serialization. NOT evaluated when the hook
///   is inactive, preserving the zero-overhead fast path
///   (`start_boundary_event_lazy`'s laziness, design §3 / major #5).
/// - `run` — the real boundary block.
/// - `reconstruct` — turns a recorded JSON value back into `T` on a lookup hit,
///   returning `None` to fall through to live execution. This is the
///   type-erased deserializer the macro provides ONCE (design §3): the
///   `DeserializeOwned` capability lives in THIS closure, never as a bound on
///   `dispatch` itself, so record-only return types compile unaffected. In
///   record-only mode the macro passes a `|_| None` closure that is never
///   reached.
/// - `extract` — the lossless result image AND the `is_error` flag, as the
///   existing record/shadow seams expect. Fidelity is fixed by the macro, not
///   chosen by a replay flag.
///
/// Internally this is implemented in terms of the (now-deprecated) seams
/// [`replay_boundary`] / [`boundary_execute_mode`] / `execute_shadow_*` /
/// [`start_boundary_event_lazy`] / [`finish_boundary_event`], so the behavior
/// is BIT-IDENTICAL to the pre-`dispatch` macro shape (design §6 Step 3).
///
/// Control flow (all owned here, never named by the macro):
/// - **inactive hook** → call `run()`, return; NEVER evaluate `args`.
/// - **execute/shadow** (only under `Policy::SelectiveExecute` for a State
///   boundary; inert by default) → run `run()`, then shadow-observe
///   `extract(&out)` against the peeked baseline and SUPPRESS the normal record.
/// - **lookup hit** → reconstruct the recorded value WITHOUT calling `run()`;
///   on `reconstruct` returning `None` (deserialize failure / `Err` sentinel),
///   FALL THROUGH to live `run()` + record (preserves the V1 "skip error arms"
///   policy).
/// - **record / no-op / lookup miss** → evaluate `args` lazily, `run()`, record
///   via the existing event seam (provenance-free event).
// NOTE: no `#[track_caller]` — the authoritative invocation address is
// `obs.caller`, captured by the macro at its own `#[track_caller]` entry and
// threaded through `CrossingObservation`. The internal seams receive it
// explicitly, so their own `Location::caller()` is never consulted.
#[allow(deprecated)] // implemented in terms of the deprecated seams it subsumes
pub fn dispatch<T, A, F, C, R>(
    obs: CrossingObservation,
    args: A,
    run: F,
    reconstruct: C,
    extract: R,
) -> T
where
    A: FnOnce() -> serde_json::Value,
    F: FnOnce() -> T,
    C: FnOnce(serde_json::Value) -> Option<T>,
    R: Fn(&T) -> (serde_json::Value, bool),
{
    // Fast path: no hook configured / inactive. NEVER evaluate `args` — this is
    // the zero-overhead inactive path the macro relied on via the lazy event
    // seam. `global_runtime_hook_from_env` is the resolver the replay/execute
    // seams use; when it is absent or inactive there is nothing to match or
    // shadow, and recording (which resolves through `global_hook_from_env`)
    // would itself be inactive too.
    let runtime_active = global_runtime_hook_from_env()
        .map(|hook| hook.is_active())
        .unwrap_or(false);

    if !runtime_active {
        // Recording may still be configured independently of the runtime hook
        // (standalone record path resolves through `global_hook_from_env`). The
        // lazy record seam itself short-circuits (without evaluating `args`)
        // when the recording hook is absent/inactive, so this stays zero-cost
        // when nothing is recording.
        return record_only_path(obs, args, run, extract);
    }

    // Bind the structured args ONCE (lazily evaluated here, after we know a
    // runtime hook is active). The same value feeds the execute peek, the replay
    // lookup, and the eventual recording, exactly as the pre-`dispatch` macro
    // did.
    let boundary_args: serde_json::Value = args();

    // EXECUTE/SHADOW path (total derivative). Inert under the default
    // `Policy::AllLookup` (`boundary_execute_mode` returns `Lookup`).
    if matches!(boundary_execute_mode(&obs.spec), ExecuteMode::Execute) {
        if let Some(token) = execute_shadow_peek_boundary(
            obs.caller,
            &obs.spec,
            &boundary_args,
            Some(&obs.identity),
        ) {
            // Run the REAL block — no substitution.
            let out = run();
            // SHADOW GUARANTEE: serialization + the shadow emit run AFTER the
            // real block produced `out`; a panic here just drops the shadow
            // observation. Suppress the normal record (we return directly).
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let (result_json, _) = extract(&out);
                execute_shadow_observe_boundary(token, result_json);
            }));
            return out;
        }
    }

    // LOOKUP-HIT path. `replay_boundary` returns None in record / no-op /
    // AllLookup-miss, so this is inert there.
    if let Some(recorded) =
        replay_boundary(obs.caller, &obs.spec, &boundary_args, Some(&obs.identity))
    {
        if let Some(replayed) = reconstruct(recorded) {
            return replayed;
        }
        // Deserialize failure / Err sentinel → fall through to live execution
        // (the V1 "skip error arms" policy). Control continues to the record
        // path below.
    }

    // RECORD / lookup-miss path. Args already bound above; reuse them.
    let event = start_boundary_event_lazy(
        obs.caller,
        obs.spec,
        obs.correlation_id,
        move || boundary_args,
        Some(obs.identity),
    );
    let out = run();
    finish_boundary_event(event, &out, &extract);
    out
}

/// The inactive / pure-record branch of [`dispatch`].
///
/// Split out so the inactive fast path stays trivially the same shape as the
/// pre-`dispatch` lazy record seam: `args` is handed to
/// `start_boundary_event_lazy`, which evaluates it ONLY when the recording hook
/// is active. When nothing is recording, the hook short-circuits before `args`
/// runs, so the inactive path serializes no arguments.
fn record_only_path<T, A, F, R>(
    obs: CrossingObservation,
    args: A,
    run: F,
    extract: R,
) -> T
where
    A: FnOnce() -> serde_json::Value,
    F: FnOnce() -> T,
    R: Fn(&T) -> (serde_json::Value, bool),
{
    let event = start_boundary_event_lazy(
        obs.caller,
        obs.spec,
        obs.correlation_id,
        args,
        Some(obs.identity),
    );
    let out = run();
    finish_boundary_event(event, &out, &extract);
    out
}

/// Async twin of [`dispatch`] for `async fn` (and boxed-future) boundaries.
///
/// Identical control flow to [`dispatch`]; the only difference is that `run`
/// yields a future that is awaited to produce `T`, so the recording / shadow
/// observation happens AFTER the real future resolves. The boundary macro emits
/// this for `async fn` bodies and for `future = "boxed"` bodies (wrapping the
/// returned `T` in `Box::pin`). See [`dispatch`] for the full rationale.
#[allow(deprecated)] // implemented in terms of the deprecated seams it subsumes
pub async fn dispatch_async<T, A, Fut, F, C, R>(
    obs: CrossingObservation,
    args: A,
    run: F,
    reconstruct: C,
    extract: R,
) -> T
where
    A: FnOnce() -> serde_json::Value,
    Fut: std::future::Future<Output = T>,
    F: FnOnce() -> Fut,
    C: FnOnce(serde_json::Value) -> Option<T>,
    R: Fn(&T) -> (serde_json::Value, bool),
{
    let runtime_active = global_runtime_hook_from_env()
        .map(|hook| hook.is_active())
        .unwrap_or(false);

    if !runtime_active {
        // Inactive / pure-record: same lazy-args record seam as `dispatch`, but
        // awaiting the real future.
        let event = start_boundary_event_lazy(
            obs.caller,
            obs.spec,
            obs.correlation_id,
            args,
            Some(obs.identity),
        );
        let out = run().await;
        finish_boundary_event(event, &out, &extract);
        return out;
    }

    let boundary_args: serde_json::Value = args();

    // EXECUTE/SHADOW path. Inert under the default `Policy::AllLookup`.
    if matches!(boundary_execute_mode(&obs.spec), ExecuteMode::Execute) {
        if let Some(token) =
            execute_shadow_peek_boundary(obs.caller, &obs.spec, &boundary_args, Some(&obs.identity))
        {
            let out = run().await;
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let (result_json, _) = extract(&out);
                execute_shadow_observe_boundary(token, result_json);
            }));
            return out;
        }
    }

    // LOOKUP-HIT path.
    if let Some(recorded) =
        replay_boundary(obs.caller, &obs.spec, &boundary_args, Some(&obs.identity))
    {
        if let Some(replayed) = reconstruct(recorded) {
            return replayed;
        }
        // Deserialize failure / Err sentinel → fall through to live execution.
    }

    // RECORD / lookup-miss path.
    let event = start_boundary_event_lazy(
        obs.caller,
        obs.spec,
        obs.correlation_id,
        move || boundary_args,
        Some(obs.identity),
    );
    let out = run().await;
    finish_boundary_event(event, &out, &extract);
    out
}

// ---------------------------------------------------------------------------
// Hook-parameterized seam (`dispatch_with_hook` / `_async`) for the delegate path
// ---------------------------------------------------------------------------

/// Matching-only inputs for the per-instance delegate seam.
///
/// The delegate path (the `delegate_<trait>!` macros) records against a hook
/// INJECTED into the wrapper (`self.$hook`), not the process-global hook the
/// boundary macro's [`dispatch`] uses. So the delegate's seam carries an
/// explicit `&dyn DejaHook` plus the matching tuple. Like [`CrossingObservation`]
/// it carries NO classification verdict — only what is needed to match a
/// recording and stamp the event.
pub struct DelegateObservation<'a> {
    /// The injected hook to record / replay through.
    pub hook: &'a dyn DejaHook,
    /// Boundary tag (e.g. `"storage"`).
    pub boundary: &'static str,
    /// Trait name at the boundary.
    pub trait_name: &'static str,
    /// Method name being invoked.
    pub method_name: &'static str,
    /// `#[track_caller]` invocation address.
    pub caller: &'static Location<'static>,
    /// Call-site identity; `occurrence` allocated ONCE by the caller.
    pub identity: CallsiteIdentity,
    /// Decorator `self`/inner type context recorded on the event.
    pub receiver: Option<serde_json::Value>,
}

/// Sync per-instance delegate seam. Collapses the delegate macro's three
/// duplicated arms (execute / replay / record) into ONE call that owns all of
/// the run/skip/shadow/record control flow, so the delegate macro names no
/// replay-only operation.
///
/// Identical semantics to the pre-`dispatch` delegate expansion, but routed
/// through the injected `obs.hook` rather than the global hook. See [`dispatch`]
/// for the control-flow contract; the only difference is the hook source and
/// that the record path goes through [`EventBuilder`] directly (to attach the
/// `receiver`) inside a panic firewall.
///
/// `args` is the ALREADY-SERIALIZED args image. Unlike [`dispatch`], the delegate
/// keeps its `if !is_active` fast path in the macro (so args are still NOT
/// serialized when the hook is inactive — the delegate's async methods desugar to
/// a returned `Box::pin` future into which both args and the run block would have
/// to be moved, which forbids a borrowing args thunk; the macro therefore
/// computes args eagerly only on the active path, exactly as before). The
/// fast-path return is the only delegate-side branch that remains, and it is a
/// recording gate, not a replay-only operation.
pub fn dispatch_with_hook<T, F, C, R>(
    obs: DelegateObservation<'_>,
    args: serde_json::Value,
    run: F,
    reconstruct: C,
    extract: R,
) -> T
where
    F: FnOnce() -> T,
    C: FnOnce(serde_json::Value) -> Option<T>,
    R: Fn(&T) -> (serde_json::Value, bool),
{
    let boundary_args = args;
    let lookup = || ReplayLookup {
        boundary: obs.boundary,
        trait_name: obs.trait_name,
        method_name: obs.method_name,
        args: &boundary_args,
        callsite_identity: Some(&obs.identity),
        caller_location: Some(obs.caller),
    };

    // EXECUTE/SHADOW path. Inert under the default `Policy::AllLookup`.
    if matches!(
        obs.hook
            .execute_mode(obs.boundary, obs.trait_name, obs.method_name),
        ExecuteMode::Execute
    ) {
        if let Some(token) = obs.hook.execute_shadow_peek(lookup()) {
            let out = run();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let (result_json, _) = extract(&out);
                obs.hook.execute_shadow_observe(token, result_json);
            }));
            return out;
        }
    }

    // LOOKUP-HIT path.
    if let Some(recorded) = obs.hook.try_replay_with_context(lookup()) {
        if let Some(replayed) = reconstruct(recorded) {
            return replayed;
        }
        // Deserialize failure → fall through to live execution.
    }

    // RECORD / lookup-miss path. Build the event through `EventBuilder` (to carry
    // the receiver) inside a firewall so a recording panic never reaches the call.
    let builder = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        EventBuilder::start_with_receiver(
            obs.hook,
            obs.boundary,
            obs.trait_name,
            obs.method_name,
            obs.caller,
            obs.receiver,
            boundary_args,
        )
        .with_callsite_identity(obs.identity)
    }))
    .ok();
    let out = run();
    if let Some(builder) = builder {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let (result_json, is_error) = extract(&out);
            builder.finish(obs.hook, result_json, is_error);
        }));
    }
    out
}

/// Async twin of [`dispatch_with_hook`] for `async` delegate methods (which the
/// macro returns as `Pin<Box<dyn Future>>`). The `run` thunk yields the inner
/// future; the macro wraps the whole call in `Box::pin`. `args` is the
/// already-serialized image (see [`dispatch_with_hook`] for why the delegate
/// computes it eagerly on the active path).
pub async fn dispatch_async_with_hook<T, Fut, F, C, R>(
    obs: DelegateObservation<'_>,
    args: serde_json::Value,
    run: F,
    reconstruct: C,
    extract: R,
) -> T
where
    Fut: std::future::Future<Output = T>,
    F: FnOnce() -> Fut,
    C: FnOnce(serde_json::Value) -> Option<T>,
    R: Fn(&T) -> (serde_json::Value, bool),
{
    let boundary_args = args;
    let lookup = || ReplayLookup {
        boundary: obs.boundary,
        trait_name: obs.trait_name,
        method_name: obs.method_name,
        args: &boundary_args,
        callsite_identity: Some(&obs.identity),
        caller_location: Some(obs.caller),
    };

    if matches!(
        obs.hook
            .execute_mode(obs.boundary, obs.trait_name, obs.method_name),
        ExecuteMode::Execute
    ) {
        if let Some(token) = obs.hook.execute_shadow_peek(lookup()) {
            let out = run().await;
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let (result_json, _) = extract(&out);
                obs.hook.execute_shadow_observe(token, result_json);
            }));
            return out;
        }
    }

    if let Some(recorded) = obs.hook.try_replay_with_context(lookup()) {
        if let Some(replayed) = reconstruct(recorded) {
            return replayed;
        }
    }

    let builder = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        EventBuilder::start_with_receiver(
            obs.hook,
            obs.boundary,
            obs.trait_name,
            obs.method_name,
            obs.caller,
            obs.receiver,
            boundary_args,
        )
        .with_callsite_identity(obs.identity)
    }))
    .ok();
    let out = run().await;
    if let Some(builder) = builder {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let (result_json, is_error) = extract(&out);
            builder.finish(obs.hook, result_json, is_error);
        }));
    }
    out
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

    // SHADOW GUARANTEE: `args()` runs user `Serialize`/`Debug` impls and the
    // builder setup may touch poisoned locks — either could panic. Catch it so a
    // recording panic can NEVER unwind into the real request; on panic we simply
    // skip recording this boundary and the caller proceeds with the real call.
    let semantics = spec.semantics();
    let event = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut event = EventBuilder::start_with_correlation_id(
            &*hook,
            spec.boundary,
            spec.trait_name,
            spec.method_name,
            caller,
            correlation_id,
            args(),
        )
        .with_semantics(semantics);
        if let Some(identity) = identity {
            event = event.with_callsite_identity(identity);
        }
        event
    }))
    .ok()?;
    Some((hook, event))
}

pub fn finish_boundary_event<T, R>(
    event: Option<(Arc<RecordingHook>, EventBuilder)>,
    output: &T,
    result: R,
) where
    R: FnOnce(&T) -> (serde_json::Value, bool),
{
    // SHADOW GUARANTEE: result serialization + the sink enqueue run AFTER the real
    // call already produced `output`. Catch any panic so a recording failure can
    // never turn a successful request into a failed one — it just drops the event.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some((hook, event)) = event {
            let (response, is_error) = result(output);
            event.finish(&*hook, response, is_error);
        }
    }));
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

    // -----------------------------------------------------------------------
    // Declarative boundary model — enum serde round-trips + Unknown tolerance.
    // -----------------------------------------------------------------------

    #[test]
    fn declared_enums_round_trip_through_serde() {
        // Channel (incl. the data-carrying Entropy variants).
        for c in [
            Channel::State,
            Channel::Egress,
            Channel::Entropy(EntropySource::Clock),
            Channel::Entropy(EntropySource::Id),
            Channel::Entropy(EntropySource::Rng),
            Channel::Entropy(EntropySource::Other("custom".to_string())),
        ] {
            let s = serde_json::to_string(&c).expect("ser");
            let back: Channel = serde_json::from_str(&s).expect("de");
            assert_eq!(c, back, "Channel round-trip for {c:?}");
        }
        // Effect.
        for e in [
            Effect::Read,
            Effect::Write,
            Effect::ReadModifyWrite,
            Effect::Append,
            Effect::VolatileRead,
            Effect::Opaque,
        ] {
            let s = serde_json::to_string(&e).expect("ser");
            let back: Effect = serde_json::from_str(&s).expect("de");
            assert_eq!(e, back, "Effect round-trip for {e:?}");
        }
        // Strategy.
        for st in [
            Strategy::Lookup,
            Strategy::SeedAndExecute,
            Strategy::LookupAndSeed,
        ] {
            let s = serde_json::to_string(&st).expect("ser");
            let back: Strategy = serde_json::from_str(&s).expect("de");
            assert_eq!(st, back, "Strategy round-trip for {st:?}");
        }
    }

    /// FORWARD-COMPAT: an OLD reader must tolerate a NEW tape — an unknown
    /// discriminant deserializes to the `Unknown` variant rather than failing.
    #[test]
    fn declared_enums_tolerate_unknown_discriminants() {
        let c: Channel = serde_json::from_str("\"some_future_channel\"").expect("de");
        assert_eq!(c, Channel::Unknown);
        let e: Effect = serde_json::from_str("\"some_future_effect\"").expect("de");
        assert_eq!(e, Effect::Unknown);
        let s: Strategy = serde_json::from_str("\"some_future_strategy\"").expect("de");
        assert_eq!(s, Strategy::Unknown);
    }

    /// The declared fields are ADDITIVE on `SemanticEvent`: absent on an old tape
    /// they default to `None` and round-trip cleanly; present they survive.
    #[test]
    fn semantic_event_carries_declared_fields_additively() {
        // Old tape (no declared fields) → all None.
        let old = serde_json::json!({
            "global_sequence": 0, "request_sequence": 0, "correlation_id": null,
            "timestamp_ns": 0, "boundary": "redis", "trait_name": "T", "method_name": "get",
            "call_file": "x.rs", "call_line": 1, "call_column": 1,
            "args": [], "result": null, "is_error": false, "duration_us": 0
        });
        let ev: SemanticEvent = serde_json::from_value(old).expect("de old tape");
        assert_eq!(ev.channel, None);
        assert_eq!(ev.effect, None);
        assert_eq!(ev.strategy, None);

        // A declared event round-trips its declarations.
        let mut ev = ev;
        ev.channel = Some(Channel::State);
        ev.effect = Some(Effect::ReadModifyWrite);
        ev.strategy = Some(Strategy::SeedAndExecute);
        let json = serde_json::to_string(&ev).expect("ser");
        let back: SemanticEvent = serde_json::from_str(&json).expect("de");
        assert_eq!(back.channel, Some(Channel::State));
        assert_eq!(back.effect, Some(Effect::ReadModifyWrite));
        assert_eq!(back.strategy, Some(Strategy::SeedAndExecute));
    }

    /// `BoundarySpec::new` stays UNDECLARED (additive); `with_semantics` carries
    /// the declarations and `semantics()` reflects them.
    #[test]
    fn boundary_spec_new_is_undeclared_with_semantics_carries() {
        let plain = BoundarySpec::new("redis", "T", "get");
        assert!(plain.semantics().is_undeclared());

        let declared = BoundarySpec::with_semantics(
            "redis",
            "T",
            "incr",
            BoundarySemantics {
                channel: Some(Channel::State),
                effect: Some(Effect::ReadModifyWrite),
                strategy: Some(Strategy::SeedAndExecute),
            },
        );
        let s = declared.semantics();
        assert!(!s.is_undeclared());
        assert_eq!(s.channel, Some(Channel::State));
        assert_eq!(s.effect, Some(Effect::ReadModifyWrite));
        assert_eq!(s.strategy, Some(Strategy::SeedAndExecute));
    }

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

    /// DECLARATIVE BOUNDARY MODEL — `EventBuilder::finish` PREFERS the declared
    /// semantics for the read_set/write_set/channel/effect/entropy_source it stamps,
    /// and FALLS BACK to the name heuristics byte-identically when undeclared.
    #[test]
    fn finish_prefers_declared_semantics_with_heuristic_fallback() {
        use crate::{Channel, Effect, EntropySource};

        fn record_one(
            dir: &std::path::Path,
            boundary: &'static str,
            method: &'static str,
            args: serde_json::Value,
            semantics: Option<BoundarySemantics>,
        ) -> SemanticEvent {
            let hook = RecordingHook::new(dir).expect("hook");
            let caller = Location::caller();
            let mut b = EventBuilder::start(&hook, boundary, "T", method, caller, args);
            if let Some(s) = semantics {
                b = b.with_semantics(s);
            }
            b.finish(&hook, serde_json::json!("0.10"), false);
            drop(hook);
            let events = read_events(dir).expect("read");
            assert_eq!(events.len(), 1);
            events.into_iter().next().unwrap()
        }

        // (1) DECLARED State + Read: channel/effect are stamped from the
        // declaration and DRIVE the read_set (read → read_set, empty write_set).
        let d1 = tempfile::tempdir().unwrap();
        let declared_read = record_one(
            d1.path(),
            "redis",
            "find_thing", // is_read_op true too, but the DECLARATION is what's used
            serde_json::json!(["settlement_rate_default"]),
            Some(BoundarySemantics {
                channel: Some(Channel::State),
                effect: Some(Effect::Read),
                strategy: None,
            }),
        );
        assert_eq!(declared_read.channel, Some(Channel::State));
        assert_eq!(declared_read.effect, Some(Effect::Read));
        assert_eq!(declared_read.read_set, vec!["settlement_rate_default"]);
        assert!(declared_read.write_set.is_empty());

        // (1b) The declaration OVERRIDES the name heuristic: a write-NAMED method
        // declared as Read still produces a read_set (proves declaration wins).
        let d1b = tempfile::tempdir().unwrap();
        let declared_read_write_name = record_one(
            d1b.path(),
            "redis",
            "set_thing", // is_read_op == FALSE; declaration says Read
            serde_json::json!(["k"]),
            Some(BoundarySemantics {
                channel: Some(Channel::State),
                effect: Some(Effect::Read),
                strategy: None,
            }),
        );
        assert_eq!(declared_read_write_name.read_set, vec!["k"]);
        assert!(declared_read_write_name.write_set.is_empty());

        // (2) DECLARED State + Write: write → write_set, empty read_set.
        let d2 = tempfile::tempdir().unwrap();
        let declared_write = record_one(
            d2.path(),
            "db",
            "select_one", // is_read_op false; declaration says Write
            serde_json::json!({ "operation": "select_one" }),
            Some(BoundarySemantics {
                channel: Some(Channel::State),
                effect: Some(Effect::Write),
                strategy: None,
            }),
        );
        assert_eq!(declared_write.channel, Some(Channel::State));
        assert_eq!(declared_write.effect, Some(Effect::Write));
        assert!(declared_write.read_set.is_empty());
        assert_eq!(declared_write.write_set, vec!["select_one"]);

        // (3) DECLARED Entropy(Id)/Entropy(Clock): entropy_source derives from the
        // declared channel ("id"/"time"), byte-identical to the legacy strings.
        let d3 = tempfile::tempdir().unwrap();
        let declared_id = record_one(
            d3.path(),
            "some_id_wrapper", // NOT the legacy "id" boundary name
            "next_id",
            serde_json::json!([]),
            Some(BoundarySemantics {
                channel: Some(Channel::Entropy(EntropySource::Id)),
                effect: None,
                strategy: None,
            }),
        );
        assert_eq!(declared_id.entropy_source.as_deref(), Some("id"));
        // Entropy is NOT State → no read/write set.
        assert!(declared_id.read_set.is_empty() && declared_id.write_set.is_empty());

        let d3b = tempfile::tempdir().unwrap();
        let declared_clock = record_one(
            d3b.path(),
            "some_clock_wrapper",
            "now",
            serde_json::json!([]),
            Some(BoundarySemantics {
                channel: Some(Channel::Entropy(EntropySource::Clock)),
                effect: None,
                strategy: None,
            }),
        );
        assert_eq!(declared_clock.entropy_source.as_deref(), Some("time"));

        // (4) UNDECLARED FALLBACK — byte-identical to the name heuristic. Record
        // the SAME redis read both declared-Read and undeclared, and assert the
        // undeclared one matches the heuristic exactly. The redis read NAME is a
        // read verb, so the heuristic also yields a read_set: identical sets.
        let d4 = tempfile::tempdir().unwrap();
        let undeclared_read = record_one(
            d4.path(),
            "redis",
            "find_thing",
            serde_json::json!(["settlement_rate_default"]),
            None,
        );
        assert!(undeclared_read.channel.is_none());
        assert!(undeclared_read.effect.is_none());
        // Heuristic: is_state_channel("redis") && is_read_op("find_thing") → read_set.
        assert_eq!(undeclared_read.read_set, vec!["settlement_rate_default"]);
        assert!(undeclared_read.write_set.is_empty());
        // Byte-identical to the declared-Read event's sets.
        assert_eq!(undeclared_read.read_set, declared_read.read_set);
        assert_eq!(undeclared_read.write_set, declared_read.write_set);

        // (4b) UNDECLARED entropy via the legacy "id"/"time" boundary names still
        // produces the entropy_source from the name heuristic.
        let d5 = tempfile::tempdir().unwrap();
        let undeclared_id = record_one(d5.path(), "id", "nonce", serde_json::json!([]), None);
        assert!(undeclared_id.channel.is_none());
        assert_eq!(undeclared_id.entropy_source.as_deref(), Some("id"));
    }

    #[test]
    fn finish_boundary_event_firewall_contains_a_panicking_serializer() {
        // SHADOW GUARANTEE: result serialization runs AFTER the real call, inside
        // the firewall in `finish_boundary_event`. A serializer that panics must be
        // contained — never allowed to turn a successful request into a failure.
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = std::sync::Arc::new(RecordingHook::new(dir.path()).expect("hook"));
        let event = EventBuilder::start(
            &*hook,
            "storage",
            "T",
            "m",
            Location::caller(),
            serde_json::json!({}),
        );
        // If the firewall were missing, this would unwind and fail the test.
        finish_boundary_event(
            Some((std::sync::Arc::clone(&hook), event)),
            &(),
            |_: &()| -> (serde_json::Value, bool) { panic!("serializer blew up") },
        );
        // Reached only because the panic was swallowed (the event is simply
        // dropped). The hook remains usable afterwards.
        assert!(hook.flush().is_ok());
    }

    #[test]
    fn primary_state_key_is_the_leading_string_in_arg_order() {
        // array args: first string scalar is the key
        assert_eq!(
            extract_primary_state_key(&serde_json::json!(["settlement_rate_premium"])),
            vec!["settlement_rate_premium".to_string()]
        );
        // write shape (key, value): the KEY, not the value, is captured
        assert_eq!(
            extract_primary_state_key(&serde_json::json!(["eu_settlement_audit", "0.20"])),
            vec!["eu_settlement_audit".to_string()]
        );
        // object args: nested string is found
        assert_eq!(
            extract_primary_state_key(&serde_json::json!({"key": "k1"})),
            vec!["k1".to_string()]
        );
        // no string scalar -> empty (best-effort, never panics)
        assert!(extract_primary_state_key(&serde_json::json!([1, 2, 3])).is_empty());
        assert!(extract_primary_state_key(&serde_json::Value::Null).is_empty());
    }

    #[test]
    fn value_digest_is_stable_and_folds_in_the_result() {
        let args = serde_json::json!(["settlement_rate_premium"]);
        let r1 = serde_json::json!("0.10");
        let r2 = serde_json::json!("0.20");
        // deterministic for the same (args, result)
        assert_eq!(value_digest_of(&args, &r1), value_digest_of(&args, &r1));
        // a changed RESULT changes the digest (so a value divergence is visible
        // even when args match) — this is the dataflow-hint property
        assert_ne!(value_digest_of(&args, &r1), value_digest_of(&args, &r2));
    }

    #[test]
    fn v1_tapes_load_with_defaults_and_v2_fields_round_trip() {
        // A legacy v1 event JSON carries NONE of the new fields. It must still
        // deserialize (forward-compat), with the new fields defaulted.
        let v1 = serde_json::json!({
            "global_sequence": 1, "request_sequence": 1, "correlation_id": "c1",
            "timestamp_ns": 1_780_000_000_000_000_000u64,
            "boundary": "redis", "trait_name": "T", "method_name": "eu_settlement_read",
            "call_file": "x.rs", "call_line": 1, "call_column": 1,
            "args": ["settlement_rate_default"], "result": "0.10",
            "is_error": false, "duration_us": 1, "event_schema_version": 1
        });
        let ev: SemanticEvent = serde_json::from_value(v1).expect("v1 tape must still load");
        assert_eq!(ev.event_schema_version, 1);
        assert!(ev.read_set.is_empty());
        assert!(ev.write_set.is_empty());
        assert_eq!(ev.value_digest, None);
        assert_eq!(ev.entropy_source, None);
        assert_eq!(ev.end_timestamp_ns, None);

        // A v2 event preserves the new fields across a JSON round-trip, and the
        // large `timestamp_ns` survives byte-exact (the reason we did NOT flatten).
        let mut ev2 = ev.clone();
        ev2.event_schema_version = CURRENT_EVENT_SCHEMA_VERSION;
        ev2.read_set = vec!["settlement_rate_premium".to_string()];
        ev2.value_digest = Some(value_digest_of(&ev2.args, &ev2.result));
        ev2.entropy_source = Some("id".to_string());
        ev2.end_timestamp_ns = Some(1_780_000_000_000_000_123u64);
        let round: SemanticEvent =
            serde_json::from_str(&serde_json::to_string(&ev2).unwrap()).unwrap();
        assert_eq!(round.event_schema_version, 2);
        assert_eq!(round.read_set, vec!["settlement_rate_premium".to_string()]);
        assert_eq!(round.value_digest, ev2.value_digest);
        assert_eq!(round.entropy_source.as_deref(), Some("id"));
        assert_eq!(round.timestamp_ns, 1_780_000_000_000_000_000u64);
        assert_eq!(round.end_timestamp_ns, Some(1_780_000_000_000_000_123u64));

        // skip_serializing_if keeps the wire clean: an unpopulated v1 event emits
        // none of the new keys.
        let wire = serde_json::to_value(&ev).unwrap();
        assert!(wire.get("read_set").is_none());
        assert!(wire.get("value_digest").is_none());
        assert!(wire.get("end_timestamp_ns").is_none());
    }

    #[test]
    fn value_digest_parses_from_vector_stringified_u64() {
        // The Kafka→Vector→MinIO record pipeline stringifies u64 > i64::MAX (a
        // value_digest is an FNV-1a hash that routinely exceeds it). Such an
        // event MUST still deserialize — otherwise the renderer/kernel drop it
        // and replay coverage silently collapses (the bug that 401'd /payments).
        let big = 15_482_056_560_522_895_781u64; // > i64::MAX, as seen on disk
        let json = serde_json::json!({
            "global_sequence": 1, "request_sequence": 1, "correlation_id": "c1",
            "timestamp_ns": 1_780_000_000_000_000_000u64,
            "boundary": "db", "trait_name": "T", "method_name": "generic_filter",
            "call_file": "x.rs", "call_line": 1, "call_column": 1,
            "args": {}, "result": {}, "is_error": false, "duration_us": 1,
            "value_digest": big.to_string()  // STRINGIFIED, as Vector emits it
        });
        let ev: SemanticEvent =
            serde_json::from_value(json).expect("stringified value_digest must parse");
        assert_eq!(ev.value_digest, Some(big));
        // and the bare-number form still works
        let ev2: SemanticEvent = serde_json::from_value(serde_json::json!({
            "global_sequence": 2, "request_sequence": 1,
            "timestamp_ns": 0, "boundary": "db", "trait_name": "T",
            "method_name": "m", "call_file": "x", "call_line": 1, "call_column": 1,
            "args": {}, "result": {}, "is_error": false, "duration_us": 0,
            "value_digest": 42
        }))
        .unwrap();
        assert_eq!(ev2.value_digest, Some(42));
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
                provenance: Provenance::default(),
                recon: Recon::default(),
                result_image: None,
                pre_image: None,
                read_set: Vec::new(),
                write_set: Vec::new(),
                value_digest: None,
                entropy_source: None,
                channel: None,
                effect: None,
                strategy: None,
                raw_draw: None,
                end_timestamp_ns: None,
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
                provenance: Provenance::default(),
                recon: Recon::default(),
                result_image: None,
                pre_image: None,
                read_set: Vec::new(),
                write_set: Vec::new(),
                value_digest: None,
                entropy_source: None,
                channel: None,
                effect: None,
                strategy: None,
                raw_draw: None,
                end_timestamp_ns: None,
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
                provenance: Provenance::default(),
                recon: Recon::default(),
                result_image: None,
                pre_image: None,
                read_set: Vec::new(),
                write_set: Vec::new(),
                value_digest: None,
                entropy_source: None,
                channel: None,
                effect: None,
                strategy: None,
                raw_draw: None,
                end_timestamp_ns: None,
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
                provenance: Provenance::default(),
                recon: Recon::default(),
                result_image: None,
                pre_image: None,
                read_set: Vec::new(),
                write_set: Vec::new(),
                value_digest: None,
                entropy_source: None,
                channel: None,
                effect: None,
                strategy: None,
                raw_draw: None,
                end_timestamp_ns: None,
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

    // -----------------------------------------------------------------------
    // `dispatch` seam tests (all four cases via the hook-parameterized twin)
    //
    // The four control-flow cases live identically in `dispatch` (global hook)
    // and `dispatch_with_hook` (injected hook). The injected variant is what the
    // delegate path uses and is fully deterministic (no process-global OnceLock /
    // env state), so the four cases are exercised against it with an in-memory
    // fake `DejaHook`. A separate test covers the global `dispatch` inactive
    // fast-path (the one global case that needs no env mutation).
    // -----------------------------------------------------------------------

    /// In-memory fake hook with knobs to drive each `dispatch` case.
    struct FakeHook {
        active: bool,
        /// When `Some`, `try_replay_with_context` returns it (a lookup hit).
        replay_value: Option<serde_json::Value>,
        /// When true, `execute_mode` returns `Execute` and `execute_shadow_peek`
        /// hands back a token (drives the execute/shadow path).
        execute: bool,
        // Observations the test asserts on.
        recorded: Mutex<Vec<SemanticEvent>>,
        shadow_observed: Mutex<Vec<serde_json::Value>>,
    }

    impl FakeHook {
        fn new(active: bool) -> Self {
            Self {
                active,
                replay_value: None,
                execute: false,
                recorded: Mutex::new(Vec::new()),
                shadow_observed: Mutex::new(Vec::new()),
            }
        }
    }

    impl DejaHook for FakeHook {
        fn is_active(&self) -> bool {
            self.active
        }
        fn record(&self, event: SemanticEvent) {
            self.recorded.lock().unwrap().push(event);
        }
        fn next_global_sequence(&self) -> u64 {
            0
        }
        fn next_request_sequence(&self, _correlation_id: Option<&str>) -> u64 {
            0
        }
        fn try_replay_with_context(&self, _query: ReplayLookup<'_>) -> Option<serde_json::Value> {
            self.replay_value.clone()
        }
        fn execute_mode(&self, _b: &str, _t: &str, _m: &str) -> ExecuteMode {
            if self.execute {
                ExecuteMode::Execute
            } else {
                ExecuteMode::Lookup
            }
        }
        fn execute_shadow_peek(&self, query: ReplayLookup<'_>) -> Option<ExecuteShadowToken> {
            if !self.execute {
                return None;
            }
            // Minimal observation; `execute_shadow_observe` fills the result.
            Some(ExecuteShadowToken::new(crate::replay::ObservedCall {
                correlation_id: None,
                boundary: query.boundary.to_string(),
                trait_name: query.trait_name.to_string(),
                method_name: query.method_name.to_string(),
                args: query.args.clone(),
                resolved: false,
                resolved_rank: None,
                source_event_global_sequence: None,
                call_file: None,
                call_line: None,
                call_column: None,
                logical_span_path: None,
                graph_node_id: None,
                synthesized: false,
                real_impl_will_fail: false,
                recorded_result: None,
                observed_result: None,
                provenance: crate::Provenance::ExecuteShadow,
                seed_gap: false,
                pre_image: None,
                result_image: None,
            }))
        }
        fn execute_shadow_observe(
            &self,
            token: ExecuteShadowToken,
            observed_result: serde_json::Value,
        ) {
            let _ = token;
            self.shadow_observed.lock().unwrap().push(observed_result);
        }
    }

    fn test_identity() -> CallsiteIdentity {
        CallsiteIdentity {
            version: 1,
            source: CallsiteSource::SyntacticHash,
            id: None,
            scope: Some("T::m".to_string()),
            occurrence: 0,
            caller_function: None,
            lexical_path: Some("crate::m".to_string()),
            syntax_hash: Some(123),
            logical_context: None,
        }
    }

    fn delegate_obs<'a>(hook: &'a FakeHook) -> DelegateObservation<'a> {
        DelegateObservation {
            hook,
            boundary: "unit",
            trait_name: "T",
            method_name: "m",
            caller: Location::caller(),
            identity: test_identity(),
            receiver: None,
        }
    }

    /// Case 1 — INACTIVE hook is handled by the GLOBAL `dispatch` fast path and by
    /// the delegate MACRO's `if !is_active` gate (the seam itself assumes the
    /// caller has gated activity). The authoritative inactive-laziness proof for
    /// the boundary path is `dispatch_global_inactive_runs_without_evaluating_args_thunk`
    /// below; for the delegate path it is the integration test
    /// `fast_path_skips_recording_when_inactive`. Here we only assert the seam
    /// contract: `run` always executes and returns its value.
    #[test]
    fn dispatch_with_hook_always_runs_the_block() {
        let hook = FakeHook::new(true);
        let out = dispatch_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || 7u64,
            |_v| None,
            |r: &u64| (serde_json::json!(*r), false),
        );
        assert_eq!(out, 7);
    }

    /// Case 2 — RECORD (active, no replay hit, lookup mode): `run` executes and
    /// exactly one event is recorded with the extracted result.
    #[test]
    fn dispatch_with_hook_records_when_active_and_no_hit() {
        let hook = FakeHook::new(true);
        let out = dispatch_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || 42u64,
            |_v| None,
            |r: &u64| (serde_json::json!(*r), false),
        );
        assert_eq!(out, 42);
        let recorded = hook.recorded.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].result, serde_json::json!(42));
        assert_eq!(recorded[0].args, serde_json::json!({"k": "v"}));
        assert!(hook.shadow_observed.lock().unwrap().is_empty());
    }

    /// Case 3a — LOOKUP HIT that reconstructs: `run` is NEVER called, the recorded
    /// value is returned, nothing new is recorded.
    #[test]
    fn dispatch_with_hook_lookup_hit_skips_run() {
        let mut hook = FakeHook::new(true);
        hook.replay_value = Some(serde_json::json!(99));
        let ran = std::cell::Cell::new(false);
        let out = dispatch_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || {
                ran.set(true);
                7u64
            },
            |v| serde_json::from_value::<u64>(v).ok(),
            |r: &u64| (serde_json::json!(*r), false),
        );
        assert_eq!(out, 99, "returned the reconstructed recorded value");
        assert!(!ran.get(), "the real block must NOT run on a lookup hit");
        assert!(hook.recorded.lock().unwrap().is_empty());
    }

    /// Case 3b — LOOKUP HIT whose reconstruct FAILS: falls through to live `run`
    /// and records (the V1 "skip error arms" / deserialize-fail policy).
    #[test]
    fn dispatch_with_hook_lookup_hit_falls_through_on_reconstruct_failure() {
        let mut hook = FakeHook::new(true);
        // A recorded value the reconstruct closure rejects (returns None).
        hook.replay_value = Some(serde_json::json!("not-a-u64"));
        let out = dispatch_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || 5u64,
            |v| serde_json::from_value::<u64>(v).ok(), // fails on the string
            |r: &u64| (serde_json::json!(*r), false),
        );
        assert_eq!(out, 5, "fell through to the real block");
        let recorded = hook.recorded.lock().unwrap();
        assert_eq!(recorded.len(), 1, "the fall-through call is recorded");
        assert_eq!(recorded[0].result, serde_json::json!(5));
    }

    /// Case 4 — EXECUTE/SHADOW: `run` executes against the live boundary, the
    /// extracted result is shadow-observed, and the NORMAL record is suppressed.
    #[test]
    fn dispatch_with_hook_execute_shadow_observes_and_suppresses_record() {
        let mut hook = FakeHook::new(true);
        hook.execute = true;
        let out = dispatch_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || 13u64,
            |_v| None,
            |r: &u64| (serde_json::json!(*r), false),
        );
        assert_eq!(out, 13, "the REAL block ran and its value is returned");
        let shadow = hook.shadow_observed.lock().unwrap();
        assert_eq!(shadow.len(), 1, "exactly one shadow observation");
        assert_eq!(shadow[0], serde_json::json!(13));
        assert!(
            hook.recorded.lock().unwrap().is_empty(),
            "the normal Recorded event is suppressed on the execute/shadow path"
        );
    }

    /// The async twin routes the same four-case control flow; smoke-test the
    /// record and lookup-hit cases through `dispatch_async_with_hook`.
    #[tokio::test]
    async fn dispatch_async_with_hook_records_and_replays() {
        // record
        let hook = FakeHook::new(true);
        let out = dispatch_async_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || async { 21u64 },
            |_v| None,
            |r: &u64| (serde_json::json!(*r), false),
        )
        .await;
        assert_eq!(out, 21);
        assert_eq!(hook.recorded.lock().unwrap().len(), 1);

        // lookup hit
        let mut hook = FakeHook::new(true);
        hook.replay_value = Some(serde_json::json!(100));
        let out = dispatch_async_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || async { 0u64 },
            |v| serde_json::from_value::<u64>(v).ok(),
            |r: &u64| (serde_json::json!(*r), false),
        )
        .await;
        assert_eq!(out, 100);
        assert!(hook.recorded.lock().unwrap().is_empty());
    }

    /// The global `dispatch` inactive fast-path: with no runtime hook AND no
    /// recording hook configured for this process, `dispatch` runs `run`, returns
    /// the value, and NEVER evaluates the `args` thunk (zero-overhead inactive
    /// path). This is the only global-`dispatch` case testable without mutating
    /// the process-wide env/OnceLock state.
    #[test]
    fn dispatch_global_inactive_runs_without_evaluating_args_thunk() {
        // No DEJA_MODE / DEJA_ARTIFACT_DIR set in this unit-test binary, so both
        // the runtime hook and the recording hook resolve to None.
        if global_runtime_hook_from_env().is_some() || global_hook_from_env().is_some() {
            // A sibling test installed a hook; skip rather than assert on shared
            // global state.
            return;
        }
        let identity = test_identity();
        let args_evaluated = std::cell::Cell::new(false);
        let out = dispatch(
            CrossingObservation::new(
                BoundarySpec::new("unit", "T", "m"),
                identity,
                Location::caller(),
            ),
            || {
                args_evaluated.set(true);
                serde_json::json!({"k": "v"})
            },
            || 55u64,
            |_v| None,
            |r: &u64| (serde_json::json!(*r), false),
        );
        assert_eq!(out, 55);
        assert!(
            !args_evaluated.get(),
            "inactive `dispatch` must NOT evaluate the args thunk"
        );
    }
}
