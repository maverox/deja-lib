//! Déjà — deterministic record/replay for service boundaries.
//!
//! Annotate a boundary with one of the feature-gated attribute macros
//! (`deja::redis`, `deja::id`, `deja::time`, `deja::http`, `deja::boundary`,
//! the `#[deja::recordable]` trait decorator, or the db helpers in
//! [`db`]): on record, every call emits a [`SemanticEvent`] (args, result,
//! correlation id, callsite identity) through the installed [`RuntimeHook`];
//! on replay (`DEJA_MODE=replay`), the recorded result is substituted in
//! place of the live call and an `ObservedCall` is emitted for divergence
//! scoring.
//!
//! This crate is the facade: it re-exports the macros (`deja-derive`), the
//! recording/replay runtime (`deja-record`), correlation context helpers
//! (`deja-context`), and provides the payload-normalization helpers macros
//! expand against ([`value`], [`http`], [`db`]). Generated code reaches the
//! runtime through [`__private`].

pub use deja_derive::recordable;
pub use deja_derive::{boundary, http, id, instrument, redis, time};

/// Re-export lookup-table replay primitives (hybrid architecture: in-process
/// lookup, orchestrator-owned policy).
pub use deja_record::replay::{
    addresses_for, canonical_args_hash, Address, FileObservedSink, InMemoryObservedSink,
    KeyStamper, LocalFileLookupSource, LookupEntry, LookupKey, LookupTable, LookupTableHook,
    LookupTableSource, ObservedCall, ObservedCallSink,
};
/// Re-export the total-derivative seeding pipeline (pure seed-plan builder,
/// pre-image join, diverged-read classification, ambient template) and the
/// execute-shadow RMW state-probe abstraction so the harness materializes seeds
/// from a recording instead of hand-coded literals.
pub use deja_record::replay::{
    build_seed_plan, complete_rmw_images, join_pre_images, probe_rmw_images, AmbientTemplate,
    InMemoryStateProbe, PreImageJoin, ReadClassification, RmwImages, SeedEntry, SeedOrigin,
    SeedPlan, StateProbe,
};
/// Re-export the correlation-propagation tracing layer, which mirrors the ingress
/// `request_id` span field into deja-context so spawned-task boundary events
/// inherit the request correlation.
pub use deja_record::DejaCorrelationLayer;
/// Convenience re-export for the hook trait (needed by generated delegation).
pub use deja_record::DejaHook;
/// Re-export the execution graph tracing layer for framework logger setup.
pub use deja_record::ExecutionGraphLayer;
/// Re-export semantic recording primitives so downstream crates only need
/// one `deja` dependency.
pub use deja_record::{
    flush_global_hook, global_hook_from_env, hook_from_env, AsyncRecordWriter, CompositeSink,
    EventBuilder, JsonlSink, LazyEventFinalizer, MarkerKind, NoOpHook, Provenance, Recon,
    RecordSink, RecordingHook, SemanticEvent, SinkPolicy, WriterConfig, WriterStatsSnapshot,
    DEJA_BATCH_SIZE_ENV_VAR, DEJA_FLUSH_INTERVAL_MS_ENV_VAR, DEJA_GRAPH_DIR_ENV_VAR,
    DEJA_QUEUE_CAPACITY_ENV_VAR, DEJA_SINK_POLICY_ENV_VAR,
};
/// Re-export callsite identity and runtime hook primitives for the
/// `DEJA_MODE=record|replay` foundation.
pub use deja_record::{
    flush_global_runtime_hook, global_runtime_hook_from_env, replay_is_active,
    runtime_hook_from_env, set_global_runtime_hook, stable_callsite_hash, CallsiteIdentity,
    CallsiteSource, ExecuteMode, ExecuteShadowToken, Policy, ReplayLookup, RuntimeHook,
};
/// Re-export the DECLARATIVE BOUNDARY MODEL primitives: the declared semantic
/// enums, the descriptor-carried [`BoundarySemantics`], and the pure decision
/// matrix [`decide_strategy`] (+ its `ExecuteMode` mapping and the hook-aware
/// runtime entry point). The vendor migration declares with these.
pub use deja_record::{
    BoundarySemantics, Channel, Effect, EntropySource, Strategy,
};
pub use deja_record::replay::{
    boundary_execute_mode_for, decide_strategy, strategy_to_execute_mode,
};
/// Re-export replay primitives so `deja::*` consumers get the full replay API.
pub use deja_record::{
    ArgMismatchPolicy, Divergence, DivergenceKind, ReplayConfig, ReplayHook, ReplayReport,
};
/// Re-export the per-request recording sampling gate. The host (e.g. Hyperswitch)
/// resolves *whether* to record from Superposition at ingress and pushes the
/// boolean here; Déjà only consumes it — `false` makes the recording hook a
/// no-op for the request, `true` records as usual. Absent any decision the gate
/// defaults to recording, so a host without a sampler is unaffected.
pub use deja_context::{
    clear_recording_decision, recording_decision, recording_decision_for_current,
    set_recording_decision,
};

/// The deja library version, for sinks that stamp provenance on the wire
/// (the recording envelope's `code.deja_version`).
pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Small JSON helpers shared by framework-specific boundary hooks.
pub mod value {
    use std::fmt::Debug;

    /// Capture the full Rust debug representation of a value.
    pub fn debug<T: Debug + ?Sized>(value: &T) -> serde_json::Value {
        serde_json::json!({
            "debug": format!("{value:?}"),
        })
    }

    /// Capture the full Rust debug representation of an error.
    pub fn error_debug<T: Debug + ?Sized>(error: &T) -> serde_json::Value {
        serde_json::json!({
            "debug": format!("{error:?}"),
        })
    }

    /// Capture a generic function return value as debug JSON and infer whether
    /// a `Result`-like value is an error from its standard debug shape.
    pub fn result_debug<T: Debug + ?Sized>(value: &T) -> (serde_json::Value, bool) {
        let debug = format!("{value:?}");
        let is_error = debug.starts_with("Err(") || debug.starts_with("Err {");
        (
            serde_json::json!({
                "debug": debug,
                "kind": if is_error { "error" } else { "value" },
            }),
            is_error,
        )
    }

    /// Capture a function return value LOSSLESSLY via `serde` for replay
    /// substitution. Unlike [`result_debug`] (which captures an unrecoverable
    /// Debug string), the produced JSON round-trips: replay can
    /// `serde_json::from_value` it back into the original type and return it
    /// without executing the real call. The boolean marks `Result::Err` using
    /// serde's `{"Err": …}` shape; non-`Result` values are never errors.
    ///
    /// Requires the value to implement `serde::Serialize`; the macro only emits
    /// a call to this for boundaries opted into replay (`#[deja::…(replay)]`).
    pub fn result_serialize<T: serde::Serialize + ?Sized>(value: &T) -> (serde_json::Value, bool) {
        let json = serde_json::to_value(value).unwrap_or(serde_json::Value::Null);
        let is_error = matches!(&json, serde_json::Value::Object(map) if map.contains_key("Err"));
        (json, is_error)
    }

    /// Lossless **Ok-only** recording for `Result`-returning boundaries whose
    /// error type is NOT serde-serializable (e.g. `error_stack::Report`). The
    /// OK value is recorded via `to_value` so replay can reconstruct it; an
    /// `Err` is recorded as a non-reconstructable sentinel (`{"deja_err": …}`)
    /// and marked `is_error`, so on replay it deserialize-fails into the OK type
    /// and the boundary falls through to live execution (the V1 "skip error
    /// arms" policy). Pairs with the macro's `replay_ok` flag.
    pub fn result_serialize_ok<T: serde::Serialize, E: std::fmt::Debug>(
        result: &Result<T, E>,
    ) -> (serde_json::Value, bool) {
        match result {
            Ok(value) => (
                serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
                false,
            ),
            Err(error) => (
                serde_json::json!({ "deja_err": format!("{error:?}") }),
                true,
            ),
        }
    }

    /// Versioned, structured record of a database boundary `Result`.
    ///
    /// Unlike [`result_serialize_ok`] (which records errors as an unrecoverable
    /// Debug-string sentinel, `{"deja_err": …}`), this captures the error in a
    /// STRUCTURED form: a stable `kind` discriminant (e.g. `"NotFound"`,
    /// `"UniqueViolation"`, `"Other"`) plus the human-readable `message`. Replay
    /// then matches on the `kind` discriminant rather than string-scanning a
    /// Debug blob, which is robust to message-text drift.
    ///
    /// IMPORTANT: the `Ok` payload is held as a raw `serde_json::Value` (NOT a
    /// typed generic) on purpose. The Kafka→Vector→MinIO transport serializes
    /// integers larger than `i64::MAX` as JSON STRINGS; a bare `u64` struct
    /// field would fail to round-trip through that path. A `serde_json::Value`
    /// tolerates a number that arrives back as either a number or a string.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    #[serde(tag = "result")]
    pub enum DejaDatabaseResultPayload {
        Ok {
            value: serde_json::Value,
            type_name: String,
        },
        Err {
            kind: String,
            message: String,
        },
    }

    /// A versioned envelope around [`DejaDatabaseResultPayload`].
    ///
    /// Keeping `version` separate lets the recorded shape evolve without
    /// breaking older recordings; replay can branch on `version` if/when the
    /// payload layout changes.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    pub struct DejaDatabaseResult {
        pub version: u8,
        #[serde(flatten)]
        pub payload: DejaDatabaseResultPayload,
    }

    impl DejaDatabaseResult {
        /// Current on-disk format version.
        pub const VERSION: u8 = 1;

        pub fn ok(value: serde_json::Value, type_name: impl Into<String>) -> Self {
            Self {
                version: Self::VERSION,
                payload: DejaDatabaseResultPayload::Ok {
                    value,
                    type_name: type_name.into(),
                },
            }
        }

        pub fn err(kind: impl Into<String>, message: impl Into<String>) -> Self {
            Self {
                version: Self::VERSION,
                payload: DejaDatabaseResultPayload::Err {
                    kind: kind.into(),
                    message: message.into(),
                },
            }
        }
    }

    /// Lossless, STRUCTURED recording for database boundaries.
    ///
    /// Emits the [`DejaDatabaseResult`] shape: an `Ok` records the value via
    /// `to_value` (so replay reconstructs it) tagged with its Rust `type_name`;
    /// an `Err` records a structured `{kind, message}` derived by the caller's
    /// `extract_kind` closure (which knows the concrete error type and can map
    /// it to a stable discriminant). The returned bool marks `Result::Err`.
    ///
    /// This replaces [`result_serialize_ok`] for the DB boundary ONLY; non-DB
    /// `replay_ok` boundaries (e.g. redis) keep using `result_serialize_ok`.
    pub fn result_serialize_db<T, E>(
        result: &Result<T, E>,
        extract_kind: impl Fn(&E) -> (String, String),
    ) -> (serde_json::Value, bool)
    where
        T: serde::Serialize,
    {
        let record = match result {
            Ok(value) => DejaDatabaseResult::ok(
                serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
                std::any::type_name::<T>(),
            ),
            Err(error) => {
                let (kind, message) = extract_kind(error);
                DejaDatabaseResult::err(kind, message)
            }
        };
        let json = serde_json::to_value(&record).unwrap_or(serde_json::Value::Null);
        (json, result.is_err())
    }

    /// Capture raw bytes without redaction or truncation.
    pub fn bytes(bytes: &[u8]) -> serde_json::Value {
        let text = std::str::from_utf8(bytes).ok();
        let json = text.and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok());

        serde_json::json!({
            "captured": true,
            "bytes_len": bytes.len(),
            "utf8": text.is_some(),
            "text": text,
            "json": json,
            "raw_bytes": bytes.to_vec(),
        })
    }

    /// Capture optional bytes while preserving why capture was unavailable.
    pub fn optional_bytes(bytes: Option<&[u8]>, missing_reason: &'static str) -> serde_json::Value {
        bytes.map_or_else(
            || {
                serde_json::json!({
                    "captured": false,
                    "reason": missing_reason,
                })
            },
            self::bytes,
        )
    }
}

/// Helpers for HTTP request/response boundary payloads.
pub mod http {
    /// Normalize headers as a map of header name to all observed values.
    pub fn headers<I, K, V>(headers: I) -> serde_json::Value
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut output = serde_json::Map::new();
        for (name, value) in headers {
            // SHADOW GUARANTEE: this only ever inserts arrays, so the downcast
            // cannot realistically fail — but never panic on a recording path.
            // Skip a malformed entry rather than unwinding into the request.
            if let Some(array) = output
                .entry(name.into())
                .or_insert_with(|| serde_json::Value::Array(Vec::new()))
                .as_array_mut()
            {
                array.push(serde_json::Value::String(value.into()));
            }
        }
        serde_json::Value::Object(output)
    }

    /// Capture an HTTP body as text, parsed JSON when possible, and raw bytes.
    pub fn body(bytes: &[u8]) -> serde_json::Value {
        crate::value::bytes(bytes)
    }

    /// Capture a missing HTTP body with a reason.
    pub fn missing_body(reason: &'static str) -> serde_json::Value {
        serde_json::json!({
            "captured": false,
            "reason": reason,
        })
    }
}

/// Helpers for database boundary payloads.
pub mod db {
    use std::fmt::Debug;

    /// Build the database request payload common to Diesel helpers.
    pub fn args(
        operation: &'static str,
        table: &str,
        sql: String,
        inputs: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "operation": operation,
            "table": table,
            "sql": sql,
            "inputs": inputs,
        })
    }

    /// Metadata for a database query boundary.
    #[derive(Debug, Clone)]
    pub struct QuerySpec {
        pub boundary: &'static str,
        pub component: &'static str,
        pub operation: &'static str,
        pub table: String,
        pub sql: String,
        pub inputs: serde_json::Value,
        pub correlation_id: Option<String>,
    }

    impl QuerySpec {
        pub fn new(
            operation: &'static str,
            table: impl Into<String>,
            sql: impl Into<String>,
            inputs: serde_json::Value,
        ) -> Self {
            Self {
                boundary: "db",
                component: "db",
                operation,
                table: table.into(),
                sql: sql.into(),
                inputs,
                correlation_id: None,
            }
        }

        pub fn component(mut self, component: &'static str) -> Self {
            self.component = component;
            self
        }

        pub fn boundary(mut self, boundary: &'static str) -> Self {
            self.boundary = boundary;
            self
        }

        pub fn correlation_id(mut self, correlation_id: Option<String>) -> Self {
            self.correlation_id = correlation_id;
            self
        }
    }

    /// Coarse result shape to record for a generic database helper.
    #[derive(Debug, Clone, Copy)]
    pub enum QueryResultKind {
        Value,
        Rows,
        Optional,
        Count,
        Bool,
        Unit,
    }

    /// The DECLARED semantics for the generic db seam (declarative boundary model).
    ///
    /// Every generic db op lives on [`Channel::State`](crate::__private::Channel::State)
    /// — the only execute-eligible channel. The [`Effect`](crate::__private::Effect)
    /// (read vs write) is the State-channel sub-classification: the generic db
    /// helpers are plain reads/writes (NO read-modify-write in this seam), so
    /// `strategy` stays `None`.
    ///
    /// BEHAVIOR-PRESERVING: this declaration replaces, for the db seam, the
    /// name-heuristic pair (`is_state_channel("db")` → `true`,
    /// `is_read_op(operation)` → read/write) that `EventBuilder::finish` and the
    /// pure analyzers used to compute. The declared [`Effect`] is therefore keyed
    /// off the SAME `is_read_op` verdict the heuristic produced, so the resulting
    /// `read_set`/`write_set` and the `decide_strategy` matrix cell are byte-identical
    /// to the pre-declaration path:
    ///
    /// - Reads (`generic_find_by_id_core`, `generic_find_one_core`, `generic_filter`,
    ///   `generic_count`, ...) → [`Effect::Read`](crate::__private::Effect::Read)
    ///   whenever `is_read_op` classifies the op as a read.
    /// - Writes (`generic_insert`, `generic_update*`, `generic_delete*`, ...) and
    ///   any op `is_read_op` does not recognize as a read →
    ///   [`Effect::Write`](crate::__private::Effect::Write).
    ///
    /// (Several `generic_*` names — e.g. `generic_find_by_id_core`, `generic_count`,
    /// `generic_filter` — are *intrinsically* reads but are NOT matched by the
    /// current `is_read_op` verb list, so they classify as `Write` today. Honoring
    /// the heuristic verdict here keeps this slice byte-identical; re-classifying
    /// those ops as `Read` is a follow-up that lands together with the heuristic
    /// deletion, NOT in this behavior-preserving migration.)
    fn db_semantics(operation: &str) -> crate::__private::BoundarySemantics {
        use crate::__private::{Channel, Effect};
        // Match the heuristic's read/write verdict EXACTLY (see doc comment): the
        // declared effect must reproduce the `is_read_op` classification the
        // pre-declaration `finish` / analyzers used, or `read_set`/`write_set`
        // would shift for the db seam.
        let effect = if crate::__private::is_read_op(operation) {
            Effect::Read
        } else {
            Effect::Write
        };
        crate::__private::BoundarySemantics {
            channel: Some(Channel::State),
            effect: Some(effect),
            // No read-modify-write in the generic db seam → no per-op strategy.
            strategy: None,
        }
    }

    /// Record AND replay a database query, Ok-only.
    ///
    /// On replay (a lookup-table hook installed), the recorded `Ok` row(s) are
    /// served from the lookup table and the real query is SKIPPED — so the
    /// candidate never touches the database. The lookup key is the query args
    /// (`{operation, table, sql, inputs}`); since ids/timestamps in `inputs` are
    /// themselves substituted, the key matches the recording. A recorded `Err`
    /// (or a replay miss) deserialize-fails / returns None and falls through to
    /// live execution (the V1 "skip error arms" policy).
    ///
    /// Generic over the `Ok` type `R` (recorded losslessly via serde so replay
    /// can reconstruct it) and the error type `E` (never serialized — only its
    /// structured `kind`/`message` are captured via `extract_kind`), which is
    /// why `error_stack::Report` works.
    ///
    /// The result is recorded in the STRUCTURED [`crate::value::DejaDatabaseResult`]
    /// shape. `extract_kind` maps a live `E` error into a stable `(kind, message)`
    /// pair at RECORD time; `recover_err` reconstructs a faithful `E` from a
    /// recorded `kind` at REPLAY time (returning `None` to fall through to live
    /// execution). Both closures live with the boundary's macro, which knows the
    /// concrete error type — the deja fn stays error-type agnostic.
    // The hand-written DB seam still calls `replay_boundary` directly: it carries
    // bespoke `recover_err` control flow (deterministic DB errors like NotFound
    // are reconstructed and replayed faithfully) that does not fit the generic
    // `dispatch` reconstruct closure. Migrating it is explicitly descoped to a
    // follow-up (design §6 Step 6), so suppress the deprecation here.
    #[allow(deprecated)]
    #[track_caller]
    pub fn record_query_async<F, R, E, K, G>(
        spec: QuerySpec,
        future: F,
        result_kind: QueryResultKind,
        extract_kind: K,
        recover_err: G,
    ) -> impl std::future::Future<Output = Result<R, E>>
    where
        F: std::future::Future<Output = Result<R, E>>,
        R: serde::Serialize + serde::de::DeserializeOwned + Debug,
        E: Debug,
        K: Fn(&E) -> (String, String),
        G: FnOnce(&str, &str) -> Option<E>,
    {
        let caller = std::panic::Location::caller();
        async move {
            // Build the lookup args once; reused for the replay key and the
            // recording so record/replay produce the SAME key.
            let request = args(spec.operation, &spec.table, spec.sql, spec.inputs);

            // Build a stable CallsiteIdentity ONCE for this query invocation and
            // reuse it for BOTH the replay lookup and the recording, so the
            // renderer and candidate hook stamp identical rank-2/3 keys. This is
            // the hand-written analogue of the boundary macro's codegen: a
            // proc-macro can't see this hand-written call site, so we derive the
            // identity from the boundary/component/operation metadata.
            //   - rank-2 syntax_hash: FNV-1a over "{boundary}::{component}::{operation}"
            //   - rank-3 lexical_path: "{component}::{operation}" (module_path!()
            //     here would be the useless `deja::db`).
            //   - occurrence: allocated EXACTLY ONCE from the runtime hook.
            let __deja_scope = format!("{}::{}", spec.component, spec.operation);
            let __deja_syntax_hash = crate::__private::stable_callsite_hash(&format!(
                "{}::{}::{}",
                spec.boundary, spec.component, spec.operation
            ));
            let __deja_occurrence = crate::__private::next_boundary_occurrence(
                spec.correlation_id.as_deref(),
                crate::__private::CallsiteSource::SyntacticHash,
                Some(__deja_scope.as_str()),
            );
            let __deja_identity = crate::__private::CallsiteIdentity {
                version: 1,
                source: crate::__private::CallsiteSource::SyntacticHash,
                id: None,
                scope: Some(__deja_scope.clone()),
                occurrence: __deja_occurrence,
                caller_function: Some(spec.component.to_string()),
                lexical_path: Some(__deja_scope.clone()),
                syntax_hash: Some(__deja_syntax_hash),
                logical_context: crate::__private::current_logical_span_path(),
            };

            // DECLARED semantics for this db op (declarative boundary model). All
            // generic db ops are `Channel::State`; the effect is the `is_read_op`
            // verdict (byte-identical to the heuristic — see `db_semantics`). Thread
            // it into the `BoundarySpec` the execute-mode / deprecated seams read
            // (so `boundary_execute_mode_for` resolves the DECLARED matrix cell —
            // which equals the heuristic's `execute_mode("db", ...)` for State) AND
            // into the `EventBuilder` (so `finish` stamps the declaration and derives
            // `read_set`/`write_set` from it). The State matrix cell equals the
            // heuristic verdict: Lookup under AllLookup, Execute under SelectiveExecute.
            let __deja_semantics = db_semantics(spec.operation);

            // EXECUTE-SHADOW (total derivative): under SelectiveExecute the db
            // boundary (a State channel) EXECUTES the real query against the
            // reconstructed db and shadow-records the observed result
            // (provenance = ExecuteShadow), instead of substituting the recorded
            // value. The ordered kernel replay rebuilds db state as it drives
            // requests in record order, so executed reads find their data.
            // Inert under AllLookup (boundary_execute_mode returns Lookup), so
            // this is the hand-written db analogue of the macro's execute arm.
            if matches!(
                crate::__private::boundary_execute_mode(
                    &crate::__private::BoundarySpec::with_semantics(
                        spec.boundary,
                        spec.component,
                        spec.operation,
                        __deja_semantics.clone(),
                    )
                ),
                crate::__private::ExecuteMode::Execute
            ) {
                let __deja_token = crate::__private::execute_shadow_peek_boundary(
                    caller,
                    &crate::__private::BoundarySpec::with_semantics(
                        spec.boundary,
                        spec.component,
                        spec.operation,
                        __deja_semantics.clone(),
                    ),
                    &request,
                    Some(&__deja_identity),
                );
                let output = future.await;
                if let Some(token) = __deja_token {
                    // Serialize the observed result with the SAME structured
                    // envelope the record path uses (`result_serialize_db`), so
                    // the post-hoc tally pairs recorded ↔ shadow like-for-like.
                    let (observed, _is_err) =
                        crate::value::result_serialize_db(&output, &extract_kind);
                    crate::__private::execute_shadow_observe_boundary(token, observed);
                }
                return output;
            }

            // REPLAY: serve the recorded result, skipping the query.
            // `replay_boundary` returns None in record/no-op mode.
            if let Some(recorded) = crate::__private::replay_boundary(
                caller,
                &crate::__private::BoundarySpec::with_semantics(
                    spec.boundary,
                    spec.component,
                    spec.operation,
                    __deja_semantics.clone(),
                ),
                &request,
                Some(&__deja_identity),
            ) {
                match decode_recorded_db_result::<R>(&recorded) {
                    // Recorded `Ok`: deserialize the row(s) and return them,
                    // skipping the live query entirely.
                    DecodedDbResult::Ok(value) => return Ok(value),
                    // Recorded `Err`: some db errors are DETERMINISTIC control
                    // flow (e.g. NotFound, which "check-then-create" logic
                    // branches on), so the boundary's `recover_err` reconstructs
                    // those from the structured `kind` and replays them
                    // FAITHFULLY. An unknown kind (`None`) falls through to live
                    // execution (the V1 "skip error arms" policy).
                    DecodedDbResult::Err { kind, message } => {
                        if let Some(err) = recover_err(&kind, &message) {
                            return Err(err);
                        }
                    }
                    // Undecodable (corrupt/foreign shape) → fall through to live.
                    DecodedDbResult::FallThrough => {}
                }
            }

            // RECORD + execute live. Thread the SAME identity (built above) so
            // the recorded event carries the rank-2/3 callsite identity.
            let event = start_query_event(
                spec.boundary,
                spec.component,
                spec.operation,
                spec.correlation_id,
                caller,
                request,
                Some(__deja_identity),
                __deja_semantics,
            );
            let output = future.await;
            finish_query_event(event, &output, result_kind, &extract_kind);
            output
        }
    }

    /// Outcome of decoding a recorded DB result during replay.
    enum DecodedDbResult<R> {
        /// A recorded `Ok` value, deserialized into the boundary's `Ok` type.
        Ok(R),
        /// A recorded structured error with its stable kind discriminant.
        Err { kind: String, message: String },
        /// The recording was undecodable for this boundary; fall through to live.
        FallThrough,
    }

    /// Decode a recorded DB result, preferring the structured
    /// [`crate::value::DejaDatabaseResult`] shape and falling back to legacy
    /// shapes so a mixed/transition recording still replays.
    ///
    /// Legacy fallbacks (only relevant during a partial re-record):
    ///   - `{"deja_err": "<Debug>"}` → structured `Err{kind:"Legacy", message}`
    ///     so a structured `recover_err` can still string-scan the message if it
    ///     chooses; an unknown kind simply falls through.
    ///   - a bare serialized `Ok` value (the pre-Phase-2 shape) →
    ///     `Ok(deserialized)`.
    fn decode_recorded_db_result<R>(recorded: &serde_json::Value) -> DecodedDbResult<R>
    where
        R: serde::de::DeserializeOwned,
    {
        use crate::value::{DejaDatabaseResult, DejaDatabaseResultPayload};

        // Preferred: the structured, versioned envelope.
        if let Ok(structured) = serde_json::from_value::<DejaDatabaseResult>(recorded.clone()) {
            return match structured.payload {
                DejaDatabaseResultPayload::Ok { value, .. } => {
                    match serde_json::from_value::<R>(value) {
                        Ok(value) => DecodedDbResult::Ok(value),
                        Err(_) => DecodedDbResult::FallThrough,
                    }
                }
                DejaDatabaseResultPayload::Err { kind, message } => {
                    DecodedDbResult::Err { kind, message }
                }
            };
        }

        // BACK-COMPAT: legacy lossy error sentinel `{"deja_err": "<Debug>"}`.
        if let Some(message) = recorded.get("deja_err").and_then(|v| v.as_str()) {
            return DecodedDbResult::Err {
                kind: "Legacy".to_string(),
                message: message.to_string(),
            };
        }

        // BACK-COMPAT: legacy bare `Ok` value (pre-structured DB recording).
        match serde_json::from_value::<R>(recorded.clone()) {
            Ok(value) => DecodedDbResult::Ok(value),
            Err(_) => DecodedDbResult::FallThrough,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn start_query_event(
        boundary: &'static str,
        component: &'static str,
        operation: &'static str,
        correlation_id: Option<String>,
        caller: &'static std::panic::Location<'static>,
        request: serde_json::Value,
        identity: Option<crate::CallsiteIdentity>,
        semantics: crate::__private::BoundarySemantics,
    ) -> Option<(std::sync::Arc<dyn crate::DejaHook>, crate::EventBuilder)> {
        use crate::DejaHook;

        let hook: std::sync::Arc<dyn DejaHook> = crate::global_hook_from_env()?;
        if !hook.is_active() {
            return None;
        }

        // SHADOW GUARANTEE: never let recording setup panic into the real query.
        let event = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut event = crate::EventBuilder::start_with_correlation_id(
                hook.as_ref(),
                boundary,
                component,
                operation,
                caller,
                correlation_id,
                request,
            )
            // DECLARED semantics so `finish` derives `read_set`/`write_set` and the
            // stamped channel/effect from the declaration (not the name heuristic).
            .with_semantics(semantics);
            if let Some(identity) = identity {
                event = event.with_callsite_identity(identity);
            }
            event
        }))
        .ok()?;
        Some((hook, event))
    }

    fn finish_query_event<R, E>(
        event: Option<(std::sync::Arc<dyn crate::DejaHook>, crate::EventBuilder)>,
        output: &Result<R, E>,
        _result_kind: QueryResultKind,
        extract_kind: impl Fn(&E) -> (String, String),
    ) where
        R: serde::Serialize,
        E: Debug,
    {
        // SHADOW GUARANTEE: result serialization + the sink enqueue run after the
        // real query already produced `output`; a panic here must never turn a
        // successful query into a failed request — it just drops the event.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let Some((hook, event)) = event else {
                return;
            };

            // Record the result in the STRUCTURED, versioned DejaDatabaseResult
            // shape: an Ok records the serialized row(s) (so replay can reconstruct
            // it); an Err records a stable `{kind, message}` derived by the
            // boundary's `extract_kind` so replay matches on the discriminant.
            let (response, is_error) = crate::value::result_serialize_db(output, extract_kind);
            event.finish(hook.as_ref(), response, is_error);
        }));
    }
}

/// Private implementation details used by the macro-generated code.
/// Not part of the public API — the `deja::*` attribute macros call these.
pub mod __private {
    pub use deja_context::current_correlation_id;
    /// Scope a closure to a correlation id (used by integration middleware to
    /// bind the request id around handler execution).
    pub use deja_context::scope as scope_correlation;
    // The single boundary-crossing seam the `#[deja::boundary]` family emits.
    // `dispatch` owns ALL replay/record/execute control flow internally, so the
    // macro names no replay-only operation. The older `replay_boundary` /
    // `boundary_execute_mode` / `execute_shadow_*` seams are re-exported only for
    // backward compatibility (they are `#[deprecated]` in deja-record and are
    // subsumed by `dispatch`).
    #[allow(deprecated)]
    pub use deja_record::{
        boundary_execute_mode, capture_is_active, current_logical_span_path, dispatch, replay_is_active,
        dispatch_async,
        execute_shadow_observe_boundary, execute_shadow_peek_boundary, finish_boundary_event,
        next_boundary_occurrence, record_boundary_async, record_boundary_async_lazy,
        record_boundary_sync, record_boundary_sync_lazy, replay_boundary, stable_callsite_hash,
        start_boundary_event_lazy, BoundarySpec, CallsiteIdentity, CallsiteSource,
        CrossingObservation, ExecuteMode, ExecuteShadowToken,
    };
    // Declarative boundary model: the declared semantic enums + the descriptor
    // payload the boundary macro emits into `BoundarySpec::with_semantics`.
    pub use deja_record::{
        BoundarySemantics, Channel, Effect, EntropySource, Strategy,
    };
    // The read/write name heuristic, re-exported ONLY so the hand-written db seam
    // can DECLARE an effect that is byte-identical to the verdict the heuristic
    // produced (see `db::db_semantics`). It remains the undeclared-boundary
    // fallback inside deja-record; deletion is a later step.
    pub use deja_record::replay::is_read_op;
}

#[cfg(test)]
mod db_result_tests {
    use crate::value::{result_serialize_db, DejaDatabaseResult, DejaDatabaseResultPayload};

    /// `DejaDatabaseResult` round-trips through serde for the Ok variant,
    /// preserving the (possibly large-integer) value and its type name.
    #[test]
    fn ok_round_trips_through_serde() {
        let original = DejaDatabaseResult::ok(serde_json::json!(42), "usize");
        let encoded = serde_json::to_value(&original).expect("encode");
        // Shape is the flattened, versioned, externally-tagged envelope.
        assert_eq!(
            encoded,
            serde_json::json!({
                "version": 1,
                "result": "Ok",
                "value": 42,
                "type_name": "usize",
            })
        );
        let decoded: DejaDatabaseResult = serde_json::from_value(encoded).expect("decode");
        assert_eq!(decoded, original);

        // A value that arrives back as a STRING (the Kafka/Vector big-int
        // stringification case) must still decode, because `value` is a raw
        // `serde_json::Value`.
        let stringified = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": "18446744073709551615",
            "type_name": "u64",
        });
        let decoded: DejaDatabaseResult =
            serde_json::from_value(stringified).expect("decode stringified big int");
        match decoded.payload {
            DejaDatabaseResultPayload::Ok { value, type_name } => {
                assert_eq!(value, serde_json::json!("18446744073709551615"));
                assert_eq!(type_name, "u64");
            }
            _ => panic!("expected Ok payload"),
        }
    }

    /// `DejaDatabaseResult` round-trips through serde for each Err kind.
    #[test]
    fn err_round_trips_for_each_kind() {
        for kind in ["NotFound", "UniqueViolation", "Other"] {
            let original = DejaDatabaseResult::err(kind, format!("{kind} message"));
            let encoded = serde_json::to_value(&original).expect("encode");
            assert_eq!(
                encoded,
                serde_json::json!({
                    "version": 1,
                    "result": "Err",
                    "kind": kind,
                    "message": format!("{kind} message"),
                })
            );
            let decoded: DejaDatabaseResult = serde_json::from_value(encoded).expect("decode");
            assert_eq!(decoded, original);
        }
    }

    /// `result_serialize_db` emits the structured shape and flags errors.
    #[test]
    fn result_serialize_db_emits_structured_shape() {
        let ok: Result<u8, &str> = Ok(7);
        let (json, is_err) = result_serialize_db(&ok, |_| ("Other".to_string(), String::new()));
        assert!(!is_err);
        assert_eq!(json["result"], serde_json::json!("Ok"));
        assert_eq!(json["value"], serde_json::json!(7));

        let err: Result<u8, &str> = Err("not found in the database");
        let (json, is_err) =
            result_serialize_db(&err, |e| ("NotFound".to_string(), (*e).to_string()));
        assert!(is_err);
        assert_eq!(json["result"], serde_json::json!("Err"));
        assert_eq!(json["kind"], serde_json::json!("NotFound"));
        assert_eq!(
            json["message"],
            serde_json::json!("not found in the database")
        );
    }

    // Stand-in for `errors::DatabaseError` (the deja crate does not depend on
    // diesel_models). The macro's structured `recover_err` maps on the recorded
    // `kind` STRING, so this mirrors that mapping exactly.
    #[derive(Debug, PartialEq)]
    enum FakeDatabaseError {
        NotFound,
        UniqueViolation,
    }

    /// Replicates the macro's structured `recover_err`: maps a recorded `kind`
    /// discriminant to a reconstructed error, returning `None` (live
    /// fall-through) for any unknown kind.
    fn structured_recover_err(kind: &str, _message: &str) -> Option<FakeDatabaseError> {
        match kind {
            "NotFound" => Some(FakeDatabaseError::NotFound),
            "UniqueViolation" => Some(FakeDatabaseError::UniqueViolation),
            _ => None,
        }
    }

    /// The structured recover_err maps NotFound/UniqueViolation correctly and
    /// falls through (returns None) on unknown kinds.
    #[test]
    fn structured_recover_err_maps_known_kinds_and_falls_through() {
        assert_eq!(
            structured_recover_err("NotFound", "msg"),
            Some(FakeDatabaseError::NotFound)
        );
        assert_eq!(
            structured_recover_err("UniqueViolation", "msg"),
            Some(FakeDatabaseError::UniqueViolation)
        );
        // Unknown discriminants → None → live fall-through (V1 policy).
        assert_eq!(structured_recover_err("Other", "msg"), None);
        assert_eq!(structured_recover_err("Legacy", "msg"), None);
        assert_eq!(structured_recover_err("anything-else", "msg"), None);
    }

    /// A recorded Err produced by `result_serialize_db` decodes back into a
    /// structured kind that the recover_err can act on end-to-end.
    #[test]
    fn record_then_recover_round_trip() {
        let err: Result<u8, &str> = Err("dup key");
        let (json, _is_err) =
            result_serialize_db(&err, |e| ("UniqueViolation".to_string(), (*e).to_string()));
        let decoded: DejaDatabaseResult =
            serde_json::from_value(json).expect("decode structured err");
        match decoded.payload {
            DejaDatabaseResultPayload::Err { kind, message } => {
                assert_eq!(
                    structured_recover_err(&kind, &message),
                    Some(FakeDatabaseError::UniqueViolation)
                );
            }
            _ => panic!("expected Err payload"),
        }
    }
}
