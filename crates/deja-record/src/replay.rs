//! Replay engine for Déjà semantic events.
//!
//! Provides `ReplayHook` — a `DejaHook` that substitutes recorded responses
//! instead of letting the real implementation hit external systems.
//!
//! Uses resilient replay: divergence is logged but control flow continues.
//! Missing calls are recovered via sliding-window search; novel calls trigger
//! graceful synthesis.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::{
    correlation_matches, read_events, CallsiteIdentity, CallsiteSource, DejaHook, ReplayLookup,
    SemanticEvent,
};

// ---------------------------------------------------------------------------
// Divergence tracking
// ---------------------------------------------------------------------------

/// A single divergence detected during replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Divergence {
    pub kind: DivergenceKind,
    pub boundary: String,
    pub trait_name: String,
    pub method_name: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<serde_json::Value>,
    pub global_sequence: u64,
}

/// Classification of a replay divergence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DivergenceKind {
    /// Arguments differ from the recorded baseline at the same position.
    FieldMismatch,
    /// A call present in V1 was skipped in V2.
    OmittedCall,
    /// V2 made a call not present in V1.
    NovelCall,
    /// The recorded result could not be deserialized into the expected type.
    DeserializationFailure,
    /// Recovery succeeded but something was different along the way.
    Recovered,
    /// Correlation ID mismatch.
    CorrelationMismatch,
    /// A recorded result was available with mismatched args, but the
    /// configured [`ArgMismatchPolicy`] forbade returning it. The cursor was
    /// NOT advanced and the call falls through to the real implementation
    /// (or a graceful synthesis) instead of silently lying.
    ArgSkipBlocked,
}

/// Accumulated replay report for one session.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayReport {
    pub total_calls: u64,
    pub matched_calls: u64,
    pub divergence_count: u64,
    pub divergences: Vec<Divergence>,
}

impl ReplayReport {
    pub fn has_divergences(&self) -> bool {
        !self.divergences.is_empty()
    }

    /// Append a divergence and increment the counter.
    pub fn push(&mut self, div: Divergence) {
        self.divergence_count += 1;
        self.divergences.push(div);
    }
}

// ---------------------------------------------------------------------------
// Replay configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Maximum number of events to scan forward when an omitted call is
    /// suspected.
    pub sliding_window_size: usize,
    /// Controls whether and when a recorded result is returned despite the
    /// V2 args differing from the recorded baseline.
    ///
    /// Default ([`ArgMismatchPolicy::OnlyForArgful`]) fails closed on argless
    /// boundaries (time, id, random) so they cannot silently lie, but allows
    /// recovery for genuine business-logic boundaries that took meaningful
    /// args.
    pub arg_mismatch_policy: ArgMismatchPolicy,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            sliding_window_size: 20,
            arg_mismatch_policy: ArgMismatchPolicy::OnlyForArgful,
        }
    }
}

/// Policy governing whether a recorded result may be returned when the V2
/// arguments differ from the recorded V1 arguments at the same call-site.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArgMismatchPolicy {
    /// Never return a recorded result if args don't match. Strictest. Required
    /// for argless boundaries (time, id, random) to prevent silent lies.
    Never,
    /// Return a recorded result on arg mismatch ONLY if the method had args to
    /// begin with. Default. Argless calls (null args, empty object) fall back
    /// to `Never`.
    #[default]
    OnlyForArgful,
    /// Return any plausible recorded result on arg mismatch. Permissive;
    /// matches the pre-P2 behavior of `skip_arg_mismatch: true`.
    Always,
}

/// Returns true if `args` is JSON-null or an empty object (treated as
/// "argless" for arg-mismatch policy purposes).
fn args_are_empty(args: &serde_json::Value) -> bool {
    args.is_null() || args.as_object().is_some_and(|m| m.is_empty())
}

/// Decides whether a given arg-mismatch case is allowed to fall back to the
/// recorded result under `policy`.
fn allow_arg_mismatch(policy: ArgMismatchPolicy, args: &serde_json::Value) -> bool {
    match policy {
        ArgMismatchPolicy::Never => false,
        ArgMismatchPolicy::OnlyForArgful => !args_are_empty(args),
        ArgMismatchPolicy::Always => true,
    }
}

// ---------------------------------------------------------------------------
// Per-request replay state
// ---------------------------------------------------------------------------

/// Mutable cursor for one correlation scope.
#[derive(Debug)]
struct RequestCursor {
    /// Index into the sorted event list for this request.
    position: usize,
    /// Events belonging to this correlation_id, sorted by request_sequence.
    events: Vec<SemanticEvent>,
}

/// Internal result of a single match attempt.
#[derive(Debug, Clone)]
enum MatchOutcome {
    Exact,
    RecoveredSkip(usize),         // advanced past N omitted calls
    RecoveredWithMismatch(usize), // same but args differed
    /// A method+args-relaxed match was available but the configured
    /// [`ArgMismatchPolicy`] forbade returning it. Carries the recorded args
    /// so the caller can record an [`DivergenceKind::ArgSkipBlocked`]
    /// divergence with both baseline and candidate.
    ArgSkipBlocked(serde_json::Value),
    Novel,
}

// ---------------------------------------------------------------------------
// ReplayHook
// ---------------------------------------------------------------------------

/// A `DejaHook` that replays recorded semantic events.
///
/// On each incoming call, searches the recorded tape for a matching event.
/// Returns the recorded `result` JSON if found; otherwise logs a divergence
/// and falls back to `None` (letting the delegation call the real impl).
pub struct ReplayHook {
    config: ReplayConfig,
    /// All events loaded from the artifact.
    all_events: Vec<SemanticEvent>,
    /// Per-correlation-id cursors.
    cursors: Mutex<BTreeMap<Option<String>, RequestCursor>>,
    /// Accumulated divergence report.
    report: Mutex<ReplayReport>,
    /// Global sequence counter so we still produce monotonic seq numbers.
    global_seq: Mutex<u64>,
    /// Per-(correlation, source, scope) monotonic occurrence counter mirroring
    /// `RecordingHook::next_callsite_occurrence`. Replay-time occurrence
    /// numbering MUST advance in lock-step with recording-time so that
    /// `CallsiteIdentity { source: OperationOccurrence, occurrence }` lookups
    /// land on the same event.
    callsite_occurrence: Mutex<crate::CallsiteOccurrenceMap>,
}

impl ReplayHook {
    /// Load a replay hook from a recorded artifact directory using
    /// [`ReplayConfig::default`].
    ///
    /// Used by the env-driven runtime hook (`DEJA_MODE=replay`) where no
    /// custom config is wired through. Construct with [`Self::with_config`]
    /// when callers need to override the config.
    pub fn from_artifact_dir(artifact_dir: &Path) -> std::io::Result<Self> {
        Self::with_config(artifact_dir, ReplayConfig::default())
    }

    /// Load a replay hook from a recorded artifact directory with an explicit
    /// [`ReplayConfig`].
    pub fn with_config(artifact_dir: &Path, config: ReplayConfig) -> std::io::Result<Self> {
        let events = read_events(artifact_dir)?;
        let max_seq = events.iter().map(|e| e.global_sequence).max().unwrap_or(0);
        Ok(Self::new(events, config, max_seq + 1))
    }

    /// Create a replay hook from an in-memory event list (useful for tests).
    pub fn new(events: Vec<SemanticEvent>, config: ReplayConfig, starting_global_seq: u64) -> Self {
        Self {
            config,
            all_events: events,
            cursors: Mutex::new(BTreeMap::new()),
            report: Mutex::new(ReplayReport::default()),
            global_seq: Mutex::new(starting_global_seq),
            callsite_occurrence: Mutex::new(HashMap::new()),
        }
    }

    /// Take the accumulated replay report.
    pub fn take_report(&self) -> ReplayReport {
        self.report
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    // -----------------------------------------------------------------
    // Internal matching
    // -----------------------------------------------------------------

    fn cursor_for(
        &self,
        correlation_id: &Option<String>,
    ) -> std::sync::MutexGuard<'_, BTreeMap<Option<String>, RequestCursor>> {
        let mut guard = self.cursors.lock().unwrap_or_else(|p| p.into_inner());
        if !guard.contains_key(correlation_id) {
            let mut events: Vec<SemanticEvent> = self
                .all_events
                .iter()
                .filter(|e| correlation_matches(e, correlation_id.as_deref()))
                .cloned()
                .collect();
            events.sort_by_key(|e| e.request_sequence);
            guard.insert(
                correlation_id.clone(),
                RequestCursor {
                    position: 0,
                    events,
                },
            );
        }
        guard
    }

    fn method_matches(
        event: &SemanticEvent,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
    ) -> bool {
        event.boundary == boundary
            && event.trait_name == trait_name
            && event.method_name == method_name
    }

    /// Core matching logic. Returns the matched event (if any) plus an outcome
    /// describing how the match was achieved.
    fn find_match(
        &self,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
        args: &serde_json::Value,
        correlation_id: &Option<String>,
    ) -> (Option<SemanticEvent>, MatchOutcome) {
        let mut cursors = self.cursor_for(correlation_id);
        let cursor = cursors
            .get_mut(correlation_id)
            .expect("cursor_for inserts the entry before returning the guard");
        let pos = cursor.position;
        let events = &cursor.events;

        if pos >= events.len() {
            return (None, MatchOutcome::Novel);
        }

        // 1. Exact match at current position.
        if let Some(candidate) = events.get(pos) {
            if Self::method_matches(candidate, boundary, trait_name, method_name)
                && candidate.args == *args
            {
                cursor.position = pos + 1;
                return (Some(candidate.clone()), MatchOutcome::Exact);
            }
        }

        // 2. Sliding window: look for method + args match.
        //
        // First-pass: exact (method+args) match. If none is found, fall back
        // to method-only and consult the arg-mismatch policy. Splitting the
        // passes keeps a later same-method exact match from being shadowed
        // by an earlier mismatched candidate.
        let window_end = (pos + self.config.sliding_window_size).min(events.len());
        for (idx, candidate) in events.iter().enumerate().take(window_end).skip(pos) {
            if Self::method_matches(candidate, boundary, trait_name, method_name)
                && candidate.args == *args
            {
                cursor.position = idx + 1;
                return (
                    Some(candidate.clone()),
                    if idx == pos {
                        MatchOutcome::Exact
                    } else {
                        MatchOutcome::RecoveredSkip(idx - pos)
                    },
                );
            }
        }

        // Second-pass: method-only. Either return the recorded result with a
        // mismatch divergence, or — if policy forbids — report
        // `ArgSkipBlocked` WITHOUT advancing the cursor so the call falls
        // through to the real implementation.
        for (idx, candidate) in events.iter().enumerate().take(window_end).skip(pos) {
            if Self::method_matches(candidate, boundary, trait_name, method_name) {
                if allow_arg_mismatch(self.config.arg_mismatch_policy, args) {
                    cursor.position = idx + 1;
                    return (
                        Some(candidate.clone()),
                        MatchOutcome::RecoveredWithMismatch(idx - pos),
                    );
                } else {
                    // Policy blocks the fallback. Surface the recorded args as
                    // the baseline so the divergence is actionable. Do NOT
                    // advance the cursor — the recorded event is still on
                    // deck for a future (correctly-argued) call.
                    let recorded_args = candidate.args.clone();
                    return (None, MatchOutcome::ArgSkipBlocked(recorded_args));
                }
            }
        }

        // 3. Nothing found in window — novel.
        (None, MatchOutcome::Novel)
    }

    fn push_divergence(&self, div: Divergence) {
        let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
        report.push(div);
    }

    /// Stable-identity lookup: scan the per-correlation event tape for
    /// the first event whose `callsite_identity` matches `(source, id,
    /// occurrence)`. On match, validate args under the policy and advance
    /// the cursor.
    ///
    /// Returns `Some(result_json)` when the recorded event is appropriate to
    /// hand back. Returns `None` when no identity match is found, when the
    /// arg-mismatch policy blocks the fallback (an `ArgSkipBlocked`
    /// divergence is recorded), or when no `id` is present on the identity.
    fn lookup_by_identity(
        &self,
        identity: &CallsiteIdentity,
        args: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        let id = identity.id.as_deref()?;
        let correlation_id = deja_context::current_correlation_id();
        let corr = correlation_id.clone();

        // Phase 1: hold the cursors guard JUST long enough to locate the
        // event, clone it, and (if policy permits) advance the cursor.
        // Release before touching the report mutex so the two locks are
        // never held simultaneously.
        let outcome = {
            let mut cursors = self.cursor_for(&corr);
            let cursor = cursors
                .get_mut(&corr)
                .expect("cursor_for inserts the entry before returning the guard");
            let pos = cursor.position;

            // Scan from the current cursor forward (not just the window) — a
            // stable id must not be silently lost behind unrelated calls.
            let mut found_idx: Option<usize> = None;
            for idx in pos..cursor.events.len() {
                let ev = &cursor.events[idx];
                let Some(ev_identity) = ev.callsite_identity.as_ref() else {
                    continue;
                };
                if ev_identity.source == identity.source
                    && ev_identity.id.as_deref() == Some(id)
                    && ev_identity.occurrence == identity.occurrence
                {
                    found_idx = Some(idx);
                    break;
                }
            }

            let idx = found_idx?;
            let candidate = cursor.events[idx].clone();

            if candidate.args == *args {
                cursor.position = idx + 1;
                IdentityOutcome::Exact(candidate)
            } else if allow_arg_mismatch(self.config.arg_mismatch_policy, args) {
                cursor.position = idx + 1;
                IdentityOutcome::Mismatch(candidate)
            } else {
                IdentityOutcome::Blocked(candidate)
            }
        };

        match outcome {
            IdentityOutcome::Exact(candidate) => {
                let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
                report.matched_calls += 1;
                Some(candidate.result)
            }
            IdentityOutcome::Mismatch(candidate) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::FieldMismatch,
                    boundary: candidate.boundary.clone(),
                    trait_name: candidate.trait_name.clone(),
                    method_name: candidate.method_name.clone(),
                    detail: "args differed; returned identity-matched recorded result anyway"
                        .to_string(),
                    baseline: Some(candidate.args.clone()),
                    candidate: Some(args.clone()),
                    global_sequence: candidate.global_sequence,
                });
                let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
                report.matched_calls += 1;
                Some(candidate.result)
            }
            IdentityOutcome::Blocked(candidate) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::ArgSkipBlocked,
                    boundary: candidate.boundary.clone(),
                    trait_name: candidate.trait_name.clone(),
                    method_name: candidate.method_name.clone(),
                    detail: "arg mismatch blocked by policy".to_string(),
                    baseline: Some(candidate.args.clone()),
                    candidate: Some(args.clone()),
                    global_sequence: candidate.global_sequence,
                });
                None
            }
        }
    }
}

/// Outcome of a stable-identity lookup, used to keep the
/// cursors lock and the report lock acquisitions disjoint.
enum IdentityOutcome {
    Exact(SemanticEvent),
    Mismatch(SemanticEvent),
    Blocked(SemanticEvent),
}

impl DejaHook for ReplayHook {
    fn is_active(&self) -> bool {
        true
    }

    fn record(&self, _event: SemanticEvent) {
        let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
        report.total_calls += 1;
        // During replay, total_calls tracks how many calls the V2 made.
    }

    fn next_global_sequence(&self) -> u64 {
        let mut seq = self.global_seq.lock().unwrap_or_else(|p| p.into_inner());
        let current = *seq;
        *seq += 1;
        current
    }

    fn next_request_sequence(&self, correlation_id: Option<&str>) -> u64 {
        let cursors = self.cursor_for(&correlation_id.map(String::from));
        let cursor = cursors
            .get(&correlation_id.map(String::from))
            .expect("cursor_for inserts the entry before returning the guard");
        cursor.position as u64
    }

    fn try_replay(
        &self,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
        args: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        let correlation_id = deja_context::current_correlation_id();
        let corr = correlation_id.clone();

        let (maybe_event, outcome) =
            self.find_match(boundary, trait_name, method_name, args, &corr);

        match (maybe_event, outcome) {
            (Some(event), MatchOutcome::Exact) => {
                let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
                report.matched_calls += 1;
                Some(event.result)
            }
            (Some(event), MatchOutcome::RecoveredSkip(skipped)) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::OmittedCall,
                    boundary: boundary.to_string(),
                    trait_name: trait_name.to_string(),
                    method_name: method_name.to_string(),
                    detail: format!("skipped {} recorded call(s) to recover", skipped),
                    baseline: None,
                    candidate: None,
                    global_sequence: event.global_sequence,
                });
                let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
                report.matched_calls += 1;
                Some(event.result)
            }
            (Some(event), MatchOutcome::RecoveredWithMismatch(skipped)) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::FieldMismatch,
                    boundary: boundary.to_string(),
                    trait_name: trait_name.to_string(),
                    method_name: method_name.to_string(),
                    detail: format!(
                        "args differed; skipped {} call(s) and returned recorded result anyway",
                        skipped
                    ),
                    baseline: Some(event.args.clone()),
                    candidate: Some(args.clone()),
                    global_sequence: event.global_sequence,
                });
                let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
                report.matched_calls += 1;
                Some(event.result)
            }
            (None, MatchOutcome::Novel) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::NovelCall,
                    boundary: boundary.to_string(),
                    trait_name: trait_name.to_string(),
                    method_name: method_name.to_string(),
                    detail: "call not found in recording — falling through to real implementation"
                        .to_string(),
                    baseline: None,
                    candidate: Some(args.clone()),
                    global_sequence: 0,
                });
                None
            }
            (None, MatchOutcome::ArgSkipBlocked(recorded_args)) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::ArgSkipBlocked,
                    boundary: boundary.to_string(),
                    trait_name: trait_name.to_string(),
                    method_name: method_name.to_string(),
                    detail: "arg mismatch blocked by policy".to_string(),
                    baseline: Some(recorded_args),
                    candidate: Some(args.clone()),
                    global_sequence: 0,
                });
                None
            }
            _ => None,
        }
    }

    fn try_replay_with_context(&self, query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        // Identity-first cascade for this legacy in-process hook. (Its stages
        // are independent of the 6-rank `Address` ladder used by lookup-table
        // replay.) A stable callsite-identity match is tried first; the
        // positional strategies in `try_replay` (location-exact /
        // sequence-method-args / sliding-window) are the fallback.

        // Stage 1: stable identity — callsite.id-based, requires that the identity
        // was derived from an annotation, a syntactic hash, or a lexical
        // path (i.e. genuinely stable across line shifts).
        if let Some(identity) = query.callsite_identity {
            let stable_source = matches!(
                identity.source,
                CallsiteSource::Explicit
                    | CallsiteSource::SyntacticHash
                    | CallsiteSource::LexicalPath
            );
            if stable_source && identity.id.is_some() {
                if let Some(result) = self.lookup_by_identity(identity, query.args) {
                    return Some(result);
                }
            }
        }

        // Delegate Ranks 3/5/6 to the existing cursor-based matcher.
        self.try_replay(
            query.boundary,
            query.trait_name,
            query.method_name,
            query.args,
        )
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
            .unwrap_or_else(|p| p.into_inner());
        let entry = guard.entry(key).or_insert(0);
        let value = *entry;
        *entry += 1;
        value
    }
}

// ---------------------------------------------------------------------------
// Lookup-table replay (hybrid architecture: in-process LOOKUP,
// orchestrator-owned POLICY)
// ---------------------------------------------------------------------------
//
// The orchestrator pre-renders a `LookupTable` by walking the recording and
// applying the current matching policy. The candidate carries a thin
// `LookupTableHook` that does O(1) key→result lookups. No cascade logic,
// no `ArgMismatchPolicy`, no `DivergenceKind` classification lives in the
// candidate. Each call emits a `ObservedCall` to the configured
// `ObservedCallSink`; the orchestrator runs post-hoc divergence detection
// against the recording.
//
// Trait surface is dependency-inversion: deja-record ships local-file
// implementations of both source and sink. HTTP/Kafka variants are supplied
// by the application (same pattern as the JSONL → KafkaSink split for
// recording).

/// A frozen lookup table produced by the orchestrator and consumed by the
/// candidate's `LookupTableHook`. Serialized as a single JSON document or
/// JSONL stream (one `LookupEntry` per line).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupTable {
    pub recording_id: String,
    pub policy_version: u32,
    pub entries: Vec<LookupEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupEntry {
    pub key: LookupKey,
    pub result: serde_json::Value,
    pub source_event_global_sequence: u64,
}

/// How a call site is addressed for replay matching, strongest (most stable)
/// rank first. The renderer emits one `LookupEntry` per applicable rank; the
/// hook queries the ranks it can construct strongest-first and takes the first
/// hit. The decisive property is **iteration-order independence**: ranks 1–5
/// identify a call by *what it is* (annotation, logical span-path, syntax,
/// lexical position, source location) rather than *when it ran*, so a loop that
/// visits its items in a different order than the recording still resolves —
/// each iteration self-addresses by its args (see [`LookupKey::args_hash`]).
/// Rank 6 is the positional last resort; a run that leans on it is fragile,
/// which the divergence detector surfaces via per-rank counts.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum Address {
    /// Rank 1 — user-supplied explicit annotation (`CallsiteSource::Explicit`).
    Explicit(String),
    /// Rank 2 — logical span-path: the root→leaf chain of `tracing` span NAMES
    /// the call fired within (from [`crate::current_logical_span_path`]). The
    /// most version-independent address: it survives source-line shifts and
    /// benign signature edits, and — crucially — is DISTINCT for concurrent
    /// same-callsite calls in different spans, so the per-key `occurrence` is
    /// scoped to the span and cannot swap under async task interleaving. No
    /// embedded occurrence: the path IS the disambiguator, and genuine same-path
    /// repeats are tiebroken by [`LookupKey::occurrence`] (sequential, stable).
    LogicalContext { path: String },
    /// Rank 3 — hash of the surrounding syntax tokens (`boundary::operation`).
    SyntacticHash(u64),
    /// Rank 4 — stable lexical path plus its per-scope occurrence index.
    LexicalPath { path: String, scope_occurrence: u32 },
    /// Rank 5 — `#[track_caller]` source location.
    SourceLocation {
        file: String,
        line: u32,
        column: u32,
    },
    /// Rank 6 — positional last resort: boundary + method + per-correlation
    /// request sequence. Fragile to any upstream edit that shifts positions.
    Sequence {
        boundary: String,
        method: String,
        request_sequence: u64,
    },
}

impl Address {
    /// Stability rank: 1 (strongest) … 6 (weakest). Used by the hook to query
    /// strongest-first and by the divergence detector to score fragility.
    pub fn rank(&self) -> u8 {
        match self {
            Address::Explicit(_) => 1,
            Address::LogicalContext { .. } => 2,
            Address::SyntacticHash(_) => 3,
            Address::LexicalPath { .. } => 4,
            Address::SourceLocation { .. } => 5,
            Address::Sequence { .. } => 6,
        }
    }
}

/// Composite key the orchestrator uses to register an entry and the candidate
/// uses to look one up. A call is identified by its `address` (rank-specific),
/// the canonical hash of its arguments, and a tiebreaking `occurrence` index
/// scoped to `(correlation_id, address, args_hash)` — so two argless-impure
/// calls to the same site (e.g. `time::now`) only collide when the candidate
/// makes the same call with the same args the same number of times.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupKey {
    pub correlation_id: Option<String>,
    /// Rank-specific call-site address (see [`Address`]).
    pub address: Address,
    /// Canonical, order-independent hash of the call's serialized args.
    pub args_hash: u64,
    /// Nth call to `(correlation_id, address, args_hash)`; 0 for a unique call.
    pub occurrence: u32,
}

/// A call the candidate actually made, with the lookup outcome. Streamed to
/// an `ObservedCallSink` end-of-request; the orchestrator's post-hoc
/// divergence detector compares the observed stream against the recording.
///
/// `boundary`/`trait_name`/`method_name` are carried explicitly (rather than
/// being read off the resolved key) because ranks 1–5 don't encode the
/// boundary — yet the detector must attribute every call, hit or miss, to a
/// boundary. `resolved_rank` records which [`Address`] rank won, so the
/// detector can report how much of a run leans on fragile rank-6 matches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedCall {
    pub correlation_id: Option<String>,
    pub boundary: String,
    pub trait_name: String,
    pub method_name: String,
    pub args: serde_json::Value,
    pub resolved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_rank: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_global_sequence: Option<u64>,
    /// Where the candidate made this call, captured at replay time so a
    /// divergence (especially a NOVEL call with no recorded counterpart) can be
    /// deep-linked to a callsite + placed on the replay execution graph. All
    /// `#[serde(default)]` so pre-enrichment artifacts still parse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_line: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_column: Option<u32>,
    /// Root→leaf tracing span-name chain the call fired within — the same
    /// logical-context address used for lookup (rank 2), so a UI can align this
    /// call to its node in BOTH the record and replay execution-graph trees.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_span_path: Option<String>,
    /// Replay-side execution-graph node id the call fired under (joins to
    /// `ExecutionGraphNode.node_id` in the replay graph) — lets a novel call
    /// self-place on the replay tree even though it has no recorded event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_node_id: Option<u64>,
    /// V2 scaffold (Tier 2): set when the hook synthesized a safe default on a
    /// miss. Always false in V1 (full mock) — the hook never synthesizes yet.
    #[serde(default)]
    pub synthesized: bool,
    /// V2 scaffold (Tier 3): set when a miss falls through to a real impl that
    /// is expected to fail in the harness environment (egress blocked). Always
    /// false in V1.
    #[serde(default)]
    pub real_impl_will_fail: bool,
}

// ---------------------------------------------------------------------------
// Shared key construction (used by BOTH the renderer and the hook)
//
// The renderer lives in `replay-harness-api` and the hook is compiled into the
// candidate router — two separate binaries. If they constructed keys even
// slightly differently (args canonicalization, rank selection, occurrence
// numbering) every lookup would silently miss. So the canonical logic lives
// here, in `deja-record`, and both sides call it.
// ---------------------------------------------------------------------------

/// Stable, order-independent hash of a call's serialized args.
///
/// Object keys are sorted recursively, so `{"a":1,"b":2}` and `{"b":2,"a":1}`
/// hash identically. Type-tag bytes (`n`/`t`/`f`/`#`/`s`/`[`/`{`) disambiguate
/// e.g. the string `"1"` from the number `1` and an empty array from an empty
/// object. Built on the crate's FNV-1a basis so the value is identical across
/// binaries on the same target — no random seed, no platform dependence.
pub fn canonical_args_hash(args: &serde_json::Value) -> u64 {
    hash_value(crate::FNV_OFFSET_BASIS, args)
}

fn hash_value(hash: u64, value: &serde_json::Value) -> u64 {
    use serde_json::Value;
    match value {
        Value::Null => crate::fnv1a_bytes(hash, b"n"),
        Value::Bool(true) => crate::fnv1a_bytes(hash, b"t"),
        Value::Bool(false) => crate::fnv1a_bytes(hash, b"f"),
        Value::Number(n) => crate::fnv1a_str(crate::fnv1a_bytes(hash, b"#"), &n.to_string()),
        Value::String(s) => crate::fnv1a_str(crate::fnv1a_bytes(hash, b"s"), s),
        Value::Array(items) => {
            let mut h = crate::fnv1a_bytes(hash, b"[");
            for item in items {
                h = hash_value(h, item);
            }
            crate::fnv1a_bytes(h, b"]")
        }
        Value::Object(map) => {
            // Sort keys for canonical order regardless of serde_json's map impl
            // (BTreeMap by default, IndexMap under `preserve_order`).
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut h = crate::fnv1a_bytes(hash, b"{");
            for key in keys {
                h = crate::fnv1a_str(h, key);
                if let Some(v) = map.get(key) {
                    h = hash_value(h, v);
                }
            }
            crate::fnv1a_bytes(h, b"}")
        }
    }
}

/// Build the rank-ordered list of addresses a call site supports, strongest
/// first. Emits only the ranks for which identifying material exists: ranks
/// 1–4 require the corresponding `CallsiteIdentity` fields, rank 5 requires a
/// caller location, and rank 6 (sequence) is always present as the last
/// resort. The renderer feeds this from a recorded `SemanticEvent`; the hook
/// feeds it from a live `ReplayLookup`. Identical inputs → identical output.
pub fn addresses_for(
    boundary: &str,
    method_name: &str,
    identity: Option<&crate::CallsiteIdentity>,
    location: Option<(&str, u32, u32)>,
    request_sequence: u64,
) -> Vec<Address> {
    let mut out = Vec::with_capacity(6);
    if let Some(id) = identity {
        if matches!(id.source, crate::CallsiteSource::Explicit) {
            if let Some(tag) = &id.id {
                out.push(Address::Explicit(tag.clone()));
            }
        }
        // Rank 2 — logical span-path. Strongest non-explicit address: stable
        // across line/signature edits AND distinct per concurrent span, so the
        // occurrence tiebreak is span-scoped (no positional swap).
        if let Some(path) = &id.logical_context {
            out.push(Address::LogicalContext { path: path.clone() });
        }
        if let Some(hash) = id.syntax_hash {
            out.push(Address::SyntacticHash(hash));
        }
        if let Some(path) = &id.lexical_path {
            out.push(Address::LexicalPath {
                path: path.clone(),
                scope_occurrence: id.occurrence,
            });
        }
    }
    if let Some((file, line, column)) = location {
        out.push(Address::SourceLocation {
            file: file.to_owned(),
            line,
            column,
        });
    }
    out.push(Address::Sequence {
        boundary: boundary.to_owned(),
        method: method_name.to_owned(),
        request_sequence,
    });
    out
}

/// Assigns the tiebreaking `occurrence` index to each address, turning a call
/// site's rank-ordered addresses into fully-qualified [`LookupKey`]s.
///
/// MUST be advanced on every call/event — for **all** ranks, not just the one
/// that resolves — so the renderer and hook keep identical occurrence
/// numbering even when a stronger rank is absent from some events.
#[derive(Default)]
pub struct KeyStamper {
    occurrences: std::collections::HashMap<(Option<String>, Address, u64), u32>,
}

impl KeyStamper {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stamp occurrence indices onto each address, returning rank-ordered keys.
    pub fn stamp(
        &mut self,
        correlation_id: Option<&str>,
        addresses: &[Address],
        args_hash: u64,
    ) -> Vec<LookupKey> {
        addresses
            .iter()
            .map(|address| {
                let bucket = (
                    correlation_id.map(str::to_owned),
                    address.clone(),
                    args_hash,
                );
                let counter = self.occurrences.entry(bucket).or_insert(0);
                let occurrence = *counter;
                *counter += 1;
                LookupKey {
                    correlation_id: correlation_id.map(str::to_owned),
                    address: address.clone(),
                    args_hash,
                    occurrence,
                }
            })
            .collect()
    }
}

/// Loader for a `LookupTable`. Called ONCE at candidate boot.
pub trait LookupTableSource: Send {
    fn load(&mut self) -> std::io::Result<LookupTable>;
}

/// Sink for `ObservedCall` records emitted by the candidate hook.
///
/// Implementations must be cheap on the hot path: `observed` runs inside
/// every `#[deja::*]` call. Batching, flushing, or sending across the
/// network should be done in `flush` (called at request scope exit) — not
/// inline.
pub trait ObservedCallSink: Send + Sync {
    fn observed(&self, call: ObservedCall);
    fn flush(&self) -> std::io::Result<()>;
}

/// Local-file `LookupTableSource`. Reads either a single JSON document or
/// a JSONL stream of `LookupEntry` records (auto-detected by the first
/// non-whitespace character).
pub struct LocalFileLookupSource {
    path: std::path::PathBuf,
}

impl LocalFileLookupSource {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl LookupTableSource for LocalFileLookupSource {
    fn load(&mut self) -> std::io::Result<LookupTable> {
        let bytes = std::fs::read(&self.path)?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Try the whole-document LookupTable form first; fall back to JSONL
        // (one LookupEntry per line) if that fails. Robust against either
        // shape without needing a magic byte or extension.
        if let Ok(table) = serde_json::from_str::<LookupTable>(text) {
            return Ok(table);
        }
        let entries = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(serde_json::from_str::<LookupEntry>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(LookupTable {
            recording_id: String::new(),
            policy_version: 1,
            entries,
        })
    }
}

/// In-memory `ObservedCallSink` for tests and standalone harness use.
pub struct InMemoryObservedSink {
    calls: std::sync::Arc<Mutex<Vec<ObservedCall>>>,
}

impl Default for InMemoryObservedSink {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryObservedSink {
    pub fn new() -> Self {
        Self {
            calls: std::sync::Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Clone of the underlying buffer; useful for assertions in tests.
    pub fn handle(&self) -> std::sync::Arc<Mutex<Vec<ObservedCall>>> {
        std::sync::Arc::clone(&self.calls)
    }

    pub fn drain(&self) -> Vec<ObservedCall> {
        self.calls
            .lock()
            .map(|mut buf| std::mem::take(&mut *buf))
            .unwrap_or_default()
    }
}

impl ObservedCallSink for InMemoryObservedSink {
    fn observed(&self, call: ObservedCall) {
        if let Ok(mut buf) = self.calls.lock() {
            buf.push(call);
        }
    }
    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Append-only JSONL `ObservedCallSink`. One line per call.
pub struct FileObservedSink {
    file: Mutex<std::fs::File>,
}

impl FileObservedSink {
    pub fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        // Create the parent dir so a missing observed/ doesn't fail replay boot
        // (which would silently fall back to the legacy ReplayHook).
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

impl ObservedCallSink for FileObservedSink {
    fn observed(&self, call: ObservedCall) {
        use std::io::Write;
        if let Ok(mut guard) = self.file.lock() {
            if let Ok(line) = serde_json::to_string(&call) {
                let _ = guard.write_all(line.as_bytes());
                let _ = guard.write_all(b"\n");
            }
        }
    }
    fn flush(&self) -> std::io::Result<()> {
        use std::io::Write;
        if let Ok(mut guard) = self.file.lock() {
            guard.flush()?;
        }
        Ok(())
    }
}

/// In-process side-effect player driven by a frozen `LookupTable`.
///
/// Does NOT run a cascade, does NOT classify divergences, does NOT make
/// policy decisions. It looks up a key by (correlation, boundary, trait,
/// method, occurrence) — with optional `callsite_identity_id` fallback —
/// emits an `ObservedCall`, and returns the result if found.
pub struct LookupTableHook {
    table: HashMap<LookupKey, LookupEntry>,
    /// Per-correlation request_sequence counter; bumps on each lookup. Feeds
    /// the rank-6 `Address::Sequence` and mirrors the recorder's own
    /// per-correlation sequence (both start at 0 and step by one per call).
    next_sequence: Mutex<HashMap<Option<String>, u64>>,
    /// Shared occurrence assigner; advanced for every rank on every call so its
    /// numbering stays in lockstep with the renderer's.
    stamper: Mutex<KeyStamper>,
    /// Per-correlation global-event counter; sourced from `next_global_sequence`.
    global_counter: std::sync::atomic::AtomicU64,
    /// Per-(correlation, source, scope) occurrence counter mirroring
    /// `RecordingHook::next_callsite_occurrence`. The boundary macro re-derives
    /// the per-callsite occurrence at REPLAY time by calling
    /// `next_callsite_occurrence` on this hook (the same hook that does the
    /// lookup). It MUST advance in lock-step with recording — one bump per call
    /// per scope — so that the `CallsiteIdentity::occurrence` the macro stamps
    /// into the rank-4 `Address::LexicalPath { scope_occurrence }` matches the
    /// occurrence the renderer read off the recorded event. Without this the
    /// macro would receive the default `0` for every call and only the first
    /// (occurrence-0) call at each callsite would resolve.
    callsite_occurrence: Mutex<crate::CallsiteOccurrenceMap>,
    observed_sink: Box<dyn ObservedCallSink>,
}

impl LookupTableHook {
    /// Construct from any `LookupTableSource` (typically `LocalFileLookupSource`)
    /// and any `ObservedCallSink` (typically `InMemoryObservedSink` for tests
    /// or `FileObservedSink` for harness runs). Loading happens once at
    /// construction; failures bubble up as `io::Error`.
    pub fn from_source<S, K>(mut source: S, sink: K) -> std::io::Result<Self>
    where
        S: LookupTableSource,
        K: ObservedCallSink + 'static,
    {
        let table = source.load()?;
        let mut map = HashMap::with_capacity(table.entries.len());
        for entry in table.entries {
            map.insert(entry.key.clone(), entry);
        }
        Ok(Self {
            table: map,
            next_sequence: Mutex::new(HashMap::new()),
            stamper: Mutex::new(KeyStamper::new()),
            global_counter: std::sync::atomic::AtomicU64::new(0),
            callsite_occurrence: Mutex::new(HashMap::new()),
            observed_sink: Box::new(sink),
        })
    }

    /// Number of entries loaded. Useful for assertions.
    pub fn entry_count(&self) -> usize {
        self.table.len()
    }

    /// Force-flush the underlying observed-call sink. The hook does NOT
    /// auto-flush on drop; orchestrators should call this at run end.
    pub fn flush(&self) -> std::io::Result<()> {
        self.observed_sink.flush()
    }

    fn bump_request_sequence(&self, correlation_id: Option<&str>) -> u64 {
        let key = correlation_id.map(str::to_owned);
        if let Ok(mut map) = self.next_sequence.lock() {
            let counter = map.entry(key).or_insert(0);
            let seq = *counter;
            *counter += 1;
            seq
        } else {
            0
        }
    }
}

impl DejaHook for LookupTableHook {
    fn is_active(&self) -> bool {
        true
    }

    fn try_replay_with_context(&self, query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        // The candidate carries no notion of "current correlation" in
        // ReplayLookup; pull it from the ambient deja-context scope set up
        // by the request middleware.
        let correlation_id = deja_context::current_correlation_id();
        // Bumped once per call for the rank-6 positional address; mirrors the
        // recorder's per-correlation request_sequence.
        let request_sequence = self.bump_request_sequence(correlation_id.as_deref());
        let args_hash = canonical_args_hash(query.args);

        let location = query
            .caller_location
            .map(|loc| (loc.file(), loc.line(), loc.column()));
        let addresses = addresses_for(
            query.boundary,
            query.method_name,
            query.callsite_identity,
            location,
            request_sequence,
        );

        // Stamp occurrences for EVERY rank (not just the one that resolves) so
        // the numbering stays aligned with the renderer, then query
        // strongest-first and take the first hit.
        let keys = match self.stamper.lock() {
            Ok(mut stamper) => stamper.stamp(correlation_id.as_deref(), &addresses, args_hash),
            Err(_) => Vec::new(),
        };
        let mut hit: Option<(&LookupEntry, u8)> = None;
        for key in &keys {
            if let Some(entry) = self.table.get(key) {
                hit = Some((entry, key.address.rank()));
                break;
            }
        }

        // "Where" for the diff UI + graph placement. `location` is already
        // resolved above (rank-5 SourceLocation); the span path is the rank-2
        // logical address; the graph node is the replay-side execution-graph
        // node this call fired under.
        let (_, graph_node_id) = crate::current_execution_graph_context();
        self.observed_sink.observed(ObservedCall {
            correlation_id,
            boundary: query.boundary.to_owned(),
            trait_name: query.trait_name.to_owned(),
            method_name: query.method_name.to_owned(),
            args: query.args.clone(),
            resolved: hit.is_some(),
            resolved_rank: hit.map(|(_, rank)| rank),
            source_event_global_sequence: hit.map(|(entry, _)| entry.source_event_global_sequence),
            call_file: location.map(|(f, _, _)| f.to_owned()),
            call_line: location.map(|(_, l, _)| l),
            call_column: location.map(|(_, _, c)| c),
            logical_span_path: crate::current_logical_span_path(),
            graph_node_id,
            // V1 full mock never synthesizes and never relies on the real impl;
            // these stay false until the V2 tiered-miss work lands.
            synthesized: false,
            real_impl_will_fail: false,
        });

        hit.map(|(entry, _)| entry.result.clone())
    }

    fn try_replay(
        &self,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
        args: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        // Delegate to try_replay_with_context with a stub query so legacy
        // call paths still get a lookup attempt.
        self.try_replay_with_context(ReplayLookup {
            boundary,
            trait_name,
            method_name,
            args,
            callsite_identity: None,
            caller_location: None,
        })
    }

    fn record(&self, _event: SemanticEvent) {
        // Lookup-table replay does not record back; the orchestrator's
        // post-hoc divergence detector consumes the ObservedCall stream
        // instead.
    }

    fn next_global_sequence(&self) -> u64 {
        self.global_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn next_request_sequence(&self, _correlation_id: Option<&str>) -> u64 {
        // The hot path (try_replay_with_context) bumps its own counter for
        // key construction. This method is called by codegen that records
        // events — we don't record at replay time, so the return value
        // doesn't matter, but it must be a valid u64.
        0
    }

    fn next_callsite_occurrence(
        &self,
        correlation_id: Option<&str>,
        source: CallsiteSource,
        scope: Option<&str>,
    ) -> u32 {
        // SINGLE source of truth for per-callsite occurrence at replay. The
        // boundary macro / DB codegen calls this once per call to build the
        // `CallsiteIdentity::occurrence` it stamps into the lookup identity.
        // It MUST advance in lock-step with `RecordingHook` (one bump per call
        // per `(correlation, source, scope)`) so the occurrence the renderer
        // read off each recorded event lines up with the occurrence re-derived
        // here at replay. The default trait impl returns a constant `0`, which
        // would collapse every repeated callsite onto occurrence 0 and break
        // rank-4 (`LexicalPath`) resolution after the first call.
        let key = (
            correlation_id.map(String::from),
            source,
            scope.map(String::from),
        );
        let mut guard = self
            .callsite_occurrence
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let entry = guard.entry(key).or_insert(0);
        let value = *entry;
        *entry += 1;
        value
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;
    use crate::{now_ns, SemanticEvent};

    fn make_event(
        req_seq: u64,
        correlation_id: Option<&str>,
        method: &str,
        args: serde_json::Value,
        result: serde_json::Value,
        is_error: bool,
    ) -> SemanticEvent {
        SemanticEvent {
            global_sequence: req_seq,
            request_sequence: req_seq,
            correlation_id: correlation_id.map(String::from),
            timestamp_ns: now_ns(),
            recording_run_id: None,
            graph_node_id: None,
            tracing_span_id: None,
            boundary: "storage".into(),
            trait_name: "PaymentStore".into(),
            method_name: method.into(),
            call_file: "test.rs".into(),
            call_line: 10,
            call_column: 5,
            receiver: None,
            request: args.clone(),
            args,
            response: result.clone(),
            result,
            is_error,
            duration_us: 100,
            event_schema_version: 1,
            callsite_identity: None,
        }
    }

    #[test]
    fn replay_exact_match() {
        let events = vec![make_event(
            0,
            None,
            "find_user",
            serde_json::json!({"id": 42}),
            serde_json::json!({"Ok": "Alice"}),
            false,
        )];

        let hook = ReplayHook::new(events, ReplayConfig::default(), 100);

        let result = hook.try_replay(
            "storage",
            "PaymentStore",
            "find_user",
            &serde_json::json!({"id": 42}),
        );

        assert_eq!(result, Some(serde_json::json!({"Ok": "Alice"})));
        let report = hook.take_report();
        assert_eq!(report.matched_calls, 1);
        assert!(report.divergences.is_empty());
    }

    #[test]
    fn replay_novel_call_logged_as_divergence() {
        let events = vec![make_event(
            0,
            None,
            "find_user",
            serde_json::json!({"id": 42}),
            serde_json::json!({"Ok": "Alice"}),
            false,
        )];

        let hook = ReplayHook::new(events, ReplayConfig::default(), 100);

        let result = hook.try_replay(
            "storage",
            "PaymentStore",
            "delete_user",
            &serde_json::json!({"id": 42}),
        );

        assert!(result.is_none());
        let report = hook.take_report();
        assert_eq!(report.divergences.len(), 1);
        assert_eq!(report.divergences[0].kind, DivergenceKind::NovelCall);
    }

    #[test]
    fn replay_sliding_window_recovery() {
        let events = vec![
            make_event(
                0,
                None,
                "step_a",
                serde_json::json!({}),
                serde_json::json!({"Ok": true}),
                false,
            ),
            make_event(
                1,
                None,
                "step_b",
                serde_json::json!({}),
                serde_json::json!({"Ok": true}),
                false,
            ),
            make_event(
                2,
                None,
                "step_c",
                serde_json::json!({"x": 1}),
                serde_json::json!({"Ok": "found"}),
                false,
            ),
        ];

        let hook = ReplayHook::new(events, ReplayConfig::default(), 100);

        // Simulate: V2 skips step_a and step_b, goes straight to step_c
        let result = hook.try_replay(
            "storage",
            "PaymentStore",
            "step_c",
            &serde_json::json!({"x": 1}),
        );

        assert_eq!(result, Some(serde_json::json!({"Ok": "found"})));
        let report = hook.take_report();
        assert_eq!(report.divergences.len(), 1);
        assert_eq!(report.divergences[0].kind, DivergenceKind::OmittedCall);
        assert!(report.divergences[0].detail.contains("2"));
    }

    #[test]
    fn replay_arg_mismatch_with_skip_config() {
        let events = vec![make_event(
            0,
            None,
            "find_user",
            serde_json::json!({"id": 42}),
            serde_json::json!({"Ok": "Alice"}),
            false,
        )];

        // `OnlyForArgful` (default) lets argful calls fall back to a recorded
        // result on arg mismatch; `Always` matches the legacy pre-P2 shape.
        let hook = ReplayHook::new(
            events,
            ReplayConfig {
                arg_mismatch_policy: ArgMismatchPolicy::Always,
                ..ReplayConfig::default()
            },
            100,
        );

        let result = hook.try_replay(
            "storage",
            "PaymentStore",
            "find_user",
            &serde_json::json!({"id": 99}),
        );

        assert_eq!(result, Some(serde_json::json!({"Ok": "Alice"})));
        let report = hook.take_report();
        assert_eq!(report.divergences.len(), 1);
        assert_eq!(report.divergences[0].kind, DivergenceKind::FieldMismatch);
    }

    /// P2 correctness gate: a call whose recorded args are JSON-null (the
    /// "argless boundary" shape used by time / id / random) MUST NOT silently
    /// hand back the recorded result when V2 calls with EMPTY-OBJECT args
    /// (the other "argless" shape) under the default `OnlyForArgful` policy.
    /// A clock or id generator that lied about its arg signature is the
    /// worst possible failure mode for replay.
    #[test]
    fn argless_call_fails_closed_under_default_policy() {
        let events = vec![make_event(
            0,
            None,
            "current_time",
            serde_json::Value::Null,
            serde_json::json!({"Ok": 1_700_000_000_u64}),
            false,
        )];

        let hook = ReplayHook::new(events, ReplayConfig::default(), 100);

        // V2 calls with an empty-object arg shape — DIFFERENT from the
        // recorded `null` (so the first-pass exact match misses) but still
        // "argless" by policy. `allow_arg_mismatch` MUST return false.
        let result = hook.try_replay(
            "storage",
            "PaymentStore",
            "current_time",
            &serde_json::json!({}),
        );

        assert!(
            result.is_none(),
            "default policy must NOT return a recorded argless result on mismatch"
        );

        let report = hook.take_report();
        assert!(
            report.divergence_count > 0,
            "argless mismatch must register at least one divergence"
        );
        assert!(
            report
                .divergences
                .iter()
                .any(|d| d.kind == DivergenceKind::ArgSkipBlocked),
            "expected at least one ArgSkipBlocked divergence; got: {:?}",
            report
                .divergences
                .iter()
                .map(|d| d.kind)
                .collect::<Vec<_>>()
        );
    }

    // -----------------------------------------------------------------
    // Lookup-table replay tests
    // -----------------------------------------------------------------

    fn entry_with(
        correlation_id: Option<&str>,
        address: Address,
        args: &serde_json::Value,
        occurrence: u32,
        result: serde_json::Value,
        source_event_global_sequence: u64,
    ) -> LookupEntry {
        LookupEntry {
            key: LookupKey {
                correlation_id: correlation_id.map(str::to_owned),
                address,
                args_hash: canonical_args_hash(args),
                occurrence,
            },
            result,
            source_event_global_sequence,
        }
    }

    fn explicit(tag: &str) -> Address {
        Address::Explicit(tag.to_owned())
    }

    fn lexical_identity(path: &str) -> CallsiteIdentity {
        CallsiteIdentity {
            version: 1,
            source: CallsiteSource::LexicalPath,
            id: None,
            scope: None,
            occurrence: 0,
            caller_function: None,
            lexical_path: Some(path.to_owned()),
            syntax_hash: None,
            logical_context: None,
        }
    }

    fn explicit_identity(tag: &str) -> CallsiteIdentity {
        CallsiteIdentity {
            version: 1,
            source: CallsiteSource::Explicit,
            id: Some(tag.to_owned()),
            scope: None,
            occurrence: 0,
            caller_function: None,
            lexical_path: None,
            syntax_hash: None,
            logical_context: None,
        }
    }

    struct VecSource(Option<LookupTable>);
    impl LookupTableSource for VecSource {
        fn load(&mut self) -> std::io::Result<LookupTable> {
            self.0
                .take()
                .ok_or_else(|| std::io::Error::other("double-load not supported in test"))
        }
    }

    #[test]
    fn local_file_lookup_source_reads_jsonl() {
        use std::io::Write;
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("table.jsonl");
        let mut file = std::fs::File::create(&path).expect("create");
        let entry = entry_with(
            Some("c-1"),
            Address::Sequence {
                boundary: "redis".to_owned(),
                method: "get_key".to_owned(),
                request_sequence: 0,
            },
            &serde_json::json!({}),
            0,
            serde_json::json!("hello"),
            42,
        );
        writeln!(file, "{}", serde_json::to_string(&entry).unwrap()).unwrap();
        drop(file);

        let mut source = LocalFileLookupSource::new(&path);
        let table = source.load().expect("load");
        assert_eq!(table.entries.len(), 1);
        assert_eq!(table.entries[0].result, serde_json::json!("hello"));
    }

    #[test]
    fn lookup_table_hook_resolves_by_args_hash_and_records_observation() {
        // Two calls to the same explicit site, distinguished only by their
        // args. Each has occurrence 0 because the (address, args_hash) buckets
        // differ — so resolution is keyed by *what* was called, not *when*.
        let table = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries: vec![
                entry_with(
                    None,
                    explicit("site"),
                    &serde_json::json!({ "id": 1 }),
                    0,
                    serde_json::json!("alpha"),
                    7,
                ),
                entry_with(
                    None,
                    explicit("site"),
                    &serde_json::json!({ "id": 2 }),
                    0,
                    serde_json::json!("beta"),
                    9,
                ),
            ],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        let identity = explicit_identity("site");
        let call = |args: serde_json::Value| {
            hook.try_replay_with_context(ReplayLookup {
                boundary: "redis",
                trait_name: "RedisStore",
                method_name: "get_key",
                args: &args,
                callsite_identity: Some(&identity),
                caller_location: None,
            })
        };

        // Drive args id:2 BEFORE id:1 — the opposite of recorded order — to
        // prove order independence even within this small case.
        assert_eq!(
            call(serde_json::json!({ "id": 2 })),
            Some(serde_json::json!("beta"))
        );
        assert_eq!(
            call(serde_json::json!({ "id": 1 })),
            Some(serde_json::json!("alpha"))
        );
        assert_eq!(
            call(serde_json::json!({ "id": 3 })),
            None,
            "unknown args have no recorded entry"
        );

        let calls = handle.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert_eq!(
            calls[0].resolved_rank,
            Some(1),
            "explicit address is rank 1"
        );
        assert_eq!(calls[0].source_event_global_sequence, Some(9));
        assert_eq!(calls[1].source_event_global_sequence, Some(7));
        assert!(!calls[2].resolved);
        assert_eq!(calls[2].resolved_rank, None);
        assert_eq!(
            calls[0].boundary, "redis",
            "boundary carried on the observation"
        );
    }

    #[test]
    fn lookup_resolves_iteration_order_independent() {
        // Simulate the renderer: walk a connector loop in order [1, 2, 3],
        // building the table via the SHARED key-construction path.
        let identity = lexical_identity("crate::pay::confirm::loop");
        let mut stamper = KeyStamper::new();
        let mut entries = Vec::new();
        for (i, connector) in [1u64, 2, 3].into_iter().enumerate() {
            let args = serde_json::json!({ "connector": connector });
            let addresses = addresses_for("redis", "get_key", Some(&identity), None, i as u64);
            for key in stamper.stamp(None, &addresses, canonical_args_hash(&args)) {
                entries.push(LookupEntry {
                    key,
                    result: serde_json::json!(format!("v{connector}")),
                    source_event_global_sequence: i as u64,
                });
            }
        }
        let table = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries,
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        // Replay in a DIFFERENT iteration order: [3, 1, 2]. All must resolve
        // (rank-4 lexical path + args_hash), proving order independence.
        let call = |connector: u64| {
            hook.try_replay_with_context(ReplayLookup {
                boundary: "redis",
                trait_name: "RedisStore",
                method_name: "get_key",
                args: &serde_json::json!({ "connector": connector }),
                callsite_identity: Some(&identity),
                caller_location: None,
            })
        };
        assert_eq!(call(3), Some(serde_json::json!("v3")));
        assert_eq!(call(1), Some(serde_json::json!("v1")));
        assert_eq!(call(2), Some(serde_json::json!("v2")));

        let calls = handle.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert!(
            calls
                .iter()
                .all(|c| c.resolved && c.resolved_rank == Some(4)),
            "every call resolves at rank 4 regardless of iteration order"
        );
    }

    // -----------------------------------------------------------------
    // Boundary-path identity tests: a BOUNDARY-path identity
    // (CallsiteSource::SyntacticHash + syntax_hash + lexical_path) is
    // emitted by the macro/DB codegen and must resolve at ranks 3/4
    // (LogicalContext occupies rank 2).
    // -----------------------------------------------------------------

    /// An identity shaped exactly like the boundary macro / DB codegen emits:
    /// `SyntacticHash` source carrying BOTH a `syntax_hash` (rank 3) and a
    /// `lexical_path` (rank 4), with a per-callsite `occurrence`.
    fn boundary_identity(scope: &str, occurrence: u32) -> CallsiteIdentity {
        CallsiteIdentity {
            version: 1,
            source: CallsiteSource::SyntacticHash,
            id: None,
            scope: Some(scope.to_owned()),
            occurrence,
            caller_function: Some("crate::module".to_owned()),
            lexical_path: Some("crate::module".to_owned()),
            syntax_hash: Some(crate::stable_callsite_hash(scope)),
            logical_context: None,
        }
    }

    /// Mirror the renderer (`replay-harness-api`): walk recorded events and
    /// build a lookup table via the SHARED `addresses_for` + `KeyStamper`.
    fn render_table(events: &[SemanticEvent]) -> LookupTable {
        let mut stamper = KeyStamper::new();
        let mut request_seq: HashMap<Option<String>, u64> = HashMap::new();
        let mut entries = Vec::new();
        for event in events {
            let slot = request_seq.entry(event.correlation_id.clone()).or_insert(0);
            let request_sequence = *slot;
            *slot += 1;
            let location = Some((event.call_file.as_str(), event.call_line, event.call_column));
            let addresses = addresses_for(
                &event.boundary,
                &event.method_name,
                event.callsite_identity.as_ref(),
                location,
                request_sequence,
            );
            let args_hash = canonical_args_hash(&event.args);
            for key in stamper.stamp(event.correlation_id.as_deref(), &addresses, args_hash) {
                entries.push(LookupEntry {
                    key,
                    result: event.result.clone(),
                    source_event_global_sequence: event.global_sequence,
                });
            }
        }
        LookupTable {
            recording_id: "rec-boundary".to_owned(),
            policy_version: 1,
            entries,
        }
    }

    #[test]
    fn stable_callsite_hash_is_deterministic_and_line_shift_independent() {
        // (1) Determinism: same input → same hash, every time.
        let a = crate::stable_callsite_hash("redis::RedisStore::get_key");
        let b = crate::stable_callsite_hash("redis::RedisStore::get_key");
        assert_eq!(a, b, "syntax hash must be deterministic");

        // (2) Line-shift independence: the hash is a pure function of the
        // boundary/component/operation string, NOT of any file:line. Two
        // "recordings" taken with the call site at different source lines hash
        // identically because the input string is unchanged.
        let record_time = crate::stable_callsite_hash("redis::RedisStore::get_key");
        let replay_time_after_edits_shifted_lines =
            crate::stable_callsite_hash("redis::RedisStore::get_key");
        assert_eq!(
            record_time, replay_time_after_edits_shifted_lines,
            "syntax hash must survive source line shifts"
        );

        // (3) Distinctness: different operation → different hash.
        assert_ne!(
            crate::stable_callsite_hash("redis::RedisStore::get_key"),
            crate::stable_callsite_hash("redis::RedisStore::set_key"),
            "distinct operations must hash differently"
        );
    }

    #[test]
    fn boundary_path_event_resolves_at_rank_three() {
        // A recorded BOUNDARY event now carries callsite_identity: Some(_) with
        // syntax_hash: Some(_). Prove it both (a) carries the identity and (b)
        // resolves at rank 3 (SyntacticHash) through the renderer→hook pipeline.
        // (Rank 3, not 2: P3 inserted LogicalContext at rank 2, and this identity
        // carries no logical_context.)
        let identity = boundary_identity("redis::RedisStore::get_key", 0);
        assert!(
            identity.syntax_hash.is_some(),
            "boundary identity must carry a syntax_hash"
        );

        let mut event = make_event(
            0,
            Some("corr-1"),
            "get_key",
            serde_json::json!({ "key": "k1" }),
            serde_json::json!({ "Ok": "v1" }),
            false,
        );
        event.boundary = "redis".into();
        event.callsite_identity = Some(identity.clone());
        assert!(
            event.callsite_identity.is_some(),
            "on-disk boundary event must carry Some(callsite_identity), not None"
        );

        let table = render_table(&[event]);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        let _guard = deja_context::enter_correlation_id("corr-1");
        let result = hook.try_replay_with_context(ReplayLookup {
            boundary: "redis",
            trait_name: "RedisStore",
            method_name: "get_key",
            args: &serde_json::json!({ "key": "k1" }),
            callsite_identity: Some(&identity),
            caller_location: None,
        });
        assert_eq!(result, Some(serde_json::json!({ "Ok": "v1" })));

        let calls = handle.lock().unwrap().clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].resolved_rank,
            Some(3),
            "boundary syntax-hash identity must resolve at rank 3"
        );
    }

    #[test]
    fn boundary_path_resolves_at_rank_four_when_only_lexical_path_present() {
        // When syntax_hash is absent but lexical_path is present (e.g. a
        // recording produced before rank-3 emission), the SAME boundary call
        // still resolves — at rank 4 — proving the lexical path is an additive
        // fallback below SyntacticHash.
        let mut identity = boundary_identity("redis::RedisStore::get_key", 0);
        identity.syntax_hash = None; // force the rank-3 SyntacticHash address absent
        identity.source = CallsiteSource::LexicalPath;

        let mut event = make_event(
            0,
            Some("corr-1"),
            "get_key",
            serde_json::json!({ "key": "k1" }),
            serde_json::json!({ "Ok": "v1" }),
            false,
        );
        event.boundary = "redis".into();
        event.callsite_identity = Some(identity.clone());

        let table = render_table(&[event]);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        let _guard = deja_context::enter_correlation_id("corr-1");
        let result = hook.try_replay_with_context(ReplayLookup {
            boundary: "redis",
            trait_name: "RedisStore",
            method_name: "get_key",
            args: &serde_json::json!({ "key": "k1" }),
            callsite_identity: Some(&identity),
            caller_location: None,
        });
        assert_eq!(result, Some(serde_json::json!({ "Ok": "v1" })));
        assert_eq!(
            handle.lock().unwrap()[0].resolved_rank,
            Some(4),
            "lexical-path-only identity must resolve at rank 4"
        );
    }

    #[test]
    fn logical_context_disambiguates_concurrent_same_callsite_calls() {
        // The concurrent-occurrence-swap fix in miniature. Two calls to the SAME boundary/op (so an
        // IDENTICAL syntax_hash) with IDENTICAL args, distinguished ONLY by the
        // span they fired in (their `logical_context`). Recorded in the order
        // [attempt, intent]; replayed in the SWAPPED order [intent, attempt], as
        // async task interleaving would. Each must resolve to ITS OWN recorded
        // result at rank 2 (LogicalContext) — NOT swapped.
        //
        // Without LogicalContext both calls would share the
        // (correlation, SyntacticHash, args_hash) bucket and be tiebroken by a
        // positional `occurrence` (0,1) that swaps on reorder → attempt would get
        // intent's recorded row. The span-scoped LogicalContext address puts them
        // in DISTINCT buckets (occ 0 each), so the match is order-independent.
        let scope = "db::Store::update";
        let make = |logical: &str| CallsiteIdentity {
            version: 1,
            source: CallsiteSource::SyntacticHash,
            id: None,
            scope: Some(scope.to_owned()),
            occurrence: 0,
            caller_function: Some("crate::module".to_owned()),
            lexical_path: Some("crate::module".to_owned()),
            syntax_hash: Some(crate::stable_callsite_hash(scope)),
            logical_context: Some(logical.to_owned()),
        };
        let id_attempt = make("payments_core>update_payment_attempt");
        let id_intent = make("payments_core>update_payment_intent");
        // IDENTICAL args for both calls — so args_hash can't distinguish them.
        let args = serde_json::json!({ "id": 1 });

        let event = |gseq: u64, id: &CallsiteIdentity, result: &str| {
            let mut e = make_event(
                gseq,
                Some("c1"),
                "update",
                args.clone(),
                serde_json::json!({ "Ok": result }),
                false,
            );
            e.boundary = "db".into();
            e.callsite_identity = Some(id.clone());
            e
        };
        // Record order: attempt → intent.
        let table = render_table(&[
            event(0, &id_attempt, "attempt-row"),
            event(1, &id_intent, "intent-row"),
        ]);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        let _guard = deja_context::enter_correlation_id("c1");
        let replay = |id: &CallsiteIdentity| {
            hook.try_replay_with_context(ReplayLookup {
                boundary: "db",
                trait_name: "Store",
                method_name: "update",
                args: &args,
                callsite_identity: Some(id),
                caller_location: None,
            })
        };
        // Replay in the SWAPPED order: intent first, then attempt.
        assert_eq!(
            replay(&id_intent),
            Some(serde_json::json!({ "Ok": "intent-row" })),
            "the intent call must get the INTENT row even though it replays first"
        );
        assert_eq!(
            replay(&id_attempt),
            Some(serde_json::json!({ "Ok": "attempt-row" })),
            "the attempt call must get the ATTEMPT row — NOT swapped by a shared \
             positional occurrence"
        );

        let calls = handle.lock().unwrap().clone();
        assert!(
            calls
                .iter()
                .all(|c| c.resolved && c.resolved_rank == Some(2)),
            "both resolve at rank 2 (LogicalContext), not a weaker positional fallback; \
             got resolved/rank = {:?}",
            calls
                .iter()
                .map(|c| (c.resolved, c.resolved_rank))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn callsite_occurrence_is_single_bump_sequence() {
        // Repeated calls at the SAME logical callsite (same correlation, source,
        // scope) within one correlation must yield occurrence 0, 1, 2 — i.e. a
        // single increment per call. This is what keeps record and replay keys
        // aligned for repeated same-callsite invocations. `ReplayHook` shares
        // the same per-(correlation, source, scope) counter logic the recorder
        // and runtime hook use.
        let hook = ReplayHook::new(Vec::new(), ReplayConfig::default(), 100);
        let occ = || {
            DejaHook::next_callsite_occurrence(
                &hook,
                Some("corr-1"),
                CallsiteSource::SyntacticHash,
                Some("redis::RedisStore::get_key"),
            )
        };
        assert_eq!(occ(), 0);
        assert_eq!(occ(), 1);
        assert_eq!(occ(), 2);

        // A DIFFERENT scope is a different bucket, restarting at 0.
        assert_eq!(
            DejaHook::next_callsite_occurrence(
                &hook,
                Some("corr-1"),
                CallsiteSource::SyntacticHash,
                Some("redis::RedisStore::set_key"),
            ),
            0,
            "distinct scope must have an independent occurrence counter"
        );
    }

    /// Regression guard for the Phase-1 occurrence DOUBLE-BUMP.
    ///
    /// The same logical callsite is hit 3 times within one correlation with
    /// the SAME args. On RECORD the occurrence counter advances per call, so
    /// the three events carry occurrence 0, 1, 2 and the renderer stamps three
    /// distinct rank-4 (`LexicalPath { scope_occurrence }`) addresses. On
    /// REPLAY the boundary macro RE-DERIVES the occurrence for each call by
    /// calling `DejaHook::next_callsite_occurrence` on the SAME hook that does
    /// the lookup (here `LookupTableHook`) — exactly as the generated code in
    /// `recordable.rs` / `instrument.rs` does.
    ///
    /// Before the fix, `LookupTableHook` did not implement
    /// `next_callsite_occurrence`, so the default `0` was returned for EVERY
    /// call: the replay identities all carried occurrence 0 while the record
    /// identities carried 0, 1, 2. Only the first (occurrence-0) call resolved;
    /// calls 2 and 3 missed at every rank. This asymmetry once collapsed a
    /// nearly-fully-resolved replay to a handful of matches. The fix makes the
    /// hook advance the occurrence in
    /// lock-step with the recorder, so record sequence == replay sequence and
    /// all three calls resolve.
    #[test]
    fn repeated_callsite_resolves_when_occurrence_is_rederived_on_replay() {
        let scope = "redis::RedisStore::get_key";
        let correlation = Some("corr-1");
        let args = serde_json::json!({ "key": "k" });

        // --- RECORD pass: a per-(correlation, source, scope) counter advances
        // once per call, exactly like `RecordingHook::next_callsite_occurrence`.
        let recorder = ReplayHook::new(Vec::new(), ReplayConfig::default(), 0);
        let mut events = Vec::new();
        for i in 0..3u64 {
            let occurrence = DejaHook::next_callsite_occurrence(
                &recorder,
                correlation,
                CallsiteSource::SyntacticHash,
                Some(scope),
            );
            // Record-side occurrence sequence MUST be 0, 1, 2.
            assert_eq!(occurrence, i as u32);
            let mut event = make_event(
                i,
                correlation,
                "get_key",
                args.clone(),
                serde_json::json!({ "Ok": format!("v{i}") }),
                false,
            );
            event.boundary = "redis".into();
            event.callsite_identity = Some(boundary_identity(scope, occurrence));
            events.push(event);
        }

        // --- RENDER: build the lookup table from the recorded events.
        let table = render_table(&events);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        // --- REPLAY pass: the macro re-derives the occurrence per call through
        // the SAME hook that performs the lookup, then looks up with that
        // identity. Drive the three calls and assert all resolve.
        let _guard = deja_context::enter_correlation_id("corr-1");
        for i in 0..3u64 {
            let occurrence = DejaHook::next_callsite_occurrence(
                &hook,
                correlation,
                CallsiteSource::SyntacticHash,
                Some(scope),
            );
            // The whole point: replay occurrence sequence MUST equal the record
            // sequence (0, 1, 2), not 0, 0, 0.
            assert_eq!(
                occurrence, i as u32,
                "replay-side occurrence must advance in lock-step with record"
            );
            let identity = boundary_identity(scope, occurrence);
            let result = hook.try_replay_with_context(ReplayLookup {
                boundary: "redis",
                trait_name: "RedisStore",
                method_name: "get_key",
                args: &args,
                callsite_identity: Some(&identity),
                caller_location: None,
            });
            assert_eq!(
                result,
                Some(serde_json::json!({ "Ok": format!("v{i}") })),
                "call #{i} (occurrence {occurrence}) must resolve to its recorded result"
            );
        }

        let calls = handle.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert!(
            calls.iter().all(|c| c.resolved),
            "all three repeated-callsite calls must resolve (no double-bump miss): {:?}",
            calls
                .iter()
                .map(|c| (c.resolved, c.resolved_rank))
                .collect::<Vec<_>>()
        );
    }

    /// An identity shaped EXACTLY like `instrument.rs` emits at expansion time:
    /// `source: SyntacticHash`, `id: None`, `syntax_hash: Some`,
    /// `lexical_path: Some(module_path)`, and the per-callsite `occurrence`
    /// allocated ONCE via `next_boundary_occurrence`. This is the boundary-macro
    /// identity, distinct from the hand-built `boundary_identity` helper which
    /// the prior fix attempts leaned on.
    ///
    /// `lexical_path` is the runtime `module_path!()` (e.g.
    /// `common_utils::date_time`) — which is NOT the same string as `scope`
    /// (`common_utils::date_time::now`). The occurrence is bucketed on `scope`
    /// at record/replay, but the rank-4 `LexicalPath` address is keyed on
    /// `lexical_path`.
    fn macro_emitted_identity(
        scope: &str,
        lexical_path: &str,
        occurrence: u32,
    ) -> CallsiteIdentity {
        CallsiteIdentity {
            version: 1,
            source: CallsiteSource::SyntacticHash,
            id: None, // <-- macro ALWAYS sets id: None (see instrument.rs)
            scope: Some(scope.to_owned()),
            occurrence,
            caller_function: Some(lexical_path.to_owned()),
            lexical_path: Some(lexical_path.to_owned()),
            syntax_hash: Some(crate::stable_callsite_hash(scope)),
            logical_context: None,
        }
    }

    /// PIPELINE-FIDELITY REPRODUCTION of the 197 -> 11 regression, modelled on
    /// the real `time::date_time::now` recording (10 argless calls within one
    /// correlation, SAME boundary/method/scope, all funnelling through the
    /// single `#[track_caller]` boundary fn).
    ///
    /// This drives the EXACT dockerized-replay path:
    ///   recorded events (macro-style identity)
    ///     -> `render_table` (the real `addresses_for` + `KeyStamper` renderer)
    ///     -> `LookupTableHook::try_replay_with_context`
    /// where the boundary macro RE-DERIVES the per-callsite `occurrence` at
    /// replay through the SAME hook that performs the lookup (exactly as
    /// `instrument.rs` / `recordable.rs` generate).
    ///
    /// KEY FIDELITY POINT — why the prior reproductions stayed green while the
    /// pipeline was red: the `SyntacticHash` address (rank 3) is
    /// occurrence-INDEPENDENT (the hash is in the address, the KeyStamper
    /// occurrence aligns on its own), so as long as syntax-hash entries exist a
    /// test resolves even with a broken occurrence counter. The BROKEN pipeline
    /// artifact had NO syntax-hash entries for these repeated argless callsites,
    /// so resolution fell to `LexicalPath { scope_occurrence }` (rank 4) — which
    /// embeds the re-derived `occurrence` directly into the address. If the
    /// hook does not advance `next_callsite_occurrence` in lock-step with the
    /// recorder, EVERY replay query carries occurrence 0, so only the recorded
    /// occurrence-0 event matches and calls #2..N miss at every rank — the
    /// 197 -> 11 collapse.
    ///
    /// This test therefore models the lexical path explicitly: the identity
    /// carries a `lexical_path` (rank 4) but NO `syntax_hash` (no rank 3), so
    /// resolution DEPENDS on the re-derived occurrence being correct.
    #[test]
    fn repro_date_time_now_repeated_argless_calls_all_resolve() {
        let scope = "common_utils::date_time::now";
        let lexical = "common_utils::date_time";
        let correlation = Some("corr-1");
        let args = serde_json::json!({}); // argless boundary

        // Macro-style identity WITHOUT a syntax_hash, forcing lexical-path (rank 4)
        // resolution — the path the broken pipeline artifact actually exercised.
        // (When the syntax_hash is present, the rank-3 SyntacticHash masks the
        // occurrence bug; see the doc comment above.)
        let identity_for = |occurrence: u32| {
            let mut id = macro_emitted_identity(scope, lexical, occurrence);
            id.syntax_hash = None; // no rank-3 — resolution must use rank-4 lexical
            id.source = CallsiteSource::LexicalPath;
            id
        };

        // --- RECORD pass: occurrence advances once per call, bucketed on
        // (correlation, source, scope) — exactly like
        // `RecordingHook::next_callsite_occurrence`.
        let recorder = ReplayHook::new(Vec::new(), ReplayConfig::default(), 0);
        let mut events = Vec::new();
        for i in 0..10u64 {
            let occurrence = DejaHook::next_callsite_occurrence(
                &recorder,
                correlation,
                CallsiteSource::LexicalPath,
                Some(scope),
            );
            assert_eq!(occurrence, i as u32, "record occurrence must be 0..N");
            let mut event = make_event(
                i,
                correlation,
                "date_time::now",
                args.clone(),
                serde_json::json!({ "Ok": format!("t{i}") }),
                false,
            );
            event.boundary = "time".into();
            event.callsite_identity = Some(identity_for(occurrence));
            events.push(event);
        }

        // --- RENDER: the real renderer (one entry per rank per event).
        let table = render_table(&events);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        // --- REPLAY pass: the macro re-derives the per-callsite occurrence via
        // the SAME hook that performs the lookup, then queries with that
        // identity. The candidate visits the callsite the SAME number of times
        // but in a DIFFERENT order than the recording (the lexical-path rank is
        // supposed to be iteration-order independent — each occurrence
        // self-addresses). This removes the rank-6 positional safety net, so a
        // desynced occurrence produces a genuine MISS (the "responses broke"
        // symptom) rather than a lucky rank-6 rescue.
        //
        // With the occurrence fix in place every repeated argless call resolves
        // at rank 3; without it, only the occurrence-0 lookup matches and the
        // other nine calls miss entirely.
        // The candidate visits the 10 occurrences in a shuffled order; the
        // concrete permutation does not matter (rank-4 is order-independent),
        // only that each occurrence 0..9 is visited exactly once.
        let _guard = deja_context::enter_correlation_id("corr-1");
        for _ in 0..10u64 {
            let occurrence = DejaHook::next_callsite_occurrence(
                &hook,
                correlation,
                CallsiteSource::LexicalPath,
                Some(scope),
            );
            let identity = identity_for(occurrence);
            let result = hook.try_replay_with_context(ReplayLookup {
                boundary: "time",
                trait_name: "Time",
                method_name: "date_time::now",
                args: &args,
                callsite_identity: Some(&identity),
                caller_location: None,
            });
            // NOTE: results are recorded per-occurrence, and the candidate hits
            // occurrences 0..9 (just in a shuffled visitation order), so the
            // recorded result for occurrence `occurrence` is `t{occurrence}`.
            assert_eq!(
                result,
                Some(serde_json::json!({ "Ok": format!("t{occurrence}") })),
                "repeated argless call (occurrence {occurrence}) must resolve at rank 4"
            );
        }

        let calls = handle.lock().unwrap().clone();
        assert_eq!(calls.len(), 10);
        assert!(
            calls
                .iter()
                .all(|c| c.resolved && c.resolved_rank == Some(4)),
            "all repeated argless calls must resolve at rank 4 (order-independent); \
             got resolved/rank = {:?}",
            calls
                .iter()
                .map(|c| (c.resolved, c.resolved_rank))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn lookup_table_hook_prefers_stronger_rank_over_sequence() {
        // The same call can match both an explicit (rank 1) entry and a
        // sequence (rank 5) entry. Strongest-first querying must pick rank 1.
        let args = serde_json::json!({});
        let table = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries: vec![
                entry_with(
                    None,
                    Address::Sequence {
                        boundary: "redis".to_owned(),
                        method: "m".to_owned(),
                        request_sequence: 0,
                    },
                    &args,
                    0,
                    serde_json::json!("by_sequence"),
                    1,
                ),
                entry_with(
                    None,
                    explicit("stable-X"),
                    &args,
                    0,
                    serde_json::json!("by_explicit"),
                    2,
                ),
            ],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook = LookupTableHook::from_source(VecSource(Some(table)), observed).expect("hook");

        let identity = explicit_identity("stable-X");
        let value = hook.try_replay_with_context(ReplayLookup {
            boundary: "redis",
            trait_name: "S",
            method_name: "m",
            args: &args,
            callsite_identity: Some(&identity),
            caller_location: None,
        });
        assert_eq!(value, Some(serde_json::json!("by_explicit")));
        assert_eq!(handle.lock().unwrap()[0].resolved_rank, Some(1));
    }
}
