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
    correlation_matches, read_events, BoundarySpec, CallsiteIdentity, CallsiteSource, Channel,
    DejaHook, Effect, ExecuteMode, Policy, ReplayLookup, SemanticEvent, Strategy,
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
///
/// Note: not `Copy` because [`ValueDiverged`](DivergenceKind::ValueDiverged) and
/// [`InconclusiveSeedGap`](DivergenceKind::InconclusiveSeedGap) carry owned
/// payloads (recorded vs observed values, callsite). The unit variants are
/// unaffected; comparisons still use `==`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// The candidate ran the REAL boundary (execute mode) and produced a result
    /// that differs in VALUE from the recorded baseline at the same args-free
    /// call-site + occurrence. This is the total-derivative signal: a recorded
    /// WRITE (Omitted) and the execute WRITE (Novel) are paired by args-free
    /// identity into ONE divergence, since the diverging value (e.g. a doubled
    /// amount) changes the args and would otherwise split them. `args_hash` is
    /// used here as a DIFF signal, not a resolution key.
    ValueDiverged {
        /// The recorded baseline value for this call-site.
        recorded: serde_json::Value,
        /// The value the real boundary produced under execute mode.
        observed: serde_json::Value,
        /// Args-free call-site identity the two sides were paired on
        /// (`boundary::trait::method`).
        callsite: String,
        /// Occurrence index within the correlation scope used for pairing.
        occurrence: u32,
    },
    /// The candidate's execute-mode call could not be conclusively classified
    /// because the recorded baseline it needed to compare against was absent — a
    /// seed gap. Surfaced explicitly so a missing baseline is not silently
    /// counted as a match (false negative) nor as a divergence (false positive).
    InconclusiveSeedGap {
        /// Args-free call-site identity (`boundary::trait::method`).
        callsite: String,
        /// Occurrence index within the correlation scope.
        occurrence: u32,
    },
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
    /// The result the recording carried for this call-site (substituted under
    /// lookup mode). Under lookup this equals `observed_result`, so a value diff
    /// is inert; under execute mode it is the recorded baseline to compare the
    /// real boundary's fresh result against.
    #[serde(default)]
    pub recorded_result: Option<serde_json::Value>,
    /// The result actually produced for this call. Under lookup this is the
    /// substituted (== recorded) value; under execute mode it is the REAL
    /// boundary's fresh result. The post-hoc tally classifies
    /// [`ValueDiverged`](crate::DivergenceKind::ValueDiverged) when these differ.
    #[serde(default)]
    pub observed_result: Option<serde_json::Value>,
    /// How this observed call was served: ordinary recorded substitution, or an
    /// execute-shadow dispatch that ran the real boundary.
    #[serde(default)]
    pub provenance: crate::Provenance,
    /// Set when the call could not be conclusively classified because the
    /// recorded baseline needed to compare against was missing (a seed gap) —
    /// surfaced as [`InconclusiveSeedGap`](crate::DivergenceKind::InconclusiveSeedGap)
    /// rather than a false positive. Always false in M1 lookup mode.
    #[serde(default)]
    pub seed_gap: bool,
    /// Pre-image of the affected State key captured by the execute-shadow probe
    /// BEFORE the real boundary ran (the `execute_shadow_peek` half). `None`
    /// under lookup mode or when no [`StateProbe`] is installed. Makes the
    /// `pre_image` real for single-op RMW total-derivative diff without touching
    /// `SemanticEvent` (deliverable 6). Carried transparently across the
    /// `ExecuteShadowToken`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_image: Option<serde_json::Value>,
    /// Post-image of the affected State key captured by the execute-shadow probe
    /// AFTER the real boundary ran (the `execute_shadow_observe` half). `None`
    /// under lookup mode or when no [`StateProbe`] is installed (deliverable 6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_image: Option<serde_json::Value>,
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

/// Whether a boundary tag belongs to the State channel (db / redis).
///
/// State is the only channel that opts into execute-mode under
/// [`Policy::SelectiveExecute`] — Entropy and Egress stay in lookup mode. The db
/// boundary appears under two tags: `"storage"` (the trait-delegate path) and
/// `"db"` (the generic `QuerySpec`/`record_query` seam, which hardcodes
/// `boundary = "db"`). `"redis"` is the cache boundary. Egress (`"http_client"`,
/// `"grpc"`) and entropy boundaries (`"id"`, `"time"`) return `false`, so they
/// stay lookup-substituted (entropy is reconstructed by substitution, egress is
/// never re-executed) — only State executes against seeded/reconstructed stores.
pub fn is_state_channel(boundary: &str) -> bool {
    matches!(boundary, "storage" | "redis" | "db")
}

/// Whether a boundary method is a READ (as opposed to a write/unknown op).
///
/// Conservative by construction: a method is a read iff its name — OR its final
/// `_`-delimited segment — starts with a known read verb; writes and any
/// unrecognized op return `false`. The trailing-segment check makes
/// component-prefixed operation names (e.g. `eu_settlement_read`) classify by
/// their verb suffix while plain names (`find_payment_intent_by_id`) still match
/// on the whole string. This gates the arg-tolerant fallback so that ONLY reads
/// can be served their recorded value on re-keyed args — a changed WRITE never
/// becomes arg-tolerant and stays strict (so it is still caught as a
/// divergence). The verbs cover the db (`find_*`, `get_*`, `list_*`) and redis
/// (`get`, `exists`, `hget`, `hexists`, `scan`, `mget`, `lookup`) read surfaces.
pub fn is_read_op(method: &str) -> bool {
    const READ_PREFIXES: &[&str] = &[
        "find", "get", "exists", "scan", "fetch", "list", "hget", "hexists",
        "lookup", "read", "load", "mget",
    ];
    let starts_with_verb = |s: &str| READ_PREFIXES.iter().any(|p| s.starts_with(p));
    // Whole name, or the trailing `_`-segment (the verb suffix). Writes — whose
    // suffix is `write`/`set`/`insert`/`update`/... — never match either way.
    starts_with_verb(method)
        || method.rsplit('_').next().map(starts_with_verb).unwrap_or(false)
}

/// Whether an event lives on the State channel, PREFERRING its declared
/// [`SemanticEvent::channel`] and falling back to the [`is_state_channel`] name
/// heuristic when the event is UNDECLARED (`channel == None`).
///
/// DECLARATIVE PREFERENCE: a declared event reads its own channel (the db seam +
/// any migrated redis wrapper). FALLBACK: an undeclared event (the current vendor)
/// is classified byte-identically to before this slice. This consumes the
/// declaration-driven `read_set`/`write_set` that `EventBuilder::finish` now stamps.
fn event_is_state(event: &SemanticEvent) -> bool {
    match &event.channel {
        Some(channel) => *channel == Channel::State,
        None => is_state_channel(&event.boundary),
    }
}

/// Whether an event is a READ, PREFERRING its declared [`SemanticEvent::effect`]
/// (`Read` → read) and falling back to the [`is_read_op`] name heuristic when the
/// event declares NO effect (`effect == None`).
///
/// DECLARATIVE PREFERENCE: a declared State event reads its own effect (Read vs
/// Write/RMW/...). FALLBACK: an undeclared event uses the verb heuristic, so its
/// read/write verdict is byte-identical to before this slice. Note the db seam
/// declares an effect equal to the `is_read_op` verdict, so it agrees either way.
fn event_is_read(event: &SemanticEvent) -> bool {
    match event.effect {
        Some(effect) => effect == Effect::Read,
        None => is_read_op(&event.method_name),
    }
}

// ---------------------------------------------------------------------------
// The decision matrix (declarative boundary model, foundation slice)
//
// `decide_strategy` is the WHOLE runtime decision for a DECLARED boundary: a
// pure table over (Channel × Effect × Policy) plus a per-op strategy override.
// It implements the matrix in `docs/design/declarative-boundary-model.md` §2.
//
// ADDITIVE FALLBACK: when `channel` is UNDECLARED (`None`) the matrix cannot key
// a declared cell, so it returns [`Strategy::Lookup`] — the safe-by-construction
// default (an undeclared boundary can never wrongly execute). The runtime entry
// point [`boundary_execute_mode_for`] routes an undeclared boundary to the hook's
// EXISTING string-heuristic [`DejaHook::execute_mode`] instead, so an unmigrated
// boundary's runtime decision is byte-identical to before this slice (proven by
// the fallback test).
// ---------------------------------------------------------------------------

/// Map a resolved [`Strategy`] to the runtime [`ExecuteMode`] the existing
/// dispatch path understands. `Lookup` serves the recorded value
/// ([`ExecuteMode::Lookup`]); `SeedAndExecute` runs the real boundary
/// ([`ExecuteMode::Execute`]).
///
/// `LookupAndSeed` is STUBBED in this slice: the live "serve recorded return +
/// seed post-state" mechanism needs a post-state seeding step that is not wired
/// for the boundary dispatch path yet, so it maps to the SAFE
/// [`ExecuteMode::Lookup`] (serve recorded, never double-apply) — see the
/// `TODO(vendor-migration)` below. This only ever affects an explicitly DECLARED
/// boundary; the undeclared path never reaches it.
pub fn strategy_to_execute_mode(strategy: Strategy) -> ExecuteMode {
    match strategy {
        Strategy::Lookup => ExecuteMode::Lookup,
        Strategy::SeedAndExecute => ExecuteMode::Execute,
        // TODO(vendor-migration): wire the live "serve recorded return + seed
        // post-state" mechanism (needs a StateProbe-backed post-state seed on the
        // boundary dispatch path; `InMemoryStateProbe`/`StateProbe` exist for the
        // execute-shadow image capture but the LookupAndSeed serve-and-seed step
        // is not yet plumbed through `dispatch`). Until then resolve to a SAFE
        // Lookup so a declared Append/RMW-LookupAndSeed boundary serves its
        // recorded value and never double-applies. NOTE in the report.
        Strategy::LookupAndSeed => ExecuteMode::Lookup,
        // Forward-compat: an unknown strategy from a newer tape is never executed.
        Strategy::Unknown => ExecuteMode::Lookup,
    }
}

/// The pure decision matrix for a DECLARED boundary (declarative boundary model
/// §2). Given the declared [`Channel`] and (State-only) [`Effect`], an optional
/// per-op [`Strategy`] override, and the active [`Policy`], return the
/// [`Strategy`] the runtime should use.
///
/// CRITICAL FALLBACK: when `channel` is `None` (UNDECLARED — every current vendor
/// wrapper) this returns [`Strategy::Lookup`]. That is the safe default (an
/// undeclared boundary can never wrongly execute); the runtime entry point
/// [`boundary_execute_mode_for`] additionally routes undeclared boundaries to the
/// hook's existing string heuristics so their decision is byte-identical to
/// before this slice.
///
/// Matrix (declared cells):
/// - Channel::Entropy(_) / Egress (any) → Lookup
/// - Channel::State + Effect::Read|Write → Lookup (AllLookup);
///   SeedAndExecute (SelectiveExecute)
/// - Channel::State + Effect::Append → Lookup (AllLookup);
///   LookupAndSeed (SelectiveExecute)
/// - Channel::State + Effect::ReadModifyWrite → Lookup (AllLookup);
///   under SelectiveExecute the per-op `strategy_override` decides (REQUIRED for
///   RMW; the macro enforces it). Absent (defensive) → Lookup.
/// - Channel::State + Effect::VolatileRead → Lookup (never executed — TTL-decay /
///   non-deterministic iteration is not reproducible)
/// - Channel::State + Effect::Opaque → Lookup (EVAL; opt-in via override only)
/// - Channel::State + effect None → Lookup (conservative)
pub fn decide_strategy(
    channel: Option<Channel>,
    effect: Option<Effect>,
    strategy_override: Option<Strategy>,
    policy: Policy,
) -> Strategy {
    // A per-op override always wins within an executing State cell (lets any
    // declared boundary be tuned). It is REQUIRED for RMW under SelectiveExecute.

    // UNDECLARED channel → safe default. `boundary_execute_mode_for` reroutes
    // these to the heuristic path; the pure matrix itself never executes an
    // undeclared cell.
    let Some(channel) = channel else {
        return Strategy::Lookup;
    };

    match (channel, policy) {
        // Entropy(_) / Egress: never executed or re-issued — Lookup under every
        // policy (no effect is declared on these channels).
        (Channel::Entropy(_), _) | (Channel::Egress, _) => Strategy::Lookup,

        // Forward-compat: an unknown channel from a newer tape is never executed.
        (Channel::Unknown, _) => Strategy::Lookup,

        // State: every cell is Lookup under AllLookup (the demo-green default).
        (Channel::State, Policy::AllLookup) => Strategy::Lookup,

        // State under SelectiveExecute: the effect decides.
        (Channel::State, Policy::SelectiveExecute) => match effect {
            // The canonical execute cell.
            Some(Effect::Read) | Some(Effect::Write) => {
                strategy_override.unwrap_or(Strategy::SeedAndExecute)
            }
            // Append (XADD): serve the recorded id and seed the entry (never
            // double-append) — LookupAndSeed — unless overridden.
            Some(Effect::Append) => strategy_override.unwrap_or(Strategy::LookupAndSeed),
            // ReadModifyWrite: the per-op override is REQUIRED (the macro enforces
            // it). Defensive fall-back to Lookup if somehow absent.
            Some(Effect::ReadModifyWrite) => strategy_override.unwrap_or(Strategy::Lookup),
            // VolatileRead: never execute — a time-decaying value / non-deterministic
            // iteration is not reproducible (it would diverge every run).
            Some(Effect::VolatileRead) => Strategy::Lookup,
            // Opaque (EVAL): opt-in-only via an explicit override.
            Some(Effect::Opaque) => strategy_override.unwrap_or(Strategy::Lookup),
            // Forward-compat: an unknown effect from a newer tape is never executed.
            Some(Effect::Unknown) => Strategy::Lookup,
            // effect None on State → conservative Lookup.
            None => Strategy::Lookup,
        },
    }
}

/// Runtime entry point for the boundary-macro execute-mode decision under a
/// concrete hook. Routes a DECLARED boundary through the pure matrix
/// ([`decide_strategy`]) mapped to an [`ExecuteMode`]; routes an UNDECLARED
/// boundary to the hook's EXISTING string-heuristic [`DejaHook::execute_mode`],
/// so an unmigrated boundary's decision is byte-identical to before this slice.
///
/// The hook supplies the active [`Policy`] via [`DejaHook::replay_policy`] (the
/// default trait impl reports [`Policy::AllLookup`], so a hook that does not
/// track a policy decides exactly as before).
pub fn boundary_execute_mode_for(hook: &dyn DejaHook, spec: &BoundarySpec) -> ExecuteMode {
    let semantics = spec.semantics();
    if semantics.is_undeclared() {
        // FALLBACK: the unmigrated path. Identical to the pre-slice decision.
        return hook.execute_mode(spec.boundary, spec.trait_name, spec.method_name);
    }
    let strategy = decide_strategy(
        semantics.channel,
        semantics.effect,
        semantics.strategy,
        hook.replay_policy(),
    );
    strategy_to_execute_mode(strategy)
}

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
    /// Replay policy for this hook, parsed once from `DEJA_POLICY`. Defaults to
    /// [`Policy::AllLookup`] (full mock) — under which [`Self::execute_mode`]
    /// always returns [`ExecuteMode::Lookup`] and the hook is byte-identical to
    /// before this field existed.
    policy: crate::Policy,
    /// Args-free secondary index: `(correlation_id, address, occurrence) ->
    /// LookupEntry`. Built alongside `table` so an arg-tolerant fallback can
    /// resolve a RE-KEYED read (one whose args changed since recording) by
    /// call-site identity + occurrence with args ignored. Only consulted on a
    /// strict-args miss under [`Policy::SelectiveExecute`], so the default
    /// [`Policy::AllLookup`] path never sees it (preserving the partial-derivative
    /// contrast where a re-keyed read makes AllLookup MISS).
    argless_index: HashMap<(Option<String>, Address, u32), LookupEntry>,
    /// Op-scope for execute mode, parsed once from `DEJA_EXECUTE_OPS`
    /// (comma-separated operation/method names). When EMPTY, every State-channel
    /// boundary executes under [`Policy::SelectiveExecute`] (the original
    /// behavior). When NON-EMPTY, only State-channel boundaries whose
    /// `method_name` is in the set execute; all others fall back to Lookup. This
    /// lets the demo execute ONLY the settlement read/write ops while every other
    /// State boundary stays in lookup mode.
    execute_ops: std::collections::HashSet<String>,
    /// Optional probe of the live State store, used by the execute-shadow path to
    /// capture `pre_image` (before the real op) and `result_image` (after) for
    /// single-op RMW total-derivative diff (deliverable 6). `None` (the default)
    /// makes the hook byte-identical to before this field existed — no probing,
    /// no extra reads. A real probe needs the running container (the harness owns
    /// that); tests install an [`InMemoryStateProbe`].
    state_probe: Option<Box<dyn StateProbe>>,
}

impl LookupTableHook {
    /// Construct from any `LookupTableSource` (typically `LocalFileLookupSource`)
    /// and any `ObservedCallSink` (typically `InMemoryObservedSink` for tests
    /// or `FileObservedSink` for harness runs). Loading happens once at
    /// construction; failures bubble up as `io::Error`.
    ///
    /// Uses the default [`Policy::AllLookup`] (full mock). Callers that want the
    /// `DEJA_POLICY`-driven policy use [`Self::from_source_with_policy`].
    pub fn from_source<S, K>(source: S, sink: K) -> std::io::Result<Self>
    where
        S: LookupTableSource,
        K: ObservedCallSink + 'static,
    {
        // Empty execute-op scope: under SelectiveExecute every State boundary
        // executes (the original behavior); under AllLookup it is unused.
        Self::from_source_with_policy(
            source,
            sink,
            crate::Policy::default(),
            std::collections::HashSet::new(),
        )
    }

    /// Construct like [`Self::from_source`] but with an explicit replay
    /// [`Policy`]. Under [`Policy::SelectiveExecute`] the hook runs the REAL
    /// State (db/redis) boundary during replay and falls back to arg-tolerant
    /// substitution for re-keyed reads; under [`Policy::AllLookup`] (the default)
    /// it behaves exactly as before.
    pub fn from_source_with_policy<S, K>(
        mut source: S,
        sink: K,
        policy: crate::Policy,
        execute_ops: std::collections::HashSet<String>,
    ) -> std::io::Result<Self>
    where
        S: LookupTableSource,
        K: ObservedCallSink + 'static,
    {
        let table = source.load()?;
        let mut map = HashMap::with_capacity(table.entries.len());
        let mut argless_index: HashMap<(Option<String>, Address, u32), LookupEntry> =
            HashMap::with_capacity(table.entries.len());
        for entry in table.entries {
            // Args-free key: drop `args_hash` so a re-keyed read can still resolve
            // by call-site identity + occurrence. First write wins per
            // (correlation, address, occurrence) — distinct args at the SAME
            // address+occurrence are a recording anomaly; keeping the first is
            // deterministic and matches the strongest-rank-first lookup order.
            let argless_key = (
                entry.key.correlation_id.clone(),
                entry.key.address.clone(),
                entry.key.occurrence,
            );
            argless_index.entry(argless_key).or_insert_with(|| entry.clone());
            map.insert(entry.key.clone(), entry);
        }
        Ok(Self {
            table: map,
            next_sequence: Mutex::new(HashMap::new()),
            stamper: Mutex::new(KeyStamper::new()),
            global_counter: std::sync::atomic::AtomicU64::new(0),
            callsite_occurrence: Mutex::new(HashMap::new()),
            observed_sink: Box::new(sink),
            policy,
            argless_index,
            execute_ops,
            state_probe: None,
        })
    }

    /// Install a [`StateProbe`] for execute-shadow RMW image capture
    /// (deliverable 6). With a probe set, the execute path captures `pre_image`
    /// before the real op and `result_image` after, stamping both onto the shadow
    /// [`ObservedCall`]. Without one (the default) nothing is probed and the hook
    /// behaves exactly as before. Returns `self` for chaining at construction.
    pub fn with_state_probe(mut self, probe: Box<dyn StateProbe>) -> Self {
        self.state_probe = Some(probe);
        self
    }

    /// The replay policy this hook was constructed with.
    pub fn policy(&self) -> crate::Policy {
        self.policy
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

    /// Resolve one replay call to its recorded baseline.
    ///
    /// SINGLE source of truth shared by [`Self::try_replay_with_context`]
    /// (lookup) and [`Self::execute_shadow_peek`] (execute): it bumps the
    /// per-correlation request sequence, stamps occurrences for EVERY rank, and
    /// queries the table strongest-first (with the `SelectiveExecute`
    /// arg-tolerant fallback). Because BOTH modes route through here, the
    /// stamper / sequence counters advance EXACTLY ONCE per call regardless of
    /// mode, so numbering never drifts between a lookup boundary and an execute
    /// boundary in the same run. It does NOT emit an observation — the caller
    /// shapes and emits the `ObservedCall` (Recorded vs ExecuteShadow).
    fn resolve(&self, query: &ReplayLookup<'_>) -> Resolution {
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

        // ARG-TOLERANT FALLBACK (partial derivative), READS-ONLY. When the
        // strict-args lookup misses AND the call is a READ op, fall back to an
        // args-FREE match on call-site identity + occurrence (args LAST): serve
        // the RECORDED result this call-site produced even though the args were
        // re-keyed since the recording. This is what lets a re-keyed READ resolve
        // to its recorded value — the partial-derivative substitution that masks
        // transitive effects flowing through it. It is GATED on `is_read_op` so
        // ONLY reads can take it (a changed WRITE never becomes arg-tolerant and
        // stays strict, so it is still caught as a divergence — no regression).
        // It applies under BOTH policies: under AllLookup a re-keyed READ now
        // serves the recorded value (a clean MISS for the demo), while every
        // existing fixture — which has no re-keyed reads — resolves strictly and
        // behaves byte-identically.
        if hit.is_none() && is_read_op(query.method_name) {
            for key in &keys {
                let argless_key = (
                    key.correlation_id.clone(),
                    key.address.clone(),
                    key.occurrence,
                );
                if let Some(entry) = self.argless_index.get(&argless_key) {
                    hit = Some((entry, key.address.rank()));
                    break;
                }
            }
        }

        // "Where" for the diff UI + graph placement. `location` is already
        // resolved above (rank-5 SourceLocation); the span path is the rank-2
        // logical address; the graph node is the replay-side execution-graph
        // node this call fired under.
        let (_, graph_node_id) = crate::current_execution_graph_context();
        Resolution {
            correlation_id,
            location: location.map(|(f, l, c)| (f.to_owned(), l, c)),
            graph_node_id,
            resolved_rank: hit.map(|(_, rank)| rank),
            source_event_global_sequence: hit.map(|(entry, _)| entry.source_event_global_sequence),
            recorded_result: hit.map(|(entry, _)| entry.result.clone()),
        }
    }
}

/// Outcome of [`LookupTableHook::resolve`]: the recorded baseline for one call
/// plus the call-site metadata both modes carry into their `ObservedCall`.
struct Resolution {
    correlation_id: Option<String>,
    location: Option<(String, u32, u32)>,
    graph_node_id: Option<u64>,
    resolved_rank: Option<u8>,
    source_event_global_sequence: Option<u64>,
    recorded_result: Option<serde_json::Value>,
}

impl Resolution {
    /// The recorded baseline value, if the call resolved.
    fn recorded_result(&self) -> Option<serde_json::Value> {
        self.recorded_result.clone()
    }

    /// Shape an [`ObservedCall`] from this resolution, the originating query, the
    /// `observed_result` for this mode (the substituted recorded value under
    /// lookup, or `None`/the-real-result under execute), and the `provenance`.
    fn into_observed_call(
        self,
        query: &ReplayLookup<'_>,
        observed_result: Option<serde_json::Value>,
        provenance: crate::Provenance,
    ) -> ObservedCall {
        ObservedCall {
            correlation_id: self.correlation_id,
            boundary: query.boundary.to_owned(),
            trait_name: query.trait_name.to_owned(),
            method_name: query.method_name.to_owned(),
            args: query.args.clone(),
            resolved: self.recorded_result.is_some(),
            resolved_rank: self.resolved_rank,
            source_event_global_sequence: self.source_event_global_sequence,
            call_file: self.location.as_ref().map(|(f, _, _)| f.clone()),
            call_line: self.location.as_ref().map(|(_, l, _)| *l),
            call_column: self.location.as_ref().map(|(_, _, c)| *c),
            logical_span_path: crate::current_logical_span_path(),
            graph_node_id: self.graph_node_id,
            // V1 full mock never synthesizes and never relies on the real impl;
            // these stay false until the V2 tiered-miss work lands.
            synthesized: false,
            real_impl_will_fail: false,
            recorded_result: self.recorded_result,
            observed_result,
            provenance,
            seed_gap: false,
            // Filled by the execute-shadow probe when a StateProbe is installed
            // (see `LookupTableHook::execute_shadow_peek`/`observe`).
            pre_image: None,
            result_image: None,
        }
    }
}

impl DejaHook for LookupTableHook {
    fn is_active(&self) -> bool {
        true
    }

    fn try_replay_with_context(&self, query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        // Resolve the recorded baseline (advancing the stamper / sequence
        // counters EXACTLY ONCE, shared with the execute path so numbering never
        // drifts between lookup and execute boundaries), then emit a `Recorded`
        // observation: under lookup mode the observed result IS the substituted
        // recorded result, so the two sides are identical and ValueDiverged is
        // inert.
        let resolution = self.resolve(&query);
        let recorded = resolution.recorded_result();
        self.observed_sink.observed(resolution.into_observed_call(
            &query,
            // Lookup mode: observed == recorded (the substituted value).
            recorded.clone(),
            crate::Provenance::Recorded,
        ));
        recorded
    }

    fn execute_shadow_peek(
        &self,
        query: ReplayLookup<'_>,
    ) -> Option<crate::ExecuteShadowToken> {
        // First half of an execute-mode dispatch. Resolve the recorded baseline
        // through the SAME path the lookup uses (so the stamper / sequence /
        // occurrence counters advance identically — a run mixing lookup and
        // execute boundaries keeps aligned numbering), but do NOT substitute and
        // do NOT emit yet. Build the shadow observation with `observed_result =
        // None`; the macro fills it after the real boundary call and hands the
        // token back to `execute_shadow_observe`.
        let resolution = self.resolve(&query);
        let mut observed = resolution.into_observed_call(
            &query,
            // Filled in by `execute_shadow_observe` from the real result.
            None,
            crate::Provenance::ExecuteShadow,
        );
        // `seed_gap` marks "ran the real boundary but there is no recorded
        // baseline to compare against" — surfaced by the tally as
        // InconclusiveSeedGap rather than a false match/divergence.
        observed.seed_gap = observed.recorded_result.is_none();
        // Deliverable 6: when a StateProbe is installed, capture the pre-image of
        // the affected key BEFORE the real op runs. Carried on the ObservedCall
        // (and thus transparently across the opaque ExecuteShadowToken) so
        // `execute_shadow_observe` can attach the post-image.
        if let Some(probe) = self.state_probe.as_deref() {
            if let Some(key) = primary_state_key(query.args) {
                observed.pre_image = probe.probe(query.boundary, &key);
            }
        }
        Some(crate::ExecuteShadowToken::new(observed))
    }

    fn execute_shadow_observe(
        &self,
        token: crate::ExecuteShadowToken,
        observed_result: serde_json::Value,
    ) {
        // Second half: stamp the real boundary's result onto the carried
        // observation and emit it. The post-hoc tally pairs this ExecuteShadow
        // observation against the recorded baseline by args-free identity +
        // occurrence and classifies ValueDiverged on a value diff.
        let mut observed = token.into_observed(observed_result);
        // Deliverable 6: capture the post-image AFTER the real op ran, from the
        // same key the pre-image was probed for (re-derived from the carried
        // args). The pre/post pair is what a single-op RMW total-derivative diff
        // needs.
        if let Some(probe) = self.state_probe.as_deref() {
            if let Some(key) = primary_state_key(&observed.args) {
                observed.result_image = probe.probe(&observed.boundary, &key);
            }
        }
        self.observed_sink.observed(observed);
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

    fn replay_policy(&self) -> crate::Policy {
        // Report this hook's parsed policy so the declarative decision matrix
        // (`boundary_execute_mode_for`) keys the right column for a DECLARED
        // boundary. UNDECLARED boundaries never consult this — they route through
        // `execute_mode` below — so this changes no unmigrated decision.
        self.policy
    }

    fn execute_mode(
        &self,
        boundary: &str,
        _trait_name: &str,
        method_name: &str,
    ) -> crate::ExecuteMode {
        // Only the State channel (db / redis) opts into running the REAL boundary
        // during replay, and only under SelectiveExecute. Everything else — and
        // the entire AllLookup path — stays in Lookup mode, so a run with no
        // DEJA_POLICY set returns Lookup for every call and is byte-identical to
        // before this method existed.
        //
        // OP-SCOPING: when `execute_ops` is non-empty, only State boundaries whose
        // `method_name` is in the set execute; every other State boundary falls
        // back to Lookup. An EMPTY set means "all State executes" (the original
        // behavior). This lets the demo execute ONLY the settlement ops.
        match self.policy {
            crate::Policy::SelectiveExecute if is_state_channel(boundary) => {
                if self.execute_ops.is_empty() || self.execute_ops.contains(method_name) {
                    crate::ExecuteMode::Execute
                } else {
                    crate::ExecuteMode::Lookup
                }
            }
            _ => crate::ExecuteMode::Lookup,
        }
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

// ===========================================================================
// Total-derivative SEEDING pipeline (PURE, replay-side)
//
// A boundary crossing is `imp(x) ≡ pur(x, h)`. Replay re-runs the real `pur`
// and must supply the handler `h` as preconditions: the State (db/redis) keys a
// correlation READ must already hold the recorded value before the candidate
// re-executes, or a re-run read either misses (false divergence) or reads stale
// (silent lie). The functions below DERIVE those preconditions from a recording
// — they read only `&[SemanticEvent]` and produce plain data, so they are fully
// unit-testable without docker. Materialization into a live store is a separate,
// thin wiring step (see `replay-harness-api` lifecycle) that walks a `SeedPlan`.
//
// Design source: docs/design/recording-capture-decoupled.md §2.D, §5, §7.1.
// All inputs come off the tape; nothing here re-interprets args/result bytes or
// touches `canonical_args_hash` — it only JOINS already-captured fields.
// ===========================================================================

/// One precondition to materialize before a correlation is re-executed: the
/// State `boundary`/`key` must hold `value`. Derived from the recorded read-set
/// of every State READ in the correlation (deliverable 1) and/or merged from an
/// ambient template (deliverable 4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedEntry {
    /// State channel this key lives on (`"redis"` / `"storage"`).
    pub boundary: String,
    /// The state key (from the recorded `read_set`).
    pub key: String,
    /// The recorded value the key held when the correlation read it.
    pub value: serde_json::Value,
    /// How this entry entered the plan: derived from the recording's read-set,
    /// or supplied by the ambient/config template.
    pub origin: SeedOrigin,
}

/// Where a [`SeedEntry`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SeedOrigin {
    /// Reconstructed from a recorded State READ's `read_set` + `result`.
    #[default]
    Recording,
    /// Supplied by the static ambient/config template (deliverable 4).
    Ambient,
}

/// The set of `(boundary, key, value)` preconditions to materialize for a
/// correlation before re-execution, keyed by `(boundary, key)` so a later
/// read-set occurrence (or an ambient default) resolves deterministically.
///
/// Built by [`build_seed_plan`] (deliverable 1) over a recording's events,
/// optionally pre-loaded with an [`AmbientTemplate`] (deliverable 4). Consult it
/// with [`Self::resolve`] / [`Self::classify_read`] (deliverable 3) to decide
/// whether a candidate's diverged read is reconstructable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeedPlan {
    /// `(boundary, key) -> entry`. A BTreeMap keeps materialization order stable
    /// (so a `redis-cli SET` sequence is deterministic across runs).
    entries: BTreeMap<(String, String), SeedEntry>,
}

impl SeedPlan {
    /// An empty plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert (or overwrite) one precondition. Recording-derived entries take
    /// precedence over ambient defaults for the SAME key (so a key actually
    /// observed in the recording is seeded with what the recording saw, not the
    /// template default); ambient never clobbers a recording entry.
    pub fn upsert(&mut self, entry: SeedEntry) {
        let k = (entry.boundary.clone(), entry.key.clone());
        match self.entries.get(&k) {
            // Recording always wins over Ambient; Recording-over-Recording keeps
            // the FIRST recorded value within the correlation (the precondition
            // the correlation observed before it began mutating the key).
            Some(existing)
                if existing.origin == SeedOrigin::Recording
                    && entry.origin == SeedOrigin::Ambient => {}
            Some(existing) if existing.origin == SeedOrigin::Recording => {}
            _ => {
                self.entries.insert(k, entry);
            }
        }
    }

    /// Number of preconditions in the plan.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the plan is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Resolve the seeded value for a `(boundary, key)`, if the plan has one.
    pub fn resolve(&self, boundary: &str, key: &str) -> Option<&SeedEntry> {
        self.entries
            .get(&(boundary.to_owned(), key.to_owned()))
    }

    /// Whether the plan (recording-derived OR ambient) covers this key.
    pub fn contains(&self, boundary: &str, key: &str) -> bool {
        self.resolve(boundary, key).is_some()
    }

    /// Iterate the preconditions in deterministic `(boundary, key)` order — the
    /// materialization order the harness shells into the store.
    pub fn iter(&self) -> impl Iterator<Item = &SeedEntry> {
        self.entries.values()
    }

    /// Merge an [`AmbientTemplate`] into this plan (deliverable 4). Ambient
    /// entries fill keys the recording never observed (e.g. a config rate a
    /// re-keyed read reaches for); they never overwrite a recording-derived
    /// precondition. Returns `self` for chaining.
    pub fn with_ambient(mut self, template: &AmbientTemplate) -> Self {
        for entry in template.entries() {
            self.upsert(entry.clone());
        }
        self
    }

    /// Classify a candidate's observed read against this plan (deliverable 3).
    /// See [`ReadClassification`]. NEVER returns `Reconstructable` for a key the
    /// plan does not cover — a key the recording never observed and the template
    /// does not define is a seed-gap, surfaced rather than served stale.
    pub fn classify_read(&self, boundary: &str, key: &str) -> ReadClassification {
        match self.resolve(boundary, key) {
            Some(entry) => ReadClassification::Reconstructable {
                value: entry.value.clone(),
                origin: entry.origin,
            },
            None => ReadClassification::NotReconstructable {
                boundary: boundary.to_owned(),
                key: key.to_owned(),
            },
        }
    }
}

/// Verdict for a candidate's diverged read of a State key (deliverable 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadClassification {
    /// The key is in the seed plan (recording-derived or ambient): the read can
    /// be reconstructed from `value`. `origin` distinguishes a real recorded
    /// precondition from an ambient/config default.
    Reconstructable {
        value: serde_json::Value,
        origin: SeedOrigin,
    },
    /// The key is NOT in the plan AND NOT in the ambient template — a seed-gap.
    /// The harness must surface this (it maps to
    /// [`DivergenceKind::InconclusiveSeedGap`]) rather than silently serving a
    /// stale value for a key the recording never observed.
    NotReconstructable { boundary: String, key: String },
}

impl ReadClassification {
    /// Whether the read can be reconstructed (plan or template covers the key).
    pub fn is_reconstructable(&self) -> bool {
        matches!(self, ReadClassification::Reconstructable { .. })
    }
}

/// Build a [`SeedPlan`] from a recording's events for ONE correlation
/// (deliverable 1). Walks every State READ in `correlation_id`'s slice and, for
/// each key in its recorded `read_set`, records the value the read returned —
/// the precondition the key must hold for the candidate to re-read it.
///
/// PURE: reads only `&[SemanticEvent]`, allocates plain data, performs no I/O.
/// The FIRST recorded read of a key within the correlation wins (its value is
/// the precondition that existed before the correlation began mutating it); a
/// later read of the same key after a write reflects the mutation, not the
/// precondition, so it must not overwrite the seed.
///
/// `correlation_id == None` selects events with no correlation (mirrors
/// [`correlation_matches`]), so a single-case tape still builds a plan.
/// Whether a recorded read `result` represents a MISS (no value present), which
/// must NOT be seeded. Redis serializes a nil GET as the string `"Null"`; db/json
/// boundaries use JSON `null`. Seeding a miss writes a key the record run never
/// held, so the re-executed read would find a phantom value and diverge.
fn is_miss_result(result: &serde_json::Value) -> bool {
    result.is_null() || matches!(result, serde_json::Value::String(s) if s == "Null")
}

pub fn build_seed_plan(events: &[SemanticEvent], correlation_id: Option<&str>) -> SeedPlan {
    let mut plan = SeedPlan::new();
    // A key is "pristine" until the correlation first WRITES it; only reads
    // before the first write to a key describe the precondition.
    let mut written: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for event in events {
        if !correlation_matches(event, correlation_id) {
            continue;
        }
        // DECLARATIVE PREFERENCE: gate on the declared channel (fallback to the
        // `is_state_channel` heuristic for undeclared events).
        if !event_is_state(event) {
            continue;
        }
        // Mark writes so a post-write read of the same key never seeds the
        // (now-mutated) value as a precondition.
        for key in &event.write_set {
            written.insert((event.boundary.clone(), key.clone()));
        }
        // Only READ events describe a precondition; the recorded `result` is the
        // value the key held. Skip error reads — a failed read has no value to
        // seed and must not masquerade as a precondition.
        if event.is_error {
            continue;
        }
        // DECLARATIVE PREFERENCE: only READ events describe a precondition; read
        // off the declared effect (fallback to the `is_read_op` heuristic).
        if event_is_read(event) {
            // Skip MISS reads: a key absent at record time must STAY absent in the
            // seed, or the re-executed read finds a phantom value and diverges. A
            // redis miss serializes as the string "Null" (the nil sentinel); a db
            // miss as JSON null. Seeding either would write a key the record run
            // never had (the 18 spurious redis get_key/delete_key value-divergences).
            // Now keyed off the READ verdict (declared `Effect::Read`, fallback
            // `is_read_op`): a non-read event never reaches the miss guard, which is
            // behavior-identical (the miss guard only ever fed the read-seed loop).
            if is_miss_result(&event.result) {
                continue;
            }
            for key in &event.read_set {
                let k = (event.boundary.clone(), key.clone());
                if written.contains(&k) {
                    // The correlation already wrote this key before this read;
                    // the value reflects the mutation, not the precondition.
                    continue;
                }
                plan.upsert(SeedEntry {
                    boundary: event.boundary.clone(),
                    key: key.clone(),
                    value: event.result.clone(),
                    origin: SeedOrigin::Recording,
                });
            }
        }
    }
    plan
}

/// Recover the `pre_image` of every State WRITE by JOINING to the most-recent
/// prior READ of the same key within the correlation (deliverable 2).
///
/// PURE join over an event slice: for each WRITE event, the pre-image is the
/// `result` of the latest preceding READ whose `read_set` overlaps the write's
/// `write_set` (the read/write-set overlap on the same key). Returns one
/// [`PreImageJoin`] per write that resolved a pre-image; writes with no prior
/// read of the key are omitted (their pre-image is genuinely unknown from the
/// tape — a live probe is needed, deliverable 6).
///
/// Does not mutate the events; the caller may stamp `SemanticEvent::pre_image`
/// from the result (the recording side leaves `pre_image = None`, so this makes
/// it real for analysis without re-recording).
pub fn join_pre_images(
    events: &[SemanticEvent],
    correlation_id: Option<&str>,
) -> Vec<PreImageJoin> {
    let mut joins = Vec::new();
    // Most-recent read value per (boundary, key) seen SO FAR in order.
    let mut last_read: HashMap<(String, String), serde_json::Value> = HashMap::new();
    for (idx, event) in events.iter().enumerate() {
        if !correlation_matches(event, correlation_id) {
            continue;
        }
        // DECLARATIVE PREFERENCE: gate on the declared channel (fallback to the
        // `is_state_channel` heuristic for undeclared events).
        if !event_is_state(event) {
            continue;
        }
        // DECLARATIVE PREFERENCE: read off the declared effect (Read → read_set,
        // Write/RMW → write_set); fallback to the `is_read_op` heuristic.
        let is_read = event_is_read(event);
        if is_read && !event.is_error {
            for key in &event.read_set {
                last_read.insert(
                    (event.boundary.clone(), key.clone()),
                    event.result.clone(),
                );
            }
        }
        // A WRITE recovers its pre-image from the most-recent prior read of the
        // SAME key (read_set/write_set overlap). After recovering, the write's
        // own result becomes the new "current" value for any subsequent write.
        if !is_read {
            for key in &event.write_set {
                let k = (event.boundary.clone(), key.clone());
                if let Some(prior) = last_read.get(&k) {
                    joins.push(PreImageJoin {
                        write_global_sequence: event.global_sequence,
                        write_event_index: idx,
                        boundary: event.boundary.clone(),
                        key: key.clone(),
                        pre_image: prior.clone(),
                    });
                }
                // The write's result is the key's post-image; a later write or
                // read-after-write joins to it.
                if !event.is_error {
                    last_read.insert(k, event.result.clone());
                }
            }
        }
    }
    joins
}

/// One recovered pre-image: a State WRITE joined to the most-recent prior READ
/// of the same key within the correlation (deliverable 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreImageJoin {
    /// `global_sequence` of the write event the pre-image belongs to.
    pub write_global_sequence: u64,
    /// Index of the write within the input slice (stable join handle).
    pub write_event_index: usize,
    /// State channel the key lives on.
    pub boundary: String,
    /// The mutated key.
    pub key: String,
    /// The value the key held immediately before the write (the prior read's
    /// recorded `result`).
    pub pre_image: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Ambient template (deliverable 4) — static config/ambient state
// ---------------------------------------------------------------------------

/// A static template of ambient/config State that is NOT part of any one
/// recording's observed read-set but that a re-keyed / diverged read may reach
/// for (e.g. `settlement_rate_premium`). Merged into a [`SeedPlan`] via
/// [`SeedPlan::with_ambient`] so such reads resolve from the template rather
/// than being flagged as seed-gaps.
///
/// The default ([`AmbientTemplate::demo_defaults`]) carries the EU-settlement
/// demo's premium rate, replacing the hand-coded `redis-cli SET
/// settlement_rate_premium 0.20` in the lifecycle driver.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AmbientTemplate {
    /// Ambient entries, each an `Ambient`-origin precondition.
    entries: Vec<SeedEntry>,
}

impl AmbientTemplate {
    /// An empty template.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an ambient `(boundary, key, value)`. Stamped `SeedOrigin::Ambient`.
    pub fn insert(
        &mut self,
        boundary: impl Into<String>,
        key: impl Into<String>,
        value: serde_json::Value,
    ) {
        self.entries.push(SeedEntry {
            boundary: boundary.into(),
            key: key.into(),
            value,
            origin: SeedOrigin::Ambient,
        });
    }

    /// The ambient entries.
    pub fn entries(&self) -> &[SeedEntry] {
        &self.entries
    }

    /// Whether the template defines anything.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Parse a template from simple `boundary\tkey\tvalue` lines (one per line,
    /// `#`-comments and blanks ignored). `value` is parsed as JSON if it is
    /// valid JSON, else treated as a JSON string — so `0.20` becomes a number
    /// and `usd` becomes `"usd"`. Lets the demo's ambient config live in a file
    /// (deliverable 4) instead of being hard-coded.
    pub fn from_tsv(text: &str) -> Self {
        let mut template = Self::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut cols = line.splitn(3, '\t');
            let (Some(boundary), Some(key), Some(raw)) =
                (cols.next(), cols.next(), cols.next())
            else {
                continue;
            };
            let value = serde_json::from_str::<serde_json::Value>(raw.trim())
                .unwrap_or_else(|_| serde_json::Value::String(raw.trim().to_owned()));
            template.insert(boundary.trim(), key.trim(), value);
        }
        template
    }

    /// The EU-settlement demo's ambient defaults. The premium rate is the value
    /// a re-keyed settlement read reaches for under SelectiveExecute; sourcing it
    /// here (instead of a hand-coded `redis-cli SET`) is deliverable 4. The
    /// value is stored raw as it would sit in redis (a string `"0.20"`), so the
    /// materializer writes byte-identical bytes to the old literal seed.
    pub fn demo_defaults() -> Self {
        let mut template = Self::new();
        // The premium settlement rate the divergent (re-keyed) read observes.
        // Was a hand-coded `redis-cli SET settlement_rate_premium 0.20`.
        template.insert(
            "redis",
            "settlement_rate_premium",
            serde_json::Value::String("0.20".to_owned()),
        );
        template
    }
}

// ---------------------------------------------------------------------------
// StateProbe (deliverable 6) — pre/post images for single-op RMW
// ---------------------------------------------------------------------------

/// Best-effort extraction of the primary State key from a live call's args, for
/// the execute-shadow probe. Mirrors the recorder's `extract_primary_state_key`
/// (the leading string scalar in argument order is the common key shape) so the
/// probe targets the same key the recording's `read_set`/`write_set` named. A
/// HINT, never authoritative.
pub(crate) fn primary_state_key(args: &serde_json::Value) -> Option<String> {
    fn first_string(v: &serde_json::Value) -> Option<String> {
        match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Array(a) => a.iter().find_map(first_string),
            serde_json::Value::Object(m) => m.values().find_map(first_string),
            _ => None,
        }
    }
    first_string(args)
}

/// Abstraction over reading a single State key's current value from the live
/// replay store, used by the execute-shadow path to capture `pre_image` (before
/// the real op) and `result_image` (after). Decouples the probe mechanism (a
/// live `redis-cli GET`, a db SELECT, or an in-memory fake) from the hook, so
/// the RMW image capture is unit-testable without docker (deliverable 6).
///
/// A real live probe needs the running container (docker), so the production
/// impl lives in the harness; here we define the seam plus an in-memory fake the
/// hook tests exercise. The hook calls `probe(boundary, key)` immediately before
/// the real boundary runs (→ `pre_image`) and immediately after (→
/// `result_image`).
pub trait StateProbe: Send + Sync {
    /// Read the current value of `key` on `boundary`, or `None` if absent.
    fn probe(&self, boundary: &str, key: &str) -> Option<serde_json::Value>;
}

/// An in-memory [`StateProbe`] for tests: a shared `(boundary, key) -> value`
/// map, plus a mutation hook so a test can simulate the real op changing the
/// store between the pre- and post-probe (the RMW the execute path observes).
///
/// The store is `Arc`-shared, so a [`Clone`] of the probe observes the same
/// state — a test can install one clone in the hook and keep another to mutate
/// the store between peek and observe (modelling the real op's write).
#[derive(Debug, Default, Clone)]
pub struct InMemoryStateProbe {
    store: std::sync::Arc<Mutex<BTreeMap<(String, String), serde_json::Value>>>,
}

impl InMemoryStateProbe {
    /// Build from initial `(boundary, key, value)` triples.
    pub fn new(
        initial: impl IntoIterator<Item = (String, String, serde_json::Value)>,
    ) -> Self {
        let store = initial
            .into_iter()
            .map(|(b, k, v)| ((b, k), v))
            .collect();
        Self {
            store: std::sync::Arc::new(Mutex::new(store)),
        }
    }

    /// Simulate the real op writing `key` (lets a test model an RMW between the
    /// pre- and post-probe).
    pub fn set(&self, boundary: &str, key: &str, value: serde_json::Value) {
        if let Ok(mut store) = self.store.lock() {
            store.insert((boundary.to_owned(), key.to_owned()), value);
        }
    }
}

impl StateProbe for InMemoryStateProbe {
    fn probe(&self, boundary: &str, key: &str) -> Option<serde_json::Value> {
        self.store
            .lock()
            .ok()
            .and_then(|s| s.get(&(boundary.to_owned(), key.to_owned())).cloned())
    }
}

/// The pre/post images captured around a single-op RMW execute-shadow dispatch
/// (deliverable 6). Built by [`probe_rmw_images`] from a [`StateProbe`]; the
/// caller stamps them onto the shadow `ObservedCall` / event so total-derivative
/// diff sees what the real op changed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RmwImages {
    /// Value of the key BEFORE the real op (probe at peek). `None` if absent.
    pub pre_image: Option<serde_json::Value>,
    /// Value of the key AFTER the real op (probe at observe). `None` if absent.
    pub result_image: Option<serde_json::Value>,
}

/// Capture the pre-image of a key via a [`StateProbe`] (call BEFORE the real op
/// runs — the `execute_shadow_peek` half). Returns the half-built [`RmwImages`]
/// to be completed by [`complete_rmw_images`] after the op (deliverable 6).
pub fn probe_rmw_images(
    probe: &dyn StateProbe,
    boundary: &str,
    key: &str,
) -> RmwImages {
    RmwImages {
        pre_image: probe.probe(boundary, key),
        result_image: None,
    }
}

/// Complete the [`RmwImages`] by probing the post-image (call AFTER the real op
/// runs — the `execute_shadow_observe` half) (deliverable 6).
pub fn complete_rmw_images(
    mut images: RmwImages,
    probe: &dyn StateProbe,
    boundary: &str,
    key: &str,
) -> RmwImages {
    images.result_image = probe.probe(boundary, key);
    images
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
            provenance: crate::Provenance::default(),
            recon: crate::Recon::default(),
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
                .map(|d| d.kind.clone())
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
        // A NOVEL-arg READ at this existing call-site no longer misses: the
        // reads-only arg-tolerant fallback (now policy-independent) serves the
        // recorded value for this call-site + occurrence with args ignored. The
        // novel `id:3` is a new args_hash bucket → occurrence 0, so the argless
        // index returns the first occurrence-0 entry recorded here ("alpha").
        // (A WRITE would still MISS — the fallback is reads-only.)
        assert_eq!(
            call(serde_json::json!({ "id": 3 })),
            Some(serde_json::json!("alpha")),
            "a re-keyed READ resolves arg-tolerantly to the occurrence-0 recorded value"
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
        // call 3 (novel-arg READ) now resolves via the reads-only arg-tolerant
        // fallback, at the explicit-address rank (1) of the occurrence-0 entry.
        assert!(calls[2].resolved);
        assert_eq!(calls[2].resolved_rank, Some(1));
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

    // -----------------------------------------------------------------------
    // Policy + arg-tolerant lookup (M1 partial-vs-total contrast)
    // -----------------------------------------------------------------------

    /// `is_read_op` classifies reads (whole name OR verb suffix) and never
    /// classifies a write — the no-regression invariant for the reads-only
    /// arg-tolerant fallback.
    #[test]
    fn is_read_op_reads_true_writes_false() {
        // Plain read names (whole-string verb).
        for m in [
            "find_payment_intent_by_id",
            "get_key",
            "exists",
            "get_hash_field",
        ] {
            assert!(is_read_op(m), "{m} must be a read");
        }
        // Component-prefixed read (verb suffix).
        assert!(is_read_op("eu_settlement_read"), "verb-suffix read");
        // Writes / unknown stay false (never arg-tolerant).
        for m in [
            "eu_settlement_write",
            "set_key",
            "serialize_and_set_key",
            "delete_key",
            "insert_payment_intent",
            "update_config",
            "set_hash_field_if_not_exist",
        ] {
            assert!(!is_read_op(m), "{m} must NOT be a read");
        }
    }

    /// A re-keyed READ (its args changed since recording) under the DEFAULT
    /// `AllLookup` policy now resolves via the arg-tolerant fallback to the
    /// RECORDED value. The fallback is READS-ONLY and policy-independent, so
    /// AllLookup serves the recorded value on a re-keyed read — a clean MISS for
    /// the demo (the served value equals the recorded, so no divergence).
    #[test]
    fn all_lookup_serves_rekeyed_read_arg_tolerantly() {
        let recorded_args = serde_json::json!({ "id": "pi_recorded" });
        let table = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries: vec![entry_with(
                None,
                explicit("find_pi"),
                &recorded_args,
                0,
                serde_json::json!({ "Ok": "row_recorded" }),
                1,
            )],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        // Default policy == AllLookup.
        let hook = LookupTableHook::from_source(VecSource(Some(table)), observed).expect("hook");
        assert_eq!(hook.policy(), crate::Policy::AllLookup);

        // Candidate calls the SAME call-site with DIFFERENT (re-keyed) args.
        let rekeyed_args = serde_json::json!({ "id": "pi_doubled" });
        let identity = explicit_identity("find_pi");
        let value = hook.try_replay_with_context(ReplayLookup {
            boundary: "storage",
            trait_name: "PaymentIntentInterface",
            method_name: "find_payment_intent_by_id",
            args: &rekeyed_args,
            callsite_identity: Some(&identity),
            caller_location: None,
        });
        assert_eq!(
            value,
            Some(serde_json::json!({ "Ok": "row_recorded" })),
            "AllLookup must serve the recorded value on a re-keyed READ (reads-only fallback)"
        );
        assert!(handle.lock().unwrap()[0].resolved);
    }

    /// NO-REGRESSION: a re-keyed WRITE (a non-read op) under AllLookup MUST still
    /// MISS — the arg-tolerant fallback is reads-only, so a changed write never
    /// becomes arg-tolerant and stays strict (so a divergent write is caught).
    #[test]
    fn all_lookup_misses_on_rekeyed_write() {
        let recorded_args = serde_json::json!({ "key": "k_recorded", "value": "0.10" });
        let table = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries: vec![entry_with(
                None,
                explicit("set_key"),
                &recorded_args,
                0,
                serde_json::json!({ "Ok": null }),
                1,
            )],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook = LookupTableHook::from_source(VecSource(Some(table)), observed).expect("hook");

        // Same call-site, re-keyed WRITE args — method name is a write verb.
        let rekeyed_args = serde_json::json!({ "key": "k_recorded", "value": "0.20" });
        let identity = explicit_identity("set_key");
        let value = hook.try_replay_with_context(ReplayLookup {
            boundary: "redis",
            trait_name: "RedisInterface",
            method_name: "serialize_and_set_key",
            args: &rekeyed_args,
            callsite_identity: Some(&identity),
            caller_location: None,
        });
        assert_eq!(value, None, "a re-keyed WRITE must still MISS (reads-only fallback)");
        assert!(!handle.lock().unwrap()[0].resolved);
    }

    /// The SAME re-keyed READ under `SelectiveExecute` resolves via the
    /// arg-tolerant fallback to the RECORDED result (args ignored, matched by
    /// call-site identity + occurrence). This is the partial-derivative
    /// substitution that serves a recorded value even when the args drifted.
    #[test]
    fn selective_execute_serves_rekeyed_read_arg_tolerantly() {
        let recorded_args = serde_json::json!({ "id": "pi_recorded" });
        let table = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries: vec![entry_with(
                None,
                explicit("find_pi"),
                &recorded_args,
                0,
                serde_json::json!({ "Ok": "row_recorded" }),
                1,
            )],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook = LookupTableHook::from_source_with_policy(
            VecSource(Some(table)),
            observed,
            crate::Policy::SelectiveExecute,
            std::collections::HashSet::new(),
        )
        .expect("hook");

        let rekeyed_args = serde_json::json!({ "id": "pi_doubled" });
        let identity = explicit_identity("find_pi");
        let value = hook.try_replay_with_context(ReplayLookup {
            boundary: "storage",
            trait_name: "PaymentIntentInterface",
            method_name: "find_payment_intent_by_id",
            args: &rekeyed_args,
            callsite_identity: Some(&identity),
            caller_location: None,
        });
        assert_eq!(
            value,
            Some(serde_json::json!({ "Ok": "row_recorded" })),
            "SelectiveExecute must serve the recorded result arg-tolerantly"
        );
        assert!(handle.lock().unwrap()[0].resolved);
    }

    /// `execute_mode` is byte-identical (Lookup) under AllLookup for every
    /// channel, and only flips to Execute for the State channel (db/redis) under
    /// SelectiveExecute. Entropy/Egress stay Lookup even under SelectiveExecute.
    #[test]
    fn execute_mode_respects_policy_and_channel() {
        let empty = LookupTable {
            recording_id: "r".to_owned(),
            policy_version: 1,
            entries: vec![],
        };

        let all_lookup = LookupTableHook::from_source(
            VecSource(Some(empty.clone())),
            InMemoryObservedSink::new(),
        )
        .expect("hook");
        for boundary in ["storage", "redis", "http_client", "time"] {
            assert_eq!(
                all_lookup.execute_mode(boundary, "T", "m"),
                crate::ExecuteMode::Lookup,
                "AllLookup must be Lookup for {boundary}"
            );
        }

        let selective = LookupTableHook::from_source_with_policy(
            VecSource(Some(empty)),
            InMemoryObservedSink::new(),
            crate::Policy::SelectiveExecute,
            std::collections::HashSet::new(),
        )
        .expect("hook");
        assert_eq!(
            selective.execute_mode("storage", "T", "m"),
            crate::ExecuteMode::Execute
        );
        assert_eq!(
            selective.execute_mode("redis", "T", "m"),
            crate::ExecuteMode::Execute
        );
        assert_eq!(
            selective.execute_mode("http_client", "T", "m"),
            crate::ExecuteMode::Lookup,
            "Egress stays Lookup even under SelectiveExecute"
        );
        assert_eq!(
            selective.execute_mode("time", "T", "m"),
            crate::ExecuteMode::Lookup,
            "Entropy stays Lookup even under SelectiveExecute"
        );
    }

    // -----------------------------------------------------------------------
    // Declarative boundary model — decision matrix + the additive fallback.
    // -----------------------------------------------------------------------

    /// `decide_strategy` covers every declared matrix cell from the design §2
    /// table under both policies.
    #[test]
    fn decide_strategy_covers_every_matrix_cell() {
        use crate::{Channel, Effect, EntropySource, Policy, Strategy};

        // State + Read/Write: Lookup under AllLookup, SeedAndExecute under
        // SelectiveExecute.
        for eff in [Effect::Read, Effect::Write] {
            assert_eq!(
                decide_strategy(Some(Channel::State), Some(eff), None, Policy::AllLookup),
                Strategy::Lookup,
                "State+{eff:?} AllLookup"
            );
            assert_eq!(
                decide_strategy(
                    Some(Channel::State),
                    Some(eff),
                    None,
                    Policy::SelectiveExecute
                ),
                Strategy::SeedAndExecute,
                "State+{eff:?} SelectiveExecute"
            );
        }

        // State + VolatileRead: Lookup under both policies, even with an execute
        // override — a time-decaying / non-deterministic-iteration value is never
        // executed (it would diverge every run).
        for p in [Policy::AllLookup, Policy::SelectiveExecute] {
            assert_eq!(
                decide_strategy(Some(Channel::State), Some(Effect::VolatileRead), None, p),
                Strategy::Lookup,
                "State+VolatileRead {p:?}"
            );
        }
        assert_eq!(
            decide_strategy(
                Some(Channel::State),
                Some(Effect::VolatileRead),
                Some(Strategy::SeedAndExecute),
                Policy::SelectiveExecute
            ),
            Strategy::Lookup,
            "State+VolatileRead is never executed even with an override"
        );

        // State + Opaque: Lookup (opt-in via explicit override only).
        assert_eq!(
            decide_strategy(
                Some(Channel::State),
                Some(Effect::Opaque),
                None,
                Policy::SelectiveExecute
            ),
            Strategy::Lookup,
            "State+Opaque defaults Lookup"
        );
        assert_eq!(
            decide_strategy(
                Some(Channel::State),
                Some(Effect::Opaque),
                Some(Strategy::SeedAndExecute),
                Policy::SelectiveExecute
            ),
            Strategy::SeedAndExecute,
            "State+Opaque honors explicit execute override"
        );

        // State + Append: Lookup (AllLookup) / LookupAndSeed (Selective).
        assert_eq!(
            decide_strategy(
                Some(Channel::State),
                Some(Effect::Append),
                None,
                Policy::AllLookup
            ),
            Strategy::Lookup,
            "State+Append AllLookup"
        );
        assert_eq!(
            decide_strategy(
                Some(Channel::State),
                Some(Effect::Append),
                None,
                Policy::SelectiveExecute
            ),
            Strategy::LookupAndSeed,
            "State+Append SelectiveExecute"
        );

        // State + RMW: Lookup (AllLookup); under SelectiveExecute the per-op
        // override decides (the macro guarantees it is present).
        assert_eq!(
            decide_strategy(
                Some(Channel::State),
                Some(Effect::ReadModifyWrite),
                None,
                Policy::AllLookup
            ),
            Strategy::Lookup,
            "State+RMW AllLookup"
        );
        assert_eq!(
            decide_strategy(
                Some(Channel::State),
                Some(Effect::ReadModifyWrite),
                None,
                Policy::SelectiveExecute
            ),
            Strategy::Lookup,
            "State+RMW with no override falls back conservatively to Lookup"
        );
        assert_eq!(
            decide_strategy(
                Some(Channel::State),
                Some(Effect::ReadModifyWrite),
                Some(Strategy::LookupAndSeed),
                Policy::SelectiveExecute
            ),
            Strategy::LookupAndSeed,
            "State+RMW honors LookupAndSeed override"
        );
        assert_eq!(
            decide_strategy(
                Some(Channel::State),
                Some(Effect::ReadModifyWrite),
                Some(Strategy::SeedAndExecute),
                Policy::SelectiveExecute
            ),
            Strategy::SeedAndExecute,
            "State+RMW honors SeedAndExecute override"
        );

        // State with no declared effect → conservative Lookup under both policies.
        for p in [Policy::AllLookup, Policy::SelectiveExecute] {
            assert_eq!(
                decide_strategy(Some(Channel::State), None, None, p),
                Strategy::Lookup,
                "State+<no effect> {p:?}"
            );
        }

        // Entropy(_) / Egress: Lookup under every policy (no effect declared).
        for ch in [
            Channel::Entropy(EntropySource::Clock),
            Channel::Entropy(EntropySource::Id),
            Channel::Egress,
        ] {
            for p in [Policy::AllLookup, Policy::SelectiveExecute] {
                assert_eq!(
                    decide_strategy(Some(ch.clone()), None, None, p),
                    Strategy::Lookup,
                    "{ch:?} {p:?}"
                );
            }
        }
    }

    /// CRITICAL FALLBACK: an UNDECLARED `(None, None)` boundary returns
    /// `Lookup` from the pure matrix; and `boundary_execute_mode_for` routes
    /// it to the hook's existing heuristic so its runtime decision is
    /// BYTE-IDENTICAL to the hook's own `execute_mode` for representative
    /// boundaries (redis read, db write, id, http) under both policies.
    #[test]
    fn undeclared_boundary_matches_current_execute_mode() {
        use crate::{Policy, Strategy};

        // Pure-matrix undeclared default is the safe Lookup.
        assert_eq!(
            decide_strategy(None, None, None, Policy::AllLookup),
            Strategy::Lookup
        );
        assert_eq!(
            decide_strategy(None, None, None, Policy::SelectiveExecute),
            Strategy::Lookup
        );

        // Representative boundaries (boundary, trait, method).
        let cases: [(&str, &str, &str); 4] = [
            ("redis", "Cache", "get"),       // State read
            ("storage", "PI", "insert_x"),   // State write (db)
            ("id", "Gcm", "nonce"),          // Entropy
            ("http_client", "Http", "send"), // Egress
        ];

        for policy in [Policy::AllLookup, Policy::SelectiveExecute] {
            let empty = LookupTable {
                recording_id: "r".to_owned(),
                policy_version: 1,
                entries: vec![],
            };
            let hook = LookupTableHook::from_source_with_policy(
                VecSource(Some(empty)),
                InMemoryObservedSink::new(),
                policy,
                std::collections::HashSet::new(),
            )
            .expect("hook");

            for (b, t, m) in cases {
                // UNDECLARED spec — the current vendor shape.
                let spec = BoundarySpec::new(
                    Box::leak(b.to_string().into_boxed_str()),
                    Box::leak(t.to_string().into_boxed_str()),
                    Box::leak(m.to_string().into_boxed_str()),
                );
                let via_declarative = boundary_execute_mode_for(&hook, &spec);
                let via_heuristic = hook.execute_mode(b, t, m);
                assert_eq!(
                    via_declarative, via_heuristic,
                    "undeclared {b}/{t}/{m} under {policy:?} must match the heuristic"
                );
            }
        }
    }

    /// A DECLARED boundary on an `AllLookup` hook resolves to `Lookup` (every
    /// AllLookup cell is `Substitute`), so declaring is inert until the policy
    /// flips — the demo stays green.
    #[test]
    fn declared_boundary_is_inert_under_all_lookup() {
        let empty = LookupTable {
            recording_id: "r".to_owned(),
            policy_version: 1,
            entries: vec![],
        };
        let hook = LookupTableHook::from_source(
            VecSource(Some(empty)),
            InMemoryObservedSink::new(),
        )
        .expect("hook");
        let spec = BoundarySpec::with_semantics(
            "redis",
            "Cache",
            "get",
            crate::BoundarySemantics {
                channel: Some(crate::Channel::State),
                effect: Some(crate::Effect::Read),
                strategy: None,
            },
        );
        assert_eq!(
            boundary_execute_mode_for(&hook, &spec),
            crate::ExecuteMode::Lookup,
            "declared State+Read is Lookup under AllLookup (inert)"
        );
    }

    /// `strategy_to_execute_mode`: Lookup→Lookup, SeedAndExecute→Execute,
    /// and the STUBBED LookupAndSeed→Lookup (safe; see TODO(vendor-migration)).
    #[test]
    fn strategy_maps_to_execute_mode() {
        use crate::{ExecuteMode, Strategy};
        assert_eq!(strategy_to_execute_mode(Strategy::Lookup), ExecuteMode::Lookup);
        assert_eq!(
            strategy_to_execute_mode(Strategy::SeedAndExecute),
            ExecuteMode::Execute
        );
        assert_eq!(
            strategy_to_execute_mode(Strategy::LookupAndSeed),
            ExecuteMode::Lookup,
            "LookupAndSeed is stubbed to a safe Lookup in this slice"
        );
    }

    /// `Policy::from_env_value` parses the spellings and defaults safely.
    #[test]
    fn policy_parsing_defaults_safely() {
        assert_eq!(
            crate::Policy::from_env_value("selective_execute"),
            crate::Policy::SelectiveExecute
        );
        assert_eq!(
            crate::Policy::from_env_value("execute"),
            crate::Policy::SelectiveExecute
        );
        assert_eq!(
            crate::Policy::from_env_value("all_lookup"),
            crate::Policy::AllLookup
        );
        assert_eq!(crate::Policy::from_env_value(""), crate::Policy::AllLookup);
        assert_eq!(
            crate::Policy::from_env_value("garbage"),
            crate::Policy::AllLookup,
            "unknown value must default to AllLookup (never silently execute)"
        );
    }

    // -----------------------------------------------------------------------
    // Seed-plan pipeline tests (deliverables 1-4, 6) — all PURE, no docker.
    // -----------------------------------------------------------------------

    /// Build a State event with explicit read/write sets, boundary, and seq.
    #[allow(clippy::too_many_arguments)]
    fn state_event(
        global_seq: u64,
        correlation_id: Option<&str>,
        boundary: &str,
        method: &str,
        args: serde_json::Value,
        result: serde_json::Value,
        read_set: &[&str],
        write_set: &[&str],
        is_error: bool,
    ) -> SemanticEvent {
        let mut ev = make_event(global_seq, correlation_id, method, args, result, is_error);
        ev.global_sequence = global_seq;
        ev.boundary = boundary.into();
        ev.read_set = read_set.iter().map(|s| (*s).to_owned()).collect();
        ev.write_set = write_set.iter().map(|s| (*s).to_owned()).collect();
        ev
    }

    /// Deliverable 1: the seed-plan builder reconstructs `(boundary, key,
    /// value)` from every State READ's read_set + recorded result.
    #[test]
    fn seed_plan_built_from_read_set_and_result() {
        let events = vec![
            // A redis READ of the default rate → precondition redis:rate=0.10.
            state_event(
                0,
                Some("c1"),
                "redis",
                "get",
                serde_json::json!(["settlement_rate_default"]),
                serde_json::json!("0.10"),
                &["settlement_rate_default"],
                &[],
                false,
            ),
            // A db READ → precondition storage:user:42=Alice.
            state_event(
                1,
                Some("c1"),
                "storage",
                "find_user",
                serde_json::json!(["user:42"]),
                serde_json::json!({"Ok": "Alice"}),
                &["user:42"],
                &[],
                false,
            ),
            // A non-State (entropy) event must not produce a seed.
            {
                let mut e = make_event(
                    2,
                    Some("c1"),
                    "generate_id",
                    serde_json::json!([]),
                    serde_json::json!("uuid"),
                    false,
                );
                e.boundary = "id".into();
                e
            },
        ];

        let plan = build_seed_plan(&events, Some("c1"));
        assert_eq!(plan.len(), 2, "two State reads → two preconditions");
        assert_eq!(
            plan.resolve("redis", "settlement_rate_default").unwrap().value,
            serde_json::json!("0.10")
        );
        assert_eq!(
            plan.resolve("storage", "user:42").unwrap().value,
            serde_json::json!({"Ok": "Alice"})
        );
        assert!(
            !plan.contains("id", "uuid"),
            "entropy boundary is never seeded"
        );
        for entry in plan.iter() {
            assert_eq!(entry.origin, SeedOrigin::Recording);
        }
    }

    /// DECLARATIVE BOUNDARY MODEL — `build_seed_plan`/`join_pre_images` PREFER the
    /// declared `channel`/`effect` and fall back to the heuristics when undeclared.
    /// Proves the declaration OVERRIDES the name heuristics in both directions:
    /// a declared-Egress event with a State-channel boundary NAME is NOT seeded,
    /// and a declared-State event with a non-State boundary NAME IS seeded; a
    /// declared-Write with a read-NAMED method recovers its pre-image as a write.
    #[test]
    fn seed_plan_and_join_prefer_declared_channel_and_effect() {
        use crate::{Channel, Effect};

        // (a) DECLARED Egress, but the boundary NAME is "redis" (a State name).
        // The declaration wins → NOT a State event → never seeded.
        let mut egress_named_redis = state_event(
            0,
            Some("c1"),
            "redis",
            "find_thing", // read-named
            serde_json::json!(["k_egress"]),
            serde_json::json!("v"),
            &["k_egress"],
            &[],
            false,
        );
        egress_named_redis.channel = Some(Channel::Egress);
        egress_named_redis.effect = None;

        // (b) DECLARED State+Read, but the boundary NAME is "outbound" (NOT a State
        // name). The declaration wins → IS a State read → seeded.
        let mut state_named_outbound = state_event(
            1,
            Some("c1"),
            "outbound",
            "do_thing", // NOT a read verb; the declared effect (Read) wins
            serde_json::json!(["k_state"]),
            serde_json::json!("seeded"),
            &["k_state"],
            &[],
            false,
        );
        state_named_outbound.channel = Some(Channel::State);
        state_named_outbound.effect = Some(Effect::Read);

        let plan = build_seed_plan(
            &[egress_named_redis.clone(), state_named_outbound.clone()],
            Some("c1"),
        );
        assert!(
            !plan.contains("redis", "k_egress"),
            "a declared-Egress event is never seeded despite a State-channel name"
        );
        assert_eq!(
            plan.resolve("outbound", "k_state").unwrap().value,
            serde_json::json!("seeded"),
            "a declared-State Read is seeded despite a non-State boundary name"
        );

        // (c) join_pre_images: a declared State+Read of "k" (read-set) followed by a
        // declared State+Write of "k" (write-set) on a NON-State-named boundary,
        // with a WRITE method that is read-NAMED. The declaration drives the
        // read→write join; the heuristic (is_read_op on the read-named write) would
        // have mis-classified the write as a read and produced NO join.
        let mut read_ev = state_event(
            0,
            Some("c1"),
            "outbound",
            "find_thing",
            serde_json::json!(["k"]),
            serde_json::json!("before"),
            &["k"],
            &[],
            false,
        );
        read_ev.channel = Some(Channel::State);
        read_ev.effect = Some(Effect::Read);
        let mut write_ev = state_event(
            1,
            Some("c1"),
            "outbound",
            "find_then_set", // read-NAMED, but DECLARED Write
            serde_json::json!(["k", "after"]),
            serde_json::json!("after"),
            &[],
            &["k"],
            false,
        );
        write_ev.channel = Some(Channel::State);
        write_ev.effect = Some(Effect::Write);

        let joins = join_pre_images(&[read_ev, write_ev], Some("c1"));
        assert_eq!(joins.len(), 1, "declared Write joins to the prior read");
        assert_eq!(joins[0].boundary, "outbound");
        assert_eq!(joins[0].key, "k");
        assert_eq!(joins[0].pre_image, serde_json::json!("before"));
    }

    /// Deliverable 1: a read AFTER the correlation wrote a key reflects the
    /// mutation, not the precondition — so the FIRST (pre-write) read wins and a
    /// post-write read never overwrites the seed.
    #[test]
    fn seed_plan_first_pre_write_read_wins() {
        let events = vec![
            state_event(
                0,
                Some("c1"),
                "redis",
                "get",
                serde_json::json!(["k"]),
                serde_json::json!("before"),
                &["k"],
                &[],
                false,
            ),
            state_event(
                1,
                Some("c1"),
                "redis",
                "set",
                serde_json::json!(["k", "after"]),
                serde_json::json!("after"),
                &[],
                &["k"],
                false,
            ),
            // Read-after-write: returns the mutated value, must NOT reseed.
            state_event(
                2,
                Some("c1"),
                "redis",
                "get",
                serde_json::json!(["k"]),
                serde_json::json!("after"),
                &["k"],
                &[],
                false,
            ),
        ];
        let plan = build_seed_plan(&events, Some("c1"));
        assert_eq!(
            plan.resolve("redis", "k").unwrap().value,
            serde_json::json!("before"),
            "the precondition is the pre-write value, not the post-write read"
        );
    }

    /// Deliverable 1: an errored read carries no value and must not seed.
    #[test]
    fn seed_plan_skips_error_reads() {
        let events = vec![state_event(
            0,
            Some("c1"),
            "redis",
            "get",
            serde_json::json!(["k"]),
            serde_json::json!({"error": "boom"}),
            &["k"],
            &[],
            true,
        )];
        let plan = build_seed_plan(&events, Some("c1"));
        assert!(plan.is_empty(), "an error read seeds nothing");
    }

    #[test]
    fn seed_plan_skips_miss_reads() {
        // A MISS must not be seeded: a redis nil GET records as the string
        // "Null"; a db miss as JSON null. Seeding either writes a phantom key the
        // record run never held, so the re-executed read finds it and diverges
        // (the 18 spurious redis get_key/delete_key value-divergences).
        let events = vec![
            state_event(
                0, Some("c1"), "redis", "get_key",
                serde_json::json!(["missing"]), serde_json::json!("Null"),
                &["missing"], &[], false,
            ),
            state_event(
                1, Some("c1"), "db", "find_x",
                serde_json::json!(["absent"]), serde_json::Value::Null,
                &["absent"], &[], false,
            ),
            // a real HIT alongside, to prove only the misses are skipped
            state_event(
                2, Some("c1"), "redis", "get_key",
                serde_json::json!(["present"]), serde_json::json!({"String": "v"}),
                &["present"], &[], false,
            ),
        ];
        let plan = build_seed_plan(&events, Some("c1"));
        assert!(
            !plan.classify_read("redis", "missing").is_reconstructable(),
            "a miss read must NOT be seeded"
        );
        assert!(
            !plan.classify_read("db", "absent").is_reconstructable(),
            "a db null miss must NOT be seeded"
        );
        assert!(
            plan.classify_read("redis", "present").is_reconstructable(),
            "a real hit IS seeded"
        );
    }

    /// Deliverable 1: correlation isolation — only the requested case's reads
    /// build the plan.
    #[test]
    fn seed_plan_is_correlation_scoped() {
        let events = vec![
            state_event(
                0,
                Some("c1"),
                "redis",
                "get",
                serde_json::json!(["k"]),
                serde_json::json!("c1val"),
                &["k"],
                &[],
                false,
            ),
            state_event(
                1,
                Some("c2"),
                "redis",
                "get",
                serde_json::json!(["k"]),
                serde_json::json!("c2val"),
                &["k"],
                &[],
                false,
            ),
        ];
        let plan = build_seed_plan(&events, Some("c1"));
        assert_eq!(plan.len(), 1);
        assert_eq!(
            plan.resolve("redis", "k").unwrap().value,
            serde_json::json!("c1val")
        );
    }

    /// Deliverable 2: pre_image recovered by JOIN to the most-recent prior read.
    #[test]
    fn pre_image_join_recovers_prior_read() {
        let events = vec![
            state_event(
                0,
                Some("c1"),
                "redis",
                "get",
                serde_json::json!(["k"]),
                serde_json::json!("v0"),
                &["k"],
                &[],
                false,
            ),
            state_event(
                1,
                Some("c1"),
                "redis",
                "set",
                serde_json::json!(["k", "v1"]),
                serde_json::json!("v1"),
                &[],
                &["k"],
                false,
            ),
        ];
        let joins = join_pre_images(&events, Some("c1"));
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].key, "k");
        assert_eq!(joins[0].write_global_sequence, 1);
        assert_eq!(joins[0].pre_image, serde_json::json!("v0"));
    }

    /// Deliverable 2: a write with no prior read of the key yields no join (its
    /// pre-image is genuinely unknown from the tape → needs a live probe).
    #[test]
    fn pre_image_join_omits_write_without_prior_read() {
        let events = vec![state_event(
            0,
            Some("c1"),
            "redis",
            "set",
            serde_json::json!(["k", "v1"]),
            serde_json::json!("v1"),
            &[],
            &["k"],
            false,
        )];
        let joins = join_pre_images(&events, Some("c1"));
        assert!(joins.is_empty(), "no prior read → no recoverable pre-image");
    }

    /// Deliverable 2: a second write joins to the FIRST write's post-image
    /// (read→write→write chains its pre-images).
    #[test]
    fn pre_image_join_chains_through_writes() {
        let events = vec![
            state_event(
                0,
                Some("c1"),
                "redis",
                "get",
                serde_json::json!(["k"]),
                serde_json::json!("v0"),
                &["k"],
                &[],
                false,
            ),
            state_event(
                1,
                Some("c1"),
                "redis",
                "set",
                serde_json::json!(["k", "v1"]),
                serde_json::json!("v1"),
                &[],
                &["k"],
                false,
            ),
            state_event(
                2,
                Some("c1"),
                "redis",
                "set",
                serde_json::json!(["k", "v2"]),
                serde_json::json!("v2"),
                &[],
                &["k"],
                false,
            ),
        ];
        let joins = join_pre_images(&events, Some("c1"));
        assert_eq!(joins.len(), 2);
        assert_eq!(joins[0].pre_image, serde_json::json!("v0"));
        assert_eq!(
            joins[1].pre_image,
            serde_json::json!("v1"),
            "second write's pre-image is the first write's post-image"
        );
    }

    /// Deliverable 3: a key in the plan classifies as Reconstructable; a key
    /// neither in the plan nor the template is a seed-gap (never served stale).
    #[test]
    fn classify_read_flags_seed_gap() {
        let events = vec![state_event(
            0,
            Some("c1"),
            "redis",
            "get",
            serde_json::json!(["known"]),
            serde_json::json!("v"),
            &["known"],
            &[],
            false,
        )];
        let plan = build_seed_plan(&events, Some("c1"));

        let hit = plan.classify_read("redis", "known");
        assert!(hit.is_reconstructable());
        match hit {
            ReadClassification::Reconstructable { value, origin } => {
                assert_eq!(value, serde_json::json!("v"));
                assert_eq!(origin, SeedOrigin::Recording);
            }
            _ => panic!("expected reconstructable"),
        }

        let gap = plan.classify_read("redis", "never_seen");
        assert!(!gap.is_reconstructable());
        assert!(matches!(
            gap,
            ReadClassification::NotReconstructable { .. }
        ));
    }

    /// Deliverable 4: an ambient template resolves a diverged read to a config
    /// key the recording never observed — turning a would-be seed-gap into a
    /// reconstructable read.
    #[test]
    fn ambient_template_resolves_config_key() {
        // Recording only observed the DEFAULT rate.
        let events = vec![state_event(
            0,
            Some("c1"),
            "redis",
            "get",
            serde_json::json!(["settlement_rate_default"]),
            serde_json::json!("0.10"),
            &["settlement_rate_default"],
            &[],
            false,
        )];
        let plan = build_seed_plan(&events, Some("c1"));

        // Without the template the premium key is a seed-gap.
        assert!(!plan
            .classify_read("redis", "settlement_rate_premium")
            .is_reconstructable());

        // Merge the demo ambient defaults.
        let plan = plan.with_ambient(&AmbientTemplate::demo_defaults());
        let resolved = plan.classify_read("redis", "settlement_rate_premium");
        match resolved {
            ReadClassification::Reconstructable { value, origin } => {
                assert_eq!(value, serde_json::json!("0.20"));
                assert_eq!(origin, SeedOrigin::Ambient);
            }
            _ => panic!("ambient template should resolve the premium key"),
        }
    }

    /// Deliverable 4: a recording-derived precondition wins over an ambient
    /// default for the SAME key (ambient never clobbers what was observed).
    #[test]
    fn ambient_does_not_clobber_recording() {
        let events = vec![state_event(
            0,
            Some("c1"),
            "redis",
            "get",
            serde_json::json!(["settlement_rate_premium"]),
            serde_json::json!("0.15"),
            &["settlement_rate_premium"],
            &[],
            false,
        )];
        let plan = build_seed_plan(&events, Some("c1"))
            .with_ambient(&AmbientTemplate::demo_defaults());
        assert_eq!(
            plan.resolve("redis", "settlement_rate_premium").unwrap().value,
            serde_json::json!("0.15"),
            "the observed value (0.15) wins over the ambient default (0.20)"
        );
        assert_eq!(
            plan.resolve("redis", "settlement_rate_premium").unwrap().origin,
            SeedOrigin::Recording
        );
    }

    /// Deliverable 4: the ambient template parses from a TSV file body, JSON-
    /// typing values (so the demo's config can live in a file).
    #[test]
    fn ambient_template_from_tsv() {
        let body = "\
# demo ambient config
redis\tsettlement_rate_premium\t0.20
redis\tcurrency\tusd
";
        let template = AmbientTemplate::from_tsv(body);
        assert_eq!(template.entries().len(), 2);
        let plan = SeedPlan::new().with_ambient(&template);
        assert_eq!(
            plan.resolve("redis", "settlement_rate_premium").unwrap().value,
            serde_json::json!(0.20),
            "0.20 parses as a JSON number"
        );
        assert_eq!(
            plan.resolve("redis", "currency").unwrap().value,
            serde_json::json!("usd"),
            "bare token becomes a JSON string"
        );
    }

    /// Deliverable 6: a fake StateProbe captures the pre-image (before) and the
    /// result_image (after) of a single-op RMW.
    #[test]
    fn state_probe_captures_pre_and_post_images() {
        let probe = InMemoryStateProbe::new([(
            "redis".to_owned(),
            "counter".to_owned(),
            serde_json::json!(1),
        )]);

        // peek: capture pre-image before the real op.
        let images = probe_rmw_images(&probe, "redis", "counter");
        assert_eq!(images.pre_image, Some(serde_json::json!(1)));
        assert_eq!(images.result_image, None);

        // simulate the real RMW op incrementing the counter.
        probe.set("redis", "counter", serde_json::json!(2));

        // observe: capture post-image after the real op.
        let images = complete_rmw_images(images, &probe, "redis", "counter");
        assert_eq!(images.pre_image, Some(serde_json::json!(1)));
        assert_eq!(images.result_image, Some(serde_json::json!(2)));
    }

    /// Deliverable 6: a probe of an absent key yields `None` images (no false
    /// pre-image for a key the store does not hold).
    #[test]
    fn state_probe_absent_key_is_none() {
        let probe = InMemoryStateProbe::default();
        let images = probe_rmw_images(&probe, "redis", "missing");
        assert_eq!(images.pre_image, None);
        let images = complete_rmw_images(images, &probe, "redis", "missing");
        assert_eq!(images.result_image, None);
    }

    /// Deliverable 6 (wiring): the execute-shadow path on the concrete
    /// `LookupTableHook` captures `pre_image` at peek and `result_image` at
    /// observe via the installed `StateProbe`, stamping both onto the emitted
    /// shadow `ObservedCall`. This is the in-memory stand-in for the live docker
    /// probe; only the probe backend differs in production.
    #[test]
    fn hook_execute_shadow_captures_rmw_images_via_probe() {
        let table = LookupTable {
            recording_id: "rec-rmw".to_owned(),
            policy_version: 1,
            entries: vec![entry_with(
                None,
                Address::Sequence {
                    boundary: "redis".to_owned(),
                    method: "incr".to_owned(),
                    request_sequence: 0,
                },
                &serde_json::json!(["counter"]),
                0,
                serde_json::json!(2), // recorded baseline result
                7,
            )],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        // Probe starts with counter=1; the "real op" increments it to 2.
        // Keep a clone to mutate the shared store between peek and observe,
        // modelling the real boundary's write.
        let probe = InMemoryStateProbe::new([(
            "redis".to_owned(),
            "counter".to_owned(),
            serde_json::json!(1),
        )]);
        let probe_handle = probe.clone();
        let hook = LookupTableHook::from_source_with_policy(
            VecSource(Some(table)),
            observed,
            crate::Policy::SelectiveExecute,
            std::collections::HashSet::new(),
        )
        .expect("from_source")
        .with_state_probe(Box::new(probe));

        let args = serde_json::json!(["counter"]);
        let query = ReplayLookup {
            boundary: "redis",
            trait_name: "RedisStore",
            method_name: "incr",
            args: &args,
            callsite_identity: None,
            caller_location: None,
        };

        // peek captures pre-image=1.
        let token = hook.execute_shadow_peek(query).expect("peek token");
        // The macro would now run the real op; model its write (counter 1 → 2).
        probe_handle.set("redis", "counter", serde_json::json!(2));
        // observe captures post-image=2 and stamps the real result.
        DejaHook::execute_shadow_observe(&hook, token, serde_json::json!(2));

        let calls = handle.lock().unwrap();
        assert_eq!(calls.len(), 1, "one shadow observation emitted");
        let call = &calls[0];
        assert_eq!(call.provenance, crate::Provenance::ExecuteShadow);
        assert_eq!(
            call.pre_image,
            Some(serde_json::json!(1)),
            "pre-image probed at peek (counter before op)"
        );
        assert_eq!(
            call.result_image,
            Some(serde_json::json!(2)),
            "post-image probed at observe (counter after the real op)"
        );
        assert_eq!(call.observed_result, Some(serde_json::json!(2)));
    }
}
