//! Post-hoc divergence detector + scorecard renderer (V1 full mock).
//!
//! Consumes three artifacts produced during a replay run and reconciles the
//! orchestrator's model of what SHOULD have happened (the lookup table, itself
//! rendered from the recording) with what the candidate ACTUALLY did (its
//! `ObservedCall` stream) and how its HTTP responses compared (the kernel's
//! `HttpDiff` stream):
//!
//!   - lookup table   → `HarnessRoot::lookup_table_path(run_id)`
//!   - observed calls → `HarnessRoot::observed_path(run_id)`
//!   - http diffs     → `HarnessRoot::http_diff_path(run_id)`
//!
//! Classification (V1):
//!   - resolved hit                         → matched (recorded per address rank)
//!   - resolved only at rank 6 (sequence)   → Recovered (fragility flag)
//!   - candidate call with no table hit     → NovelCall (blocking)
//!     …on an egress boundary               → EnvironmentalMiss (tolerated)
//!   - table entry the candidate never hit  → OmittedCall (blocking)
//!   - http status / body diffs             → StatusMismatch / BodyMismatch
//!
//! V1 is "full mock": the table is the complete source of truth, containers are
//! empty, and a miss is a divergence — never a legitimate data source. The
//! tiered miss strategy (seeded containers, synthesis, content-addressed
//! fallback) is deferred future work. The
//! `synthesized` / `real_impl_will_fail` fields on `ObservedCall` are the inert
//! scaffold for that work and are always false here.

use std::collections::{BTreeMap, HashSet};
use std::io;

use deja::{Address, LocalFileLookupSource, LookupTable, LookupTableSource, ObservedCall};
use replay_harness_kernel::HttpDiff;
use serde::{Deserialize, Serialize};

use crate::HarnessRoot;

pub mod ledger;
pub use ledger::CallRecord;

/// Boundaries whose live calls cannot run in the harness (egress is blocked).
/// A *novel* call here is an `EnvironmentalMiss`, never a candidate bug.
fn tier_for(boundary: &str) -> Tier {
    match boundary {
        "http_outgoing" | "http_client" | "grpc" => Tier::Environmental,
        "redis" | "db" | "database" | "storage" | "pg" => Tier::Stateful,
        "time" | "id" | "id_generation" | "uuid" | "rng" => Tier::Pure,
        _ => Tier::Unknown,
    }
}

/// A boundary whose recorded-vs-replayed mismatch is NOT a real divergence and so
/// must not block the verdict:
///   - `Tier::Pure` (time/id/rng): an entropy SEAM whose recorded value is
///     substituted on replay, after which everything downstream is pure. These are
///     fully substituted in practice (they never miss), so the non-blocking status
///     is a safety net, not a load-bearing exclusion.
///   - `http_incoming`: the request boundary the kernel re-drives by construction,
///     not a side effect at all.
///
/// NB there is deliberately no `crypto` tier. Crypto is pure computation, not a
/// seam: its only entropy is the AEAD nonce, recorded at its own seam
/// (`common_utils::crypto::NonceSequence::new`), so AES reproduces byte-identically
/// when run live. It carries no boundary and therefore needs no exclusion — see the
/// note on `crypto_operation` in `hyperswitch_domain_models::type_encryption`.
fn is_nonblocking_boundary(boundary: &str) -> bool {
    tier_for(boundary) == Tier::Pure || boundary == "http_incoming"
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier {
    Environmental,
    Stateful,
    Pure,
    Unknown,
}

impl Tier {
    fn label(self) -> &'static str {
        match self {
            Tier::Environmental => "environmental",
            Tier::Stateful => "stateful",
            Tier::Pure => "pure",
            Tier::Unknown => "unknown",
        }
    }
}

fn rank_label(rank: u8) -> String {
    format!("rank_{rank}")
}

/// The weakest, positional `Address` rank (`Address::Sequence`) — a match here
/// means the call resolved only by its boundary+method+request-sequence position,
/// which is fragile to any upstream reorder. Tracked as "Recovered" (a fragility
/// signal), not a divergence. MUST equal `Address::Sequence`'s `rank()`; bump this
/// in lock-step if the rank ladder is renumbered again.
const POSITIONAL_FALLBACK_RANK: u8 = 6;

// ---------------------------------------------------------------------------
// Scorecard data model (`replay-scorecard/v1`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scorecard {
    pub schema_version: u32,
    pub r#type: String,
    pub run_id: String,
    pub recording_id: Option<String>,
    pub summary: Summary,
    pub per_boundary: BTreeMap<String, BoundaryStats>,
    pub per_correlation: Vec<CorrelationOutcome>,
    pub verdict: Verdict,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Summary {
    pub total_correlations: u64,
    pub matched_correlations: u64,
    pub http_status_mismatches: u64,
    pub http_body_mismatches: u64,
    /// Blocking side-effect divergences (Omitted + Novel on non-egress,
    /// correlated boundaries).
    pub side_effect_divergences: u64,
    pub matched_side_effect_calls: u64,
    pub omitted_calls: u64,
    pub novel_calls: u64,
    /// Execute-mode value divergences: the candidate ran the REAL boundary and
    /// produced a result differing in VALUE from the recorded baseline at the
    /// same args-free call-site + occurrence (the total-derivative catch). A
    /// re-keyed write's would-be Omitted+Novel split is collapsed into ONE entry
    /// here. Always 0 under the default AllLookup policy (observed == recorded).
    #[serde(default)]
    pub value_divergences: u64,
    /// Execute-mode calls that could not be conclusively classified because the
    /// recorded baseline to compare against was absent (a seed gap). Surfaced
    /// separately so a missing baseline is neither a false match nor a false
    /// divergence. Always 0 under AllLookup.
    #[serde(default)]
    pub inconclusive_seed_gaps: u64,
    /// Novel calls on egress boundaries — tolerated, surfaced separately so a
    /// blocked outbound integration is never read as a candidate bug.
    pub environmental_misses: u64,
    /// Calls that resolved only at the positional `Sequence` rank (rank 6).
    /// A healthy run resolves almost everything at ranks 1–5;
    /// heavy positional reliance is fragile. (The `rank5` field name is
    /// legacy, from before `Sequence` was renumbered to 6 — kept so the
    /// serialized scorecard shape stays stable; see `POSITIONAL_FALLBACK_RANK`.)
    pub recovered_rank5_calls: u64,
    /// Histogram of resolved calls by address rank — the fragility metric.
    pub resolved_by_rank: BTreeMap<String, u64>,
    pub uncorrelated_events_seen: u64,
    pub uncorrelated_events_tolerated: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BoundaryStats {
    pub matched: u64,
    pub diverged: u64,
    pub kinds: BTreeMap<String, u64>,
    pub resolved_by_rank: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl BoundaryStats {
    /// Record a divergence of `kind` (also bumps `diverged`).
    fn bump_kind(&mut self, kind: &str) {
        *self.kinds.entry(kind.to_owned()).or_insert(0) += 1;
        self.diverged += 1;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationOutcome {
    pub correlation_id: String,
    pub http_status_match: bool,
    pub http_body_match: bool,
    pub side_effect_divergences: u64,
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub pass: bool,
    /// True when there is nothing to judge yet (no artifacts ingested) or a
    /// structurally-required artifact is missing — distinct from a real fail.
    pub inconclusive: bool,
    pub reason: String,
}

impl Scorecard {
    /// An empty, not-yet-judged scorecard. Retained for callers that want a
    /// well-typed placeholder before a run has produced artifacts.
    pub fn empty(run_id: String) -> Self {
        Self {
            schema_version: 1,
            r#type: "replay-scorecard".to_owned(),
            run_id,
            recording_id: None,
            summary: Summary {
                uncorrelated_events_tolerated: true,
                ..Summary::default()
            },
            per_boundary: BTreeMap::new(),
            per_correlation: Vec::new(),
            verdict: Verdict {
                pass: false,
                inconclusive: true,
                reason: "run not yet completed".to_owned(),
            },
            warnings: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Detection
// ---------------------------------------------------------------------------

/// The three artifact streams a run produces, loaded into memory.
pub struct RunArtifacts {
    pub run_id: String,
    pub recording_id: Option<String>,
    pub table: LookupTable,
    pub observed: Vec<ObservedCall>,
    pub http_diffs: Vec<HttpDiff>,
    pub warnings: Vec<String>,
}

/// Get-or-create a boundary's stats, stamping its tier (and an egress note) the
/// first time it is seen.
/// Whether a boundary tag is the database channel (which assigns serial PKs).
fn is_db_boundary(boundary: &str) -> bool {
    matches!(boundary, "db" | "storage")
}

/// Two db results are equivalent MODULO DB-assigned serial primary keys.
///
/// A row's integer `id` is a postgres SERIAL that the replay DB assigns from its
/// OWN fresh sequence, so it cannot match the record-side sequence — it is
/// genuinely non-reconstructable DB infrastructure, NOT application logic and NOT
/// a capturable draw (unlike a crypto nonce, which we instrument). Normalize both
/// sides by dropping integer `id` fields, then compare. App-set ids
/// (`payment_id`, uuids) are strings, not integers, so they stay compared and a
/// real value divergence is still caught. The HTTP byte-exact gate remains the
/// authoritative regression signal; this only stops a serial PK from
/// manufacturing a false side-effect divergence.
fn db_equiv_modulo_serial(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    fn strip_serial(v: &serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(m) => serde_json::Value::Object(
                m.iter()
                    .filter(|(k, val)| !(k.as_str() == "id" && (val.is_i64() || val.is_u64())))
                    .map(|(k, val)| (k.clone(), strip_serial(val)))
                    .collect(),
            ),
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(strip_serial).collect())
            }
            other => other.clone(),
        }
    }
    strip_serial(a) == strip_serial(b)
}

fn boundary_entry<'a>(
    map: &'a mut BTreeMap<String, BoundaryStats>,
    boundary: &str,
) -> &'a mut BoundaryStats {
    let stats = map.entry(boundary.to_owned()).or_default();
    if stats.tier.is_none() {
        let tier = tier_for(boundary);
        stats.tier = Some(tier.label().to_owned());
        if tier == Tier::Environmental {
            stats.note = Some(
                "egress blocked; novel calls are environmental misses, not candidate bugs"
                    .to_owned(),
            );
        }
    }
    stats
}

/// Reconcile the artifact streams into a `replay-scorecard/v1`.
pub fn detect(art: &RunArtifacts) -> Scorecard {
    // V1: uncorrelated (background-task) events are tolerated; the deja-tokio
    // correlation-propagation fix is a separate plan.
    let uncorrelated_tolerated = true;

    let mut per_boundary: BTreeMap<String, BoundaryStats> = BTreeMap::new();

    // --- expected side-effect calls, deduped by source event -----------------
    // Each recorded event yields up to one entry per address rank; we collapse
    // them by `source_event_global_sequence`. The boundary AND method live on the
    // rank-6 `Sequence` address, which every event always emits. We also carry the
    // recorded `result` here — the recorded operand the args-free pairing compares
    // an execute-shadow `observed_result` against to classify ValueDiverged.
    struct Expected {
        boundary: Option<String>,
        method: Option<String>,
        correlation: Option<String>,
        result: serde_json::Value,
    }
    let mut expected: BTreeMap<u64, Expected> = BTreeMap::new();
    for entry in &art.table.entries {
        let slot = expected
            .entry(entry.source_event_global_sequence)
            .or_insert(Expected {
                boundary: None,
                method: None,
                correlation: entry.key.correlation_id.clone(),
                result: entry.result.clone(),
            });
        if let Address::Sequence {
            boundary, method, ..
        } = &entry.key.address
        {
            slot.boundary = Some(boundary.clone());
            slot.method = Some(method.clone());
        }
    }
    let uncorrelated_events_seen = expected
        .values()
        .filter(|e| e.correlation.is_none())
        .count() as u64;

    // --- args-free pairing for execute-mode value divergence -----------------
    // GOTCHA #1: a diverged WRITE carries a mutated operand (e.g. a doubled
    // amount), so its `args_hash` no longer matches the recorded baseline. Under
    // the strict-args lookup path that miss splits the SAME logical write into a
    // recorded OmittedCall + an execute NovelCall. To recover the single truth —
    // ONE ValueDiverged — we pair the unresolved observed calls to the unconsumed
    // expected events by ARGS-FREE call-site identity (`correlation, boundary,
    // method`) + occurrence (the Nth such call in stream / source order). args_hash
    // is the DIFF signal here, never the resolution key.
    //
    // NO-REGRESSION: this pairing only reaches calls that did NOT resolve normally.
    // Under the default AllLookup policy every recorded call resolves via args_hash
    // (observed_result == recorded_result, substituted), so it never enters this
    // path and ValueDiverged stays inert.

    // Recorded side: unconsumed expected events grouped by args-free identity,
    // ordered by source sequence, occurrence = position within the group.
    type Identity = (Option<String>, String, String);
    let identity_of = |corr: &Option<String>, boundary: &str, method: &str| -> Identity {
        (corr.clone(), boundary.to_owned(), method.to_owned())
    };
    // (identity -> queue of (source_seq, recorded_result)); FIFO by source order.
    let mut recorded_pairing: BTreeMap<Identity, std::collections::VecDeque<(u64, serde_json::Value)>> =
        BTreeMap::new();
    for (seq, exp) in &expected {
        // Only events that carry a concrete boundary+method (every event does, via
        // the rank-6 Sequence address) are pair-able; uncorrelated/tolerated events
        // still queue but are filtered out when we decide to emit (see below).
        let (Some(boundary), Some(method)) = (&exp.boundary, &exp.method) else {
            continue;
        };
        recorded_pairing
            .entry(identity_of(&exp.correlation, boundary, method))
            .or_default()
            .push_back((*seq, exp.result.clone()));
    }

    let mut value_divergences = 0u64;
    let mut inconclusive_seed_gaps = 0u64;
    // Expected events claimed by a ValueDiverged pairing: counted as the
    // divergence, NOT as an OmittedCall in the omitted pass below.
    let mut paired_consumed: HashSet<u64> = HashSet::new();

    // --- observed calls: matched (+ recovered) and novel ---------------------
    let mut consumed: HashSet<u64> = HashSet::new();
    let mut resolved_by_rank: BTreeMap<String, u64> = BTreeMap::new();
    let mut matched_side_effect_calls = 0u64;
    let mut recovered_rank5_calls = 0u64;
    let mut novel_calls = 0u64;
    let mut environmental_misses = 0u64;
    let mut blocking_side_effect = 0u64;
    let mut corr_side_effect: BTreeMap<String, u64> = BTreeMap::new();

    for obs in &art.observed {
        let stats = boundary_entry(&mut per_boundary, &obs.boundary);
        if obs.resolved {
            // The recorded baseline was found (args still aligned). Under lookup
            // mode observed_result == recorded_result (substituted) so this is a
            // plain match. Under execute mode the recorded baseline was located by
            // args-aligned occurrence but the REAL boundary ran: if its
            // observed_result differs from the recorded baseline this is a
            // ValueDiverged (the args-aligned flavor — a READ, or a WRITE whose
            // operand did not change). The re-keyed WRITE whose operand DID change
            // misses args and is paired args-free in the Novel branch below.
            let diverged = obs.provenance == deja::Provenance::ExecuteShadow
                && match (&obs.observed_result, &obs.recorded_result) {
                    (Some(o), Some(r)) => {
                        // A db row that differs ONLY in its serial PK is not a
                        // real divergence (replay assigns the id from its own
                        // sequence — non-reconstructable DB infra).
                        o != r && !(is_db_boundary(&obs.boundary) && db_equiv_modulo_serial(o, r))
                    }
                    _ => false,
                };
            if diverged {
                // The args-aligned execute divergence is the ORIGIN of a
                // total-derivative cascade: the candidate ran the REAL boundary
                // (typically a READ) and got a value differing from the recorded
                // baseline (e.g. re-keyed read 0.10 -> 0.20). Tag it distinctly
                // (`ValueDivergedOrigin`) so the UI can tell the CAUSE (this read)
                // from the CONSEQUENCE (a downstream write paired args-free below).
                stats.bump_kind("ValueDivergedOrigin");
                value_divergences += 1;
                blocking_side_effect += 1;
                if let Some(corr) = &obs.correlation_id {
                    *corr_side_effect.entry(corr.clone()).or_insert(0) += 1;
                }
                if let Some(seq) = obs.source_event_global_sequence {
                    // Claim the recorded twin so the omitted pass does not also
                    // flag it; this is one logical write, classified once.
                    consumed.insert(seq);
                }
                continue;
            }
            stats.matched += 1;
            matched_side_effect_calls += 1;
            if let Some(seq) = obs.source_event_global_sequence {
                consumed.insert(seq);
            }
            let rank = obs.resolved_rank.unwrap_or(0);
            *resolved_by_rank.entry(rank_label(rank)).or_insert(0) += 1;
            *stats.resolved_by_rank.entry(rank_label(rank)).or_insert(0) += 1;
            if rank == POSITIONAL_FALLBACK_RANK {
                // The `rank5` field name is legacy (pre-renumber); it counts
                // positional (rank-6 `Sequence`) matches. Kept so persisted
                // scorecard JSON keeps one stable shape across runs.
                recovered_rank5_calls += 1;
                // Recovered is a fragility signal, not a divergence — track it
                // without bumping `diverged`.
                *stats.kinds.entry("Recovered".to_owned()).or_insert(0) += 1;
            }
        } else if tier_for(&obs.boundary) == Tier::Environmental {
            stats.bump_kind("EnvironmentalMiss");
            environmental_misses += 1;
        } else if is_nonblocking_boundary(&obs.boundary) {
            // Deterministic-live (crypto/time/id/rng) or the request boundary
            // (http_incoming) — not a real divergence. See is_nonblocking_boundary.
            stats.bump_kind("DeterministicMiss");
        } else if obs.correlation_id.is_none() && uncorrelated_tolerated {
            // Background-task call with no correlation — tolerated in V1.
            stats.bump_kind("NovelCall");
        } else if let Some((twin_seq, recorded)) = recorded_pairing
            .get_mut(&identity_of(
                &obs.correlation_id,
                &obs.boundary,
                &obs.method_name,
            ))
            .and_then(|q| {
                // Pop the next recorded twin for this identity, skipping any that a
                // resolved (args-aligned) call already claimed — so a mixed run that
                // resolves some calls normally and re-keys others never double-binds
                // a single recorded event.
                while let Some((seq, _)) = q.front() {
                    if consumed.contains(seq) {
                        q.pop_front();
                    } else {
                        return q.pop_front();
                    }
                }
                None
            })
        {
            // GOTCHA #1 resolution: this unresolved observed call pairs args-free
            // (correlation+boundary+method, FIFO occurrence) with a recorded twin
            // that the candidate "omitted" because its args were re-keyed. The
            // recorded WRITE (would-be Omitted) and the execute WRITE (would-be
            // Novel) are ONE logical write — classify it once.
            let observed_val = obs.observed_result.clone().unwrap_or(serde_json::Value::Null);
            // Serial-PK-only db diffs are non-reconstructable infra, not a catch.
            let serial_only =
                is_db_boundary(&obs.boundary) && db_equiv_modulo_serial(&observed_val, &recorded);
            if observed_val != recorded && !serial_only {
                // Value diff under execute mode: the total-derivative catch.
                stats.bump_kind("ValueDiverged");
                value_divergences += 1;
                blocking_side_effect += 1;
                if let Some(corr) = &obs.correlation_id {
                    *corr_side_effect.entry(corr.clone()).or_insert(0) += 1;
                }
            } else {
                // Re-keyed but identical value — the write reproduced. Count it as
                // a (recovered) match rather than a Novel+Omitted split.
                stats.matched += 1;
                matched_side_effect_calls += 1;
            }
            // Either way the recorded twin is accounted for here, not omitted.
            paired_consumed.insert(twin_seq);
        } else if obs.seed_gap {
            // Execute-mode State call that ran the REAL boundary but found no
            // recorded baseline to compare against (no pairing either). Surface as
            // inconclusive rather than a false Novel — see InconclusiveSeedGap.
            stats.bump_kind("InconclusiveSeedGap");
            inconclusive_seed_gaps += 1;
        } else {
            stats.bump_kind("NovelCall");
            novel_calls += 1;
            blocking_side_effect += 1;
            if let Some(corr) = &obs.correlation_id {
                *corr_side_effect.entry(corr.clone()).or_insert(0) += 1;
            }
        }
    }

    // --- omitted calls: expected events the candidate never resolved ---------
    // `paired_consumed` are recorded twins already classified as ValueDiverged
    // (their execute-mode counterpart was paired args-free above); excluding them
    // here is what collapses a re-keyed write's Omitted+Novel split into ONE
    // ValueDiverged instead of double-counting.
    let mut omitted_calls = 0u64;
    for (seq, exp) in &expected {
        if consumed.contains(seq) || paired_consumed.contains(seq) {
            continue;
        }
        let boundary = exp.boundary.clone().unwrap_or_else(|| "unknown".to_owned());
        let stats = boundary_entry(&mut per_boundary, &boundary);
        stats.bump_kind("OmittedCall");
        if exp.correlation.is_none() && uncorrelated_tolerated {
            // tolerated
        } else if is_nonblocking_boundary(&boundary) {
            // tolerated: deterministic-live (crypto/time/id/rng) or the request
            // boundary (http_incoming). See is_nonblocking_boundary.
        } else {
            omitted_calls += 1;
            blocking_side_effect += 1;
            if let Some(corr) = &exp.correlation {
                *corr_side_effect.entry(corr.clone()).or_insert(0) += 1;
            }
        }
    }

    // --- HTTP response dimension (from the kernel) ---------------------------
    let mut http_status_mismatches = 0u64;
    let mut http_body_mismatches = 0u64;
    let mut corr_http: BTreeMap<String, (bool, bool)> = BTreeMap::new();
    {
        let stats = boundary_entry(&mut per_boundary, "http_incoming");
        for diff in &art.http_diffs {
            if diff.status_match && diff.body_diff.is_empty() {
                stats.matched += 1;
            }
            if !diff.status_match {
                http_status_mismatches += 1;
                stats.bump_kind("StatusMismatch");
            }
            if !diff.body_diff.is_empty() {
                http_body_mismatches += 1;
                for _ in &diff.body_diff {
                    stats.bump_kind("BodyMismatch");
                }
            }
            let slot = corr_http
                .entry(diff.correlation_id.clone())
                .or_insert((true, true));
            slot.0 &= diff.status_match;
            slot.1 &= diff.body_diff.is_empty();
        }
    }

    // --- per-correlation outcomes --------------------------------------------
    let mut per_correlation = Vec::new();
    let mut matched_correlations = 0u64;
    for (corr, (status_match, body_match)) in &corr_http {
        let side_effect_divergences = corr_side_effect.get(corr).copied().unwrap_or(0);
        let passed = *status_match && *body_match && side_effect_divergences == 0;
        if passed {
            matched_correlations += 1;
        }
        per_correlation.push(CorrelationOutcome {
            correlation_id: corr.clone(),
            http_status_match: *status_match,
            http_body_match: *body_match,
            side_effect_divergences,
            passed,
        });
    }
    let total_correlations = per_correlation.len() as u64;

    // --- verdict --------------------------------------------------------------
    let nothing =
        art.table.entries.is_empty() && art.observed.is_empty() && art.http_diffs.is_empty();
    let mut reasons = Vec::new();
    if http_status_mismatches > 0 {
        reasons.push(format!("{http_status_mismatches} http status mismatch(es)"));
    }
    if http_body_mismatches > 0 {
        reasons.push(format!("{http_body_mismatches} http body mismatch(es)"));
    }
    if omitted_calls > 0 {
        reasons.push(format!("{omitted_calls} omitted side-effect call(s)"));
    }
    if novel_calls > 0 {
        reasons.push(format!("{novel_calls} novel side-effect call(s)"));
    }
    if value_divergences > 0 {
        // The total-derivative catch: a real-boundary value diff flips the
        // correlation to diverged (per-correlation `passed` already saw it via
        // `corr_side_effect`).
        reasons.push(format!("{value_divergences} value divergence(s)"));
    }
    // Seed gaps are reported but do NOT by themselves fail the verdict — a
    // missing baseline is inconclusive, not a divergence.
    if inconclusive_seed_gaps > 0 {
        reasons.push(format!(
            "{inconclusive_seed_gaps} inconclusive seed gap(s) (non-blocking)"
        ));
    }
    // The seed-gap line is informational, not a divergence; exclude it from the
    // blocking count so a run whose only "reason" is a seed gap still passes
    // (while the line stays visible in the verdict text).
    let blocking_reasons = reasons.len() - usize::from(inconclusive_seed_gaps > 0);
    let inconclusive = nothing;
    let pass = !inconclusive && blocking_reasons == 0;
    let reason = if inconclusive {
        "no artifacts ingested for this run yet".to_owned()
    } else if pass && reasons.is_empty() {
        "full-mock replay clean: http responses match and every side-effect call resolved"
            .to_owned()
    } else {
        reasons.join("; ")
    };

    Scorecard {
        schema_version: 1,
        r#type: "replay-scorecard".to_owned(),
        run_id: art.run_id.clone(),
        recording_id: art.recording_id.clone(),
        summary: Summary {
            total_correlations,
            matched_correlations,
            http_status_mismatches,
            http_body_mismatches,
            side_effect_divergences: blocking_side_effect,
            matched_side_effect_calls,
            omitted_calls,
            novel_calls,
            value_divergences,
            inconclusive_seed_gaps,
            environmental_misses,
            recovered_rank5_calls,
            resolved_by_rank,
            uncorrelated_events_seen,
            uncorrelated_events_tolerated: uncorrelated_tolerated,
        },
        per_boundary,
        per_correlation,
        verdict: Verdict {
            pass,
            inconclusive,
            reason,
        },
        warnings: art.warnings.clone(),
    }
}

// ---------------------------------------------------------------------------
// Loading + scoring
// ---------------------------------------------------------------------------

/// Load a run's three artifact streams off disk. Missing files are treated as
/// empty (a run mid-flight); parse failures are surfaced as `warnings` rather
/// than silently dropped, so a corrupt stream can't masquerade as a clean run.
pub fn load_artifacts(root: &HarnessRoot, run_id: &str) -> io::Result<RunArtifacts> {
    let recording_id = crate::read_json::<crate::Run>(&root.run_path(run_id))
        .ok()
        .and_then(|run| run.recording_id.or(run.spec.recording_id));

    let mut warnings = Vec::new();
    let table = load_table(&root.lookup_table_path(run_id), &mut warnings);
    let observed = load_jsonl::<ObservedCall>(&root.observed_path(run_id), &mut warnings);
    let http_diffs = load_jsonl::<HttpDiff>(&root.http_diff_path(run_id), &mut warnings);

    Ok(RunArtifacts {
        run_id: run_id.to_owned(),
        recording_id,
        table,
        observed,
        http_diffs,
        warnings,
    })
}

/// Load + detect (read-through). Used by `GET /runs/{id}/scorecard`.
pub fn scorecard(root: &HarnessRoot, run_id: &str) -> io::Result<Scorecard> {
    let art = load_artifacts(root, run_id)?;
    Ok(detect(&art))
}

/// Compute the scorecard and persist it next to the run record. Called by the
/// lifecycle worker when a run completes. Also builds + persists the per-call
/// ledger sidecar (best-effort — a ledger failure never fails scoring).
pub fn detect_and_score(root: &HarnessRoot, run_id: &str) -> io::Result<Scorecard> {
    let art = load_artifacts(root, run_id)?;
    let card = detect(&art);
    let path = root
        .root
        .join("runs")
        .join(format!("{run_id}.scorecard.json"));
    crate::write_json(&path, &card)?;

    // Ledger: the per-call detail the scorecard summary drops. Best-effort.
    match build_ledger(root, &art) {
        Ok(rows) => {
            if let Err(e) = write_ledger(&root.call_ledger_path(run_id), &rows) {
                eprintln!("divergence: ledger write failed for {run_id}: {e}");
            }
        }
        Err(e) => eprintln!("divergence: ledger build failed for {run_id}: {e}"),
    }
    Ok(card)
}

/// Build the per-call ledger for a run: join the recording's events (recorded
/// side) to the candidate's observed calls, classified like `detect()`.
pub fn build_ledger(root: &HarnessRoot, art: &RunArtifacts) -> io::Result<Vec<CallRecord>> {
    let events = match &art.recording_id {
        Some(rec) => {
            let mut warnings = Vec::new();
            load_jsonl::<deja::SemanticEvent>(&root.recording_events_path(rec), &mut warnings)
        }
        None => Vec::new(),
    };
    let expected = ledger::expected_sequences(&art.table);
    let span_paths = ledger::recorded_span_paths(&art.table);
    Ok(ledger::build(
        &events,
        &art.observed,
        &expected,
        &span_paths,
    ))
}

/// Read-through ledger for `GET /runs/{id}/calls` (recomputes from artifacts;
/// works for runs scored before the sidecar existed).
pub fn call_ledger(root: &HarnessRoot, run_id: &str) -> io::Result<Vec<CallRecord>> {
    let art = load_artifacts(root, run_id)?;
    build_ledger(root, &art)
}

fn write_ledger(path: &std::path::Path, rows: &[CallRecord]) -> io::Result<()> {
    use std::io::Write as _;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut out = std::io::BufWriter::new(std::fs::File::create(path)?);
    for row in rows {
        let line = serde_json::to_vec(row).map_err(io::Error::other)?;
        out.write_all(&line)?;
        out.write_all(b"\n")?;
    }
    out.flush()
}

fn load_table(path: &std::path::Path, warnings: &mut Vec<String>) -> LookupTable {
    let empty = || LookupTable {
        recording_id: String::new(),
        policy_version: 0,
        entries: Vec::new(),
    };
    if !path.exists() {
        return empty();
    }
    let mut source = LocalFileLookupSource::new(path);
    match source.load() {
        Ok(table) => table,
        Err(e) => {
            warnings.push(format!(
                "lookup-table load failed ({}): {e}",
                path.display()
            ));
            empty()
        }
    }
}

fn load_jsonl<T: for<'de> Deserialize<'de>>(
    path: &std::path::Path,
    warnings: &mut Vec<String>,
) -> Vec<T> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            warnings.push(format!("read {} failed: {e}", path.display()));
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for (i, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<T>(line) {
            Ok(value) => out.push(value),
            Err(e) => warnings.push(format!("{}:{}: parse error: {e}", path.display(), i + 1)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use deja::{LookupEntry, LookupKey};
    use replay_harness_kernel::JsonFieldDiff;

    #[test]
    fn db_serial_pk_only_diff_is_not_a_divergence() {
        // A db insert that differs ONLY in its integer serial id is equivalent
        // (the replay DB assigned id=1 from its fresh sequence; record saw id=2).
        let rec = serde_json::json!({"result":"Ok","type_name":"UserRole",
            "value":{"id":2,"user_id":"u-abc","role_id":"org_admin","status":"Active"}});
        let obs = serde_json::json!({"result":"Ok","type_name":"UserRole",
            "value":{"id":1,"user_id":"u-abc","role_id":"org_admin","status":"Active"}});
        assert!(db_equiv_modulo_serial(&rec, &obs), "serial-id-only diff must be equivalent");

        // A diff in a REAL field (string id, or any value) is a genuine divergence.
        let obs_real = serde_json::json!({"result":"Ok","type_name":"UserRole",
            "value":{"id":1,"user_id":"u-DIFFERENT","role_id":"org_admin","status":"Active"}});
        assert!(!db_equiv_modulo_serial(&rec, &obs_real), "a real field diff must NOT be masked");

        // An app-set STRING id is not an integer → stays compared.
        let s1 = serde_json::json!({"value":{"id":"pay_aaa"}});
        let s2 = serde_json::json!({"value":{"id":"pay_bbb"}});
        assert!(!db_equiv_modulo_serial(&s1, &s2), "string ids are app-set, not serial → compared");

        // Identical rows are trivially equivalent; redis (non-db) is unaffected here.
        assert!(db_equiv_modulo_serial(&rec, &rec));
    }

    fn obs(
        boundary: &str,
        corr: Option<&str>,
        resolved: bool,
        rank: Option<u8>,
        src: Option<u64>,
    ) -> ObservedCall {
        ObservedCall {
            correlation_id: corr.map(str::to_owned),
            boundary: boundary.to_owned(),
            trait_name: "T".to_owned(),
            method_name: "m".to_owned(),
            args: serde_json::json!({}),
            resolved,
            resolved_rank: rank,
            source_event_global_sequence: src,
            call_file: None,
            call_line: None,
            call_column: None,
            logical_span_path: None,
            graph_node_id: None,
            synthesized: false,
            real_impl_will_fail: false,
            recorded_result: None,
            observed_result: None,
            provenance: deja::Provenance::default(),
            seed_gap: false,
            pre_image: None,
            result_image: None,
        }
    }

    fn seq_entry(corr: Option<&str>, boundary: &str, src: u64) -> LookupEntry {
        seq_entry_res(corr, boundary, src, serde_json::json!("v"))
    }

    /// A rank-6 `Sequence` entry with an explicit recorded `result` — lets a test
    /// set the recorded operand the args-free value pairing compares against.
    fn seq_entry_res(
        corr: Option<&str>,
        boundary: &str,
        src: u64,
        result: serde_json::Value,
    ) -> LookupEntry {
        LookupEntry {
            key: LookupKey {
                correlation_id: corr.map(str::to_owned),
                address: Address::Sequence {
                    boundary: boundary.to_owned(),
                    method: "m".to_owned(),
                    request_sequence: 0,
                },
                args_hash: 0,
                occurrence: 0,
            },
            result,
            source_event_global_sequence: src,
        }
    }

    /// An execute-shadow observed call: the candidate ran the REAL boundary
    /// (`provenance = ExecuteShadow`) and produced `observed`. `recorded` is the
    /// baseline the hook located (or `None` => `seed_gap`), `resolved` reflects
    /// whether args still aligned to that baseline.
    fn exec_obs(
        boundary: &str,
        corr: Option<&str>,
        resolved: bool,
        src: Option<u64>,
        recorded: Option<serde_json::Value>,
        observed: serde_json::Value,
    ) -> ObservedCall {
        let mut o = obs(boundary, corr, resolved, resolved.then_some(3), src);
        o.provenance = deja::Provenance::ExecuteShadow;
        o.seed_gap = recorded.is_none();
        o.recorded_result = recorded;
        o.observed_result = Some(observed);
        o
    }

    fn http(corr: &str, status_match: bool, body: Vec<JsonFieldDiff>) -> HttpDiff {
        HttpDiff {
            correlation_id: corr.to_owned(),
            request_sequence: 0,
            request_path: "/p".to_owned(),
            status_baseline: 200,
            status_candidate: if status_match { 200 } else { 500 },
            status_match,
            body_diff: body,
            baseline_body: None,
            candidate_body: None,
        }
    }

    fn art(
        entries: Vec<LookupEntry>,
        observed: Vec<ObservedCall>,
        http: Vec<HttpDiff>,
    ) -> RunArtifacts {
        RunArtifacts {
            run_id: "run-1".to_owned(),
            recording_id: Some("rec-1".to_owned()),
            table: LookupTable {
                recording_id: "rec-1".to_owned(),
                policy_version: 1,
                entries,
            },
            observed,
            http_diffs: http,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn clean_self_replay_passes() {
        let card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 7)],
            vec![obs("redis", Some("c1"), true, Some(3), Some(7))],
            vec![http("c1", true, vec![])],
        ));
        assert!(card.verdict.pass, "{}", card.verdict.reason);
        assert_eq!(card.summary.omitted_calls, 0);
        assert_eq!(card.summary.novel_calls, 0);
        assert_eq!(card.summary.matched_correlations, 1);
        assert_eq!(card.summary.resolved_by_rank.get("rank_3"), Some(&1));
    }

    #[test]
    fn omitted_call_fails() {
        let card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 7)],
            vec![],
            vec![http("c1", true, vec![])],
        ));
        assert!(!card.verdict.pass);
        assert_eq!(card.summary.omitted_calls, 1);
        assert_eq!(card.summary.matched_correlations, 0);
        assert_eq!(
            card.per_boundary["redis"].kinds.get("OmittedCall"),
            Some(&1)
        );
    }

    #[test]
    fn novel_call_fails() {
        let card = detect(&art(
            vec![],
            vec![obs("redis", Some("c1"), false, None, None)],
            vec![],
        ));
        assert!(!card.verdict.pass);
        assert_eq!(card.summary.novel_calls, 1);
    }

    #[test]
    fn novel_egress_call_is_tolerated() {
        let card = detect(&art(
            vec![],
            vec![obs("http_outgoing", Some("c1"), false, None, None)],
            vec![http("c1", true, vec![])],
        ));
        assert!(card.verdict.pass, "{}", card.verdict.reason);
        assert_eq!(card.summary.environmental_misses, 1);
        assert_eq!(card.summary.novel_calls, 0);
        assert_eq!(
            card.per_boundary["http_outgoing"].tier.as_deref(),
            Some("environmental")
        );
    }

    #[test]
    fn http_body_mismatch_fails() {
        let card = detect(&art(
            vec![],
            vec![],
            vec![http(
                "c1",
                true,
                vec![JsonFieldDiff {
                    json_path: "$.amount".to_owned(),
                    baseline: serde_json::json!(100),
                    candidate: serde_json::json!(200),
                }],
            )],
        ));
        assert!(!card.verdict.pass);
        assert_eq!(card.summary.http_body_mismatches, 1);
    }

    #[test]
    fn positional_rank6_resolution_flagged_recovered_but_passes() {
        // A match at the weakest positional rank (Sequence == rank 6 after the P3
        // renumber) is a fragility signal, tracked as "Recovered", not a divergence.
        let card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 7)],
            vec![obs("redis", Some("c1"), true, Some(6), Some(7))],
            vec![http("c1", true, vec![])],
        ));
        assert!(card.verdict.pass, "{}", card.verdict.reason);
        // Field name kept for dashboard stability; now counts rank-6 positional hits.
        assert_eq!(card.summary.recovered_rank5_calls, 1);
        assert_eq!(card.summary.resolved_by_rank.get("rank_6"), Some(&1));
    }

    #[test]
    fn empty_run_is_inconclusive_not_pass() {
        let card = detect(&art(vec![], vec![], vec![]));
        assert!(!card.verdict.pass);
        assert!(card.verdict.inconclusive);
    }

    #[test]
    fn uncorrelated_omitted_is_tolerated() {
        // A background-task (null-correlation) recorded event the candidate
        // didn't reproduce is counted but does not block.
        let card = detect(&art(vec![seq_entry(None, "redis", 7)], vec![], vec![]));
        assert_eq!(card.summary.uncorrelated_events_seen, 1);
        assert_eq!(
            card.summary.omitted_calls, 0,
            "uncorrelated omission not blocking"
        );
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    // --- M1: ValueDiverged + args-free pairing -------------------------------

    #[test]
    fn rekeyed_write_pairs_args_free_into_one_value_divergence() {
        // GOTCHA #1: the diverged WRITE carries a mutated operand, so its args
        // miss the recorded baseline → recorded twin would be Omitted, the execute
        // call would be Novel. The args-free pairing must collapse them into ONE
        // ValueDiverged (NOT Novel+Omitted), and flip the correlation to diverged.
        let card = detect(&art(
            vec![seq_entry_res(Some("c1"), "storage", 7, serde_json::json!(100))],
            vec![exec_obs(
                "storage",
                Some("c1"),
                false, // re-keyed args missed the baseline → unresolved
                None,  // no source_event_global_sequence (it didn't resolve)
                None,  // hook found no args-aligned baseline (seed_gap on hook side)
                serde_json::json!(200), // the doubled amount
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 1, "one value divergence");
        assert_eq!(card.summary.novel_calls, 0, "not a Novel");
        assert_eq!(card.summary.omitted_calls, 0, "not an Omitted");
        assert_eq!(
            card.per_boundary["storage"].kinds.get("ValueDiverged"),
            Some(&1)
        );
        assert!(!card.verdict.pass, "value divergence flips the verdict");
        assert!(
            card.verdict.reason.contains("value divergence"),
            "{}",
            card.verdict.reason
        );
        // The correlation outcome must show the divergence.
        let c1 = card
            .per_correlation
            .iter()
            .find(|c| c.correlation_id == "c1")
            .unwrap();
        assert!(!c1.passed);
        assert_eq!(c1.side_effect_divergences, 1);
    }

    #[test]
    fn args_aligned_execute_value_diff_is_value_diverged() {
        // Execute mode where args STILL align (a READ, or a write whose operand
        // did not change): the baseline resolves (resolved=true) but the REAL
        // boundary's observed_result differs → ValueDiverged via the resolved arm.
        let card = detect(&art(
            vec![seq_entry_res(Some("c1"), "storage", 7, serde_json::json!("old"))],
            vec![exec_obs(
                "storage",
                Some("c1"),
                true,    // args aligned → baseline resolved
                Some(7), // consumed the recorded twin
                Some(serde_json::json!("old")),
                serde_json::json!("new"), // real boundary diverged in value
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 1);
        assert_eq!(card.summary.matched_side_effect_calls, 0);
        assert_eq!(card.summary.omitted_calls, 0, "twin consumed, not omitted");
        assert!(!card.verdict.pass);
    }

    #[test]
    fn execute_value_match_is_matched_not_diverged() {
        // Execute mode, real boundary reproduced the recorded value exactly:
        // inert — a plain match, the no-regression backbone of the policy.
        let card = detect(&art(
            vec![seq_entry_res(Some("c1"), "storage", 7, serde_json::json!("same"))],
            vec![exec_obs(
                "storage",
                Some("c1"),
                true,
                Some(7),
                Some(serde_json::json!("same")),
                serde_json::json!("same"),
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.matched_side_effect_calls, 1);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    #[test]
    fn execute_seed_gap_is_inconclusive_not_blocking() {
        // Execute-mode State call ran the real boundary but found NO recorded
        // baseline AND no args-free twin to pair with → InconclusiveSeedGap, which
        // is reported but does NOT fail the verdict.
        let card = detect(&art(
            vec![], // nothing recorded → no twin to pair
            vec![exec_obs(
                "storage",
                Some("c1"),
                false,
                None,
                None, // seed gap
                serde_json::json!("fresh"),
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.inconclusive_seed_gaps, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.novel_calls, 0, "seed gap is not a Novel");
        assert!(
            card.verdict.pass,
            "seed gap is non-blocking: {}",
            card.verdict.reason
        );
        assert!(card.verdict.reason.contains("seed gap"));
    }

    #[test]
    fn lookup_mode_observed_equals_recorded_keeps_value_diverged_inert() {
        // NO-REGRESSION: under the default AllLookup policy every recorded call
        // resolves and observed_result == recorded_result (substituted), so the
        // ValueDiverged classifier stays inert — byte-identical to pre-M1.
        let card = detect(&art(
            vec![seq_entry_res(Some("c1"), "redis", 7, serde_json::json!("v"))],
            vec![exec_obs(
                "redis",
                Some("c1"),
                true,
                Some(7),
                Some(serde_json::json!("v")),
                serde_json::json!("v"), // lookup: observed == recorded
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.matched_side_effect_calls, 1);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    #[test]
    fn rekeyed_write_with_same_value_is_recovered_match_not_split() {
        // A re-keyed call (args missed) whose VALUE nonetheless reproduced is
        // paired args-free and counted as a match — never a Novel+Omitted split.
        let card = detect(&art(
            vec![seq_entry_res(Some("c1"), "storage", 7, serde_json::json!("v"))],
            vec![exec_obs(
                "storage",
                Some("c1"),
                false,
                None,
                None,
                serde_json::json!("v"),
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.novel_calls, 0);
        assert_eq!(card.summary.omitted_calls, 0);
        assert_eq!(card.summary.matched_side_effect_calls, 1);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }
}
