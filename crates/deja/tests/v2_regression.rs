// Test loops count request_sequence explicitly to mirror the recorder's own
// sequencing logic.
#![allow(clippy::explicit_counter_loop)]

//! V2 cross-version regression harness.
//!
//! deja exists to catch *cross-version* regressions: record a baseline (V1),
//! replay a candidate (V2), and ask "did V2 diverge?". For that question to be
//! meaningful the lookup/match key must be VERSION-INDEPENDENT — stable across
//! benign edits (a signature tweak, code motion, a reordering of independent
//! calls) yet sensitive to genuine behavioral change.
//!
//! # How the harness models "two versions" in one process
//!
//! V1 and V2 of each call site are TWO real functions compiled into one test
//! binary. The `#[deja::boundary]` macro emits each function's REAL
//! `CallsiteIdentity` (syntactic hash at compile time) and `#[track_caller]`
//! captures each call's REAL source line at runtime. Because V1 and V2 sit at
//! different source lines, a cross-version match can NEVER come from rank-5
//! (`SourceLocation`) — it must come from a version-stable rank (1–4), or it
//! falls all the way to the fragile rank-6 positional `Sequence`. That is
//! exactly the property the instrumentation overhaul hardens, so this
//! harness is the gate each phase validates against. (Post-P3 ranks: Explicit=1,
//! LogicalContext=2, SyntacticHash=3, LexicalPath=4, SourceLocation=5, Sequence=6.
//! This bare test has no tracing subscriber, so `logical_context` is None and the
//! strongest available matcher is rank-3 SyntacticHash.)
//!
//! # Why it reuses the real matching primitives
//!
//! The lookup table and the candidate-side resolution are rebuilt here with the
//! SAME public primitives the production renderer (`replay-harness-api`) and the
//! candidate hook (`LookupTableHook`) call — `addresses_for` +
//! `canonical_args_hash` + `KeyStamper`. So the gate tracks the real matching
//! policy rather than a mock of it; when P3 inserts an `Address::LogicalContext`
//! rank, the `resolved_rank` asserted below shifts on its own.
//!
//! # The contract under test
//!
//!  * **Benign edits MUST NOT diverge** — the candidate call resolves at a
//!    version-stable rank (≤ 4) and returns the recorded result.
//!  * **A real behavioral change MUST diverge** — a candidate that sends
//!    different args into a boundary fails to resolve at any rank (a miss the
//!    divergence detector classifies as a `NovelCall`).

use std::collections::HashMap;

use deja::{addresses_for, canonical_args_hash, KeyStamper, LookupKey, SemanticEvent};
use serde_json::json;

// ---------------------------------------------------------------------------
// Scenario B1 — benign signature edit (+ inevitable source-line shift).
//
// V2 gains a parameter (`_extra`) and lives on a different source line. Same
// `boundary::operation` ("seam::sig"), same args. The cross-version match must
// land on the de-signatured rank-3 syntactic hash; if the signature ever creeps
// back into that hash, rank-3 misses and resolution drops to the fragile rank-6
// positional fallback — which this scenario's `resolved_rank == 3` assertion
// rejects.
// ---------------------------------------------------------------------------

#[deja::boundary(
    boundary = "seam",
    component = "V2Regression",
    operation = "sig",
    correlation = Some("b1-base".to_string()),
    args = json!({ "x": x }),
    result = (json!({ "output": *__deja_result }), false),
)]
fn sig_v1(x: u64) -> u64 {
    x
}

#[deja::boundary(
    boundary = "seam",
    component = "V2Regression",
    operation = "sig",
    correlation = Some("b1-cand".to_string()),
    args = json!({ "x": x }),
    result = (json!({ "output": *__deja_result }), false),
)]
fn sig_v2(x: u64, _extra: &str) -> u64 {
    x
}

// ---------------------------------------------------------------------------
// Scenario B3 — benign reorder of two independent boundary calls.
//
// V1 fires alpha then beta; V2 fires beta then alpha. Each call self-addresses
// by its own identity + args, so both still resolve at a stable rank even though
// their per-correlation request_sequence (rank 5) is now swapped. This is the
// "content/identity beats positional" guarantee that makes loops and async
// reordering safe. Four functions (two ops × two versions) keep each version's
// correlation id fixed at the macro site.
// ---------------------------------------------------------------------------

#[deja::boundary(
    boundary = "seam",
    component = "V2Regression",
    operation = "alpha",
    correlation = Some("b3-base".to_string()),
    args = json!({ "n": n }),
    result = (json!({ "output": *__deja_result }), false),
)]
fn alpha_base(n: u64) -> u64 {
    n
}

#[deja::boundary(
    boundary = "seam",
    component = "V2Regression",
    operation = "beta",
    correlation = Some("b3-base".to_string()),
    args = json!({ "n": n }),
    result = (json!({ "output": *__deja_result }), false),
)]
fn beta_base(n: u64) -> u64 {
    n + 100
}

#[deja::boundary(
    boundary = "seam",
    component = "V2Regression",
    operation = "alpha",
    correlation = Some("b3-cand".to_string()),
    args = json!({ "n": n }),
    result = (json!({ "output": *__deja_result }), false),
)]
fn alpha_cand(n: u64) -> u64 {
    n
}

#[deja::boundary(
    boundary = "seam",
    component = "V2Regression",
    operation = "beta",
    correlation = Some("b3-cand".to_string()),
    args = json!({ "n": n }),
    result = (json!({ "output": *__deja_result }), false),
)]
fn beta_cand(n: u64) -> u64 {
    n + 100
}

// ---------------------------------------------------------------------------
// Scenario R1 — a real behavioral change MUST diverge.
//
// Both versions share one identity ("seam::lookup_user") — the call site is
// unchanged — but V2 sends a different value into the boundary (user 99 instead
// of 42). The args hash is part of every LookupKey at every rank, so the
// recorded result for user 42 is found at no rank: an honest miss the detector
// would flag as a NovelCall. Identity-stable, content-sensitive.
// ---------------------------------------------------------------------------

#[deja::boundary(
    boundary = "seam",
    component = "V2Regression",
    operation = "lookup_user",
    correlation = Some("r1-base".to_string()),
    args = json!({ "user_id": user_id }),
    result = (json!({ "output": *__deja_result }), false),
)]
fn lookup_user_base(user_id: u64) -> u64 {
    user_id
}

#[deja::boundary(
    boundary = "seam",
    component = "V2Regression",
    operation = "lookup_user",
    correlation = Some("r1-cand".to_string()),
    args = json!({ "user_id": user_id }),
    result = (json!({ "output": *__deja_result }), false),
)]
fn lookup_user_cand(user_id: u64) -> u64 {
    user_id
}

// ---------------------------------------------------------------------------
// Cross-version matching, rebuilt from the SAME primitives the renderer and the
// candidate hook use. Both sides normalize the correlation id to a single value
// (`req`) because one request_id is what is shared when a baseline is replayed
// against a candidate.
// ---------------------------------------------------------------------------

/// Collect a correlation's events in record order (mirrors the per-correlation
/// stream the renderer and hook each see).
fn by_corr<'a>(events: &'a [SemanticEvent], corr: &str) -> Vec<&'a SemanticEvent> {
    let mut stream: Vec<&SemanticEvent> = events
        .iter()
        .filter(|event| event.correlation_id.as_deref() == Some(corr))
        .collect();
    stream.sort_by_key(|event| event.global_sequence);
    stream
}

/// Renderer side: build the lookup table (`LookupKey` → recorded result) from a
/// baseline event stream. One `KeyStamper` and one per-correlation request
/// sequence span the whole stream — exactly as `render_lookup_table` advances
/// them — and every rank is registered so a candidate can match at whatever
/// rank it can construct.
fn build_table(events: &[&SemanticEvent], corr: &str) -> HashMap<LookupKey, serde_json::Value> {
    let mut stamper = KeyStamper::new();
    let mut request_sequence: u64 = 0;
    let mut table = HashMap::new();
    for event in events {
        let args_hash = canonical_args_hash(&event.args);
        let location = Some((event.call_file.as_str(), event.call_line, event.call_column));
        let addresses = addresses_for(
            &event.boundary,
            &event.method_name,
            event.callsite_identity.as_ref(),
            location,
            request_sequence,
        );
        request_sequence += 1;
        for key in stamper.stamp(Some(corr), &addresses, args_hash) {
            table.insert(key, event.result.clone());
        }
    }
    table
}

/// Candidate side: for each candidate call (in order) build its rank-ordered
/// keys with the SAME primitives and resolve strongest-first against `table`.
/// Returns, per call, the winning `(rank, result)` or `None` — a miss being the
/// divergence the detector would classify as a `NovelCall`.
fn resolve_stream(
    events: &[&SemanticEvent],
    corr: &str,
    table: &HashMap<LookupKey, serde_json::Value>,
) -> Vec<Option<(u8, serde_json::Value)>> {
    let mut stamper = KeyStamper::new();
    let mut request_sequence: u64 = 0;
    let mut resolved = Vec::new();
    for event in events {
        let args_hash = canonical_args_hash(&event.args);
        let location = Some((event.call_file.as_str(), event.call_line, event.call_column));
        let addresses = addresses_for(
            &event.boundary,
            &event.method_name,
            event.callsite_identity.as_ref(),
            location,
            request_sequence,
        );
        request_sequence += 1;
        // `addresses_for` yields strongest-rank first and `stamp` preserves that
        // order, so the first key that hits is the strongest available match —
        // the exact policy `LookupTableHook::try_replay_with_context` applies.
        let keys = stamper.stamp(Some(corr), &addresses, args_hash);
        let hit = keys.iter().find_map(|key| {
            table
                .get(key)
                .map(|result| (key.address.rank(), result.clone()))
        });
        resolved.push(hit);
    }
    resolved
}

#[test]
fn v2_benign_edits_resolve_while_a_real_change_diverges() {
    let artifacts = tempfile::tempdir().expect("tempdir");
    std::env::set_var("DEJA_MODE", "record");
    std::env::set_var("DEJA_ARTIFACT_DIR", artifacts.path());

    // ---- record a baseline (V1) and a candidate (V2) for each scenario ----
    // B1: signature edit + source-line shift.
    assert_eq!(sig_v1(7), 7);
    assert_eq!(sig_v2(7, "ignored"), 7);
    // B3: V1 fires alpha→beta; V2 fires beta→alpha (reordered).
    assert_eq!(alpha_base(7), 7);
    assert_eq!(beta_base(9), 109);
    assert_eq!(beta_cand(9), 109);
    assert_eq!(alpha_cand(7), 7);
    // R1: a real change sends a different value into the boundary.
    assert_eq!(lookup_user_base(42), 42);
    assert_eq!(lookup_user_cand(99), 99);

    deja_record::flush_global_hook().expect("flush events");
    let events = deja_record::read_events(artifacts.path()).expect("read events");
    assert_eq!(events.len(), 8, "one boundary event per call");

    // === Scenario B1: benign signature edit must NOT diverge ===============
    let b1_base = by_corr(&events, "b1-base");
    let b1_cand = by_corr(&events, "b1-cand");
    assert_eq!(b1_base.len(), 1);
    assert_eq!(b1_cand.len(), 1);

    // The harness is only meaningful if the two versions genuinely sit at
    // different source lines (so rank-5 SourceLocation cannot be the matcher)
    // yet share the de-signatured syntactic hash. NOTE: this bare test records
    // with no tracing subscriber, so `logical_context` is None and the rank-2
    // `LogicalContext` address is absent — the strongest matcher here is the
    // rank-3 SyntacticHash. (The demo pipeline, which runs the correlation layer,
    // additionally resolves at rank-2 LogicalContext.)
    assert_ne!(
        b1_base[0].call_line, b1_cand[0].call_line,
        "V1/V2 must occupy different source lines, else rank-5 could mask a rank-3 regression"
    );
    let base_hash = b1_base[0]
        .callsite_identity
        .as_ref()
        .and_then(|id| id.syntax_hash);
    let cand_hash = b1_cand[0]
        .callsite_identity
        .as_ref()
        .and_then(|id| id.syntax_hash);
    assert!(base_hash.is_some(), "rank-3 syntactic hash present on V1");
    assert_eq!(
        base_hash, cand_hash,
        "a benign signature edit must leave the syntactic hash unchanged"
    );

    let b1_table = build_table(&b1_base, "req");
    let b1_resolved = resolve_stream(&b1_cand, "req", &b1_table);
    assert_eq!(
        b1_resolved[0],
        Some((3, json!({ "output": 7 }))),
        "benign signature edit resolves at the version-stable rank 3 with the recorded result"
    );

    // === Scenario B3: benign call reorder must NOT diverge =================
    let b3_base = by_corr(&events, "b3-base"); // [alpha, beta]
    let b3_cand = by_corr(&events, "b3-cand"); // [beta, alpha]
    assert_eq!(b3_base.len(), 2);
    assert_eq!(b3_cand.len(), 2);
    assert_eq!(b3_base[0].method_name, "alpha");
    assert_eq!(b3_cand[0].method_name, "beta", "candidate fires beta first");

    let b3_table = build_table(&b3_base, "req");
    let b3_resolved = resolve_stream(&b3_cand, "req", &b3_table);
    // beta (now first) resolves to beta's recorded result; alpha (now second)
    // to alpha's — each by its own identity, NOT by position. A rank-6 positional
    // match would be impossible here (the sequence indices are swapped), so a
    // stable rank proves content/identity addressing beat positional matching.
    assert_eq!(
        b3_resolved[0],
        Some((3, json!({ "output": 109 }))),
        "reordered beta resolves to beta's recording at a stable rank"
    );
    assert_eq!(
        b3_resolved[1],
        Some((3, json!({ "output": 7 }))),
        "reordered alpha resolves to alpha's recording at a stable rank"
    );

    // === Scenario R1: a real behavioral change MUST diverge ================
    let r1_base = by_corr(&events, "r1-base");
    let r1_cand = by_corr(&events, "r1-cand");
    assert_eq!(r1_base.len(), 1);
    assert_eq!(r1_cand.len(), 1);

    // The call-site identity is unchanged across versions...
    let r1_base_hash = r1_base[0]
        .callsite_identity
        .as_ref()
        .and_then(|id| id.syntax_hash);
    let r1_cand_hash = r1_cand[0]
        .callsite_identity
        .as_ref()
        .and_then(|id| id.syntax_hash);
    assert_eq!(
        r1_base_hash, r1_cand_hash,
        "R1 keeps the call site identical so the divergence is purely behavioral"
    );

    // ...but the args changed (user 42 → 99), so no recorded result is found.
    let r1_table = build_table(&r1_base, "req");
    let r1_resolved = resolve_stream(&r1_cand, "req", &r1_table);
    assert_eq!(
        r1_resolved[0], None,
        "a real change to the args flowing into a boundary must NOT resolve (a true divergence)"
    );

    // Sanity: the SAME candidate args (no behavioral change) WOULD have
    // resolved — proving R1's miss is caused by the args change, not a broken
    // table. We resolve the baseline against its own table.
    let r1_self = resolve_stream(&r1_base, "req", &r1_table);
    assert!(
        r1_self[0].is_some(),
        "an unchanged candidate resolves against the baseline table"
    );
}
