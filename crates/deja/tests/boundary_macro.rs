// The `result = { ... }` attribute blocks expand to braced expressions; the
// braces are the macro grammar, not style.
#![allow(unused_braces)]

use serde_json::json;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

#[deja::boundary(
    boundary = "unit",
    component = "BoundaryMacroTest",
    operation = "add_one",
    correlation = Some("req-boundary-1".to_string()),
    args = json!({ "input": value }),
    result = {
        (
            json!({ "output": *__deja_result }),
            false,
        )
    },
)]
fn add_one(value: u64) -> u64 {
    value + 1
}

#[deja::boundary(
    boundary = "unit_async",
    component = "BoundaryMacroTest",
    operation = "async_add_one",
    correlation = Some("req-boundary-2".to_string()),
    args = json!({ "input": value }),
    result = {
        (
            json!({ "output": __deja_result.as_ref().copied().ok() }),
            __deja_result.is_err(),
        )
    },
)]
async fn async_add_one(value: u64) -> Result<u64, &'static str> {
    Ok(value + 1)
}

struct Counter(u64);

impl Counter {
    #[deja::boundary(
        boundary = "unit_method",
        component = "BoundaryMacroTest",
        operation = "counter_add",
        correlation = Some("req-boundary-3".to_string()),
        args = json!({ "base": self.0, "input": value }),
        result = {
            (
                json!({ "output": *__deja_result }),
                false,
            )
        },
    )]
    fn add(&self, value: u64) -> u64 {
        self.0 + value
    }
}

#[deja::boundary(
    boundary = "unit_future",
    component = "BoundaryMacroTest",
    operation = "boxed_add_one",
    future = "boxed",
    correlation = Some("req-boundary-4".to_string()),
    args = json!({ "input": value }),
    result = {
        (
            json!({ "output": __deja_result.as_ref().copied().ok() }),
            __deja_result.is_err(),
        )
    },
)]
fn boxed_add_one(value: u64) -> Pin<Box<dyn Future<Output = Result<u64, &'static str>>>> {
    Box::pin(async move { Ok(value + 1) })
}

#[deja::instrument(
    correlation = Some("req-instrument-1".to_string()),
    skip(secret),
    fields(extra = value + 10),
)]
fn instrument_add(value: u64, secret: u64) -> Result<u64, &'static str> {
    let _ = secret;
    Ok(value + 1)
}

#[deja::instrument(
    correlation = Some("req-instrument-2".to_string()),
    skip_all,
    fields(kind = "async"),
)]
async fn instrument_async_error(value: u64) -> Result<u64, &'static str> {
    let _ = value;
    Err("boom")
}

#[deja::boundary(correlation = Some("req-boundary-default".to_string()))]
fn boundary_default(value: u64) -> u64 {
    value * 2
}

#[deja::redis(correlation = Some("req-redis-profile".to_string()), skip_all)]
fn redis_profile_get(key: &str) -> Result<&str, &'static str> {
    let _ = key;
    Ok("value")
}

#[deja::http(incoming, correlation = Some("req-http-profile".to_string()), skip_all)]
async fn http_profile_incoming() -> Result<u16, &'static str> {
    Ok(200)
}

fn block_on_ready<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    match Future::poll(future.as_mut(), &mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("test future unexpectedly pending"),
    }
}

// De-signatured syntax hash (rank 3): two boundaries sharing the SAME
// `boundary::operation` but with DIFFERENT signatures must produce the SAME
// syntax_hash — a benign signature edit on V2 must not change a call-site's
// cross-version identity. These two differ in arity AND parameter types.
#[deja::boundary(
    boundary = "sigtest",
    component = "BoundaryMacroTest",
    operation = "sig_probe",
    correlation = Some("req-sig-a".to_string()),
    args = json!({ "x": x }),
    result = { (json!({ "output": *__deja_result }), false) },
)]
fn sig_probe_a(x: u64) -> u64 {
    x
}

#[deja::boundary(
    boundary = "sigtest",
    component = "BoundaryMacroTest",
    operation = "sig_probe",
    correlation = Some("req-sig-b".to_string()),
    args = json!({ "x": x }),
    result = { (json!({ "output": *__deja_result }), false) },
)]
fn sig_probe_b(x: u64, _y: &str) -> u64 {
    x
}

#[test]
fn boundary_macro_records_sync_function() {
    let artifacts = tempfile::tempdir().expect("tempdir");
    std::env::set_var("DEJA_MODE", "record");
    std::env::set_var("DEJA_ARTIFACT_DIR", artifacts.path());

    assert_eq!(add_one(41), 42);
    assert_eq!(block_on_ready(async_add_one(99)), Ok(100));
    assert_eq!(Counter(5).add(9), 14);
    assert_eq!(block_on_ready(boxed_add_one(7)), Ok(8));
    let instrument_line = line!() + 1;
    assert_eq!(instrument_add(5, 999), Ok(6));
    assert_eq!(block_on_ready(instrument_async_error(10)), Err("boom"));
    assert_eq!(boundary_default(21), 42);
    let redis_line = line!() + 1;
    assert_eq!(redis_profile_get("k1"), Ok("value"));
    assert_eq!(block_on_ready(http_profile_incoming()), Ok(200));
    let db_line = line!() + 2;
    assert_eq!(
        block_on_ready(deja::db::record_query_async(
            deja::db::QuerySpec::new("select_one", "test_table", "SELECT 1", json!({ "id": 1 }),)
                .component("BoundaryMacroTest")
                .correlation_id(Some("req-db-helper".to_string())),
            async { Ok::<usize, &'static str>(1) },
            deja::db::QueryResultKind::Count,
            // Record-side error-kind extraction (unused on this Ok path).
            |err: &&'static str| ("Other".to_string(), (*err).to_string()),
            // V1 policy: never reconstruct a recorded error — fall through to live.
            |_kind: &str, _msg: &str| None::<&'static str>,
        )),
        Ok(1)
    );

    // De-signature probe calls: same boundary::operation ("sigtest::sig_probe"), different signatures.
    assert_eq!(sig_probe_a(7), 7);
    assert_eq!(sig_probe_b(8, "ignored"), 8);

    deja_record::flush_global_hook().expect("flush events");
    let events = deja_record::read_events(artifacts.path()).expect("events");
    assert_eq!(events.len(), 12);

    // De-signatured syntax hash — the two "sigtest" boundaries share one
    // boundary::operation but differ in signature, so their syntax_hash MUST be
    // identical. If the signature ever creeps back into the hash, this fails.
    let sig_hashes: Vec<Option<u64>> = events
        .iter()
        .filter(|e| e.boundary == "sigtest")
        .map(|e| e.callsite_identity.as_ref().and_then(|id| id.syntax_hash))
        .collect();
    assert_eq!(sig_hashes.len(), 2, "two sigtest boundaries recorded");
    assert!(sig_hashes[0].is_some(), "rank-2 syntax_hash present");
    assert_eq!(
        sig_hashes[0], sig_hashes[1],
        "rank-2 syntax_hash must be signature-INDEPENDENT (de-signatured)"
    );
    assert_eq!(events[0].boundary, "unit");
    assert_eq!(events[0].trait_name, "BoundaryMacroTest");
    assert_eq!(events[0].method_name, "add_one");
    assert_eq!(events[0].correlation_id.as_deref(), Some("req-boundary-1"));
    assert_eq!(events[0].args, json!({ "input": 41 }));
    assert_eq!(events[0].result, json!({ "output": 42 }));
    assert_eq!(events[1].boundary, "unit_async");
    assert_eq!(events[1].method_name, "async_add_one");
    assert_eq!(events[1].correlation_id.as_deref(), Some("req-boundary-2"));
    assert_eq!(events[1].args, json!({ "input": 99 }));
    assert_eq!(events[1].result, json!({ "output": 100 }));
    assert_eq!(events[2].boundary, "unit_method");
    assert_eq!(events[2].method_name, "counter_add");
    assert_eq!(events[2].correlation_id.as_deref(), Some("req-boundary-3"));
    assert_eq!(events[2].args, json!({ "base": 5, "input": 9 }));
    assert_eq!(events[2].result, json!({ "output": 14 }));
    assert_eq!(events[3].boundary, "unit_future");
    assert_eq!(events[3].method_name, "boxed_add_one");
    assert_eq!(events[3].correlation_id.as_deref(), Some("req-boundary-4"));
    assert_eq!(events[3].args, json!({ "input": 7 }));
    assert_eq!(events[3].result, json!({ "output": 8 }));
    assert_eq!(events[4].boundary, "function");
    assert!(events[4].trait_name.ends_with("boundary_macro"));
    assert_eq!(events[4].method_name, "instrument_add");
    assert_eq!(
        events[4].correlation_id.as_deref(),
        Some("req-instrument-1")
    );
    assert_eq!(
        events[4].args,
        json!({
            "value": { "debug": "5" },
            "extra": { "debug": "15" },
        })
    );
    assert_eq!(
        events[4].result,
        json!({ "debug": "Ok(6)", "kind": "value" })
    );
    assert!(!events[4].is_error);
    assert!(events[4].call_file.ends_with("boundary_macro.rs"));
    assert_eq!(events[4].call_line, instrument_line);
    assert!(events[4].call_column > 0);
    assert_eq!(events[5].boundary, "function");
    assert_eq!(events[5].method_name, "instrument_async_error");
    assert_eq!(events[5].args, json!({ "kind": { "debug": "\"async\"" } }));
    assert_eq!(
        events[5].result,
        json!({ "debug": "Err(\"boom\")", "kind": "error" })
    );
    assert!(events[5].is_error);
    assert_eq!(events[6].boundary, "function");
    assert_eq!(events[6].method_name, "boundary_default");
    assert_eq!(events[6].args, json!({ "value": { "debug": "21" } }));
    assert_eq!(events[6].result, json!({ "debug": "42", "kind": "value" }));
    assert_eq!(events[7].boundary, "redis");
    assert_eq!(events[7].method_name, "redis_profile_get");
    assert_eq!(events[7].call_line, redis_line);
    assert_eq!(events[8].boundary, "http_incoming");
    assert_eq!(events[8].method_name, "http_profile_incoming");
    assert_eq!(events[9].boundary, "db");
    assert_eq!(events[9].trait_name, "BoundaryMacroTest");
    assert_eq!(events[9].method_name, "select_one");
    assert_eq!(events[9].correlation_id.as_deref(), Some("req-db-helper"));
    assert_eq!(events[9].args["operation"], json!("select_one"));
    assert_eq!(events[9].args["table"], json!("test_table"));
    assert_eq!(events[9].args["sql"], json!("SELECT 1"));
    assert_eq!(events[9].args["inputs"], json!({ "id": 1 }));
    // The DB boundary records the Ok value LOSSLESSLY in the STRUCTURED,
    // versioned `DejaDatabaseResult` shape (Phase 2 DB leaf-raw work):
    // `Ok(1usize)` records its serde value plus its Rust type name.
    assert_eq!(
        events[9].result,
        json!({ "version": 1, "result": "Ok", "value": 1, "type_name": "usize" })
    );
    assert_eq!(events[9].call_line, db_line);
    // Phase 1: the DB boundary now carries a structured CallsiteIdentity
    // (rank-2 SyntacticHash + rank-3 LexicalPath) instead of `None`.
    let db_identity = events[9]
        .callsite_identity
        .as_ref()
        .expect("db event must carry a callsite identity");
    assert!(
        db_identity.syntax_hash.is_some(),
        "db identity must carry a rank-2 syntax hash"
    );
    assert!(
        db_identity.lexical_path.is_some(),
        "db identity must carry a rank-3 lexical path"
    );

    // DECLARATIVE BOUNDARY MODEL: the hand-written db seam now DECLARES its
    // semantics. `select_one` is a State-channel op; `is_read_op("select_one")` is
    // false (it is not a recognized read verb), so the declared effect is `Write`
    // — byte-identical to the verdict the name heuristic produced. The declared
    // channel/effect drive `read_set`/`write_set`: a Write seeds the write_set
    // (and leaves read_set empty), exactly as the heuristic did.
    assert_eq!(
        events[9].channel,
        Some(deja::Channel::State),
        "db seam must declare Channel::State"
    );
    assert_eq!(
        events[9].effect,
        Some(deja::Effect::Write),
        "db `select_one` declares Effect::Write (matches is_read_op's verdict)"
    );
    assert!(events[9].strategy.is_none(), "no RMW in the generic db seam");
    assert!(
        events[9].read_set.is_empty(),
        "a declared db Write contributes no read_set"
    );
    assert!(
        !events[9].write_set.is_empty(),
        "a declared db Write seeds the write_set from the primary key"
    );

    // UNDECLARED FALLBACK: the `#[deja::boundary]` / `#[deja::redis]` events here
    // declare NO semantics (the macro emits the legacy `BoundarySpec::new`), so
    // their channel/effect stay `None` and `finish` falls back to the name
    // heuristics — byte-identical to before this slice. The redis read here has an
    // empty read_set because its args were `skip_all`'d (no captured key), exactly
    // as before; the point under test is that the channel/effect are UNDECLARED.
    assert!(
        events[0].channel.is_none() && events[0].effect.is_none(),
        "an undeclared unit boundary carries no declared channel/effect"
    );
    assert!(
        events[7].channel.is_none() && events[7].effect.is_none(),
        "an undeclared redis boundary falls back to the heuristic (no declaration)"
    );
}
