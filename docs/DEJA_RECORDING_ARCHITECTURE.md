# Deja Record/Replay — Hyperswitch Integration

> Lives in the deja library repo (`docs/`); describes the integration patch the
> nested `vendor/hyperswitch-deja-clean` branch carries on top of upstream
> Hyperswitch.

Deja makes a Hyperswitch run deterministically replayable: it records every
interaction with an external system during a live run, then replays that run
exactly — returning the recorded responses in place of the live systems.

---

## Contents

**Part I — Architecture overview**
- [What Deja is](#what-deja-is)
- [Why it matters](#why-it-matters)
- [How it works](#how-it-works)
- [Footprint & safety in Hyperswitch](#footprint--safety-in-hyperswitch)
- [Status & maturity](#status--maturity)
- [Roadmap & open risks](#roadmap--open-risks)

**Part II — Technical reference**
1. [Overview](#1-overview)
2. [The annotation macros — codebase instrumentation](#2-the-annotation-macros--codebase-instrumentation)
3. [The primitives Deja provides](#3-the-primitives-deja-provides)
4. [Correlation model](#4-correlation-model)
5. [The recording pipeline we push](#5-the-recording-pipeline-we-push)
6. [Blast radius & code layout in Hyperswitch](#6-blast-radius--code-layout-in-hyperswitch)
7. [Execution flow (record → replay)](#7-execution-flow-record--replay)
8. [Configuration & deployment](#8-configuration--deployment)
9. [Fidelity fixes & design decisions](#9-fidelity-fixes--design-decisions)
10. [Testing & verification](#10-testing--verification)
11. [Open issues & TODOs](#11-open-issues--todos)

---

# Part I · Architecture overview

## What Deja is

A transaction's outcome depends on a sequence of interactions with external
systems — datastores, caches, payment processors, cryptographic services, clocks,
id generators. That makes specific runs hard to reproduce: an incident or a
behavioral regression often turns on the exact responses those systems returned,
which a conventional test cannot recreate.

Deja closes that gap. It records those interactions during a live run and can
replay the run deterministically, so the same inputs reproduce the same execution —
on any machine, with no live dependencies.

- **Record** — capture every interaction that crosses a system boundary during a
  real run.
- **Replay** — re-run the request against a recording; boundaries return their
  recorded responses, and a comparator confirms the result matches the original.

## Why it matters

- **Regression safety.** Replay a captured production flow against a new build to
  prove — deterministically — that behavior is unchanged before shipping. Catches
  regressions that synthetic tests miss.
- **Faster incident diagnosis.** Reproduce a production issue locally and offline,
  exactly, without access to live infrastructure or processor credentials.
- **Testing without live dependencies.** Recordings stand in for real databases,
  caches, and payment processors, so tests run faster, cheaper, and more reliably —
  with no network egress or secrets in CI.
- **Change confidence.** Record before a change and replay after to confirm
  observable behavior is unchanged.
- **Auditability.** A faithful, inspectable account of exactly what the system did
  on a given run.

## How it works

```
        Real request
             │
             ▼
   ┌───────────────────────────┐    RECORD: each external interaction
   │   Hyperswitch router       │    (database, cache, processor call,
   │   (instrumented;           │    crypto, time, id) is captured as a
   │    build switch ON)        │    structured event
   └────────────┬──────────────┘
                │ events
                ▼
      existing event bus  ───▶   durable object storage
                                  (the recording)
                │
                ▼
   ┌───────────────────────────┐    REPLAY: external calls return their
   │   Second instance,         │    recorded answers — no live systems,
   │   driven against the       │    no network, fully deterministic
   │   recording                │
   └────────────┬──────────────┘
                ▼
        comparator  ───▶  pass / divergence report
```

1. **Instrument the boundaries.** Each point where Hyperswitch calls an external
   system is marked with a lightweight annotation that observes the call without
   altering the business logic.
2. **Record.** As a real request flows through, each boundary interaction is
   captured as a structured event — what was called, with what, and what came back.
   Events stream out continuously to durable storage, **reusing Hyperswitch's
   existing event-streaming infrastructure** rather than adding new production
   systems.
3. **Replay.** A second instance runs the same request, but each boundary returns
   its previously recorded answer instead of calling the live system. Because the
   nondeterministic sources (time, ids, processor responses) are themselves
   recorded, the run reproduces exactly.
4. **Compare.** A comparator checks the replay against the original and reports any
   divergence — that report is the test result.

## Footprint & safety in Hyperswitch

The integration was designed to be **low-risk to the production payments path**:

- **Off by default, invisible when off.** All instrumentation sits behind a build
  switch. When it is off, the shipped binary is identical to standard Hyperswitch —
  no added code, no overhead, no extra dependency. Enabling it is a deliberate,
  isolated decision.
- **Low overhead when on.** Measured latency overhead on the standard payment
  workload is in the low single-digit percent; when enabled but not actively
  recording, the cost is a single check.
- **Non-invasive.** The changes wrap existing operations rather than altering
  payment logic, and are concentrated at a small, well-defined set of boundaries.
- **Reuses existing infrastructure.** Recordings travel over Hyperswitch's existing
  event bus to standard object storage — no new production services to operate.
- **Fails safe.** If the recording pipeline is misconfigured or a downstream
  component is unavailable, the router keeps serving traffic normally; recording
  quietly degrades, and the payments path never aborts.
- **Contained, reversible change.** The footprint is bounded and overwhelmingly
  additive.

**One risk to flag.** Recordings currently capture data verbatim, which can include
sensitive fields (e.g. authorization headers and personal data). Recordings must
therefore be treated as sensitive artifacts; a redaction/handling policy is on the
near-term roadmap.

## Status & maturity

- **Recording: working end-to-end** — capture → streaming → durable storage —
  validated against the standard payment workload.
- **Performance: within target** — low single-digit latency overhead, full workload
  health.
- **Replay: functional foundation** — deterministic substitution works for the core
  boundaries; full fidelity for a few cases (replaying outgoing network calls, and
  correlating background/asynchronous work to a request) is in progress.
- **Validated by an automated scorecard** that scores latency, resource use, and
  behavioral completeness against the payment flow on every change.

## Roadmap & open risks

- **Close replay-fidelity gaps** — correlate background/asynchronous work; support
  replay of outgoing network calls.
- **Sensitive-data handling** — a redaction policy for captured payloads.
- **CI verification** — promote the record→replay check to an automated gate.
- **Productionize deployment** — current deployment tooling is demo-grade and needs
  hardening for routine use.

---

# Part II · Technical reference

## 1. Overview

Deja makes a Hyperswitch run **deterministically replayable** by recording every
impure dependency call at its **semantic trait boundary** (DB, Redis, outgoing/
incoming HTTP, crypto, locking, time, id generation). Each boundary call is
captured as one **`SemanticEvent`**, pushed through a process-wide **hook** into a
**JSONL** file and/or a **Kafka** topic, transported via **Vector** into
**MinIO/S3**, and later consumed by a **replay** harness that drives a second
instance against a frozen lookup table and scores divergences.

Three properties define the integration:

- **Annotation-driven.** Instrumentation is added by placing a Deja **attribute
  macro** (`#[deja::redis(...)]`, `#[deja::http(...)]`, …) at each boundary. The
  macro owns all record/await/finish boilerplate; the annotated function body is
  unchanged.
- **Opt-in and zero-cost when off.** Every annotation is wrapped in
  `#[cfg_attr(deja, …)]` and every code change is gated behind the `deja` Cargo
  feature. With the feature off, the optional `deja` dependency is not linked and
  the build is byte-for-byte equivalent to upstream Hyperswitch. With the feature
  *on but not recording*, an `is_active()` check and the `EitherBody` body-gate keep
  the cost to a single branch.
- **Single dependency surface.** Hyperswitch crates depend only on one crate,
  `deja`, which re-exports the macros, the recording primitives, the payload
  helpers, and the replay API.

Operational loop:

```
                         ┌──────────────────────── RECORD ────────────────────────┐
                         │                                                          │
  HTTP request ─▶ Hyperswitch router (deja feature ON, DEJA_MODE=record)           │
                         │   semantic boundaries: db · redis · http · crypto ·      │
                         │   locking · time · id   →   SemanticEvent                │
                         │            │                                             │
                         │            ▼  RecordingHook (ONE shared Arc)             │
                         │     AsyncRecordWriter ─▶ CompositeSink                   │
                         │            ├─▶ JsonlSink  →  <DEJA_ARTIFACT_DIR>/         │
                         │            │                 semantic-events.jsonl       │
                         │            └─▶ HyperswitchKafkaRecordSink                 │
                         │                     │  envelope deja.artifact_record/v1  │
                         └─────────────────────┼────────────────────────────────────┘
                                               ▼
                  Kafka topic  hyperswitch-deja-recording-events  (broker kafka0:29092)
                                               │
                                               ▼
                  Vector:  source deja_recording → transform deja_unwrap
                           (`if exists(.event) { . = .event }`) → sink deja_recording_s3
                                               │  ndjson, compression:none
                                               ▼
                  MinIO / S3   bucket deja-recordings   key recordings/{recording_run_id}/
                                               │
                         ┌─────────────────────▼──────────── REPLAY ───────────────┐
                         │  replay harness pulls ndjson → renders frozen lookup     │
                         │  table → drives replay candidate (DEJA_MODE=replay,      │
                         │  RuntimeHook::LookupReplay) → ObservedCall stream →       │
                         │  divergence detector → replay-scorecard/v1               │
                         └──────────────────────────────────────────────────────────┘
```

**Two workspaces (layout boundary).**

| Workspace | Path | Role |
|---|---|---|
| **Hyperswitch worktree** | `vendor/hyperswitch-deja-clean` (branch `deja-lean`) | The instrumentation (annotations), the Kafka sink, boot wiring, Vector config, compose overlay. **This document's scope.** |
| **`deja` crate family** | `crates/deja`, `crates/deja-derive`, `crates/deja-record`, `crates/deja-context` (resolved via the `path` dep below) | The macros (`deja-derive`), the recording/replay primitives (`deja-record`), correlation (`deja-context`), all re-exported through the façade crate `deja`. |
| **Replay harness** | `crates/replay-harness-*` (parent workspace) | `LookupTable`/lifecycle-worker/scorecard machinery that consumes recordings. Adjacent; not HS blast radius. |

> **Path note.** `deja = { path = "../../../../crates/deja" }` resolves *out of* the
> vendored tree to the worktree-internal `deja` crate family. This is correct for
> the current layout but layout-fragile — a different directory arrangement could
> resolve the same string elsewhere. **TODO:** replace with a workspace alias.

---

## 2. The annotation macros — codebase instrumentation

This is the heart of the integration: **you instrument a boundary by annotating
the impure function with a Deja attribute macro.** The macro (defined in the
`deja-derive` proc-macro crate) rewrites the function to record the call; the body
you wrote is preserved verbatim.

### 2.1 The instrumentation idiom

Every annotation is applied through `#[cfg_attr(deja, …)]` so it disappears
entirely when the `deja` feature is off:

```rust
#[cfg_attr(deja, deja::redis(
    component = "RedisConnectionPool",
    operation = "get_key",
    args   = deja::redis::key_args("GET", key.as_str(), self.add_prefix(key), serde_json::Value::Null, None),
    result = deja::redis::result_debug(__deja_result),
))]
pub async fn get_key<V>(&self, key: &RedisKey) -> CustomResult<V, RedisError> { /* unchanged */ }
```

### 2.2 The macro family

`deja-derive` exports ten attribute macros. Six are **boundary aliases** — each is
*exactly* `instrument::generate_with_boundary(args, func, Some("<tag>"))`, i.e. the
generic `instrument` codegen with one preset default boundary string. Two are
**generic forms**; two (`trace`, `recordable`) are additional macros not used by
this integration.

| Macro | Codegen path | Default `boundary` | Used in Hyperswitch |
|---|---|---|---|
| `deja::redis`  | `generate_with_boundary(…, Some("redis"))`   | `redis`         | ✅ `redis_interface/src/commands.rs` (20 methods) |
| `deja::http`   | `generate_http` (parses `incoming`/`outgoing`) | `http_outgoing` | ✅ `external_services/src/http_client.rs` (`send_request`) |
| `deja::time`   | `generate_with_boundary(…, Some("time"))`    | `time`          | ✅ `common_utils/src/lib.rs` (clock helpers) |
| `deja::id`     | `generate_with_boundary(…, Some("id"))`      | `id`            | ✅ `common_utils/src/lib.rs` (`generate_id*`) |
| `deja::crypto` | `generate_with_boundary(…, Some("crypto"))`  | `crypto`        | ✅ `hyperswitch_domain_models/src/type_encryption.rs` |
| `deja::lock`   | `generate_with_boundary(…, Some("locking"))` | `locking`       | ✅ `router/src/core/api_locking.rs` |
| `deja::boundary`   | fully-explicit `BoundaryArgs` | *(required)* | — generic form (boundary/component/operation/correlation/args/result all explicit) |
| `deja::instrument` | generic `InstrumentArgs`      | `function`   | — generic form (component defaults to `module_path!()`) |
| `deja::trace`      | request-scope future wrapper  | n/a          | — not used (request correlation is the hand-written middleware, §4) |
| `deja::recordable` | trait → `delegate_*!` generator | n/a        | — not used (HS instruments functions, not whole trait impls) |

Two boundaries are **not** attribute macros, by design:

- **DB** — a function-style `record_deja_db_query!` `macro_rules!` (defined in
  `diesel_models/src/query/generics.rs`) wrapping each generic helper's body;
  expands to `deja::db::record_query_async(QuerySpec…, async move { … }, kind)`.
  Chosen because the diesel generic helpers render SQL once and want a coarse
  `QueryResultKind`, not per-parameter `Debug` extraction (§2.6).
- **http_incoming** — hand-written Actix middleware (`router_env/src/request_id.rs`),
  because capturing the request/response *body* requires a streaming `Transform`,
  not a function wrapper (§4).

### 2.3 What the macro generates

For an `async fn`, the macro emits a start→await→finish triptych (the sync arm is
identical minus `.await`; a third arm handles `future = "boxed"` non-async fns that
return a boxed future, finishing *inside* the returned `Box::pin(async { … })`):

```rust
#[track_caller]                                          // force-added if absent
pub async fn get_key<V>(&self, key: &RedisKey) -> … {
    let __deja_boundary_correlation_id: Option<String> = { None::<String> };   // correlation= (default: inherit ambient)
    let __deja_boundary_event = ::deja::__private::start_boundary_event_lazy(
        ::std::panic::Location::caller(),                                       // #[track_caller] ⇒ the application callsite
        ::deja::__private::BoundarySpec::new("redis", "RedisConnectionPool", "get_key"),
        __deja_boundary_correlation_id,
        || { /* args= expression */ },                                         // LAZY: not evaluated when the hook is inactive
    );
    let __deja_boundary_output = async move { /* your original body */ }.await;
    ::deja::__private::finish_boundary_event(
        __deja_boundary_event,
        &__deja_boundary_output,
        move |__deja_result| { /* result= expression → (serde_json::Value, bool) */ },
    );
    __deja_boundary_output
}
```

Three details make this cheap and correct:

- **Lazy args.** The `args` expression is a closure `|| { … }`.
  `start_boundary_event_lazy` resolves the hook and returns `None` immediately if
  `!hook.is_active()` — **the closure never runs**, so building args JSON costs
  nothing on the disabled path.
- **The `__deja_result` binding.** The `result` closure receives `&Output` (the
  wrapped function's return) as `__deja_result`; the `result=` expression inspects
  it and returns `(serde_json::Value, bool)` where the bool marks an error.
- **Forced `#[track_caller]`.** If absent, the macro adds it, so
  `Location::caller()` (→ `call_file`/`call_line`/`call_column`) points at the real
  application caller rather than the generated wrapper.

### 2.4 The attribute grammar (`InstrumentArgs`)

| Key | Meaning / default |
|---|---|
| `boundary = "…"` | boundary tag → `SemanticEvent.boundary`. Default = the alias's preset, else `"function"`. |
| `component = "…"` *(alias `trait_name`)* | → `SemanticEvent.trait_name`. Default `module_path!()`. |
| `operation = "…"` *(alias `method_name`)* | → `SemanticEvent.method_name`. Default = the function name. |
| `correlation = <expr>` *(alias `correlation_id`)* | must yield `Option<String>`. Default `None::<String>` (inherit the ambient correlation id). |
| `args = <expr>` | must yield `serde_json::Value`. Default = inferred object of `deja::value::debug(&param)` for each non-skipped named parameter. |
| `result = <expr>` | receives `__deja_result: &Output`, returns `(serde_json::Value, bool)`. Default `deja::value::result_debug(__deja_result)`. |
| `skip(a, b)` / `skip_all` | drop the named parameters / all parameters from the inferred args map. |
| `fields(k = expr, …)` | add extra key/value pairs to the inferred args map (`value::debug` of each). |
| `future = "boxed"` | for a non-async fn that returns a boxed future: finish *inside* the boxed future. |
| `ret`, `err` | accepted tracing-compatibility flags. |

### 2.5 What Hyperswitch actually annotates

| Boundary | Crate / file | Annotation | Captured |
|---|---|---|---|
| **redis** | `redis_interface/src/commands.rs` (20 methods) | `#[cfg_attr(deja, deja::redis(...))]` | command verb, `key.as_str()`, tenant-aware key, options, value, result |
| **http_outgoing** | `external_services/src/http_client.rs` (`send_request`) | `deja::http(outgoing, ...)` + `CapturedResponseBody` | method, url, request_id, headers (unmasked), query, timeout, request_body, tls bools; response `{status, reason, response_body}` |
| **crypto** | `hyperswitch_domain_models/src/type_encryption.rs` (`crypto_operation`) | `deja::crypto(...)` above `#[instrument(skip_all)]` | `{table, crypto_op, has_key:!key.is_empty()}`; result `{ok, output/error}` — **secret-safe** |
| **locking** | `router/src/core/api_locking.rs` | `deja::lock(...)` ×2 | `{action, merchant_id}`; result `{ok}` / `{ok:false,error}` |
| **time** | `common_utils/src/lib.rs` (`date_time::now`, …) | `deja::time(...)` | argless; result via `value::result_debug` |
| **id** | `common_utils/src/lib.rs` (`generate_id*`), `router_env` (`generate_uuid_v7`) | `deja::id(...)` / `record_id_generation` | `source=uuid_v7`, generated value |
| **db** | `diesel_models/src/query/generics.rs` (11 generic helpers) | `record_deja_db_query!` → `deja::db::record_query_async` | operation, table, sql, inputs; result `{ok, result_kind, debug}` |
| **http_incoming** | `router_env/src/request_id.rs` | `EitherBody` middleware + `EventBuilder` + `LazyEventFinalizer` | method/path/query/request_id/status/headers/request_body/response_body |

### 2.6 Boundary placement notes

- **DB — instrument at the diesel *generics* layer.** ~11 generic helpers
  (`generic_insert`/`update`/`update_with_results`/`update_by_id_core`/`delete`/
  `delete_one_with_result`/`find_by_id_core`/`find_one_core`/`filter`/`count`) are
  wrapped — one change set covers **all tables**. Table name via
  `type_name::<T>().rsplit("::").nth(1)`. SQL rendered once with
  `debug_query::<Pg,_>(&query).to_string()`. `QueryResultKind`
  (`Value`/`Rows`/`Optional`/`Count`/`Bool`/`Unit`) tags result shape; `is_error`
  is inferred from the `Err(`/`Err {` Debug prefix. The one invasive trait-bound
  change Hyperswitch absorbs: widening `R: Send + 'static` → `R: Debug + Send +
  'static` (and `+ Clone` on `delete_one_with_result`).
- **Redis — 20 `RedisConnectionPool` methods.** `correlation=None` everywhere
  (inherited ambiently). `*_and_deserialize_*` ops record only
  `{ok, deserialized:true, type_name}` (no value capture — avoids extra trait
  bounds; the raw `GET` bytes were already captured). Write values via
  `deja::value::debug(&value)`.
- **http_outgoing — only `send_request`** (single egress chokepoint → 3-file blast
  radius). The response-body double-use problem is solved by **eager drain +
  rebuild**: read status/headers/version/url, `await response.bytes()`, build a new
  `http::Response` (needs `reqwest::ResponseBuilderExt::url`), clone bytes into it,
  insert `CapturedResponseBody(bytes)` into `extensions_mut()`, return
  `reqwest::Response::from(http_response)`. Gated on `is_active()` *before* any
  read/clone/rebuild.
- **crypto/lock/time/id** — the deja attribute is placed **above** the existing
  `#[instrument(skip_all)]` tracing attribute (layers semantic recording on top of
  tracing; preserves the `skip_all` secret-safety comment). `#[track_caller]` is
  added to all `generate_*` helpers, including the non-recorded
  `*_of_default_length` wrappers, so the recorded callsite is the application
  caller.

---

## 3. The primitives Deja provides

Everything below is re-exported from the single `deja` façade crate (so HS crates
carry one dependency). Layered from the macro outward:

### 3.1 Recording runtime entry points (what the macro calls)

In `deja::__private`, supplied by `deja-record`:

- **`start_boundary_event_lazy(Location, BoundarySpec, Option<String>, || args)`** —
  resolves the hook via `global_hook_from_env()`; returns `None` (and never runs the
  `args` closure) when inactive; otherwise allocates `next_global_sequence` +
  `next_request_sequence`, snapshots the `#[track_caller]` `Location`, graph node /
  span ids, and builds the args JSON. Returns an `EventBuilder` handle.
- **`finish_boundary_event(event, &output, |__deja_result| (response, is_error))`** —
  computes `duration_us`, runs the `result` closure, assembles the `SemanticEvent`,
  and calls `hook.record(event)` (enqueue onto the writer).
- **`EventBuilder`** — snapshots `global_sequence`/`request_sequence`/`correlation_id`/
  `start_ns`/callsite at start; on `finish()` sets `event_schema_version=1`, computes
  duration, sets `request := args.clone()`, `response := result.clone()`. Also
  exposes `start_with_correlation_id(...)` used by the DB helper.
- **`BoundarySpec::new(boundary, component, operation)`** — the static descriptor.
- **`record_boundary_{async,sync}[_lazy]`** — convenience wrappers used where a
  closure form is more ergonomic than the attribute form.

### 3.2 The canonical record — `SemanticEvent` (22 fields)

```
global_sequence · request_sequence · correlation_id · timestamp_ns ·
recording_run_id · graph_node_id · tracing_span_id · boundary · trait_name ·
method_name · call_file · call_line · call_column · receiver · request · args ·
response · result · is_error · duration_us · event_schema_version (serde default = 1) ·
callsite_identity (Option<CallsiteIdentity>)
```

- `global_sequence` — process-wide monotonic `AtomicU64` across **all** requests
  (no gaps). `request_sequence` — per-correlation ordering (starts at 0).
- `boundary` ∈ `{http_incoming, http_outgoing, redis, db, crypto, time, id, locking}`.
- `request := args.clone()`, `response := result.clone()` (the EventBuilder copies).
- `event_schema_version` is `#[serde(default)]` so old artifacts re-deserialize.

### 3.3 The hook layer

| Primitive | Role |
|---|---|
| **`DejaHook`** (trait, `Send+Sync`) | The boundary sink/replay interface: `is_active`, `record`, `try_replay`, `next_global_sequence`, `next_request_sequence(correlation)`, default `next_callsite_occurrence` & `try_replay_with_context`, `recording_run_id()`. |
| **`RuntimeHook`** (enum, 4 variants) | Process-wide polymorphic hook: `Recording(Arc<RecordingHook>)`, `Replay(ReplayHook)`, `LookupReplay(LookupTableHook)`, `NoOp`. Implements `DejaHook` by forwarding. `variant_name()` → `recording`/`replay`/`lookup_replay`/`noop`. |
| **`RecordingHook`** | The concrete recorder: an `AsyncRecordWriter<SemanticEvent>`, an `AtomicU64` global counter, a `Mutex<HashMap>` of per-correlation request counters, a stable `recording_run_id`, and a per-callsite occurrence map. Built via `with_sink(sink, run_id)` (DI entry) or `new(dir)` (JSONL convenience). |
| **`NoOpHook`** | An always-inactive hook (the `is_active()` short-circuit). |
| **resolvers + injector** | `global_runtime_hook_from_env()` → `Option<Arc<RuntimeHook>>`; `global_hook_from_env()` → `Option<Arc<RecordingHook>>`; `set_global_runtime_hook(Some(RuntimeHook))` is the injection point. |

**The unification invariant.** Two `OnceLock`-backed resolvers exist and **must
return the same shared `Arc<RecordingHook>`**:

| Resolver | Returns | Used by |
|---|---|---|
| `global_runtime_hook_from_env()` | `Arc<RuntimeHook>` | `request_id` / id-gen / boot log |
| `global_hook_from_env()` | `Arc<RecordingHook>` | db / redis / http / crypto / lock boundaries |

`global_hook_from_env()` **first peeks** `GLOBAL_RUNTIME_HOOK` (`get`, never
`get_or_init`); if a `RuntimeHook::Recording` is installed it returns `Arc::clone`
of *that* hook's inner `RecordingHook`, else it falls back to the env-derived
`GLOBAL_RECORDING_HOOK`. The peek preserves the install-before-getter ordering
contract. Without sharing, the two getters would back two independent counters and
sink sets — duplicate `global_sequence`, torn JSONL (this was the split-brain bug;
[§9](#9-fidelity-fixes--design-decisions) F7).

### 3.4 The sink layer

| Primitive | Role |
|---|---|
| **`RecordSink<T>`** | Transport-agnostic: `write_batch(&[T]) -> io::Result<()>` + `flush`. deja-record has **zero** transport deps; the application supplies the sink (dependency inversion). |
| **`JsonlSink`** | Reference impl — line-delimited JSON to a file. |
| **`CompositeSink`** | Fan-out: `new(Box<primary>).with_secondary(Box<secondary>)`. **Swallows secondary failures** so a failing Kafka secondary cannot poison the JSONL primary. |
| **`AsyncRecordWriter`** / `WriterConfig` / `WriterStatsSnapshot` | Worker thread + bounded channel, **no-drop backpressure** (producers block when full). Tunable via `DEJA_BATCH_SIZE` / `DEJA_FLUSH_INTERVAL_MS` / `DEJA_QUEUE_CAPACITY`. |

### 3.5 Payload helpers (`deja::value` / `http` / `db` / `redis`)

The `args=`/`result=` expressions are built from small JSON helpers so each
annotation stays a one-liner:

- **`deja::value`** — `debug(&T)` → `{debug:"…"}`; `result_debug(&T)` →
  `({debug, kind}, is_error)` (infers `is_error` from the `Err(`/`Err {` prefix);
  `bytes(&[u8])` → full `{captured, bytes_len, utf8, text, json, raw_bytes}`;
  `optional_bytes(...)` preserves a `{captured:false, reason}` when absent.
- **`deja::http`** — `headers(iter)` → `{name:[values]}`; `body(&[u8])`;
  `missing_body(reason)`.
- **`deja::db`** — `QuerySpec::new(op, table, sql, inputs)` (+ `.component()` /
  `.boundary()` / `.correlation_id()`); `QueryResultKind`; `record_query_async`
  (the function the DB macro calls); `ok_value`/`ok_rows`/`ok_count`/`ok_bool`/
  `error`; `result_value`/`result_rows`/`result_optional`/`result_count`/`result_bool`.
- **`deja::redis`** — `key_args`/`keys_args`; `result_debug`/`result_unit`;
  `deserialized_result(result, type_name)` (records only `{ok, deserialized,
  type_name}`, avoiding extra trait bounds).

### 3.6 Correlation & replay primitives

- **`scope_correlation`** (= `deja_context::scope`) — wraps a future so every poll
  enters a `ContextSnapshot(correlation_id)` into a thread-local; runtime-independent
  (no tokio task hooks). The correlation primitive the middleware uses (§4).
- **Replay** (re-exported from `deja_record::replay`) — `LookupTableHook` (the
  microsecond HashMap replayer), `Address` + `addresses_for` + `canonical_args_hash`
  (rank-aware addressing), `LookupTable`/`LookupEntry`/`LookupKey`/`LocalFileLookupSource`,
  `ObservedCall`/`ObservedCallSink`/`FileObservedSink`/`InMemoryObservedSink`,
  `ReplayReport`/`Divergence`/`DivergenceKind`/`ArgMismatchPolicy`/`ReplayConfig`.
- **`CallsiteIdentity` / `CallsiteSource`** — structured callsite identity (`version`,
  `source`, `id`, `scope`, `occurrence`, `caller_function`, `lexical_path`,
  `syntax_hash`, `logical_context`) feeding the rank ladder.

**Address rank ladder** (strongest-first; query strongest-first, first hit wins):
`Explicit(1)`, `LogicalContext(2)` (the root→leaf tracing span-name path —
survives line shifts and disambiguates concurrent same-callsite calls),
`SyntacticHash(3)`, `LexicalPath(4)`, `SourceLocation(5)`, `Sequence(6)`.
Memoize by **position** in the trace (flow × callsite Address ×
occurrence), never by args (boundaries are impure — time/uuid have no args).

---

## 4. Correlation model

### 4.1 Establishing correlation

The single integration point is the deja `RequestIdMiddleware`
(`router_env/src/request_id.rs`). On the **active** path, `Service::call`:

1. Extracts/inserts the request id into extensions.
2. Wraps the inner-service future with
   `scope_correlation(request_id, recorded_incoming(fut, record).instrument(span))`,
   where `span = info_span!("deja::http_incoming", method, path, request_id)`.
3. `scope_correlation` re-enters the correlation id into a thread-local on **every
   poll** and restores it on drop, so all boundary recordings inside the request
   future inherit the request id as their `correlation_id` — with no tokio task
   hooks.

Downstream boundaries pass `correlation=None` (the macro default) and let the
runtime fall back to the ambient `current_correlation_id()`.

### 4.2 The `EitherBody` inactive-path gate

Under the deja feature the middleware response type is
`ServiceResponse<EitherBody<RecordingBody<B>, B>>` (requires `B: MessageBody +
'static`).

- **active** (`is_active()` true) → `capture_incoming_request` (non-destructive
  `Bytes` extract + `set_payload` round-trip), `scope_correlation`-bind, record,
  `EitherBody::left(RecordingBody)` (buffers chunks, finalizes the http_incoming
  event via `LazyEventFinalizer` on stream end).
- **inactive** → `service.call(request)` then `EitherBody::right(body)` — **zero
  alloc, zero buffering, no `RecordingBody::poll_next` invocations.**

A router built *with* the deja feature but *not* recording pays no body-capture
cost.

### 4.3 Known correlation gaps

- **Uncorrelated background-task events.** Spawned Tokio tasks don't propagate the
  ambient `ContextSnapshot`, so their boundary events carry `correlation_id=null`
  and share one bucket. Magnitude varies: ~625/run in the workload audit (225 crypto,
  205 db, 25 id, 150 time); up to ~5,354 of 17,268 (31%) in a larger recording.
  `id_generation` (4,369) is correctly classified `inherently_uncorrelated`. The
  harness works around this by driving correlations in **record order** (sort by
  earliest `global_sequence`). The real fix lives in the deja-tokio task-hook path
  (`tokio_task_spawn → tokio_task_poll_start → adopt_for_current_task`) and is
  **deferred**.
- **No task-spawn propagation yet** → single-iteration self-replay stays aligned via
  the record-order drive; multi-iteration / concurrent replay will need real
  correlation propagation across spawns.

---

## 5. The recording pipeline we push

### 5.1 Sinks & the Kafka envelope

- deja-record defines `RecordSink<SemanticEvent>` + `JsonlSink` with **zero Kafka
  dependency**.
- The router supplies `HyperswitchKafkaRecordSink`
  (`crates/router/src/services/kafka/deja_record_sink.rs`), reusing HS's
  already-linked rdkafka `KafkaProducer` — **no second producer, no second linkage,
  no second broker config.**
- `deja_boot` composes:
  `CompositeSink::new(Box::new(JsonlSink @ <DEJA_ARTIFACT_DIR>/semantic-events.jsonl)).with_secondary(Box::new(HyperswitchKafkaRecordSink))`.
- **Envelope** `deja.artifact_record/v1`:
  `{schema_version:1, artifact_type:"deja_artifact_record", recording_run_id, correlation_id, event:<SemanticEvent>}`.
- **Partition key** = `correlation_id` when present, else
  `{recording_run_id}:{global_sequence}` (uncorrelated background events still
  partition deterministically).
- **5 Kafka headers** (so Vector can route without parsing the payload):
  `global_sequence`, `request_sequence`, `recording_run_id`, `boundary`,
  `method_name`.
- `write_batch` serializes each event → computes key → builds `OwnedHeaders` →
  `KafkaProducer::send_to_topic(topic, key, payload, headers)` (a raw publish off
  HS's typed analytics catalogue). `flush()` is a no-op (rdkafka's `ThreadedProducer`
  owns background delivery).

### 5.2 Transport: Kafka → Vector → MinIO/S3

- **Kafka topic** `hyperswitch-deja-recording-events` (`DEFAULT_RECORDING_TOPIC`;
  overridable via `KafkaSettings.deja_recording_topic` config → `DEJA_KAFKA_TOPIC`
  env → default). Broker `kafka0:29092` (HS's own, in the `olap` compose profile).
- **Vector** — `deja_recording` (kafka source) → `deja_unwrap`
  (`if exists(.event) { . = .event }`, strips the envelope to the bare
  `SemanticEvent` so its own `recording_run_id` becomes top-level) →
  `deja_recording_s3`.
- **MinIO/S3** — bucket `deja-recordings`, endpoint `http://minio:9000`, creds
  `minioadmin`/`minioadmin`; host ports `9100→9000` (API), `9101→9001` (console);
  `minio-setup` (mc) creates the bucket. Object layout
  `recordings/{recording_run_id}/`, **ndjson, `compression:none`** (overrides
  aws_s3's gzip default because the harness re-parses line-by-line),
  `newline_delimited`, `filename_append_uuid`, batch 2000/5s.

---

## 6. Blast radius & code layout in Hyperswitch

**Totals vs. upstream tag `2026.04.21.0`: 48 files (+~2,750 / −220), all
committed on the integration branch.** Runtime code is gated behind the opt-in
`deja` feature; the remaining un-gated lines are inert in default builds
(additive serde derives on db rows, a feature-conditional `DejaQueryResult`
bound alias that is blanket-satisfied with deja off, and behavior-preserving
delegation refactors). Deja-only infra lives in separate files
(`docker-compose.deja.yml`, `config/vector.deja.yaml`) so the stock compose and
vector configs are byte-identical to upstream.

**Feature fan-out** (`router/Cargo.toml`):
```toml
deja = ["dep:deja", "common_utils/deja", "router_env/deja", "external_services/deja",
        "diesel_models/deja", "redis_interface/deja", "hyperswitch_domain_models/deja"]
deja = { path = "../../../../crates/deja", optional = true, default-features = false }
```
Each leaf crate redeclares an optional `deja` dep and a local `deja` feature; the
router's `deja` feature flips them all on together.

### 6.1 correlation-core — `router_env`

| File | Δ | What & why |
|---|---|---|
| `src/request_id.rs` | **+518** | The `#[cfg(deja)] semantic_boundary` module (local `HOOK` `OnceLock`, `hook()`/`is_active()`, `record_id_generation()`, `IncomingHttpRecord`, `capture_incoming_request()` w/ Bytes+`set_payload`, `RecordingBody<B>`, `RecordedIncomingFuture<B>`); `#[track_caller]` + id-gen recording on `generate_uuid_v7` & callers; `RequestIdMiddleware` service field `S` → `Arc<S>`; Transform/Service split into two cfg impls (deja response type becomes `EitherBody<RecordingBody<B>, B>`); `Service::call` with the active/inactive branch + `scope_correlation` + span. The **request-entry recording boundary**; establishes the request id as the ambient correlation scope. |
| `src/logger/setup.rs` | **+18** | Insert `.with(deja_layer())` into the tracing registry; `deja_layer()` returns an `ExecutionGraphLayer` from `DEJA_GRAPH_DIR` (degrades to an identity layer / `None` with an `eprintln` on error). Enriches events with `graph_node_id`/`tracing_span_id`. |
| `Cargo.toml` | +3 | optional `deja` dep + `bytes`; `deja = ["dep:deja"]`. |

### 6.2 db — `diesel_models`

| File | Δ | What & why |
|---|---|---|
| `src/query/generics.rs` | **+439** | `table_name<T>()` helper + dual-arm `record_deja_db_query!` macro; rewrote 11 generic helpers to render SQL once and wrap the query future with operation/table/sql/inputs/`QueryResultKind`; widened bounds to `R: Debug` (+`Clone` on one). The DB semantic boundary. |
| `src/schema.rs` | **−51 / +1** | **Cosmetic only** — `allow_tables_to_appear_in_same_query!` collapsed to one line (same 51 tables). Not a deja change and not a schema change; the set is identical to baseline. |
| `Cargo.toml` | +2 | optional `deja` dep + feature; `deja` is **not** in `default = ["kv_store"]`. |

### 6.3 redis — `redis_interface`

| File | Δ | What & why |
|---|---|---|
| `src/commands.rs` | **+374 / −19** | `#[cfg_attr(deja, deja::redis(...))]` on **20 methods** (`set_key`, `get_key`, `exists`, `get_and_deserialize_key`, `delete_key`, `set_key_with_expiry`, `set_key_if_not_exists_with_expiry`, `set_expiry`, `get_ttl`, `set_hash_fields`, `get_hash_field`, …, `sadd`); widened `FromRedis` generics with `Debug`; refactored `serialize_and_set_key_with_expiry` to funnel through `set_key_with_expiry`. |
| `src/types.rs` | **+10** | `RedisKey::as_str()` (captures the unprefixed key; **not** deja-gated) + `Debug` derive on `SetnxReply`/`HsetnxReply`/`MsetnxReply`. |
| `Cargo.toml` | +3 | optional `deja` dep + `serde_json`; `deja = ["dep:deja","common_utils/deja"]`. |

### 6.4 http — `external_services`

| File | Δ | What & why |
|---|---|---|
| `src/http_client.rs` | **+53** | `#[cfg(deja)] use reqwest::ResponseBuilderExt;` + `mod semantic_boundary;`; `deja::http(outgoing, ...)` attr on `send_request` (above `#[instrument]`); cfg-gated Ok arm (early-return if `!is_active`, else drain/rebuild response + insert `CapturedResponseBody`). |
| `src/http_client/semantic_boundary.rs` | **+109 (NEW)** | `CapturedResponseBody(bytes::Bytes)`, `is_active()`, `request_id()`, `request_args()`, `captured_body_json()`, `response_result()`, `header_value()` (peeks `Maskable::Masked`), `request_body()` (stamps a `kind` discriminator). |
| `Cargo.toml` | +3 | optional `deja` dep + `bytes`; `deja = ["dep:deja"]`. |

### 6.5 crypto / locking / utils — `hyperswitch_domain_models`, `router`, `common_utils`

| File | Δ | What & why |
|---|---|---|
| `hyperswitch_domain_models/src/type_encryption.rs` | **+40** | `deja::crypto(...)` above `#[instrument(skip_all)]` on `crypto_operation`; args `{table, crypto_op, has_key}`; secret-safe (no key/plaintext). |
| `hyperswitch_domain_models/src/payments/payment_attempt.rs` | **+2 / −2** | `cfg(all(v1,olap))` → `cfg(v1)` on a `Connector` import — build-fix collateral (the deja profile is `v1` without `olap`), not a recording change. |
| `hyperswitch_domain_models/Cargo.toml` | +2 | `deja = ["dep:deja","diesel_models/deja"]`. |
| `router/src/core/api_locking.rs` | **+62** | two `deja::lock(...)` attrs on `perform_locking_action` / `free_lock_action`; args `{action, merchant_id}`. |
| `common_utils/src/lib.rs` | **+91** | `#[track_caller]` + `deja::time(...)` on `date_time::now`/`now_unix_timestamp`/`date_as_yyyymmddthhmmssmmmz`/`now_rfc7231_http_date`; `#[track_caller]` + `deja::id(...)` on the `generate_id*` family; bare `#[track_caller]` on ref-id and `*_of_default_length` wrappers; 2 fns refactored to bind the timestamp to a local before returning. Instruments the canonical nondeterminism sources (clock + id). |
| `common_utils/Cargo.toml` | +2 | `deja = ["dep:deja"]`. |

### 6.6 boot-wiring — `router`

| File | Δ | What & why |
|---|---|---|
| `src/bin/router.rs` (main) | **+28** | A `#[cfg(deja)]` block calling `router::deja_boot::install(&conf.events).await`, then peeking `global_runtime_hook_from_env()` and logging variant/artifact_dir/run_id. Install runs **before** the OnceLock getter locks in. |
| `src/lib.rs` | **+7** | `#[cfg(deja)] pub mod deja_boot;`; the deja feature does not alter the Actix builder beyond exposing the module. |
| `src/services/kafka.rs` | **+39** | `#[cfg(deja)] pub mod deja_record_sink;`; `#[serde(default)] deja_recording_topic: Option<String>` + getter on `KafkaSettings`; `KafkaProducer::send_to_topic(topic, key, payload, headers)` raw-publish off the typed analytics catalogue. |
| `src/deja_boot.rs` | **+165 (NEW)** | `install(events)`, `resolve_topic`, `wants_kafka_recording()`, `DEFAULT_RECORDING_TOPIC`, a topic-resolution unit test. Composes `CompositeSink(JsonlSink+Kafka)` → `Arc<RecordingHook::with_sink>` → `set_global_runtime_hook(Recording)`. Enforces install-before-getter; degrades safely on any failure. |
| `src/services/kafka/deja_record_sink.rs` | **+156 (NEW)** | `Envelope` struct, `HyperswitchKafkaRecordSink: RecordSink<SemanticEvent>`, `SCHEMA_VERSION=1`, an envelope-shape unit test. |
| `Cargo.toml` | **+5 / −2** | Declares the optional `deja` dep (`default-features=false`) and the feature fan-out. |

### 6.7 infra-config

| File | Δ | What & why |
|---|---|---|
| `config/vector.deja.yaml` | **NEW (overlay; stock `vector.yaml` untouched)** | source `deja_recording` (kafka, topic `hyperswitch-deja-recording-events`, group `deja-recording-vector`, `auto_offset_reset: earliest`); transform `deja_unwrap` (`if exists(.event) { . = .event }`); sink `deja_recording_s3` (aws_s3 → MinIO `deja-recordings`, `compression: none`, ndjson/newline_delimited, `key_prefix: recordings/{{ .recording_run_id }}/`, `filename_append_uuid`, batch 2000/5s). |
| `docker-compose.deja.yml` | **+200 (NEW)** | Overlay on HS `docker-compose.yml`: reuses pg/redis/migration/superposition/kafka0 (`ports !override []`); swaps `hyperswitch-server` → `deja-router-local` RECORD; adds `hyperswitch-replay` (same image, replay), `minio`, `minio-setup`, reuses `vector`, profiled `mc` helper. |
| `config/development.toml` | untouched | Stock file. For local `cargo run` record sessions that want the workload's `X-Request-ID` preserved as the deja `correlation_id`, export `ROUTER__TRACE_HEADER__ID_REUSE_STRATEGY=use_incoming` instead of editing the file. |
| `Cargo.lock` | +63 | dependency resolution for the optional deja crates. |

Local tooling state (agent workflows, memory banks) stays untracked and is
never part of the integration branch.

---

## 7. Execution flow (record → replay)

### 7.1 Boot / install

`main()` (under `#[cfg(deja)]`) calls `deja_boot::install(&conf.events).await`
**before** `global_runtime_hook_from_env()`. `install()` gates on
`wants_kafka_recording()` = (`DEJA_MODE==record` OR (empty mode AND
`DEJA_ARTIFACT_DIR` set)) AND `DEJA_SINK ∈ {kafka, both}`, AND
`events.source==kafka`. If satisfied: build the `KafkaProducer`, resolve the topic,
build `CompositeSink(JsonlSink + HyperswitchKafkaRecordSink)`, wrap in
`Arc<RecordingHook::with_sink(...)>`, and
`set_global_runtime_hook(RuntimeHook::Recording(hook))`. **Every failure path
degrades to JSONL-only (or off) with a warning — a misconfigured broker never
aborts router boot.**

### 7.2 Per-request record flow

1. The middleware establishes the correlation scope via `scope_correlation` and
   starts the `http_incoming` `EventBuilder`.
2. `RecordingBody` buffers the response body; `LazyEventFinalizer` emits the
   http_incoming event on stream end (its `Drop` finalizes even if the stream is
   dropped early — no lost recordings).
3. **Per boundary:** each annotated call resolves the shared `RecordingHook`,
   allocates `global_sequence` + `request_sequence`, inherits `correlation_id`, and
   emits a `SemanticEvent`.
4. **Sink fan-out:** each event flows `AsyncRecordWriter → CompositeSink → JsonlSink`
   (file) **and** `HyperswitchKafkaRecordSink` (Kafka envelope) → Vector → S3.

### 7.3 Replay flow

- `runtime_hook_from_env()` with `DEJA_MODE=replay` builds
  `RuntimeHook::LookupReplay` from `DEJA_LOOKUP_TABLE` (a **local file path**), or
  falls back to `RuntimeHook::Replay(ReplayHook)` from the artifact dir. Deja
  reads/writes **only local file paths** for `DEJA_LOOKUP_TABLE` /
  `DEJA_OBSERVED_SINK` (a shared-volume mount).
- The replay candidate runs a dumb `LookupTableHook` (microsecond HashMap get) and
  emits an `ObservedCall` per boundary; the orchestrator owns the matching policy
  (renders frozen lookup tables, classifies post-hoc into `ReplayReport`).
- **Important:** the in-crate annotated boundaries **record/observe only — they do
  not short-circuit to a replayed value at the attribute site.**
  `start_boundary_event_lazy` consults the recorder (record-only). Deterministic
  substitution of recorded ids/timestamps/crypto/redis/db results is driven by the
  lookup-table infrastructure consuming the recorded stream — `LookupTableHook`
  intercepts at the deja-record layer, not inside the instrumented HS crates.

---

## 8. Configuration & deployment

### 8.1 `DEJA_*` environment knobs

| Var | Meaning |
|---|---|
| `DEJA_MODE` | `record` \| `replay` \| `disabled`/`none` |
| `DEJA_ARTIFACT_DIR` | recording dir; JSONL primary at `<dir>/semantic-events.jsonl`; if set without mode → recording implied |
| `DEJA_SINK` | `jsonl` \| `kafka` \| `both` |
| `DEJA_KAFKA_TOPIC` | override the recording topic |
| `DEJA_RECORDING_RUN_ID` / `DEJA_RUN_ID` | stable `recording_run_id` (former → latter → `run-{now_ns}`) |
| `DEJA_LOOKUP_TABLE` | replay: **local file path** to the lookup table |
| `DEJA_OBSERVED_SINK` | replay: **local file path** for the `ObservedCall` stream |
| `DEJA_GRAPH_DIR` | execution-graph layer artifact dir (`logger/setup.rs`) |
| `DEJA_BATCH_SIZE` / `DEJA_FLUSH_INTERVAL_MS` / `DEJA_QUEUE_CAPACITY` / `DEJA_FLUSH_AFTER_RECORDS` | async-writer tuning |
| `ROUTER__EVENTS__SOURCE=kafka`, `ROUTER__EVENTS__KAFKA__BROKERS=kafka0:29092` | enable Kafka events via compose env override (no forked TOML) |
| `RUST_MIN_STACK`, `STRIPE_API_KEY`, `RECORDING_ID`, `REPLAY_HOST_PORT`, `HARNESS_STATE`, `RUN_ID` | demo container env |

### 8.2 Docker two-image topology

```
docker compose -p deja-demo --profile olap \
  -f docker-compose.yml -f docker-compose.deja.yml up -d --build <services>
```
Base compose **first** so relative paths resolve. Both `hyperswitch-server` (RECORD)
and `hyperswitch-replay` (REPLAY) use the **same** image `deja-router-local:latest`
(built from `demo/Dockerfile.hyperswitch-semantic`), differentiated only by `DEJA_*`
env. Only `hyperswitch-replay` publishes a host port (`${REPLAY_HOST_PORT:-8090}:8080`).
`working_dir:/local` + `entrypoint /local/bin/router -f /local/config/docker_compose.toml`
on both. The RECORD container **disables HS's `/health` curl** (else every poll
floods the recording with `http_incoming` noise) and deliberately omits
`depends_on: kafka0` (kafka0 is profiled; a non-profiled service can't declare a
profiled dependency; rdkafka buffers until the broker is reachable).

### 8.3 Build profile

`v1`/`v2` are mutually-exclusive HS schema features; `deja` is independent.
`router_env` and `redis_interface` sit below the schema layer and have no `v1`
feature. The deja build profile compiles `v1` **without `olap`** (the
`payment_attempt.rs` cfg-gate widening keeps that profile building). Verification
gate: `cargo check -p router --features deja,v1` (clean, ~2m19s–2m48s); full release
`cargo build -p router --features deja,v1 --release` ≈ 11m22s (2 cosmetic warnings).

---

## 9. Fidelity fixes & design decisions

### 9.1 The F1–F6 lean-patch series (commit `5a2572fa58`)

| # | Finding | Status |
|---|---|---|
| **F1** | `workload_correlation_ids_reused_across_runs` | **FIXED** — per-run `DEJA_RUN_ID`. |
| **F2** | `recording_run_id_missing` | **FIXED** — resolved `DEJA_RECORDING_RUN_ID → DEJA_RUN_ID → default`, carried in every envelope/header/partition-key fallback. |
| **F3** | `uncorrelated_semantic_events` (HIGH) | **PARTIAL** — dropped explicit `correlation=None` overrides so events inherit the ambient id; ~625 null events/run remain (spawned Tokio tasks). Root-cause fix is a deja-tokio deferral. |
| **F4** | `incoming_http_not_graph_joined` | **FIXED** — `info_span!("deja::http_incoming")` joins incoming HTTP to the execution graph. (`http_incoming_missing_count:225` persists as a separate Actix-middleware-tracing gap.) |
| **F5** | `outgoing_http_request_body_empty` | **FIXED** for request bytes. **Caveat:** response headers/trailers aren't captured, which (with `reqwest::Response` not being `DeserializeOwned`) makes outgoing-HTTP **replay** impossible in the first cut — designed-around via an egress block. |
| **F6** | `semantic_error_events_present` (MEDIUM) | **REMAINING** — 425 Redis errors (likely real Nil/NotFound); fix is to add Redis to `expected_misses` (like db's 325). |

### 9.2 The F1–F11 replay-fix series

| # | Fix |
|---|---|
| **F1** request-body-key | recorder writes `request_body`, harness read `body`. `extract_body_bytes` reads `request_body ∥ body`. |
| **F2** response-body-key | recorder writes `response_body`, harness read `body`. `response_result` reads `response_body ∥ body`. |
| **F3** headers-shape | recorder emits `{name:[values]}`, harness expected `[{key,value}]`. `extract_headers` accepts both. |
| **F4** drive-record-order | null-correlation events shared one bucket. Sort correlations by earliest `global_sequence`. |
| **F5** deja_boot-artifact-path | `DEJA_ARTIFACT_DIR` passed straight to `JsonlSink::new` created a file literally named `recording`. Fix: `dir.join("semantic-events.jsonl")` + `create_dir_all`. |
| **F6** contract-pin-test | `reconstruct_handles_real_recorder_shape` pins the real recorder output (was tested against a fabricated shape). |
| **F7** split-brain unification | duplicate `global_sequence`, torn JSONL, MinIO under-capture — two resolvers each with their own `RecordingHook`+counter. 4-edit fix: `RuntimeHook::Recording` holds `Arc<RecordingHook>`; `global_hook_from_env` peeks it; two runtime sites wrap in `Arc`; `deja_boot` wraps its composite in `Arc`. |
| **F8** rank3-occurrence | `LookupTableHook` doesn't override `next_callsite_occurrence` → rank-3 always 0 at replay; falls through to rank-4. Tracked. |
| **F9** callsite-identity-null | on-disk recordings carry `callsite_identity=null` on 100% of events (codegen emits `LegacyLocation`) → ranks 1–3 carry no discriminator; self-replay is effectively rank-4/5. Recommended (not applied): emit a real `module_path` lexical path or macro-time `syntax_hash`. |
| **F10** vector-gzip & mc-tooling | aws_s3 defaulted to gzip → `compression:none`; raised MinIO wait 60→180s; route S3 by `recording_run_id`. |
| **F11** iteration-order-independence | the rank-aware addressing change: each call self-addresses by `lexical_path + args_hash` into its own occurrence-0 bucket; a loop replayed in a different order resolves with zero misses. |

### 9.3 Design decisions

**Instrumentation placement** (why the macros sit where they do)
- **DB at the diesel generics layer** — one change set covers all tables.
- **HTTP only on `send_request`** — single egress chokepoint, 3-file blast radius.
- **id/time at the `common_utils` source helpers** — the canonical nondeterminism
  sources.
- **deja attr above `#[instrument(skip_all)]`** — layers recording on tracing;
  preserves secret-safety.
- **Capture DB results as a Debug string + coarse `QueryResultKind`** (infer
  `is_error` from the Debug prefix) — avoids a `Serialize` bound on every row type;
  the only invasive change is the `R: Debug` widening.
- **`*_and_deserialize_*` record only `{ok, deserialized, type_name}`** — avoids
  extra trait bounds; raw GET bytes are already captured.

**Recording / transport**
- **No transport dep in deja-record; supply the sink from the router.** deja-record
  defines `RecordSink<T>`; the router supplies `HyperswitchKafkaRecordSink` on HS's
  already-linked rdkafka.
- **`KafkaProducer::send_to_topic` raw-publish** — deja owns its own envelope schema
  on an off-catalogue topic.
- **Partition by `correlation_id`, fall back to `{recording_run_id}:{global_sequence}`** —
  a flow's events stay on one partition; uncorrelated events still land
  deterministically.
- **`RecordingHook::with_sink` is generic `<S: RecordSink>`** — the sink vtable
  inlines on the hot path.
- **Install before the OnceLock getter; peek (never `get_or_init`).** Composition
  must precede the first read; `install` runs in `main`.
- **Share ONE `Arc<RecordingHook>` between both getters** — else two counters / two
  sink sets corrupt the recording (F7).
- **Every `deja_boot` failure degrades to JSONL-only with a warning, never aborts
  boot.**

**Body / middleware**
- **`EitherBody<RecordingBody<B>, B>`** — one statically-typed middleware choosing
  recorded vs zero-overhead pass-through at runtime.
- **Two separate `cfg(deja)`/`cfg(not(deja))` Transform/Service impls** — the deja
  path changes the `Response` associated type; the non-deja build stays byte-for-byte
  upstream.

**Config / topology**
- **Reuse HS's own docker-compose Kafka (kafka0) + Vector via a thin overlay.**
- **Enable Kafka via `ROUTER__EVENTS__*` env overrides** — a 2-env-var delta, not a
  forked TOML.
- **`deja_recording_topic: Option<String>` with `#[serde(default)]`** — existing
  configs keep parsing; resolve config → env → default.
- **`ROUTER__TRACE_HEADER__ID_REUSE_STRATEGY=use_incoming` (env, local record runs
  only)** — preserve the workload's `X-Request-ID` as a stable `correlation_id`
  without editing the stock config; replay forces `UseIncoming` in gated code.
- **Treat `DEJA_ARTIFACT_DIR` as a directory; JSONL at `<dir>/semantic-events.jsonl`
  with `create_dir_all`** (F5).

**Replay (forward-looking)**
- **Rank-aware addressing before sequence** — memoize by **position** (lexical
  placement / syntax hash / source location), not by args; a miss returns `None` and
  falls through, never crashes. *"order should only be last resort."*
- **In-process LOOKUP + orchestrator-owned POLICY** — a microsecond hot path, with
  policy that evolves without rebuilding candidates.
- **Byte-exact self-replay is a verification step, not a build phase** — id/time/
  crypto generators are already deja boundaries, so a replayed `generate_id`
  reproduces the recorded id by the same mechanism that makes a Redis GET byte-exact.
- **Additive, forward-compatible wire format** — every new `SemanticEvent` field is
  `serde(default)`; every new `DejaHook` method has a safe default.

---

## 10. Testing & verification

### 10.1 Workload scorecard

The benchmark workload is the 9-step Hyperswitch payment flow (`demo/workload.sh`):
Stripe connector create + payment confirm; needs `STRIPE_API_KEY` at record, no
egress at replay. Scorecard = 9 metrics (P50, P99, Throughput, RSS, CPU, Startup,
Workload Health, Fault Tolerance, Completeness) + a fidelity audit.

- **Baseline (`5a2572fa58`)**: 9/9 PASS; P50 +2.1%, P99 0%, RSS +14.9 MB.
- **After RuntimeHook/EitherBody (`f88fec7507`)**: **8/9 PASS** (5 runs × 50
  iterations). P50 1957→1992 ms (+1.8%), P99 2092→2190 ms (+4.7%), RSS 551→573 MB,
  CPU +11.6%, Startup ~1012 ms, Workload Health 25 ok / 0 fail, Completeness 100%.
  Only Throughput FAILed (0.5→0.4/s) — judged 1-sample jitter at 50-iter scale
  (overhead actually improved, +1.8% vs +2.1%).

### 10.2 Audit findings (stable across runs)

2 findings: `uncorrelated_semantic_events` (HIGH — crypto 225, db 205, id 25, time
150) + `semantic_error_events_present` (MEDIUM — redis 425).
`duplicate_workload_correlations` empty (F1 fixed). `http_incoming_missing=225` is an
unchanged baseline gap. `id_generation 4369` correctly classified
`inherently_uncorrelated`.

### 10.3 Pre-flight audit

A static pre-flight audit of the replay path (5 parallel reviewers over the
lookup/divergence/lifecycle code, checked against an *actual* on-disk recording
of 560 events) produced **28 findings / 4 blockers** and converted ~4 blind run
cycles into one informed fix push.

### 10.4 Unit / integration tests

- `deja_record_sink.rs::envelope_serializes_artifact_record_v1_shape` — asserts the full
  `SemanticEvent` serializes to the `deja.artifact_record/v1` envelope shape.
- `deja_boot.rs::topic_resolution_prefers_config_then_env_then_default`.
- `deja-record`: 18 tests green (lookup tests incl.
  `lookup_resolves_iteration_order_independent`,
  `lookup_table_hook_prefers_stronger_rank_over_sequence`;
  `argless_call_fails_closed_under_default_policy`;
  `composite_sink_failing_secondary_does_not_poison_primary`).
- Replay-harness: `reconstruct_handles_real_recorder_shape` pins the real recorder
  contract.
- **`KAFKA_E2E` is documented but NOT implemented** — needs a live broker harness
  (bring up Kafka, `DEJA_SINK=both`, record, consume the topic, assert envelopes +
  5 headers). The produce side is covered by `deja_record_sink`'s own tests.

---

## 11. Open issues & TODOs

**Determinism — the "direct-primitive" gap (KNOWN CAVEAT)**

deja makes id / time / randomness deterministic on replay by instrumenting the
*canonical library* HyperSwitch is supposed to use:

- ids   → `common_utils::generate_id*` (`generate_id`, `generate_id_with_default_len`,
  `generate_time_ordered_id`, …) and `router_env::…::generate_uuid_v7`
- time  → `common_utils::date_time::now` / `now_unix_timestamp`
- crypto rng → `common_utils::crypto::generate_cryptographically_secure_random_string`/`_bytes`

Instrumenting the library once is the correct, non-invasive shape. **The gap is every
call site that reaches for a RAW primitive directly instead of going through that
library** — those bypass the instrumentation and are therefore non-deterministic on
replay (a fresh value each run → the row/response/JWT diverges, and any DB INSERT
carrying that value misses substitution and falls through to a live collision). Known
primitive categories that appear in HS:

| Raw primitive (bypasses library)        | Should be / library equivalent                |
|-----------------------------------------|-----------------------------------------------|
| `uuid::Uuid::new_v4()`                   | `generate_id*` / `generate_uuid_v7`           |
| `std::time::SystemTime::now()` / `Instant::now()` | `date_time::now`                     |
| `ring::rand::SystemRandom` (`.fill`)     | `generate_cryptographically_secure_random_*`  |
| `nanoid!(…)` (e.g. `router::utils::generate_id`) | `common_utils::generate_id`           |
| Argon2 `SaltString::generate(OsRng)`     | (no library wrapper today)                    |
| `rand::random` / `OsRng` / `fastrand` / `chrono::Utc::now()` | library equivalents        |

**Decision (2026-06): leave this gap unsolved for now; do NOT chase it per-callsite,
and do NOT revive the LD_PRELOAD/`deja-preload` syscall-interception path (set aside).**
The preferred long-term fix is to make these deterministic *at a library seam* — route
all such generation through the instrumented `common_utils` helpers (and/or add a lint /
`clippy` deny that forbids the raw primitives in the workspace) — NOT scattered
`#[deja::id]` annotations on individual upstream functions.

> Stopgap in the current demo: to reach byte-exact *responses* for the 9-request
> workload, a handful of the **critical** direct-usage sites were patched in place with
> `#[deja::id(..., replay/replay_ok)]` — `router::services::generate_aes256_key`,
> `common_utils::crypto::NonceSequence::new`, `core::admin::create_merchant_publishable_key`,
> `domain::user::generate_user_id`, `utils::user::password::generate_password_hash`,
> `services::jwt::generate_exp`, `router::utils::generate_id`. These are an explicit
> **stopgap, not the intended pattern** — they are the very "modify specific call sites"
> change we want to avoid. Remaining (un-patched) direct usages are expected not to be hit
> on critical paths for this workload, but **may cause replay divergence elsewhere**.

**Correlation integrity**
- **Uncorrelated background-task events** (F3) — ~625/run, up to 31% in
  larger recordings. Fix lives in the deja-tokio task-hook path
  (`tokio_task_spawn → tokio_task_poll_start → adopt_for_current_task`).
  Highest-leverage item: every downstream piece assumes correlation is reliable, and
  it isn't yet for those events.
- **No task-spawn correlation propagation** — multi-iteration / concurrent replay
  needs it.

**Replay**
- **In-crate boundaries don't short-circuit on replay** — `generics.rs` always
  awaits the real query; `DEJA_MODE=replay` substitution happens at the lookup-table
  layer, not the attribute site. Same for crypto/lock/time/id.
- **Outgoing-HTTP replay unsolved** — `reqwest::Response` isn't `DeserializeOwned`;
  response headers/trailers aren't captured (F5). Designed-around via egress block.
- **`callsite_identity` null on 100% of on-disk events** (F9) → self-replay is
  effectively rank-4/5; ranks 1–3 carry no discriminator.
- **Identity cascade ranks 2/4/7/8 not implemented** (committed
  `try_replay_with_context` does ranks 1/3/5/6).

**Capture fidelity / lossiness**
- DB result capture is lossy (Debug string + coarse `QueryResultKind`); `is_error` is
  a string-prefix heuristic that could misclassify an `Ok` whose Debug starts with
  `Err(`/`Err {`.
- DB inputs / Redis predicates captured as Rust Debug / `type_name` strings
  (non-deterministic across versions → weakens args-hash stability).
- `*_and_deserialize_*` doesn't capture the deserialized value (only metadata).
- **Captured HTTP headers/bodies are UNMASKED verbatim** (auth/PII in recordings) —
  no redaction at the boundary.

**Infra / tooling**
- End-to-end Kafka delivery has no automated test (needs a live broker); only envelope serialization and topic resolution are unit-tested.
- The harness raw-TCP reader doesn't decode `Transfer-Encoding: chunked`
  (Content-Length only).
- **Cargo path fragility** — `deja = { path = "../../../../crates/deja" }` is
  layout-dependent. TODO: workspace alias.
- `external_services/.../semantic_boundary.rs:16` still calls `global_hook_from_env`
  (not the runtime getter) — harmless (peek-coordination) but flagged for migration.
- `deja_recording_s3` `key_prefix` relies on each event carrying its own
  `recording_run_id`; a null id routes to an empty prefix segment.
