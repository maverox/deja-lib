//! Replay-harness API service entry.
//!
//! axum server hosting one API surface (`/api/v1`) plus the embedded
//! dashboard SPA. Nothing lives outside `/api/v1`, so the SPA owns the whole
//! page URL space via the fallback — no content negotiation anywhere.
//!
//!   GET  /api/v1/healthz                  → liveness
//!   GET  /api/v1/recordings               → recordings catalog
//!   POST /api/v1/runs                     → create a run (spawns the worker)
//!   GET  /api/v1/runs                     → run list
//!   GET  /api/v1/runs/{id}                → store row + live worker snapshot
//!   GET  /api/v1/runs/{id}/stages         → stage history
//!   GET  /api/v1/runs/{id}/logs           → persisted worker logs
//!   GET  /api/v1/runs/{id}/artifacts      → registered artifacts
//!   GET  /api/v1/runs/{id}/scorecard      → divergence scorecard
//!   GET  /api/v1/runs/{id}/stream         → SSE run progress
//!   GET  /api/v1/artifacts/{id}/raw       → stream an artifact file
//!   GET  /api/v1/audit                    → append-only audit log
//!
//! The lifecycle worker (compose up → record/replay → score → tear down) is
//! spawned per run by the create handler; this binary hosts the API and
//! persists/serves run state.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Router,
};
use replay_harness_api::{api::runs, divergence, HarnessRoot, Run, RunStatus};
use replay_harness_store::Store;

/// The built dashboard SPA (web/dist), embedded at compile time so the
/// orchestrator stays a single deployable binary. `npm run build` in web/
/// refreshes it; the dist is committed so cargo builds never need node.
#[derive(rust_embed::RustEmbed)]
#[folder = "../../web/dist"]
struct WebAssets;

#[derive(Clone)]
struct AppState {
    root: Arc<HarnessRoot>,
    store: Option<Arc<Store>>,
}

#[tokio::main]
async fn main() {
    let bind_addr = std::env::var("HARNESS_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let root_dir =
        std::env::var("HARNESS_STATE_DIR").unwrap_or_else(|_| "./harness-state".to_string());
    let root = match HarnessRoot::new(&root_dir) {
        Ok(r) => Arc::new(r),
        Err(err) => {
            eprintln!("replay-harness-api: HARNESS_STATE_DIR setup failed: {err}");
            std::process::exit(1);
        }
    };
    // Optional Postgres store: dashboard state, stage history, audit. Runs
    // still execute without it (file-backed worker state); store-backed
    // surfaces return 503 until it is up (demo/lib.sh boots the orchestrator
    // pg).
    let db_url = std::env::var("DEJA_DB_URL")
        .unwrap_or_else(|_| replay_harness_store::DEFAULT_DB_URL.to_string());
    let store = match Store::connect(&db_url).await {
        Ok(s) => {
            eprintln!("replay-harness-api: store connected + migrated ({db_url})");
            Some(Arc::new(s))
        }
        Err(err) => {
            eprintln!(
                "replay-harness-api: store unavailable ({db_url}): {err} — running file-only; \
                 start it with: docker compose -p deja-orchestrator -f demo/docker-compose.orchestrator.yml up -d"
            );
            None
        }
    };
    let state = AppState {
        root: root.clone(),
        store,
    };

    let api_v1 = Router::new()
        .route("/healthz", get(healthz))
        .route("/recordings", get(v1_list_recordings))
        .route("/runs", post(v1_create_run).get(v1_list_runs))
        .route("/runs/{run_id}", get(v1_get_run))
        .route("/runs/{run_id}/stages", get(v1_run_stages))
        .route("/runs/{run_id}/logs", get(v1_run_logs))
        .route("/runs/{run_id}/artifacts", get(v1_run_artifacts))
        .route("/runs/{run_id}/scorecard", get(v1_scorecard))
        .route("/runs/{run_id}/calls", get(v1_calls))
        .route("/runs/{run_id}/http-diffs", get(v1_http_diffs))
        .route("/runs/{run_id}/graph", get(v1_graph))
        .route("/runs/{run_id}/stream", get(run_stream))
        .route("/artifacts/{id}/raw", get(v1_artifact_raw))
        .route("/audit", get(v1_audit));

    let app = Router::new()
        .nest("/api/v1", api_v1)
        // SPA: real assets by path; any other GET falls back to index.html
        // (client-side routing). The API is entirely under /api/v1, so the
        // page URL space (/runs/..., /recordings, ...) is the SPA's alone.
        .fallback(get(spa_fallback))
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
        Ok(l) => l,
        Err(err) => {
            eprintln!("replay-harness-api: bind {bind_addr} failed: {err}");
            std::process::exit(1);
        }
    };
    eprintln!(
        "replay-harness-api: listening on http://{bind_addr} (state: {})",
        root.root.display()
    );

    if let Err(err) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        eprintln!("replay-harness-api: server error: {err}");
        std::process::exit(1);
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("replay-harness-api: shutting down");
}

// ---------------------------------------------------------------------------
// /api/v1 handlers
// ---------------------------------------------------------------------------

async fn healthz() -> Response {
    json_ok(serde_json::json!({ "status": "ok" }))
}

/// Shorthand: the Postgres store, or a 503 telling the operator how to start it.
#[allow(clippy::result_large_err)] // the Err is an axum Response; cold path
fn require_store(st: &AppState) -> Result<Arc<Store>, Response> {
    st.store.clone().ok_or_else(|| {
        error_resp(
            503,
            "store unavailable — start it: docker compose -p deja-orchestrator -f demo/docker-compose.orchestrator.yml up -d",
        )
    })
}

/// `POST /api/v1/runs` — create a run and spawn its lifecycle worker.
///
/// `X-Deja-Actor` is required (auth-light but audit-ready: the audit log never
/// has anonymous mutations). `expectation` rides alongside the RunSpec
/// (scripts/UI annotate what a human expects: "pass" / "diverge"); it is
/// store/audit metadata only.
async fn v1_create_run(
    State(st): State<AppState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let Some(actor) = headers
        .get("x-deja-actor")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .map(str::to_owned)
    else {
        return error_resp(400, "X-Deja-Actor header required");
    };
    let raw: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return error_resp(400, &format!("parse RunSpec: {e}")),
    };
    let expectation = raw
        .get("expectation")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let spec: replay_harness_api::RunSpec = match serde_json::from_value(raw) {
        Ok(s) => s,
        Err(e) => return error_resp(400, &format!("parse RunSpec: {e}")),
    };
    let run = match runs::persist_new(&st.root, spec) {
        Ok(run) => run,
        Err(e) => return error_resp(500, &format!("create run: {e}")),
    };
    // Store row + audit BEFORE the worker spawns (stage rows FK the run row).
    let ctx = if let Some(store) = &st.store {
        let candidate = serde_json::to_value(&run.spec.candidate_spec).unwrap_or_default();
        let params = serde_json::json!({ "workload": run.spec.workload });
        if let Err(e) = store
            .insert_run(
                &run.run_id,
                runs::mode_str(run.spec.mode),
                run.spec.recording_id.as_deref(),
                &candidate,
                &params,
                expectation.as_deref(),
                &actor,
            )
            .await
        {
            eprintln!("replay-harness-api: store insert_run failed: {e}");
        }
        let _ = store
            .audit(
                &actor,
                "run.create",
                "run",
                &run.run_id,
                &serde_json::json!({ "spec": run.spec, "expectation": expectation }),
            )
            .await;
        replay_harness_api::lifecycle::StoreCtx::new(
            &run.run_id,
            Some((tokio::runtime::Handle::current(), store.clone())),
        )
    } else {
        replay_harness_api::lifecycle::StoreCtx::disabled(&run.run_id)
    };
    runs::spawn_worker(&st.root, &run.run_id, ctx);
    json_ok(
        serde_json::to_value(&runs::CreateRunResponse {
            run_id: run.run_id,
            status: run.status,
        })
        .unwrap_or_default(),
    )
}

/// `GET /api/v1/recordings` — the recordings catalog (Postgres-backed).
async fn v1_list_recordings(State(st): State<AppState>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_recordings(200).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("list recordings: {e}")),
    }
}

/// `GET /api/v1/runs` — run list (Postgres-backed; newest first).
async fn v1_list_runs(State(st): State<AppState>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_runs(200).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("list runs: {e}")),
    }
}

/// The worker's live file-store snapshot as the `live` sub-object.
fn live_json(live: &Run) -> serde_json::Value {
    serde_json::json!({
        "status": live.status,
        "stage": live.stage,
        "step": live.step,
        "steps_total": live.steps_total,
        "stage_updated_ms": live.stage_updated_ms,
        "failure_reason": live.failure_reason,
        "candidate_image": live.candidate_image,
    })
}

/// `GET /api/v1/runs/{id}` — store row + live file-store snapshot merged: the
/// row carries dashboard fields (verdict, expectation, candidate sha, actor),
/// the snapshot carries the worker's live stage/step (file store is the
/// worker's source of truth mid-run). Degrades to the snapshot alone when the
/// store is down, so script polling works file-only too.
async fn v1_get_run(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let row = match &st.store {
        Some(store) => match store.get_run(&id).await {
            Ok(row) => row,
            Err(e) => return error_resp(500, &format!("get run: {e}")),
        },
        None => None,
    };
    let live = runs::get(&st.root, &id).ok();
    let mut body = match (row, &live) {
        (Some(row), _) => serde_json::to_value(&row).unwrap_or_default(),
        (None, Some(live)) => serde_json::json!({
            "run_id": live.run_id,
            "state": format!("{:?}", live.status).to_lowercase(),
            "recording_id": live.recording_id,
        }),
        (None, None) => return error_resp(404, "run not found"),
    };
    if let Some(live) = &live {
        body["live"] = live_json(live);
    }
    json_ok(body)
}

/// `GET /api/v1/runs/{id}/scorecard` — compute + serve the divergence scorecard.
async fn v1_scorecard(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    match divergence::scorecard(&st.root, &id) {
        Ok(card) => json_ok(serde_json::to_value(&card).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("scorecard: {e}")),
    }
}

/// `GET /api/v1/runs/{id}/calls` — the per-call divergence ledger (recorded vs
/// observed, classified + located) that backs the interactive diff view.
/// Read-through: recomputes from artifacts so it works for older runs too.
async fn v1_calls(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    match divergence::call_ledger(&st.root, &id) {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("call ledger: {e}")),
    }
}

/// `GET /api/v1/runs/{id}/http-diffs` — the kernel's per-request HTTP diffs
/// (status + field-level body diff), parsed from the run's http-diff stream.
async fn v1_http_diffs(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let rows: Vec<serde_json::Value> = std::fs::read_to_string(st.root.http_diff_path(&id))
        .map(|c| {
            c.lines()
                .filter(|l| !l.trim().is_empty())
                .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                .collect()
        })
        .unwrap_or_default();
    json_ok(serde_json::Value::Array(rows))
}

/// `GET /api/v1/runs/{id}/graph` — the record-side and replay-side execution
/// graphs (raw nodes) for the cascade/tree view. The UI builds the tree from
/// node_id/parent_id and hangs boundary events off nodes via graph_node_id
/// (recorded events + the call ledger's observed side).
async fn v1_graph(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    // recording_id comes from the run record (replay) or the run itself.
    let rec = runs::get(&st.root, &id)
        .ok()
        .and_then(|r| r.recording_id.or(r.spec.recording_id));
    let read_nodes = |path: std::path::PathBuf| -> Vec<serde_json::Value> {
        std::fs::read_to_string(&path)
            .map(|c| {
                c.lines()
                    .filter(|l| !l.trim().is_empty())
                    .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                    .collect()
            })
            .unwrap_or_default()
    };
    let record = rec
        .as_deref()
        .map(|r| read_nodes(st.root.graph_record_dir(r).join("execution-graph.jsonl")))
        .unwrap_or_default();
    let replay = read_nodes(st.root.graph_replay_dir(&id).join("execution-graph.jsonl"));
    json_ok(serde_json::json!({ "record": record, "replay": replay }))
}

/// `GET /api/v1/runs/{id}/stages` — append-only stage history.
async fn v1_run_stages(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_stages(&id).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("list stages: {e}")),
    }
}

#[derive(serde::Deserialize)]
struct LogsQuery {
    stage: Option<String>,
    #[serde(default)]
    after_seq: i64,
}

/// `GET /api/v1/runs/{id}/logs?stage=&after_seq=` — persisted worker logs.
async fn v1_run_logs(
    State(st): State<AppState>,
    Path(id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<LogsQuery>,
) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_logs(&id, q.stage.as_deref(), q.after_seq).await {
        Ok(rows) => {
            let body: Vec<serde_json::Value> = rows
                .into_iter()
                .map(|(stage, seq, lines)| {
                    serde_json::json!({ "stage": stage, "seq": seq, "lines": lines })
                })
                .collect();
            json_ok(serde_json::Value::Array(body))
        }
        Err(e) => error_resp(500, &format!("list logs: {e}")),
    }
}

/// `GET /api/v1/runs/{id}/artifacts` — registered artifacts for a run.
async fn v1_run_artifacts(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_artifacts(&id).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("list artifacts: {e}")),
    }
}

/// `GET /api/v1/artifacts/{id}/raw` — stream a registered artifact file.
/// HTML renders inline (the embedded visualization); JSONL downloads as ndjson.
async fn v1_artifact_raw(State(st): State<AppState>, Path(id): Path<i64>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let art = match store.get_artifact(id).await {
        Ok(Some(a)) => a,
        Ok(None) => return error_resp(404, "artifact not found"),
        Err(e) => return error_resp(500, &format!("get artifact: {e}")),
    };
    let content_type = if art.kind == "visualization_html" {
        "text/html; charset=utf-8"
    } else if art.uri.ends_with(".json") {
        "application/json"
    } else {
        "application/x-ndjson"
    };
    match std::fs::read(&art.uri) {
        Ok(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, content_type)],
            bytes,
        )
            .into_response(),
        Err(e) => error_resp(404, &format!("artifact file unreadable: {e}")),
    }
}

/// `GET /api/v1/audit` — the append-only audit log (newest first).
async fn v1_audit(State(st): State<AppState>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.audit_list(500).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("audit list: {e}")),
    }
}

/// `GET /api/v1/runs/{id}/stream` — SSE run progress.
///
/// Emits a `run` event with the full run snapshot whenever it changes, then a
/// terminal `done` event once the run reaches a terminal status. Implemented
/// as a store poll (500ms) so it is backend-agnostic: identical behavior over
/// the file store today and the Postgres store later (which can tighten it to
/// LISTEN/NOTIFY wake-ups without changing the wire contract).
async fn run_stream(
    State(st): State<AppState>,
    Path(run_id): Path<String>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        let mut last: Option<String> = None;
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            let run: Run = match runs::get(&st.root, &run_id) {
                Ok(r) => r,
                Err(_) => {
                    yield Ok(Event::default().event("error").data(
                        serde_json::json!({ "error": "run not found" }).to_string(),
                    ));
                    break;
                }
            };
            let snapshot = serde_json::to_string(&run).unwrap_or_default();
            if last.as_deref() != Some(snapshot.as_str()) {
                last = Some(snapshot.clone());
                yield Ok(Event::default().event("run").data(snapshot));
            }
            if matches!(run.status, RunStatus::Completed | RunStatus::Failed) {
                yield Ok(Event::default().event("done").data(
                    serde_json::json!({ "status": run.status }).to_string(),
                ));
                break;
            }
        }
    };
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ka"),
    )
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn json_ok(value: serde_json::Value) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_vec(&value).unwrap_or_default(),
    )
        .into_response()
}

fn error_resp(status: u16, msg: &str) -> Response {
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::json!({ "error": msg }).to_string(),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Embedded dashboard
// ---------------------------------------------------------------------------

async fn spa_fallback(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let (name, asset) = match WebAssets::get(path) {
        Some(a) if !path.is_empty() => (path, a),
        _ => match WebAssets::get("index.html") {
            Some(a) => ("index.html", a),
            None => return error_resp(404, "dashboard not built"),
        },
    };
    let mime = mime_guess::from_path(name).first_or_octet_stream();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, mime.as_ref().to_owned())],
        asset.data.into_owned(),
    )
        .into_response()
}
