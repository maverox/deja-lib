//! Run lifecycle worker (Phase B of the capstone demo).
//!
//! `api::runs::create` persists a Pending run and spawns [`drive`] on a
//! background thread. The worker advances the run's status and orchestrates the
//! demo by shelling out to `docker compose` (which builds the candidate image),
//! pulling the recording back out of MinIO (the full Kafka→S3→replay loop), and
//! calling the in-process lookup renderer + divergence detector.
//!
//! It reuses Hyperswitch's OWN compose (`vendor/.../docker-compose.yml`) plus a
//! thin overlay (`docker-compose.deja.yml`) that swaps the router to a local
//! deja build and adds MinIO + a replay service; HS's kafka0 and vector are
//! reused as-is. Profiled services (kafka0, vector) are started BY NAME so the
//! heavy olap stack (opensearch/clickhouse) is not pulled in. The worker does
//! NOT tear the stack down; the one-click script owns teardown so MinIO persists
//! between the record run and the replay run.
//!
//! Runtime config (env, with demo defaults):
//!   DEMO_COMPOSE_BASE    HS compose (default vendor/hyperswitch-deja-clean/docker-compose.yml)
//!   DEMO_COMPOSE_OVERLAY deja overlay (default vendor/hyperswitch-deja-clean/docker-compose.deja.yml)
//!   DEMO_PROJECT         docker compose project name (default deja-demo)
//!   DEMO_REPLAY_PORT     host port for the replay candidate (default 8090; the
//!                        only host-published port — the host kernel hits it)
//!   DEMO_KERNEL_BIN      replay-harness-kernel binary (default target/release/replay-harness-kernel)
//!   DEMO_KAFKA_TOPIC     recording topic (default hyperswitch-deja-recording-events)
//!   STRIPE_API_KEY       forwarded to the record workload (steps 7 & 9)

use std::io::BufRead;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::{read_json, write_json, CandidateSpec, HarnessRoot, Run, RunMode, RunStatus};

pub mod store_ctx;
pub use store_ctx::StoreCtx;

/// Resolved runtime configuration for the demo orchestration.
struct Demo {
    compose_base: String,
    compose_overlay: String,
    project: String,
    replay_port: u16,
    kernel_bin: String,
    topic: String,
    harness_state: String,
    /// Image tag for the candidate services; defaults to the overlay's local
    /// build, overridden when a `local_binary` candidate is baked per-run.
    candidate_image: Option<String>,
}

impl Demo {
    fn from_env(root: &HarnessRoot) -> Self {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_owned());
        Self {
            compose_base: env(
                "DEMO_COMPOSE_BASE",
                "vendor/hyperswitch-deja-clean/docker-compose.yml",
            ),
            compose_overlay: env(
                "DEMO_COMPOSE_OVERLAY",
                "vendor/hyperswitch-deja-clean/docker-compose.deja.yml",
            ),
            project: env("DEMO_PROJECT", "deja-demo"),
            replay_port: env("DEMO_REPLAY_PORT", "8090").parse().unwrap_or(8090),
            kernel_bin: env("DEMO_KERNEL_BIN", "target/release/replay-harness-kernel"),
            topic: env("DEMO_KAFKA_TOPIC", "hyperswitch-deja-recording-events"),
            harness_state: root.root.display().to_string(),
            candidate_image: None,
        }
    }

    /// `docker compose -p <project> -f <base> -f <overlay>` prefix.
    fn compose_base_args(&self) -> Vec<String> {
        vec![
            "compose".into(),
            "-p".into(),
            self.project.clone(),
            "-f".into(),
            self.compose_base.clone(),
            "-f".into(),
            self.compose_overlay.clone(),
        ]
    }

    /// Common env every compose invocation needs for `${VAR}` interpolation.
    fn compose_env(&self, recording_id: &str, run_id: &str) -> Vec<(String, String)> {
        vec![
            ("RUN_ID".into(), run_id.to_owned()),
            ("RECORDING_ID".into(), recording_id.to_owned()),
            ("HARNESS_STATE".into(), self.harness_state.clone()),
            ("DEJA_RECORDING_TOPIC".into(), self.topic.clone()),
            ("REPLAY_HOST_PORT".into(), self.replay_port.to_string()),
            (
                "STRIPE_API_KEY".into(),
                std::env::var("STRIPE_API_KEY").unwrap_or_default(),
            ),
            (
                "CANDIDATE_IMAGE".into(),
                self.candidate_image
                    .clone()
                    .unwrap_or_else(|| "deja-router-local:latest".to_owned()),
            ),
            // Code identity for the envelope's `code.sha` (resolved by the
            // demo script from the vendor git head; empty when unknown).
            (
                "DEJA_CODE_REF".into(),
                std::env::var("DEJA_CODE_REF").unwrap_or_default(),
            ),
        ]
    }
}

/// Entry point spawned by the run-creation handler on a background thread.
pub fn drive(root: &HarnessRoot, run_id: &str, ctx: &StoreCtx) {
    let mut run = match read_json::<Run>(&root.run_path(run_id)) {
        Ok(run) => run,
        Err(e) => {
            eprintln!("lifecycle: cannot read run {run_id}: {e}");
            return;
        }
    };
    let mut demo = Demo::from_env(root);
    if let Err(e) = resolve_candidate(&mut demo, root, &mut run, ctx) {
        eprintln!("lifecycle: run {run_id} failed: {e}");
        ctx.finish(false, Some(&e));
        set_status(root, &mut run, RunStatus::Failed, Some(e));
        return;
    }
    let outcome = match run.spec.mode {
        RunMode::Record => drive_record(root, &demo, &mut run, ctx),
        RunMode::Replay => drive_replay(root, &demo, &mut run, ctx),
    };
    match outcome {
        Ok(()) => {
            ctx.finish(true, None);
            set_status(root, &mut run, RunStatus::Completed, None);
        }
        Err(e) => {
            eprintln!("lifecycle: run {run_id} failed: {e}");
            ctx.log("failure", &e);
            ctx.finish(false, Some(&e));
            set_status(root, &mut run, RunStatus::Failed, Some(e));
        }
    }
}

// ---------------------------------------------------------------------------
// Candidate resolution
// ---------------------------------------------------------------------------

/// Resolve the run's `CandidateSpec` into the image tag compose will use.
///
/// - `PrebuiltImage` keeps the legacy behavior: the overlay's default image,
///   built by compose itself (`--build`).
/// - `LocalPath` ("paste a router binary path" — the Phase 1 web-matrix form):
///   validate the binary, sha256 it (the UI's compile-neutral signal), stage a
///   minimal docker context, bake `deja-candidate:<run8>`, and point compose at
///   it (the overlay's `image: ${CANDIDATE_IMAGE:-…}`). Build-from-ref
///   variants land with M3.
fn resolve_candidate(
    demo: &mut Demo,
    root: &HarnessRoot,
    run: &mut Run,
    ctx: &StoreCtx,
) -> Result<(), String> {
    let CandidateSpec::LocalPath { binary_or_source } = &run.spec.candidate_spec else {
        return Ok(()); // legacy paths (prebuilt image / compose build)
    };
    let binary = binary_or_source.clone();
    ctx.stage("resolving candidate binary", 0, 0);

    let bytes = std::fs::read(&binary)
        .map_err(|e| format!("candidate binary {}: {e}", binary.display()))?;
    if bytes.len() < 20 || &bytes[0..4] != b"\x7fELF" {
        return Err(format!(
            "candidate {} is not an ELF executable",
            binary.display()
        ));
    }
    // e_machine (offset 18, LE): 62 = x86-64 — the demo stack is linux/amd64.
    let e_machine = u16::from_le_bytes([bytes[18], bytes[19]]);
    if e_machine != 62 {
        return Err(format!(
            "candidate {} is not x86_64 (e_machine={e_machine})",
            binary.display()
        ));
    }
    let sha256 = {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(&bytes);
        hex::encode(h.finalize())
    };
    ctx.candidate_sha(&sha256);
    let msg = format!(
        "candidate binary {} ({} bytes, sha256 {})",
        binary.display(),
        bytes.len(),
        &sha256[..12]
    );
    eprintln!("lifecycle: {msg}");
    ctx.log("resolving candidate binary", &msg);

    // Stage a minimal, self-contained build context (no repo-root context, no
    // .dockerignore coupling): the candidate Dockerfile pattern of
    // demo/Dockerfile.hyperswitch-semantic with the binary COPY'd in place.
    let stage_dir = root.candidate_stage_dir(&run.run_id);
    std::fs::create_dir_all(&stage_dir).map_err(|e| format!("stage dir: {e}"))?;
    std::fs::write(stage_dir.join("router"), &bytes).map_err(|e| format!("stage binary: {e}"))?;
    for (src, name) in [
        ("demo/workload.sh", "workload.sh"),
        ("demo/superposition_seed.toml", "superposition_seed.toml"),
    ] {
        std::fs::copy(src, stage_dir.join(name))
            .map_err(|e| format!("stage {name} (run from the repo root): {e}"))?;
    }
    std::fs::write(stage_dir.join("Dockerfile"), CANDIDATE_DOCKERFILE)
        .map_err(|e| format!("stage Dockerfile: {e}"))?;

    let short = run.run_id.rsplit('-').next().unwrap_or("cand");
    let tag = format!("deja-candidate:{short}");
    let mut cmd = Command::new("docker");
    cmd.args(["build", "-t", &tag, "."]).current_dir(&stage_dir);
    let status = run_streamed(cmd, ctx, "resolving candidate binary", "docker build")?;
    if !status.success() {
        return Err(format!("candidate image build failed (status {status})"));
    }
    run.candidate_image = Some(crate::CandidateImage {
        docker_image: tag.clone(),
        source_ref: binary.display().to_string(),
    });
    write_json(&root.run_path(&run.run_id), run).map_err(|e| format!("persist run: {e}"))?;
    demo.candidate_image = Some(tag);
    Ok(())
}

const CANDIDATE_DOCKERFILE: &str = r#"FROM --platform=linux/amd64 debian:trixie-slim
RUN apt-get update     && apt-get install -y --no-install-recommends        libpq5 libssl3 zlib1g ca-certificates curl jq bc procps openssl     && rm -rf /var/lib/apt/lists/*
COPY router /local/bin/router
RUN chmod +x /local/bin/router
COPY workload.sh /workload.sh
RUN chmod +x /workload.sh
COPY superposition_seed.toml /local/config/superposition_seed.toml
WORKDIR /local
ENTRYPOINT ["/local/bin/router"]
CMD ["-f", "/local/config/docker_compose.toml"]
"#;

fn set_status(root: &HarnessRoot, run: &mut Run, status: RunStatus, failure: Option<String>) {
    run.status = status;
    run.failure_reason = failure;
    if let Err(e) = write_json(&root.run_path(&run.run_id), run) {
        eprintln!(
            "lifecycle: failed to persist status for {}: {e}",
            run.run_id
        );
    }
}

/// Update the human-facing progress (step `step`/`total`, labelled `label`) and
/// persist it so `GET /runs/{id}` clients can render a live progress bar.
fn set_stage(
    root: &HarnessRoot,
    run: &mut Run,
    ctx: &StoreCtx,
    step: u32,
    total: u32,
    label: &str,
) {
    run.step = step;
    run.steps_total = total;
    run.stage = Some(label.to_owned());
    run.stage_updated_ms = crate::now_ms();
    eprintln!("lifecycle: [{step}/{total}] {label}");
    ctx.stage(label, step, total);
    if let Err(e) = write_json(&root.run_path(&run.run_id), run) {
        eprintln!("lifecycle: failed to persist stage for {}: {e}", run.run_id);
    }
}

/// Run a child process streaming its stdout+stderr line-by-line to BOTH the
/// console (live script UX preserved) and the run's persisted log chunks
/// (batched 25 lines per row to keep insert volume sane on docker builds).
fn run_streamed(
    mut cmd: Command,
    ctx: &StoreCtx,
    stage: &str,
    label: &str,
) -> Result<std::process::ExitStatus, String> {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("spawn {label}: {e}"))?;

    let mut readers = Vec::new();
    for pipe in [
        child
            .stdout
            .take()
            .map(|p| Box::new(p) as Box<dyn std::io::Read + Send>),
        child
            .stderr
            .take()
            .map(|p| Box::new(p) as Box<dyn std::io::Read + Send>),
    ]
    .into_iter()
    .flatten()
    {
        let ctx = ctx.clone();
        let stage = stage.to_owned();
        readers.push(thread::spawn(move || {
            let reader = std::io::BufReader::new(pipe);
            let mut batch: Vec<String> = Vec::with_capacity(25);
            for line in reader.lines().map_while(Result::ok) {
                eprintln!("{line}");
                batch.push(line);
                if batch.len() >= 25 {
                    ctx.log(&stage, &batch.join("\n"));
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                ctx.log(&stage, &batch.join("\n"));
            }
        }));
    }
    let status = child.wait().map_err(|e| format!("wait {label}: {e}"))?;
    for r in readers {
        let _ = r.join();
    }
    Ok(status)
}

// ---------------------------------------------------------------------------
// Record: bring up the stack, drive the workload, pull the recording from MinIO
// ---------------------------------------------------------------------------

fn drive_record(
    root: &HarnessRoot,
    demo: &Demo,
    run: &mut Run,
    ctx: &StoreCtx,
) -> Result<(), String> {
    let recording_id = run
        .spec
        .recording_id
        .clone()
        .or_else(|| run.recording_id.clone())
        .unwrap_or_else(|| run.run_id.clone());
    run.recording_id = Some(recording_id.clone());
    ctx.run_recording(&recording_id);
    let _ = std::fs::create_dir_all(root.graph_record_dir(&recording_id));

    let total = 6;
    set_status(root, run, RunStatus::Building, None);
    ctx.run_state("building");
    // Kafka FIRST and wait until it actually accepts connections: HS's event
    // handler (events.source=kafka) connects at boot and aborts the router if the
    // broker isn't ready. (A compose depends_on can't be used — kafka0 is in the
    // olap profile, which a non-profiled service may not depend on.)
    set_stage(
        root,
        run,
        ctx,
        1,
        total,
        "building images + starting kafka/minio",
    );
    compose_up(
        demo,
        ctx,
        "building images + starting kafka/minio",
        &recording_id,
        &run.run_id,
        &["kafka0", "minio", "minio-setup"],
        run.candidate_image.is_none(),
    )?;

    set_stage(
        root,
        run,
        ctx,
        2,
        total,
        "waiting for kafka broker to be ready",
    );
    wait_kafka_ready(demo, &recording_id, Duration::from_secs(150))?;

    set_stage(
        root,
        run,
        ctx,
        3,
        total,
        "starting record router (DEJA_MODE=record)",
    );
    compose_up(
        demo,
        ctx,
        "starting record router (DEJA_MODE=record)",
        &recording_id,
        &run.run_id,
        &["vector", "hyperswitch-server"],
        run.candidate_image.is_none(),
    )?;
    set_status(root, run, RunStatus::Running, None);
    ctx.run_state("running");
    // record candidate isn't published to the host; check health from inside.
    wait_health_exec(
        demo,
        &recording_id,
        "hyperswitch-server",
        Duration::from_secs(240),
    )?;

    set_stage(
        root,
        run,
        ctx,
        4,
        total,
        "driving payment workload (HS → Kafka → Vector → MinIO)",
    );
    run_workload(demo, ctx, &recording_id, run_iterations(run))?;

    // Graceful stop of the record router BEFORE the landing wait: SIGTERM →
    // hook drop → writer shutdown flush → producer drain → `eof` sink marker.
    // Without this the eof only fires at compose-down, after the seal.
    set_stage(
        root,
        run,
        ctx,
        5,
        total,
        "stopping record router (flush + eof)",
    );
    stop_service(demo, &recording_id, "hyperswitch-server");

    set_stage(
        root,
        run,
        ctx,
        5,
        total,
        "waiting for recording to land in MinIO (S3)",
    );
    // The full 9-step Stripe workload keeps producing events while this stage is
    // already counting down, then the router→Kafka→Vector→S3 drain adds a tail
    // (Vector batches every 5s). Observed first-object latency is ~60s, so give
    // a comfortable budget; the stable-count check returns early once the flush
    // settles, so a healthy run does NOT wait the whole window.
    wait_s3_objects(&recording_id, Duration::from_secs(180))?;

    set_stage(
        root,
        run,
        ctx,
        6,
        total,
        "compacting + pulling session from S3",
    );
    pull_recording(root, ctx, &recording_id)?;

    // Register what this run produced. The execution graph lands directly in
    // the bind-mounted state dir (DEJA_GRAPH_DIR=/harness-state/graph/<id>).
    ctx.artifact(
        Some(&recording_id),
        "events",
        &root.recording_events_path(&recording_id),
    );
    ctx.artifact(
        Some(&recording_id),
        "graph",
        &root
            .graph_record_dir(&recording_id)
            .join("execution-graph.jsonl"),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Replay: pull recording from MinIO, render lookup table, drive kernel, score
// ---------------------------------------------------------------------------

fn drive_replay(
    root: &HarnessRoot,
    demo: &Demo,
    run: &mut Run,
    ctx: &StoreCtx,
) -> Result<(), String> {
    let recording_id = run
        .spec
        .recording_id
        .clone()
        .or_else(|| run.recording_id.clone())
        .ok_or_else(|| "replay run requires recording_id".to_string())?;
    run.recording_id = Some(recording_id.clone());
    ctx.run_recording(&recording_id);
    let _ = std::fs::create_dir_all(root.graph_replay_dir(&run.run_id));

    let total = 6;
    set_status(root, run, RunStatus::Resolving, None);
    ctx.run_state("resolving");
    // Full loop: the recording comes back out of MinIO. (If a prior record run
    // on this host already pulled it to disk, reuse that.)
    set_stage(
        root,
        run,
        ctx,
        1,
        total,
        "pulling recording from MinIO (S3)",
    );
    let recording_path = root.recording_events_path(&recording_id);
    if !recording_path.exists() {
        pull_recording(root, ctx, &recording_id)?;
    }
    if !recording_path.exists() {
        return Err(format!(
            "recording {recording_id} not found in S3 or on disk"
        ));
    }

    // Render the lookup table (whole-document JSON; round-trips through both the
    // candidate's LocalFileLookupSource and the divergence detector).
    set_stage(root, run, ctx, 2, total, "rendering lookup table");
    let table = crate::lookup::render_lookup_table(&recording_path, &recording_id, 1)
        .map_err(|e| format!("render lookup table: {e}"))?;
    write_json(&root.lookup_table_path(&run.run_id), &table)
        .map_err(|e| format!("write lookup table: {e}"))?;
    if table.entries.is_empty() {
        return Err("rendered lookup table is empty".to_string());
    }

    set_status(root, run, RunStatus::Building, None);
    ctx.run_state("building");
    // Replay candidate; pg/redis/migration/superposition-init come up as deps.
    set_stage(
        root,
        run,
        ctx,
        3,
        total,
        "starting replay router (DEJA_MODE=replay)",
    );
    compose_up(
        demo,
        ctx,
        "starting replay router (DEJA_MODE=replay)",
        &recording_id,
        &run.run_id,
        &["hyperswitch-replay"],
        run.candidate_image.is_none(),
    )?;

    set_status(root, run, RunStatus::Running, None);
    ctx.run_state("running");
    set_stage(root, run, ctx, 4, total, "waiting for replay router");
    wait_health(demo.replay_port, Duration::from_secs(240))?;

    set_stage(
        root,
        run,
        ctx,
        5,
        total,
        "driving recorded requests (kernel)",
    );
    // Reset redis to the empty state the record run started from (post `down -v`).
    // Redis is record-only — its reads are served LIVE on replay, not substituted —
    // and some cache keys the record run wrote carry no TTL (e.g.
    // `merchant_key_store_*`). Without this flush, the FIRST replayed request whose
    // recording observed a cache MISS instead reads a STALE HIT and diverges
    // (signup's merchant-existence check finds the key store the record run wrote →
    // short-circuits → "merchant already exists" / UR_15). The in-memory moka cache
    // is already fresh per replay process; only redis carries record's writes over.
    flush_redis(demo, &recording_id, &run.run_id)?;
    run_kernel(demo, root, ctx, &recording_id, &run.run_id)?;

    set_stage(root, run, ctx, 6, total, "scoring divergence (byte-exact)");
    let card = crate::divergence::detect_and_score(root, &run.run_id)
        .map_err(|e| format!("score: {e}"))?;
    let verdict_line = format!(
        "run {} verdict pass={} ({})",
        run.run_id, card.verdict.pass, card.verdict.reason
    );
    eprintln!("lifecycle: {verdict_line}");
    ctx.log("scoring divergence (byte-exact)", &verdict_line);
    let verdict = if card.verdict.inconclusive {
        "inconclusive"
    } else if card.verdict.pass {
        "pass"
    } else {
        "fail"
    };
    ctx.result(Some(verdict), serde_json::to_value(&card).ok().as_ref());

    // Register replay artifacts (best-effort; absent files are skipped).
    ctx.artifact(
        Some(&recording_id),
        "lookup_table",
        &root.lookup_table_path(&run.run_id),
    );
    ctx.artifact(
        Some(&recording_id),
        "observed",
        &root.observed_path(&run.run_id),
    );
    ctx.artifact(
        Some(&recording_id),
        "http_diffs",
        &root.http_diff_path(&run.run_id),
    );
    ctx.artifact(
        Some(&recording_id),
        "scorecard",
        &root.scorecard_path(&run.run_id),
    );
    ctx.artifact(
        Some(&recording_id),
        "call_ledger",
        &root.call_ledger_path(&run.run_id),
    );
    ctx.artifact(
        Some(&recording_id),
        "graph_replay",
        &root
            .graph_replay_dir(&run.run_id)
            .join("execution-graph.jsonl"),
    );
    // Static HTML visualization (the demo's existing visualize-replay.py);
    // best-effort — python3 may be absent.
    let viz = root.root.join("replay-visualization.html");
    let viz_ok = Command::new("python3")
        .args(["demo/visualize-replay.py", &root.root.display().to_string()])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if viz_ok {
        ctx.artifact(Some(&recording_id), "visualization_html", &viz);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shell-out helpers
// ---------------------------------------------------------------------------

fn run_iterations(run: &Run) -> u64 {
    run.spec
        .workload
        .get("iterations")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
}

#[allow(clippy::too_many_arguments)] // worker plumbing, internal
fn compose_up(
    demo: &Demo,
    ctx: &StoreCtx,
    stage: &str,
    recording_id: &str,
    run_id: &str,
    services: &[&str],
    build: bool,
) -> Result<(), String> {
    let mut args = demo.compose_base_args();
    args.extend(["up".into(), "-d".into()]);
    // A baked `local_binary` candidate image must NOT be rebuilt by compose:
    // `--build` would re-run the overlay's build context and re-tag over it.
    if build {
        args.push("--build".into());
    }
    args.extend(services.iter().map(|s| s.to_string()));
    let cmdline = format!("docker {}", args.join(" "));
    eprintln!("lifecycle: {cmdline}");
    ctx.log(stage, &cmdline);
    let mut cmd = Command::new("docker");
    cmd.args(&args).envs(demo.compose_env(recording_id, run_id));
    let status = run_streamed(cmd, ctx, stage, "docker compose up")?;
    if !status.success() {
        return Err(format!("docker compose up failed (status {status})"));
    }
    Ok(())
}

/// `docker compose exec -T redis-standalone redis-cli FLUSHALL` — wipe the
/// candidate's redis so the replay run begins from the same empty cache the
/// record run started with. See the call site in `drive_replay` for why this is
/// required for byte-exact self-replay. Best-effort: if redis isn't reachable
/// (e.g. a deployment without the standalone service) the flush is skipped
/// rather than failing the whole replay.
fn flush_redis(demo: &Demo, recording_id: &str, run_id: &str) -> Result<(), String> {
    let mut args = demo.compose_base_args();
    args.extend(
        ["exec", "-T", "redis-standalone", "redis-cli", "FLUSHALL"]
            .iter()
            .map(|s| s.to_string()),
    );
    eprintln!("lifecycle: docker {}", args.join(" "));
    match Command::new("docker")
        .args(&args)
        .envs(demo.compose_env(recording_id, run_id))
        .status()
    {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            eprintln!("lifecycle: redis FLUSHALL exited {status}; continuing (best-effort)");
            Ok(())
        }
        Err(e) => {
            eprintln!("lifecycle: could not run redis FLUSHALL: {e}; continuing (best-effort)");
            Ok(())
        }
    }
}

fn run_workload(
    demo: &Demo,
    ctx: &StoreCtx,
    recording_id: &str,
    iterations: u64,
) -> Result<(), String> {
    let mut args = demo.compose_base_args();
    args.extend(
        [
            "exec",
            "-T",
            "-e",
            "BASE_URL=http://127.0.0.1:8080",
            "-e",
            "ADMIN_API_KEY=test_admin",
            "-e",
            "WORKLOAD_REQUIRE_CONFIRM_SUCCESS=true",
            "-e",
            "WORKLOAD_FAIL_ON_ANY_ERROR=true",
            "hyperswitch-server",
            "/workload.sh",
        ]
        .iter()
        .map(|s| s.to_string()),
    );
    args.push(iterations.to_string());
    let mut cmd = Command::new("docker");
    cmd.args(&args).envs(demo.compose_env(recording_id, ""));
    let status = run_streamed(
        cmd,
        ctx,
        "driving payment workload (HS → Kafka → Vector → MinIO)",
        "workload",
    )?;
    if !status.success() {
        return Err(format!("workload failed (status {status})"));
    }
    Ok(())
}

/// Graceful `docker compose stop <service>` (best-effort): the router's
/// SIGTERM handler drops the recording hook, whose writer shutdown flushes
/// the Kafka producer and emits the `eof` sink marker.
fn stop_service(demo: &Demo, recording_id: &str, service: &str) {
    let mut args = demo.compose_base_args();
    args.extend(
        ["stop", "--timeout", "30", service]
            .iter()
            .map(|s| s.to_string()),
    );
    match Command::new("docker")
        .args(&args)
        .envs(demo.compose_env(recording_id, ""))
        .output()
    {
        Ok(o) if o.status.success() => eprintln!("lifecycle: stopped {service}"),
        Ok(o) => eprintln!(
            "lifecycle: stop {service} failed (continuing): {}",
            String::from_utf8_lossy(&o.stderr)
        ),
        Err(e) => eprintln!("lifecycle: stop {service} failed (continuing): {e}"),
    }
}

fn run_kernel(
    demo: &Demo,
    root: &HarnessRoot,
    ctx: &StoreCtx,
    recording_id: &str,
    run_id: &str,
) -> Result<(), String> {
    let recording_path = root.recording_events_path(recording_id);
    let diff_sink = root.http_diff_path(run_id);
    let mut cmd = Command::new(&demo.kernel_bin);
    cmd.env("KERNEL_RECORDING_PATH", &recording_path)
        .env("KERNEL_TARGET_HOST", "127.0.0.1")
        .env("KERNEL_TARGET_PORT", demo.replay_port.to_string())
        .env("KERNEL_HTTP_DIFF_SINK", &diff_sink);
    // empty allowlist by default = byte-exact gate; override via
    // KERNEL_BODY_ALLOWLIST on the harness-api process during bring-up.
    let status = run_streamed(cmd, ctx, "driving recorded requests (kernel)", "kernel")?;
    if !status.success() {
        return Err(format!("kernel failed (status {status})"));
    }
    Ok(())
}

/// Poll a candidate's `/health` from INSIDE the container via `docker compose
/// exec` — for services not published to the host (the record candidate). Fails
/// FAST (with container logs) if the container has exited, instead of spinning
/// until the timeout.
fn wait_health_exec(
    demo: &Demo,
    recording_id: &str,
    service: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut args = demo.compose_base_args();
        args.extend(
            [
                "exec",
                "-T",
                service,
                "curl",
                "-fsS",
                "-o",
                "/dev/null",
                "--max-time",
                "3",
                "http://localhost:8080/health",
            ]
            .iter()
            .map(|s| s.to_string()),
        );
        match Command::new("docker")
            .args(&args)
            .envs(demo.compose_env(recording_id, ""))
            .output()
        {
            Ok(o) if o.status.success() => return Ok(()),
            Ok(o) => {
                let err = String::from_utf8_lossy(&o.stderr);
                // Container exited → no point waiting; surface the crash logs now.
                if err.contains("is not running") || err.contains("no such service") {
                    return Err(format!(
                        "{service} exited during boot. Recent logs:\n{}",
                        tail_logs(demo, service)
                    ));
                }
                // otherwise: still booting (connection refused) — keep waiting
            }
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{service} not healthy within timeout. Recent logs:\n{}",
                tail_logs(demo, service)
            ));
        }
        thread::sleep(Duration::from_secs(2));
    }
}

/// Wait until kafka0 actually accepts connections (cp-kafka logs "Started" well
/// before it is ready). Uses the broker's own CLI over the internal listener.
fn wait_kafka_ready(demo: &Demo, recording_id: &str, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut args = demo.compose_base_args();
        args.extend(
            [
                "exec",
                "-T",
                // Blank JMX for the CLI: the image sets JMX_PORT=9997 for the
                // BROKER, but every kafka CLI is also a JVM that would try to
                // re-bind 9997 (already held by the broker) and die before
                // contacting it. These overrides apply only to this process.
                "-e",
                "JMX_PORT=",
                "-e",
                "KAFKA_JMX_OPTS=",
                "kafka0",
                "kafka-topics",
                "--bootstrap-server",
                // PLAINTEXT_HOST listener binds 0.0.0.0:9092 → reachable via
                // loopback inside the container (the 29092 listener is bound to
                // the kafka0 interface, not localhost).
                "localhost:9092",
                "--list",
            ]
            .iter()
            .map(|s| s.to_string()),
        );
        let ok = Command::new("docker")
            .args(&args)
            .envs(demo.compose_env(recording_id, ""))
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if ok {
            eprintln!("lifecycle: kafka0 ready");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("kafka0 not ready within timeout".to_string());
        }
        thread::sleep(Duration::from_secs(3));
    }
}

/// Last ~60 log lines for a service (used to surface boot crashes in the
/// run's failure_reason so the next iteration doesn't need a manual `logs`).
fn tail_logs(demo: &Demo, service: &str) -> String {
    let mut args = demo.compose_base_args();
    args.extend(
        ["logs", "--tail=60", "--no-color", service]
            .iter()
            .map(|s| s.to_string()),
    );
    match Command::new("docker").args(&args).output() {
        Ok(o) => {
            let mut s = String::from_utf8_lossy(&o.stdout).into_owned();
            s.push_str(&String::from_utf8_lossy(&o.stderr));
            s
        }
        Err(e) => format!("(could not fetch logs: {e})"),
    }
}

/// Poll the candidate's `/health` on a host-published port until 200 or timeout.
fn wait_health(port: u16, timeout: Duration) -> Result<(), String> {
    let url = format!("http://127.0.0.1:{port}/health");
    let deadline = Instant::now() + timeout;
    loop {
        let ok = Command::new("curl")
            .args(["-fsS", "-o", "/dev/null", "--max-time", "3", &url])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("candidate at {url} not healthy within timeout"));
        }
        thread::sleep(Duration::from_secs(2));
    }
}

/// Wait until at least one object exists under the session's landing prefix
/// and the count stops growing (Vector batch flush settled). Native S3 list —
/// no `mc` container round-trips.
fn wait_s3_objects(recording_id: &str, timeout: Duration) -> Result<(), String> {
    let cfg = crate::s3::S3Config::from_env();
    let deadline = Instant::now() + timeout;
    let mut last = 0usize;
    let mut stable = 0u8;
    loop {
        let count = crate::s3::count_session_objects(&cfg, recording_id).unwrap_or(0);
        if count > 0 && count == last {
            stable += 1;
            if stable >= 2 {
                eprintln!("lifecycle: S3 has {count} landing object(s) for {recording_id}");
                return Ok(());
            }
        } else {
            stable = 0;
        }
        last = count;
        if Instant::now() >= deadline {
            if last > 0 {
                return Ok(());
            }
            return Err(format!(
                "no recording objects appeared in S3 for {recording_id} within timeout"
            ));
        }
        thread::sleep(Duration::from_secs(3));
    }
}

/// Pull the session out of S3 into the canonical
/// `{root}/recordings/{id}/events.jsonl` slot the kernel + renderer read.
/// Compacts the session first if it isn't sealed (manifest absent), then
/// streams the data parts (see `deja-compactor`). The ingest report and the
/// sealing manifest are persisted next to the events file and registered as
/// artifacts; the recording catalog row upserts from the manifest.
fn pull_recording(root: &HarnessRoot, ctx: &StoreCtx, recording_id: &str) -> Result<(), String> {
    let cfg = crate::s3::S3Config::from_env();
    let dest = root.recording_events_path(recording_id);
    let (report, manifest) = crate::s3::pull_recording(&cfg, recording_id, &dest)?;
    let gaps: usize = manifest.instances.iter().map(|i| i.gaps.len()).sum();
    let line = format!(
        "ingested {recording_id}: {} landing object(s), {} line(s), {} duplicate(s) dropped → \
         {} event(s), {} correlation(s), {} gap(s), sealed",
        report.landing_objects,
        report.lines_in,
        report.duplicates_dropped,
        report.events_out,
        report.correlations,
        gaps,
    );
    eprintln!("lifecycle: {line}");
    ctx.log("ingest", &line);
    if report.events_out == 0 {
        return Err(format!("recording {recording_id} pulled empty from S3"));
    }
    // Consumer shim: deja-tui / deja-semantic-metrics historically read the
    // JSONL primary at {root}/recording/semantic-events.jsonl. Kafka is the
    // only sink now, so materialize the pulled copy there too.
    let legacy_copy = root.root.join("recording").join("semantic-events.jsonl");
    if let Some(parent) = legacy_copy.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::copy(&dest, &legacy_copy) {
        eprintln!("lifecycle: semantic-events.jsonl shim copy failed: {e}");
    }
    let report_path = dest.with_file_name("ingest-report.json");
    if let Err(e) = write_json(&report_path, &report) {
        eprintln!("lifecycle: ingest report write failed: {e}");
    }
    ctx.artifact(Some(recording_id), "ingest_report", &report_path);
    let manifest_path = dest.with_file_name("manifest.json");
    if let Err(e) = write_json(&manifest_path, &manifest) {
        eprintln!("lifecycle: manifest copy write failed: {e}");
    }
    ctx.artifact(Some(recording_id), "manifest", &manifest_path);
    let bytes = std::fs::metadata(&dest).ok().map(|m| m.len() as i64);
    ctx.recording(
        recording_id,
        dest.to_str(),
        Some(report.events_out as i64),
        Some(report.correlations as i64),
        bytes,
        serde_json::to_value(&manifest).ok().as_ref(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CandidateSpec, RunSpec};

    fn run_with_workload(workload: serde_json::Value) -> Run {
        Run {
            run_id: "r1".into(),
            spec: RunSpec {
                mode: RunMode::Record,
                candidate_spec: CandidateSpec::PrebuiltImage { image: "x".into() },
                recording_id: None,
                workload,
            },
            status: RunStatus::Pending,
            recording_id: None,
            candidate_image: None,
            failure_reason: None,
            stage: None,
            step: 0,
            steps_total: 0,
            stage_updated_ms: 0,
        }
    }

    #[test]
    fn iterations_defaults_to_one() {
        assert_eq!(run_iterations(&run_with_workload(serde_json::json!({}))), 1);
    }

    #[test]
    fn iterations_read_from_workload() {
        assert_eq!(
            run_iterations(&run_with_workload(serde_json::json!({ "iterations": 25 }))),
            25
        );
    }
}
