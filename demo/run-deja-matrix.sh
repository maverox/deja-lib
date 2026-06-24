#!/usr/bin/env bash
# Deja CROSS-VERSION A/B MATRIX: record ONE V1 baseline, then replay SEVEN
# candidates against that SAME recording — EACH under BOTH replay policies, so a
# single case yields TWO scorecard rows (one per mode):
#   DEJA_POLICY=AllLookup        → mode Lookup  (full mock = PARTIAL derivative; V1 baseline)
#   DEJA_POLICY=SelectiveExecute → mode Execute (real {db} side = TOTAL derivative; M1)
# The contrast is the point: a TOTAL/transitive divergence that AllLookup MISSES
# (its substituted value hides it) is CAUGHT under SelectiveExecute. The scorecard
# JSON for each cell is stamped with `mode_provenance` so the report self-identifies
# which mode produced which verdict.
#
#   STRIPE_API_KEY=sk_test_... demo/run-deja-matrix.sh [--iterations N] [--keep]
#
#   self   — V1, unchanged                 → expect PASS        (faithful self-replay)
#   benign — V2, comment/line-shift edit   → expect PASS        (no false divergence)
#   real   — V2, changed persisted value   → expect DIVERGENCE  (the gate catches it)
#
# Why one recording, many replays: this is the correct mental model for
# regression detection — a single GOLDEN baseline recording, multiple candidate
# builds scored against it. The expensive real-Stripe workload runs ONCE, and every
# verdict is apples-to-apples against IDENTICAL expected behavior.
#
# Pipeline: HS(record) → Kafka → Vector → MinIO (recorded once); then for each
# candidate: rebuild the host router → orchestrator renders the lookup table from
# the SAME recording, boots the candidate under DEJA_MODE=replay, drives the
# recorded requests, scores byte-exact divergence.
#
# Run from the repo root. Requires: docker (+ compose), cargo, curl, jq.
set -euo pipefail
cd "$(dirname "$0")/.."   # repo root

ITERATIONS=1
KEEP=0
VENDOR="vendor/hyperswitch-deja-clean"
while [ $# -gt 0 ]; do
  case "$1" in
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    *) echo "unknown arg: $1"; exit 2 ;;
  esac
done

# The two V2 candidate patches (vendor-only; see demo/cross-version/README.md).
BENIGN_PATCH="$(pwd)/demo/cross-version/benign-line-shift.patch"
REAL_PATCH="$(pwd)/demo/cross-version/real-change.patch"
# Extra regression scenarios — each exercises a DISTINCT detector cell:
#   earlier-fork  — arg change at the payment_INTENT insert (before the attempt):
#                   modified pair (novel+omitted) on db, fork origin EARLIER than `real`.
#   dropped-write — candidate skips a fire-and-forget redis cache populate:
#                   pure OMITTED on redis, HTTP response IDENTICAL (silent regression).
#   response-only — overrides one response field, no boundary-call change:
#                   HTTP body mismatch with ZERO side-effect divergences.
#   extra-call    — candidate issues a db find V1 never made: pure NOVEL, no omitted pair.
EARLIER_FORK_PATCH="$(pwd)/demo/cross-version/earlier-fork.patch"
DROPPED_WRITE_PATCH="$(pwd)/demo/cross-version/dropped-write.patch"
RESPONSE_ONLY_PATCH="$(pwd)/demo/cross-version/response-only.patch"
EXTRA_CALL_PATCH="$(pwd)/demo/cross-version/extra-call.patch"
EU_OVERCHARGE_PATCH="$(pwd)/demo/cross-version/eu-overcharge.patch"
for p in "$BENIGN_PATCH" "$REAL_PATCH" "$EARLIER_FORK_PATCH" "$DROPPED_WRITE_PATCH" "$RESPONSE_ONLY_PATCH" "$EXTRA_CALL_PATCH"; do
  [ -f "$p" ] || { echo "missing candidate patch: $p"; exit 1; }
done

# Shared constants + build/orchestrator/poll/candidate-patch helpers.
source demo/lib.sh

require_tools
init_run_state
echo "── run tag: ${RUN_TAG}  ·  baseline recording: ${REC_ID}  ·  state: ${STATE_DIR} ──"

API_PID=""
cleanup() {
  # Revert any in-flight candidate patch FIRST, on every exit path, so the dirty
  # vendor tree is restored even on failure/Ctrl-C.
  revert_candidate_patch
  [ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null || true
  if [ "$KEEP" -eq 0 ]; then
    echo "── tearing down (use --keep to inspect) ──"
    docker compose -p "$PROJECT" -f "$BASE" -f "$OVERLAY" down -v >/dev/null 2>&1 || true
  else
    echo "── stack left running (--keep); state in $STATE_DIR ──"
  fi
}
trap cleanup EXIT

echo "── building deja router (V1) + kernel + orchestrator + tui ──"
build_binaries
start_api

# ── RECORD ONCE: the golden V1 baseline ──────────────────────────────────────
echo "── RECORD (V1 baseline): drive ${ITERATIONS} workload iteration(s); HS → Kafka → Vector → MinIO ──"
REC_RUN=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" --argjson it "$ITERATIONS" \
  '{mode:"record", candidate_spec:$c, recording_id:$r, workload:{iterations:$it}}')")
[ "$(poll "$REC_RUN")" = "completed" ] || { echo "RECORD run failed"; curl -fsS "${API}/api/v1/runs/${REC_RUN}" | jq .; exit 1; }
echo "   baseline recorded → recording_id=${REC_ID}"
RECORDING_ID="$REC_ID" docker compose -p "$PROJECT" -f "$BASE" -f "$OVERLAY" run --rm -T mc \
  "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; \
   mc ls --recursive local/deja-recordings/landing/v1/session=${REC_ID}/" || true

# The two M1 A/B policies every case is replayed under. AllLookup is the V1
# full-mock baseline (PARTIAL derivative — substitutes every boundary result, so
# a TOTAL/transitive divergence hides behind the substituted value). SelectiveExecute
# runs the REAL {db} side of each boundary (TOTAL derivative — the doubled WRITE
# actually executes and post-hoc ValueDiverged tally catches it). Driving the SAME
# case under both yields the contrast: two scorecard rows from one recording.
POLICIES=(AllLookup SelectiveExecute)

# Execute-mode scope. EMPTY = execute EVERY reconstructable State boundary
# (all db + redis ops) under SelectiveExecute — full-scale total-derivative.
# Entropy (id/time) and Egress (http) are structurally excluded by
# is_state_channel, so they stay lookup-substituted regardless. The ordered,
# sequential kernel replay reconstructs the workload's own DB/redis state as it
# drives requests in record order, so executed reads find their data without a
# pre-seeded template. Only affects SelectiveExecute; AllLookup ignores it.
# (Was scoped to "eu_settlement_read,eu_settlement_write" for the M1 redis demo.)
export DEJA_EXECUTE_OPS=""

# Assemble a clean, single-run view dir for one candidate so deja-tui /
# visualize-replay.py (which each expect ONE run per dir) render it unambiguously.
assemble_view() { # assemble_view <label> <run_id>
  local label="$1" rid="$2" vdir="$STATE_DIR/view-$label"
  mkdir -p "$vdir/observed" "$vdir/http-diffs" "$vdir/lookup-tables" "$vdir/runs"
  ln -sfn ../recordings "$vdir/recordings" 2>/dev/null || true
  ln -sfn ../recording  "$vdir/recording"  2>/dev/null || true
  for sub in observed http-diffs lookup-tables; do
    [ -f "$STATE_DIR/$sub/$rid.jsonl" ] && cp -f "$STATE_DIR/$sub/$rid.jsonl" "$vdir/$sub/" || true
  done
  for ext in json scorecard.json; do
    [ -f "$STATE_DIR/runs/$rid.$ext" ] && cp -f "$STATE_DIR/runs/$rid.$ext" "$vdir/runs/" || true
  done
  python3 demo/visualize-replay.py "$vdir" >/dev/null 2>&1 || true
}

# ── REPLAY one candidate against the SAME baseline recording ─────────────────
# Each row is one (candidate × policy) cell:
RESULTS=()  # "label|policy|mode|expect_div|pass|caught|ok|matched|total|diverg|httpbody|run_id|reason"

# Replay ONE (candidate, policy) cell against the SAME baseline recording (REC_ID).
# Each cell gets its own run_id; the orchestrator renders the lookup table from
# REC_ID, boots the candidate under DEJA_MODE=replay + this policy, drives the
# recorded requests, scores. The candidate patch is assumed already applied/built
# by the caller (we replay both policies against the SAME built binary).
replay_cell() { # replay_cell <label> <policy> <expect_divergence 0|1>
  local label="$1" policy="$2" expect_div="$3"
  local mode; mode=$(policy_to_mode "$policy")
  echo
  echo "  ── ${label} · DEJA_POLICY=${policy} (mode=${mode}) · expect $( [ "$expect_div" -eq 1 ] && echo DIVERGENCE || echo PASS ) ──"

  local rep card pass reason matched total diverg httpbody caught ok
  local expect_note=$( [ "$expect_div" -eq 1 ] && echo "diverge" || echo "pass" )
  # DEJA_POLICY rides BOTH the run spec (deja_policy, for a policy-aware
  # orchestrator) and the process env exported here (forwarded to the replay
  # container by the orchestrator's compose env once wired). Default AllLookup
  # keeps the no-regression contract: byte-identical to today's full mock.
  rep=$(DEJA_POLICY="$policy" post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" \
    '{mode:"replay", candidate_spec:$c, recording_id:$r}')" "$expect_note" "$policy")
  [ "$(poll "$rep")" = "completed" ] || { echo "REPLAY ($label/$policy) run failed"; curl -fsS "${API}/api/v1/runs/${rep}" | jq .; exit 1; }
  card=$(curl -fsS "${API}/api/v1/runs/${rep}/scorecard")
  pass=$(echo "$card"   | jq -r '.verdict.pass')
  reason=$(echo "$card" | jq -r '.verdict.reason')
  matched=$(echo "$card" | jq -r '.summary.matched_correlations')
  total=$(echo "$card"   | jq -r '.summary.total_correlations')
  diverg=$(echo "$card"  | jq -r '.summary.side_effect_divergences')
  httpbody=$(echo "$card" | jq -r '.summary.http_body_mismatches')

  # CAUGHT = this policy/mode flagged a divergence (verdict not PASS).
  caught=$( [ "$pass" = "true" ] && echo 0 || echo 1 )

  ok=0
  if [ "$expect_div" -eq 1 ]; then [ "$caught" -eq 1 ] && ok=1; else [ "$caught" -eq 0 ] && ok=1; fi
  if [ "$ok" -eq 1 ]; then echo "  ✅ ${label}/${policy}: outcome matched expectation"; else echo "  ❌ ${label}/${policy}: outcome did NOT match expectation — ${reason}"; fi
  echo "$card" | jq '{verdict: .verdict.pass, matched: .summary.matched_correlations, total: .summary.total_correlations, side_effect_divergences: .summary.side_effect_divergences, http_body_mismatches: .summary.http_body_mismatches, value_diverged: (.summary.value_divergences // 0), resolved_by_rank: .summary.resolved_by_rank}'

  # MINIMAL mode-provenance: stamp the policy/mode INTO the scorecard JSON so the
  # report self-identifies which mode produced this verdict (not just shell state).
  stamp_scorecard_mode "$rep" "$policy" "$mode"

  assemble_view "${label}-${mode}" "$rep"
  RESULTS+=("$label|$policy|$mode|$expect_div|$pass|$caught|$ok|$matched|$total|$diverg|$httpbody|$rep|$reason")
}

# Replay one candidate under BOTH policies against the SAME baseline recording —
# two scorecard rows from one case. AllLookup carries today's exact expectation
# (the no-regression anchor: <expect_div> is the AllLookup verdict). SelectiveExecute
# is the TOTAL-derivative pass: it must be AT LEAST as strong as AllLookup, so its
# expectation is "diverge if AllLookup expected diverge, else don't-constrain"
# (a SelectiveExecute catch on an AllLookup-pass case is the M1 WIN, reported via
# the CAUGHT column rather than failing the gate).
replay_candidate() { # replay_candidate <label> <patch|""> <expect_div 0|1> [se_expect 0|1]
  local label="$1" patch="$2" expect_div="$3"
  # se_expect: the SelectiveExecute cell's expectation. Defaults to expect_div
  # for back-compat (SE is never weaker than AllLookup). Set it explicitly when a
  # case is an AllLookup-PASS but a SelectiveExecute-CATCH (the total-derivative
  # win, e.g. eu-overcharge: AllLookup 0, SE 1) — otherwise the SE catch on an
  # AllLookup-pass case would be wrongly scored as failing.
  local se_expect="${4:-$expect_div}"
  echo
  echo "════════════════════════════════════════════════════════════"
  echo "  CANDIDATE: ${label}  (AllLookup expect $( [ "$expect_div" -eq 1 ] && echo DIVERGENCE || echo PASS ))"
  echo "════════════════════════════════════════════════════════════"

  if [ -n "$patch" ]; then
    apply_candidate_patch "$patch"
    rebuild_router_v2 "$label"
  fi

  local policy
  for policy in "${POLICIES[@]}"; do
    if [ "$policy" = "SelectiveExecute" ]; then
      # SelectiveExecute is never weaker than AllLookup: it must catch whatever
      # AllLookup caught. On AllLookup-pass cases it MAY catch more (total
      # derivative) — for those the caller sets se_expect=1 to REQUIRE the catch
      # (e.g. eu-overcharge). Defaults to expect_div.
      replay_cell "$label" "$policy" "$se_expect"
    else
      replay_cell "$label" "$policy" "$expect_div"
    fi
  done

  # Revert the candidate patch so the source is back to V1 for the next candidate.
  revert_candidate_patch
}

# self FIRST (V1 binary as built at startup — no rebuild), then the V2 candidates.
replay_candidate "self"          ""                     0
replay_candidate "benign"        "$BENIGN_PATCH"        0
replay_candidate "real"          "$REAL_PATCH"          1
replay_candidate "earlier-fork"  "$EARLIER_FORK_PATCH"  1
replay_candidate "dropped-write" "$DROPPED_WRITE_PATCH" 1
replay_candidate "response-only" "$RESPONSE_ONLY_PATCH" 1
replay_candidate "extra-call"    "$EXTRA_CALL_PATCH"    1
# eu-overcharge: the M1 TOTAL-DERIVATIVE case. AllLookup expects PASS (0) — full
# mock substitutes the re-keyed read and MISSES the transitive overcharge; the
# SelectiveExecute cell CATCHES it (ValueDiverged on the settlement write),
# surfaced via the CAUGHT column rather than failing the gate.
replay_candidate "eu-overcharge" "$EU_OVERCHARGE_PATCH" 0 1

# ── SUMMARY MATRIX ───────────────────────────────────────────────────────────
echo
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  DEJA A/B MATRIX — one V1 recording (${REC_ID}); each case × {AllLookup, SelectiveExecute}"
echo "════════════════════════════════════════════════════════════════════════════════"
printf "  %-13s %-9s %-12s %-9s %-7s %-7s %-9s %s\n" "CAND" "MODE" "EXPECT" "VERDICT" "CAUGHT" "OK?" "MATCHED" "DIVERG"
ALL_OK=1
for r in "${RESULTS[@]}"; do
  IFS='|' read -r label policy mode expect_div pass caught ok matched total diverg httpbody run_id reason <<<"$r"
  exp=$( [ "$expect_div" -eq 1 ] && echo "diverge" || echo "pass" )
  vd=$( [ "$pass" = "true" ] && echo "PASS" || echo "DIVERGE" )
  caughtmark=$( [ "$caught" -eq 1 ] && echo "YES" || echo "no" )
  if [ "$ok" -eq 1 ]; then okmark="✅"; else okmark="❌"; ALL_OK=0; fi
  printf "  %-13s %-9s %-12s %-9s %-7s %-7s %-9s %s\n" "$label" "$mode" "$exp" "$vd" "$caughtmark" "$okmark" "${matched}/${total}" "$diverg"
done
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  MODE = boundary dispatch: Lookup (AllLookup, partial derivative) vs Execute"
echo "         (SelectiveExecute, total derivative). CAUGHT = this mode flagged divergence."
echo "  A SelectiveExecute YES on a row where Lookup said PASS is the M1 total-derivative win."
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  per-cell replay reports (record→replay substitution, mode-stamped scorecard):"
for r in "${RESULTS[@]}"; do
  IFS='|' read -r label policy mode _ _ _ _ _ _ _ _ run_id _ <<<"$r"
  echo "    ${label}/${mode} (policy=${policy}):"
  echo "          web  → $(run_url "$run_id")"
  echo "          card → ${API}/api/v1/runs/${run_id}/scorecard   (mode_provenance stamped)"
  echo "          TUI  → target/release/deja-tui \"$STATE_DIR/view-${label}-${mode}\""
  echo "          HTML → $STATE_DIR/view-${label}-${mode}/replay-visualization.html"
done
echo "════════════════════════════════════════════════════════════════════════"

# Exit 0 iff EVERY candidate's outcome matched its expectation (self+benign pass,
# real diverges) — a correct gate for the whole matrix.
[ "$ALL_OK" -eq 1 ]
