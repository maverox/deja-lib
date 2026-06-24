import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { api, RunRow } from "../lib/api";

function shortSha(sha: string | null): string {
  return sha ? sha.slice(0, 12) : "—";
}

// The replay execution policy a run ran under. The API does NOT persist
// deja_policy in the run row's params today (only { workload }), so this is
// read defensively and falls back to "—". See report for the API gap.
function policyOf(r: RunRow): string {
  if (r.mode !== "replay") return "—";
  const p = r.params?.deja_policy;
  return typeof p === "string" && p ? p : "—";
}

// A run-list outcome badge. value_divergences > 0 on the embedded scorecard is
// the total-derivative CATCH (SelectiveExecute); otherwise fall back to the
// coarse verdict so every replay run shows an at-a-glance outcome.
function OutcomeBadge({ r }: { r: RunRow }) {
  if (r.mode !== "replay") return <>—</>;
  const vd = r.scorecard?.summary.value_divergences ?? 0;
  if (vd > 0) {
    return (
      <span className="chip solid fail" title={`${vd} value divergence(s) — total-derivative catch`}>
        CAUGHT
      </span>
    );
  }
  if (!r.verdict) return <span className="chip muted">—</span>;
  if (r.verdict === "pass") return <span className="chip solid pass">PASS</span>;
  if (r.verdict === "inconclusive")
    return <span className="chip solid inconclusive">INCONCLUSIVE</span>;
  return <span className="chip solid fail">FAIL</span>;
}

export default function RunsPage() {
  const nav = useNavigate();
  const runs = useQuery({
    queryKey: ["runs"],
    queryFn: api.runs,
    refetchInterval: 5000,
  });

  if (runs.isLoading) return <p className="hint">loading…</p>;
  if (runs.error)
    return (
      <p className="err">
        {String(runs.error)} — is the orchestrator running? (demo/lib.sh starts
        it, or run replay-harness-api directly)
      </p>
    );

  return (
    <>
      <h1>Runs</h1>
      <table>
        <thead>
          <tr>
            <th>run</th>
            <th>mode</th>
            <th>policy</th>
            <th>outcome</th>
            <th>state</th>
            <th>verdict</th>
            <th>expect</th>
            <th>recording</th>
            <th>candidate sha</th>
            <th>actor</th>
            <th>created</th>
          </tr>
        </thead>
        <tbody>
          {runs.data?.map((r) => (
            <tr
              key={r.run_id}
              className="clickable"
              onClick={() => nav(`/runs/${r.run_id}`)}
            >
              <td>
                <Link to={`/runs/${r.run_id}`}>{r.run_id.slice(0, 16)}…</Link>
              </td>
              <td>{r.mode}</td>
              <td className="mono">{policyOf(r)}</td>
              <td>
                <OutcomeBadge r={r} />
              </td>
              <td>
                <span className={`chip ${r.state}`}>{r.state}</span>
              </td>
              <td>
                {r.verdict ? (
                  <span className={`chip ${r.verdict}`}>{r.verdict}</span>
                ) : (
                  "—"
                )}
              </td>
              <td>{r.expectation ?? "—"}</td>
              <td>{r.recording_id ?? "—"}</td>
              <td title={r.candidate_sha256 ?? undefined}>
                {shortSha(r.candidate_sha256)}
              </td>
              <td>{r.created_by}</td>
              <td>{new Date(r.created_at).toLocaleTimeString()}</td>
            </tr>
          ))}
          {runs.data?.length === 0 && (
            <tr>
              <td colSpan={11} className="hint">
                no runs yet — <Link to="/replays/new">schedule one</Link> or run
                demo/run-deja-demo.sh
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </>
  );
}
