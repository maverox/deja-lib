import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { api } from "../lib/api";

function shortSha(sha: string | null): string {
  return sha ? sha.slice(0, 12) : "—";
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
              <td colSpan={9} className="hint">
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
