import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { api } from "../lib/api";
import { ConfidenceBadge, ConfidenceLadder, overallConfidence } from "../components/Confidence";

export default function ScorecardPage() {
  const { runId = "" } = useParams();
  const card = useQuery({ queryKey: ["scorecard", runId], queryFn: () => api.scorecard(runId) });

  if (card.isLoading) return <p className="hint">loading…</p>;
  if (card.error || !card.data) return <p className="err">{String(card.error)}</p>;
  const c = card.data;
  const s = c.summary;
  const verdict = c.verdict.inconclusive ? "inconclusive" : c.verdict.pass ? "pass" : "fail";
  const conf = overallConfidence(s.resolved_by_rank ?? {});

  return (
    <>
      <div className="crumb">
        <Link to="/runs">Runs</Link> <span>/</span> <Link to={`/runs/${runId}`}>{runId}</Link>{" "}
        <span>/</span> <span>scorecard</span>
      </div>
      <h1>Scorecard</h1>

      <div className={`scorestrip ${verdict === "pass" ? "clean" : "diverged"}`}>
        <span className={`chip solid ${verdict}`}>{verdict}</span>
        <ConfidenceBadge level={conf} title="overall run confidence = weakest address rank relied on" />
        <span className="lede">{c.verdict.reason}</span>
      </div>

      <div className="panel grid">
        <div className="metric"><div className="v">{s.matched_correlations}/{s.total_correlations}</div><div className="k">correlations matched</div></div>
        <div className="metric"><div className="v">{s.http_status_mismatches}</div><div className="k">http status mismatches</div></div>
        <div className="metric"><div className="v">{s.http_body_mismatches}</div><div className="k">http body mismatches</div></div>
        <div className="metric"><div className="v">{s.side_effect_divergences}</div><div className="k">side-effect divergences</div></div>
        <div className="metric"><div className="v">{s.omitted_calls ?? 0}</div><div className="k">omitted calls</div></div>
        <div className="metric"><div className="v">{s.novel_calls ?? 0}</div><div className="k">novel calls</div></div>
      </div>

      <h2>Resolution confidence</h2>
      <div className="panel">
        <ConfidenceLadder byRank={s.resolved_by_rank ?? {}} />
        <p className="hint" style={{ marginTop: "var(--s3)" }}>
          Confidence is the weakest address rank a match relied on. CERTAIN/HIGH are version-independent;
          UNMATCHED (positional) survived on ordering alone and is the fragility signal.
        </p>
      </div>

      {c.per_correlation && c.per_correlation.length > 0 && (
        <>
          <h2>Per-request</h2>
          <table>
            <thead>
              <tr>
                <th>correlation</th>
                <th>http status</th>
                <th>http body</th>
                <th className="num">side-effects</th>
                <th>outcome</th>
              </tr>
            </thead>
            <tbody>
              {c.per_correlation.map((row) => (
                <tr key={row.correlation_id} className="clickable" onClick={() => (window.location.href = `/runs/${runId}?tab=diff`)}>
                  <td className="mono">{row.correlation_id}</td>
                  <td><span className={`chip ${row.http_status_match ? "pass" : "fail"}`}>{row.http_status_match ? "match" : "mismatch"}</span></td>
                  <td><span className={`chip ${row.http_body_match ? "pass" : "fail"}`}>{row.http_body_match ? "match" : "mismatch"}</span></td>
                  <td className="num">{row.side_effect_divergences ?? 0}</td>
                  <td><span className={`chip ${row.passed ? "pass" : "fail"}`}>{row.passed ? "pass" : "diverged"}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </>
  );
}
