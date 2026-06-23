import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { fetchStats, StatsResponse } from "../api/client";

export default function StatsPage() {
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchStats()
      .then(setStats)
      .catch((e) => setError(String(e)));
  }, []);

  if (error) return <div className="card error">{error}</div>;
  if (!stats) return <div className="card">Loading statistics...</div>;

  return (
    <div className="card">
      <h2>Crawl Statistics</h2>
      <div className="stat-grid">
        <div className="stat-box">
          <strong>{stats.jobs_total}</strong>
          Jobs
        </div>
        <div className="stat-box">
          <strong>{stats.pages_indexed}</strong>
          Pages Indexed
        </div>
        <div className="stat-box">
          <strong>{stats.crawl_rate_24h ?? 0}</strong>
          Crawled (24h)
        </div>
        <div className="stat-box">
          <strong>{stats.failed_urls ?? 0}</strong>
          Failed URLs
        </div>
        {Object.entries(stats.urls_by_status).map(([status, count]) => (
          <div className="stat-box" key={status}>
            <strong>{count}</strong>
            URLs ({status})
          </div>
        ))}
      </div>

      {stats.pages_by_domain && Object.keys(stats.pages_by_domain).length > 0 && (
        <>
          <h3 style={{ marginTop: "1.5rem" }}>Pages by Domain</h3>
          <table className="table">
            <thead>
              <tr>
                <th>Domain</th>
                <th>URLs</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(stats.pages_by_domain).map(([domain, count]) => (
                <tr key={domain}>
                  <td>{domain}</td>
                  <td>{count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {stats.recent_jobs && stats.recent_jobs.length > 0 && (
        <>
          <h3 style={{ marginTop: "1.5rem" }}>Recent Jobs</h3>
          <table className="table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Status</th>
                <th>Queued</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {stats.recent_jobs.map((job) => (
                <tr key={job.id}>
                  <td>
                    <Link to={`/jobs`}>{job.id.slice(0, 8)}...</Link>
                  </td>
                  <td>
                    <span className={`status status-${job.status}`}>{job.status}</span>
                  </td>
                  <td>{job.urls_queued}</td>
                  <td>{job.created_at ? new Date(job.created_at).toLocaleString() : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
