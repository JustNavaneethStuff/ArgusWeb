import { useEffect, useState } from "react";
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
        {Object.entries(stats.urls_by_status).map(([status, count]) => (
          <div className="stat-box" key={status}>
            <strong>{count}</strong>
            URLs ({status})
          </div>
        ))}
      </div>
      <p className="muted" style={{ marginTop: "1rem" }}>
        For operational metrics (latency, throughput, errors), use Grafana at port 3000.
      </p>
    </div>
  );
}
