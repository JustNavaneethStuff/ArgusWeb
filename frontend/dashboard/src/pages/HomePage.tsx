import { useEffect, useState } from "react";
import { fetchHealth, fetchReadiness, fetchStats } from "../api/client";

export default function HomePage() {
  const [health, setHealth] = useState<string>("checking...");
  const [ready, setReady] = useState<string>("checking...");
  const [pages, setPages] = useState<number | null>(null);

  useEffect(() => {
    fetchHealth()
      .then((h) => setHealth(h.status))
      .catch(() => setHealth("unreachable"));
    fetchReadiness()
      .then((r) => setReady(r.status))
      .catch(() => setReady("not ready"));
    fetchStats()
      .then((s) => setPages(s.pages_indexed))
      .catch(() => setPages(null));
  }, []);

  return (
    <div className="card">
      <h2>Argus Crawler Platform</h2>
      <div className="stat-grid">
        <div className="stat-box">
          <strong>{health}</strong>
          API Health
        </div>
        <div className="stat-box">
          <strong>{ready}</strong>
          Readiness
        </div>
        <div className="stat-box">
          <strong>{pages ?? "—"}</strong>
          Pages Indexed
        </div>
      </div>
      <p className="muted" style={{ marginTop: "1rem" }}>
        Submit crawl jobs, search indexed pages, and manage cron schedules from the navigation above.
        Operational metrics are available in Grafana on port 3000.
      </p>
    </div>
  );
}
