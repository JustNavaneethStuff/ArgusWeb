import { useEffect, useState } from "react";
import { fetchHealth } from "../api/client";

export default function HomePage() {
  const [health, setHealth] = useState<string>("checking...");

  useEffect(() => {
    fetchHealth()
      .then((h) => setHealth(h.status))
      .catch(() => setHealth("unreachable"));
  }, []);

  return (
    <div className="card">
      <h2>Argus Crawler Platform</h2>
      <p className="muted">
        Distributed web crawler and data extraction pipeline. API status:{" "}
        <strong>{health}</strong>
      </p>
      <p>
        Submit crawl jobs via the Scheduler API, then monitor throughput in Grafana
        and browse indexed pages here once Phase 3 search is enabled.
      </p>
    </div>
  );
}
