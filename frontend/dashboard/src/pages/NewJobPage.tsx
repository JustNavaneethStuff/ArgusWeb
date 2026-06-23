import { FormEvent, useState } from "react";
import { useNavigate } from "react-router-dom";
import { createJob } from "../api/client";

export default function NewJobPage() {
  const navigate = useNavigate();
  const [seedUrls, setSeedUrls] = useState("https://example.com");
  const [maxDepth, setMaxDepth] = useState(1);
  const [incremental, setIncremental] = useState(false);
  const [allowedDomains, setAllowedDomains] = useState("example.com");
  const [staleHours, setStaleHours] = useState(24);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      const domains = allowedDomains.split(",").map((d) => d.trim()).filter(Boolean);
      const result = await createJob(
        incremental
          ? {
              incremental: true,
              allowed_domains: domains,
              recrawl_stale_hours: staleHours,
              max_depth: maxDepth,
            }
          : {
              seed_urls: seedUrls.split("\n").map((u) => u.trim()).filter(Boolean),
              max_depth: maxDepth,
              allowed_domains: domains.length ? domains : undefined,
            }
      );
      navigate("/jobs");
      alert(`Job created: ${result.job_id} (${result.urls_queued} URLs queued)`);
    } catch (err) {
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="card">
      <h2>Submit Crawl Job</h2>
      <form onSubmit={onSubmit} className="form">
        <label>
          <input
            type="checkbox"
            checked={incremental}
            onChange={(e) => setIncremental(e.target.checked)}
          />
          Incremental recrawl (stale URLs only)
        </label>

        {!incremental && (
          <label>
            Seed URLs (one per line)
            <textarea
              rows={4}
              value={seedUrls}
              onChange={(e) => setSeedUrls(e.target.value)}
            />
          </label>
        )}

        <label>
          Allowed domains (comma-separated)
          <input
            type="text"
            value={allowedDomains}
            onChange={(e) => setAllowedDomains(e.target.value)}
          />
        </label>

        <label>
          Max depth
          <input
            type="number"
            min={0}
            max={10}
            value={maxDepth}
            onChange={(e) => setMaxDepth(Number(e.target.value))}
          />
        </label>

        {incremental && (
          <label>
            Recrawl stale after (hours)
            <input
              type="number"
              min={1}
              value={staleHours}
              onChange={(e) => setStaleHours(Number(e.target.value))}
            />
          </label>
        )}

        <button type="submit" disabled={loading}>
          {loading ? "Submitting..." : "Submit Job"}
        </button>
      </form>
      {error && <p className="error">{error}</p>}
    </div>
  );
}
