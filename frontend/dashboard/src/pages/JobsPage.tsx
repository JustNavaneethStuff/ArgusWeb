import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { fetchJobs, JobSummary } from "../api/client";

export default function JobsPage() {
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchJobs()
      .then((data) => {
        setJobs(data.items);
        setTotal(data.total);
      })
      .catch((e) => setError(String(e)));
  }, []);

  if (error) return <div className="card error">{error}</div>;

  return (
    <div className="card">
      <div className="row">
        <h2>Crawl Jobs ({total})</h2>
        <Link to="/jobs/new" className="btn-link">
          + New Job
        </Link>
      </div>
      {jobs.length === 0 ? (
        <p className="muted">No jobs yet. <Link to="/jobs/new">Create one</Link>.</p>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Status</th>
              <th>Seeds</th>
              <th>Queued</th>
              <th>URLs</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr key={job.id}>
                <td>{job.id.slice(0, 8)}...</td>
                <td>
                  <span className={`status status-${job.status}`}>{job.status}</span>
                </td>
                <td>{job.seed_urls.slice(0, 2).join(", ")}{job.seed_urls.length > 2 ? "..." : ""}</td>
                <td>{job.urls_queued}</td>
                <td>{job.url_count}</td>
                <td>{job.created_at ? new Date(job.created_at).toLocaleString() : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
