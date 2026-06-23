import { FormEvent, useState } from "react";
import { searchPages, SearchResponse } from "../api/client";

export default function SearchPage() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    setResults(null);
    setLoading(true);
    try {
      const data = await searchPages(query);
      setResults(data);
    } catch (err) {
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="card">
      <h2>Search</h2>
      <p className="muted">Full-text search over indexed pages using pg_trgm.</p>
      <form onSubmit={onSubmit}>
        <input
          type="search"
          placeholder="Search indexed pages..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <button type="submit" disabled={loading}>
          {loading ? "Searching..." : "Search"}
        </button>
      </form>
      {error && <p className="error">{error}</p>}
      {results && (
        <div style={{ marginTop: "1.5rem" }}>
          <p className="muted">
            {results.total} result{results.total !== 1 ? "s" : ""} for &ldquo;{results.query}&rdquo;
          </p>
          {results.results.map((r) => (
            <div key={r.normalized_url} className="result-card">
              <a href={r.url} target="_blank" rel="noreferrer">
                <strong>{r.title || r.url}</strong>
              </a>
              <span className="badge">score {r.score.toFixed(2)}</span>
              <p className="muted">{r.domain}</p>
              <p>{r.text_snippet || r.description}</p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
