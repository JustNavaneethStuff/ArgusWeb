import { FormEvent, useState } from "react";
import { searchPages } from "../api/client";

export default function SearchPage() {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    setResult(null);
    try {
      await searchPages(query);
    } catch (err) {
      setError(String(err));
    }
  }

  return (
    <div className="card">
      <h2>Search</h2>
      <p className="muted">Phase 3: full-text search via pg_trgm (currently returns 501).</p>
      <form onSubmit={onSubmit}>
        <input
          type="search"
          placeholder="Search indexed pages..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <button type="submit">Search</button>
      </form>
      {error && <p className="error">{error}</p>}
      {result && <pre>{result}</pre>}
    </div>
  );
}
