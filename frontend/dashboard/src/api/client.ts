const API_BASE = import.meta.env.VITE_API_URL || "/api";

export interface StatsResponse {
  jobs_total: number;
  pages_indexed: number;
  urls_by_status: Record<string, number>;
}

export async function fetchStats(): Promise<StatsResponse> {
  const res = await fetch(`${API_BASE}/stats`);
  if (!res.ok) throw new Error(`Stats request failed: ${res.status}`);
  return res.json();
}

export async function searchPages(q: string): Promise<unknown> {
  const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(q)}`);
  const body = await res.json();
  if (!res.ok) throw new Error(body.detail?.message || `Search failed: ${res.status}`);
  return body;
}

export async function fetchHealth(): Promise<{ status: string }> {
  const res = await fetch(`${API_BASE}/health`);
  return res.json();
}
