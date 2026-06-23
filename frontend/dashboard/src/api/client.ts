const API_BASE = import.meta.env.VITE_API_URL || "/api";
const SCHEDULER_BASE = import.meta.env.VITE_SCHEDULER_URL || "/scheduler";

export interface StatsResponse {
  jobs_total: number;
  pages_indexed: number;
  urls_by_status: Record<string, number>;
  failed_urls?: number;
  pages_by_domain?: Record<string, number>;
  crawl_rate_24h?: number;
  recent_jobs?: RecentJob[];
}

export interface RecentJob {
  id: string;
  status: string;
  urls_queued: number;
  url_count: number;
  created_at: string | null;
}

export interface SearchResult {
  title: string | null;
  url: string;
  normalized_url: string;
  description: string | null;
  text_snippet: string | null;
  domain: string;
  score: number;
}

export interface SearchResponse {
  query: string;
  total: number;
  limit: number;
  offset: number;
  results: SearchResult[];
}

export interface JobSummary {
  id: string;
  seed_urls: string[];
  max_depth: number;
  allowed_domains: string[];
  status: string;
  urls_queued: number;
  url_count: number;
  created_at: string | null;
}

export interface JobDetail extends JobSummary {
  urls_by_status: Record<string, number>;
  progress_percent: number;
  updated_at: string | null;
}

export interface Schedule {
  id: string;
  name: string;
  cron_expression: string;
  enabled: boolean;
  job_config: JobCreatePayload;
  last_run_at: string | null;
  next_run_at: string | null;
  created_at: string | null;
}

export interface JobCreatePayload {
  seed_urls?: string[];
  max_depth?: number;
  allowed_domains?: string[];
  incremental?: boolean;
  recrawl_stale_hours?: number;
}

export async function fetchStats(): Promise<StatsResponse> {
  const res = await fetch(`${API_BASE}/stats`);
  if (!res.ok) throw new Error(`Stats request failed: ${res.status}`);
  return res.json();
}

export async function searchPages(
  q: string,
  limit = 20,
  offset = 0
): Promise<SearchResponse> {
  const res = await fetch(
    `${API_BASE}/search?q=${encodeURIComponent(q)}&limit=${limit}&offset=${offset}`
  );
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail?.message || `Search failed: ${res.status}`);
  }
  return res.json();
}

export async function fetchHealth(): Promise<{ status: string }> {
  const res = await fetch(`${API_BASE}/health`);
  return res.json();
}

export async function fetchReadiness(): Promise<{ status: string; checks: Record<string, string> }> {
  const res = await fetch(`${API_BASE}/health/ready`);
  const body = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(body.detail || body));
  return body;
}

export async function fetchJobs(limit = 20, offset = 0): Promise<{ total: number; items: JobSummary[] }> {
  const res = await fetch(`${API_BASE}/jobs?limit=${limit}&offset=${offset}`);
  if (!res.ok) throw new Error(`Jobs request failed: ${res.status}`);
  return res.json();
}

export async function fetchJob(id: string): Promise<JobDetail> {
  const res = await fetch(`${API_BASE}/jobs/${id}`);
  if (!res.ok) throw new Error(`Job request failed: ${res.status}`);
  return res.json();
}

export async function createJob(payload: JobCreatePayload): Promise<{ job_id: string; urls_queued: number }> {
  const res = await fetch(`${SCHEDULER_BASE}/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const body = await res.json();
  if (!res.ok) throw new Error(body.detail || `Create job failed: ${res.status}`);
  return body;
}

export async function fetchSchedules(): Promise<{ items: Schedule[] }> {
  const res = await fetch(`${API_BASE}/schedules`);
  if (!res.ok) throw new Error(`Schedules request failed: ${res.status}`);
  return res.json();
}

export async function createSchedule(payload: {
  name: string;
  cron_expression: string;
  job_config: JobCreatePayload;
}): Promise<Schedule> {
  const res = await fetch(`${SCHEDULER_BASE}/schedules`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const body = await res.json();
  if (!res.ok) throw new Error(body.detail || `Create schedule failed: ${res.status}`);
  return body;
}

export async function deleteSchedule(id: string): Promise<void> {
  const res = await fetch(`${SCHEDULER_BASE}/schedules/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`Delete schedule failed: ${res.status}`);
}
