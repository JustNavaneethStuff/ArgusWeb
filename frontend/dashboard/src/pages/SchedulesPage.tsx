import { FormEvent, useEffect, useState } from "react";
import { createSchedule, deleteSchedule, fetchSchedules, Schedule } from "../api/client";

export default function SchedulesPage() {
  const [schedules, setSchedules] = useState<Schedule[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [cron, setCron] = useState("0 */6 * * *");
  const [seedUrls, setSeedUrls] = useState("https://example.com");
  const [loading, setLoading] = useState(false);

  function load() {
    fetchSchedules()
      .then((data) => setSchedules(data.items))
      .catch((e) => setError(String(e)));
  }

  useEffect(() => {
    load();
  }, []);

  async function onCreate(e: FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      await createSchedule({
        name,
        cron_expression: cron,
        job_config: {
          seed_urls: seedUrls.split("\n").map((u) => u.trim()).filter(Boolean),
          max_depth: 1,
        },
      });
      setName("");
      load();
    } catch (err) {
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }

  async function onDelete(id: string) {
    if (!confirm("Delete this schedule?")) return;
    try {
      await deleteSchedule(id);
      load();
    } catch (err) {
      setError(String(err));
    }
  }

  return (
    <div className="card">
      <h2>Cron Schedules</h2>
      <p className="muted">Cron format: minute hour day month day_of_week (e.g. 0 */6 * * * = every 6 hours)</p>

      <form onSubmit={onCreate} className="form">
        <label>
          Name
          <input type="text" value={name} onChange={(e) => setName(e.target.value)} required />
        </label>
        <label>
          Cron expression
          <input type="text" value={cron} onChange={(e) => setCron(e.target.value)} required />
        </label>
        <label>
          Seed URLs
          <textarea rows={2} value={seedUrls} onChange={(e) => setSeedUrls(e.target.value)} />
        </label>
        <button type="submit" disabled={loading}>
          {loading ? "Creating..." : "Create Schedule"}
        </button>
      </form>

      {error && <p className="error">{error}</p>}

      {schedules.length > 0 && (
        <table className="table" style={{ marginTop: "1.5rem" }}>
          <thead>
            <tr>
              <th>Name</th>
              <th>Cron</th>
              <th>Enabled</th>
              <th>Next Run</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {schedules.map((s) => (
              <tr key={s.id}>
                <td>{s.name}</td>
                <td><code>{s.cron_expression}</code></td>
                <td>{s.enabled ? "yes" : "no"}</td>
                <td>{s.next_run_at ? new Date(s.next_run_at).toLocaleString() : "—"}</td>
                <td>
                  <button type="button" className="btn-danger" onClick={() => onDelete(s.id)}>
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
