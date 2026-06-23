import { Link, Route, Routes } from "react-router-dom";
import HomePage from "./pages/HomePage";
import JobsPage from "./pages/JobsPage";
import NewJobPage from "./pages/NewJobPage";
import SchedulesPage from "./pages/SchedulesPage";
import SearchPage from "./pages/SearchPage";
import StatsPage from "./pages/StatsPage";

export default function App() {
  return (
    <div className="layout">
      <header className="header">
        <h1>Argus</h1>
        <nav>
          <Link to="/">Home</Link>
          <Link to="/stats">Statistics</Link>
          <Link to="/search">Search</Link>
          <Link to="/jobs">Jobs</Link>
          <Link to="/jobs/new">New Job</Link>
          <Link to="/schedules">Schedules</Link>
        </nav>
      </header>
      <main className="main">
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/stats" element={<StatsPage />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/jobs" element={<JobsPage />} />
          <Route path="/jobs/new" element={<NewJobPage />} />
          <Route path="/schedules" element={<SchedulesPage />} />
        </Routes>
      </main>
    </div>
  );
}
