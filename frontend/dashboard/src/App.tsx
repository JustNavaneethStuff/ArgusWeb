import { Link, Route, Routes } from "react-router-dom";
import HomePage from "./pages/HomePage";
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
        </nav>
      </header>
      <main className="main">
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/stats" element={<StatsPage />} />
          <Route path="/search" element={<SearchPage />} />
        </Routes>
      </main>
    </div>
  );
}
