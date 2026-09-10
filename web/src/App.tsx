import { Link, Route, Routes } from "react-router";
import { RunnersPage } from "./RunnersPage";
import { SecurityBanner } from "./components/SecurityBanner";
import { JobLogsPage } from "./pages/JobLogsPage";
import { NewRunPage } from "./pages/NewRunPage";
import { RunDetailPage } from "./pages/RunDetailPage";
import { RunsListPage } from "./pages/RunsListPage";

// App shell: security banner + nav are always visible (T-M1-11). Runs is the
// default/landing page; Runners is the T-M0-07 page, reused unchanged.
function App() {
  return (
    <div>
      <SecurityBanner />
      <nav
        style={{
          display: "flex",
          gap: 16,
          padding: "10px 24px",
          borderBottom: "1px solid #e5e7eb",
          fontSize: 14,
        }}
      >
        <Link to="/">Runs</Link>
        <Link to="/runs/new">New run</Link>
        <Link to="/runners">Runners</Link>
      </nav>
      <Routes>
        <Route path="/" element={<RunsListPage />} />
        <Route path="/runs" element={<RunsListPage />} />
        <Route path="/runs/new" element={<NewRunPage />} />
        <Route path="/runs/:id" element={<RunDetailPage />} />
        <Route path="/jobs/:id" element={<JobLogsPage />} />
        <Route path="/runners" element={<RunnersPage />} />
      </Routes>
    </div>
  );
}

export default App;
