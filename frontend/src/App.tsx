import { Routes, Route } from "react-router-dom";
import { AppLayout } from "@/components/layout/AppLayout";
import { SplashPage } from "@/pages/SplashPage";
import { DashboardPage } from "@/pages/DashboardPage";
import { RosterPage } from "@/pages/RosterPage";
import { PlayerDetailPage } from "@/pages/PlayerDetailPage";
import { SalaryCapPage } from "@/pages/SalaryCapPage";
import { CalendarPage } from "@/pages/CalendarPage";
import { ContractManagementPage } from "@/pages/ContractManagementPage";

function App() {
  return (
    <Routes>
      <Route index element={<SplashPage />} />
      <Route element={<AppLayout />}>
        <Route path="dashboard" element={<DashboardPage />} />
        <Route path="roster" element={<RosterPage />} />
        <Route path="roster/:playerId" element={<PlayerDetailPage />} />
        <Route path="cap" element={<SalaryCapPage />} />
        <Route path="contracts" element={<ContractManagementPage />} />
        <Route path="calendar" element={<CalendarPage />} />
      </Route>
    </Routes>
  );
}

export default App;
