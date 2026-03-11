import { BrowserRouter, Routes, Route } from "react-router-dom"
import { AppLayout } from "@/components/layout/AppLayout"
import { DashboardPage } from "@/pages/DashboardPage"
import { RosterPage } from "@/pages/RosterPage"
import { PlayerDetailPage } from "@/pages/PlayerDetailPage"
import { SalaryCapPage } from "@/pages/SalaryCapPage"

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<AppLayout />}>
          <Route index element={<DashboardPage />} />
          <Route path="roster" element={<RosterPage />} />
          <Route path="roster/:playerId" element={<PlayerDetailPage />} />
          <Route path="cap" element={<SalaryCapPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

export default App
