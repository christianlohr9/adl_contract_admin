import { useEffect } from "react"
import { Outlet, useNavigate } from "react-router-dom"
import { SidebarProvider, SidebarInset, SidebarTrigger } from "@/components/ui/sidebar"
import { TooltipProvider } from "@/components/ui/tooltip"
import { AppSidebar } from "@/components/layout/AppSidebar"
import { Separator } from "@/components/ui/separator"
import { useTeamSelection } from "@/hooks/useTeamSelection"

export function AppLayout() {
  const { selectedTeam } = useTeamSelection()
  const navigate = useNavigate()

  useEffect(() => {
    if (!selectedTeam) navigate("/", { replace: true })
  }, [selectedTeam, navigate])

  if (!selectedTeam) return null

  return (
    <TooltipProvider>
      <SidebarProvider>
        <AppSidebar />
        <SidebarInset>
          <header className="flex h-12 shrink-0 items-center gap-2 border-b px-4">
            <SidebarTrigger className="-ml-1" />
            <Separator orientation="vertical" className="mr-2 h-4" />
          </header>
          <main className="flex-1 p-6">
            <Outlet />
          </main>
        </SidebarInset>
      </SidebarProvider>
    </TooltipProvider>
  )
}
