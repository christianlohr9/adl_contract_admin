import { Link, useLocation, useNavigate } from "react-router-dom"
import { Home, Users, DollarSign, FileText, Calendar, ArrowLeftRight } from "lucide-react"
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
} from "@/components/ui/sidebar"
import { PlayerSearch } from "@/components/player/PlayerSearch"
import { useTeamSelection } from "@/hooks/useTeamSelection"

const navItems = [
  { title: "Dashboard", url: "/dashboard", icon: Home },
  { title: "Roster", url: "/roster", icon: Users },
  { title: "Salary Cap", url: "/cap", icon: DollarSign },
  { title: "Contracts", url: "/contracts", icon: FileText },
  { title: "Calendar", url: "/calendar", icon: Calendar },
]

export function AppSidebar() {
  const location = useLocation()
  const navigate = useNavigate()
  const { selectedTeam, clearTeam } = useTeamSelection()

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader className="border-b border-sidebar-border px-4 py-3 space-y-3">
        <div className="flex items-center justify-between group-data-[collapsible=icon]:justify-center">
          <span className="text-lg font-semibold tracking-tight group-data-[collapsible=icon]:hidden">
            ADL Contract Admin
          </span>
          {selectedTeam && (
            <span className="text-sm font-bold text-primary">
              {selectedTeam.abbr}
            </span>
          )}
        </div>
        <div className="group-data-[collapsible=icon]:hidden">
          <PlayerSearch />
        </div>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Navigation</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {navItems.map((item) => {
                const isActive =
                  item.url === "/dashboard"
                    ? location.pathname === "/dashboard"
                    : location.pathname.startsWith(item.url)

                return (
                  <SidebarMenuItem key={item.title}>
                    <SidebarMenuButton
                      render={<Link to={item.url} />}
                      isActive={isActive}
                      tooltip={item.title}
                    >
                      <item.icon />
                      <span>{item.title}</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                )
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton
                  tooltip="Switch Team"
                  onClick={() => {
                    clearTeam()
                    navigate("/")
                  }}
                >
                  <ArrowLeftRight />
                  <span>Switch Team</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarRail />
    </Sidebar>
  )
}
