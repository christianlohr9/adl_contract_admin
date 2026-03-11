import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"

export function DashboardPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Dashboard</h1>
        <p className="text-muted-foreground mt-2">
          GM action center — what can you do right now?
        </p>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle>Available Actions</CardTitle>
            <CardDescription>
              Contract tools ready for your team
            </CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-muted-foreground text-sm">
              Extensions, tags, and tenders available to your team this season.
              Phase 8 will populate this with actionable items pulled from
              GET /api/teams/:teamId/snapshot.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Cap Summary</CardTitle>
            <CardDescription>
              Salary cap at a glance
            </CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-muted-foreground text-sm">
              Total cap, used cap, dead money, and remaining space.
              Phase 8 will render a cap bar chart and key figures from
              GET /api/cap/teams/:teamId.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Roster Overview</CardTitle>
            <CardDescription>
              Team roster snapshot
            </CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-muted-foreground text-sm">
              Roster count, pending transactions, and recent changes.
              Phase 8 will show roster size, IR count, and pending moves from
              GET /api/teams/:teamId/roster.
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
