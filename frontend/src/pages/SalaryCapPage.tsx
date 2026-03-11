import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"

export function SalaryCapPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Salary Cap</h1>
        <p className="text-muted-foreground mt-2">
          Team salary cap summary and allotments.
        </p>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle>Cap Summary</CardTitle>
            <CardDescription>
              Overall cap position
            </CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-muted-foreground text-sm">
              Total cap, total used, dead money, and remaining cap space.
              Phase 8 will render cap figures and a visual bar from
              GET /api/cap/teams/:teamId.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Player Penalties</CardTitle>
            <CardDescription>
              Per-player cap hits
            </CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-muted-foreground text-sm">
              Breakdown of cap hit by player, including base salary, bonuses,
              and dead money charges. Phase 8 will render a sortable table from
              GET /api/cap/teams/:teamId.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Allotments</CardTitle>
            <CardDescription>
              Team allotment usage
            </CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-muted-foreground text-sm">
              Extensions, franchise tags, and tenders remaining for the season.
              Phase 8 will show used vs. available allotments from
              GET /api/cap/teams/:teamId/allotments.
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
